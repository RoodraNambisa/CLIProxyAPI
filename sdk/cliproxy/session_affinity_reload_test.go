package cliproxy

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func newAffinityReloadFixture(t *testing.T, mode string) (*Service, *config.Config, core.Options, func(context.Context, core.Options) string) {
	t.Helper()
	cfg := &config.Config{Routing: config.RoutingConfig{Strategy: "fill-first", SessionAffinity: true, SessionAffinitySubagents: mode == "subagents", SessionAffinityLCP: mode == "history"}}
	service, err := NewBuilder().WithConfig(cfg).WithConfigPath(filepath.Join(t.TempDir(), "config.yaml")).Build()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(service.coreManager.StopAutoRefresh)
	t.Cleanup(service.proxyPoolManager.Stop)
	t.Cleanup(func() { service.coreManager.SetSelector(nil) })
	manager := service.coreManager
	ctx := coreauth.WithSkipPersist(t.Context())
	for _, id := range []string{"reload-a", "reload-b"} {
		if _, err := manager.Register(ctx, &coreauth.Auth{ID: id, Provider: "weighted-test", Status: coreauth.StatusActive}); err != nil {
			t.Fatal(err)
		}
	}
	options := core.Options{Headers: http.Header{"Session-Id": {"reload-fixture"}}, SourceFormat: sdktranslator.FormatCodex, Metadata: map[string]any{core.CallerScopeMetadataKey: "reload-fixture-caller"}}
	if mode == "history" {
		options.Headers = nil
	}
	run := func(ctx context.Context, options core.Options) string {
		t.Helper()
		manager.RegisterExecutor(&weightedRoutingEntryExecutor{})
		request := core.Request{Payload: []byte(`{"input":[{"role":"user","content":"original question"},{"role":"assistant","content":"original answer"}]}`)}
		response, err := manager.Execute(ctx, []string{"weighted-test"}, request, options)
		if err != nil {
			t.Fatal(err)
		}
		return string(response.Payload)
	}
	pinned := options
	pinned.Metadata = map[string]any{core.CallerScopeMetadataKey: "reload-fixture-caller", core.PinnedAuthMetadataKey: "reload-b"}
	if run(ctx, pinned) != "reload-b" || run(ctx, options) != "reload-b" {
		t.Fatal("initial binding was not established")
	}
	return service, cfg, options, run
}

func TestSessionAffinitySurvivesUnrelatedRuntimeConfigUpdates(t *testing.T) {
	for _, mode := range []string{"basic", "subagents", "history"} {
		for _, change := range []string{"debug", "request-limit", "client-key", "equivalent-defaults", "priority-rules"} {
			t.Run(mode+"/"+change, func(t *testing.T) {
				service, cfg, options, run := newAffinityReloadFixture(t, mode)
				next, err := config.Clone(cfg)
				if err != nil {
					t.Fatal(err)
				}
				switch change {
				case "debug":
					next.Debug = true
				case "request-limit":
					next.Routing.PerAuthRequestLimit = 1
				case "client-key":
					next.APIKeys = []string{"reload-client"}
					next.APIKeyGroups = []config.APIKeyGroup{{APIKey: "reload-client", AllowedPriorities: []int{0}}}
				case "equivalent-defaults":
					failover := true
					next.Routing.Strategy = "ff"
					next.Routing.FillFirstRange = 1
					next.Routing.SessionAffinityTTL = "60m"
					next.Routing.SessionAffinityFailover = &failover
				case "priority-rules":
					next.Routing.PriorityOverrides = []config.RoutingPriorityOverride{{Priority: 0, Strategy: "random"}}
				}
				result, err := service.ApplyRuntimeConfig(t.Context(), next)
				if err != nil || !result.Applied || result.RestartRequired {
					t.Fatal("runtime update failed", err)
				}
				if run(t.Context(), options) != "reload-b" {
					t.Fatal("an unrelated update discarded the active binding")
				}
				if change == "request-limit" && run(t.Context(), options) != "reload-a" {
					t.Fatal("keeping the binding prevented the updated request limit from taking effect")
				}
			})
		}
	}
}

func TestSessionAffinityTTLUpdateKeepsInFlightCacheSeparate(t *testing.T) {
	service, cfg, options, run := newAffinityReloadFixture(t, "basic")
	frozen := service.coreManager.WithRoutingPolicySnapshot(t.Context())
	next, err := config.Clone(cfg)
	if err != nil {
		t.Fatal(err)
	}
	next.Routing.SessionAffinityTTL = "2h"
	if _, err := service.ApplyRuntimeConfig(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	if run(t.Context(), options) != "reload-a" || run(frozen, options) != "reload-b" {
		t.Fatal("TTL update mixed the new cache with an in-flight policy")
	}
}

func TestSessionAffinityRetainedCacheDoesNotRestoreRetiredCredential(t *testing.T) {
	for _, mode := range []string{"basic", "subagents", "history"} {
		t.Run(mode, func(t *testing.T) {
			service, cfg, options, run := newAffinityReloadFixture(t, mode)
			next, err := config.Clone(cfg)
			if err != nil {
				t.Fatal(err)
			}
			next.Debug = true
			if _, err := service.ApplyRuntimeConfig(t.Context(), next); err != nil {
				t.Fatal(err)
			}
			ctx := coreauth.WithSkipPersist(t.Context())
			if err := service.coreManager.Delete(ctx, "reload-b"); err != nil {
				t.Fatal(err)
			}
			if _, err := service.coreManager.Register(ctx, &coreauth.Auth{ID: "reload-b", Provider: "weighted-test", Status: coreauth.StatusActive}); err != nil {
				t.Fatal(err)
			}
			if run(ctx, options) != "reload-a" {
				t.Fatal("cache retention revived a deleted credential's binding")
			}
		})
	}
}
