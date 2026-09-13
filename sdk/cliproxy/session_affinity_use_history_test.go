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

func TestSessionAffinityUseHistoryBuilderAndRuntimeSnapshot(t *testing.T) {
	for _, mode := range []string{"basic", "subagents", "lcp"} {
		t.Run(mode, func(t *testing.T) {
			on := true
			cfg := &config.Config{Routing: config.RoutingConfig{Strategy: "fill-first", SessionAffinity: true, SessionAffinityUseHistory: &on, SessionAffinitySubagents: mode == "subagents", SessionAffinityLCP: mode == "lcp"}}
			service, err := NewBuilder().WithConfig(cfg).WithConfigPath(filepath.Join(t.TempDir(), "config.yaml")).Build()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(service.coreManager.StopAutoRefresh)
			t.Cleanup(service.proxyPoolManager.Stop)
			t.Cleanup(func() { service.coreManager.SetSelector(nil) })
			manager := service.coreManager
			for _, id := range []string{"a", "b"} {
				if _, err := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{ID: id, Provider: "weighted-test", Status: coreauth.StatusActive}); err != nil {
					t.Fatal(err)
				}
			}
			opts := core.Options{Headers: make(http.Header), SourceFormat: sdktranslator.FormatCodex, OriginalRequest: []byte(`{"input":"same initial user message"}`), Metadata: map[string]any{core.CallerScopeMetadataKey: "fixture-caller"}}
			pinned := opts
			pinned.Metadata = map[string]any{core.CallerScopeMetadataKey: "fixture-caller", core.PinnedAuthMetadataKey: "b"}
			pick := func(ctx context.Context, options core.Options, want string) {
				t.Helper()
				manager.RegisterExecutor(&weightedRoutingEntryExecutor{})
				response, err := manager.Execute(ctx, []string{"weighted-test"}, core.Request{}, options)
				if err != nil || string(response.Payload) != want {
					t.Fatalf("picked %q, want %s: %v", response.Payload, want, err)
				}
			}
			pick(t.Context(), pinned, "b")
			pick(t.Context(), opts, "b")
			frozen := manager.WithRoutingPolicySnapshot(t.Context())
			next, err := config.Clone(cfg)
			if err != nil {
				t.Fatal(err)
			}
			off := false
			next.Routing.SessionAffinityUseHistory = &off
			if result, err := service.ApplyRuntimeConfig(t.Context(), next); err != nil || !result.Applied || result.RestartRequired {
				t.Fatal("history disable did not hot reload", err)
			}
			pick(frozen, opts, "b")
			pick(t.Context(), pinned, "b")
			pick(t.Context(), opts, "a")
			if result, err := service.ApplyRuntimeConfig(t.Context(), cfg); err != nil || !result.Applied || result.RestartRequired {
				t.Fatal("history enable did not hot reload", err)
			}
			pick(t.Context(), pinned, "b")
			pick(t.Context(), opts, "b")
		})
	}
}
