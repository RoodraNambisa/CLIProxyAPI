package cliproxy

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestSubagentAffinityBuilderRuntimeAndGeneralDependency(t *testing.T) {
	for _, mode := range []string{"off", "general", "legacy-only"} {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/subagents=%t", mode, enabled), func(t *testing.T) {
				cfg := &config.Config{Routing: config.RoutingConfig{Strategy: "fill-first", SessionAffinity: mode == "general", ClaudeCodeSessionAffinity: mode == "legacy-only", SessionAffinitySubagents: enabled}}
				service, err := NewBuilder().WithConfig(cfg).WithConfigPath(filepath.Join(t.TempDir(), "config.yaml")).Build()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(service.coreManager.StopAutoRefresh)
				t.Cleanup(service.proxyPoolManager.Stop)
				manager := service.coreManager
				manager.RegisterExecutor(&weightedRoutingEntryExecutor{})
				ctx := coreauth.WithSkipPersist(t.Context())
				for _, id := range []string{"a", "b"} {
					if _, err := manager.Register(ctx, &coreauth.Auth{ID: id, Provider: "weighted-test", Status: coreauth.StatusActive}); err != nil {
						t.Fatal(err)
					}
				}
				parent := core.Options{Headers: http.Header{"Session-Id": {"root"}}, Metadata: map[string]any{core.PinnedAuthMetadataKey: "b", core.CallerScopeMetadataKey: "fixture-tenant/weighted-test"}}
				sequence := 0
				assertPick := func(ctx context.Context, opts core.Options, want string) {
					t.Helper()
					manager.RegisterExecutor(&weightedRoutingEntryExecutor{})
					got, err := manager.Execute(ctx, []string{"weighted-test"}, core.Request{}, opts)
					if err != nil || string(got.Payload) != want {
						t.Fatalf("expected %s, got %q: %v", want, got.Payload, err)
					}
				}
				checkChild := func(ctx context.Context, inherit bool) {
					t.Helper()
					sequence++
					child := core.Options{Headers: http.Header{"Session-Id": {fmt.Sprintf("child-%d", sequence)}, "X-Codex-Parent-Thread-Id": {"root"}}, Metadata: map[string]any{core.CallerScopeMetadataKey: "fixture-tenant/weighted-test"}}
					want := "a"
					if inherit {
						want = "b"
					}
					assertPick(ctx, child, want)
				}
				assertPick(ctx, parent, "b")
				checkChild(ctx, mode == "general" && enabled)
				for _, nextEnabled := range []bool{!enabled, enabled} {
					frozen := manager.WithRoutingPolicySnapshot(ctx)
					oldEnabled := cfg.Routing.SessionAffinity && cfg.Routing.SessionAffinitySubagents
					next, _ := config.Clone(cfg)
					next.Routing.SessionAffinitySubagents = nextEnabled
					result, err := service.ApplyRuntimeConfig(ctx, next)
					if err != nil || !result.Applied || result.RestartRequired {
						t.Fatalf("runtime update failed: %v", err)
					}
					checkChild(frozen, oldEnabled)
					assertPick(ctx, parent, "b")
					checkChild(ctx, mode == "general" && nextEnabled)
					cfg = next
				}
			})
		}
	}
}
