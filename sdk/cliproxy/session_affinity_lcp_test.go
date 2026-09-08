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
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestLCPAffinityBuilderRuntimeAndGeneralDependency(t *testing.T) {
	for _, mode := range []string{"off", "general", "legacy-only"} {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/lcp=%t", mode, enabled), func(t *testing.T) {
				cfg := &config.Config{Routing: config.RoutingConfig{Strategy: "fill-first", SessionAffinity: mode == "general", ClaudeCodeSessionAffinity: mode == "legacy-only", SessionAffinityLCP: enabled}}
				service, err := NewBuilder().WithConfig(cfg).WithConfigPath(filepath.Join(t.TempDir(), "config.yaml")).Build()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(service.coreManager.StopAutoRefresh)
				t.Cleanup(service.proxyPoolManager.Stop)
				m := service.coreManager
				m.RegisterExecutor(&weightedRoutingEntryExecutor{})
				ctx := coreauth.WithSkipPersist(t.Context())
				for _, id := range []string{"a", "b"} {
					if _, err := m.Register(ctx, &coreauth.Auth{ID: id, Provider: "weighted-test", Status: coreauth.StatusActive}); err != nil {
						t.Fatal(err)
					}
				}
				options := func(branch string) core.Options {
					return core.Options{Headers: make(http.Header), SourceFormat: sdktranslator.FormatCodex, Metadata: map[string]any{core.CallerScopeMetadataKey: "fixture-tenant/weighted-test"}, OriginalRequest: []byte(fmt.Sprintf(`{"input":[{"role":"user","content":"root question"},{"role":"assistant","content":"root answer"},{"role":"user","content":%q}]}`, branch))}
				}
				assertPick := func(ctx context.Context, opts core.Options, want string) {
					t.Helper()
					m.RegisterExecutor(&weightedRoutingEntryExecutor{})
					got, err := m.Execute(ctx, []string{"weighted-test"}, core.Request{}, opts)
					if err != nil || string(got.Payload) != want {
						t.Fatalf("expected %s, got %q: %v", want, got.Payload, err)
					}
				}
				prime := func() {
					for _, branch := range []string{"a", "b"} {
						opts := options(branch)
						opts.Metadata[core.PinnedAuthMetadataKey] = branch
						assertPick(ctx, opts, branch)
					}
				}
				wantFor := func(lcp bool) string {
					if mode == "off" || (mode == "general" && lcp) {
						return "a"
					}
					return "b"
				}
				prime()
				assertPick(ctx, options("a"), wantFor(enabled))
				for _, nextEnabled := range []bool{!enabled, enabled} {
					frozen := m.WithRoutingPolicySnapshot(ctx)
					oldWant := wantFor(cfg.Routing.SessionAffinityLCP)
					next, _ := config.Clone(cfg)
					next.Routing.SessionAffinityLCP = nextEnabled
					result, err := service.ApplyRuntimeConfig(ctx, next)
					if err != nil || !result.Applied || result.RestartRequired {
						t.Fatalf("runtime update failed: %v", err)
					}
					assertPick(frozen, options("a"), oldWant)
					prime()
					assertPick(ctx, options("a"), wantFor(nextEnabled))
					cfg = next
				}
			})
		}
	}
}
