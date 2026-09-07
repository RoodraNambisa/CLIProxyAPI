package cliproxy

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type weightedRoutingEntryExecutor struct {
	antigravityModelRefreshExecutor
}

func (*weightedRoutingEntryExecutor) Identifier() string { return "weighted-test" }
func (*weightedRoutingEntryExecutor) Execute(_ context.Context, auth *coreauth.Auth, _ coreexecutor.Request, _ coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{Payload: []byte(auth.ID)}, nil
}

func TestWeightedRoutingBuilderAndRuntimeSelection(t *testing.T) {
	for _, strategy := range []string{"", "round-robin", "fill-first", "random", "weighted-round-robin", "weightedroundrobin", "wrr"} {
		t.Run(strategy, func(t *testing.T) {
			cfg := &config.Config{Routing: config.RoutingConfig{Strategy: strategy}}
			service, err := NewBuilder().WithConfig(cfg).WithConfigPath(filepath.Join(t.TempDir(), "config.yaml")).Build()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(service.coreManager.StopAutoRefresh)
			t.Cleanup(service.proxyPoolManager.Stop)
			ctx := coreauth.WithSkipPersist(t.Context())
			for _, entry := range []struct{ id, weight, priority string }{{"zero", "0", "10"}, {"positive", "1", "0"}} {
				credential := &coreauth.Auth{ID: entry.id, Provider: "weighted-test", Status: coreauth.StatusActive, Attributes: map[string]string{"weight": entry.weight, "priority": entry.priority}}
				if _, err := service.coreManager.Register(ctx, credential); err != nil {
					t.Fatal(err)
				}
			}
			assertPick := func(want string) {
				t.Helper()
				service.coreManager.RegisterExecutor(&weightedRoutingEntryExecutor{})
				got, err := service.coreManager.Execute(ctx, []string{"weighted-test"}, coreexecutor.Request{}, coreexecutor.Options{})
				if err != nil || string(got.Payload) != want {
					t.Fatalf("expected credential %s, got %q: %v", want, got.Payload, err)
				}
			}
			want := "zero"
			if normalizeRuntimeRoutingStrategy(strategy) == "weighted-round-robin" {
				want = "positive"
			}
			assertPick(want)
			for _, next := range []string{"weighted-round-robin", "round-robin"} {
				result, err := service.ApplyRuntimeConfig(t.Context(), &config.Config{Routing: config.RoutingConfig{Strategy: next}})
				if err != nil || !result.Applied || result.RestartRequired {
					t.Fatalf("strategy reload failed: %v", err)
				}
				want := "zero"
				if next == "weighted-round-robin" {
					want = "positive"
				}
				assertPick(want)
			}
		})
	}
}
