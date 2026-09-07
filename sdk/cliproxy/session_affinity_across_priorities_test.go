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

func TestAcrossPriorityAffinityBuilderAndRuntimePolicy(t *testing.T) {
	for _, affinity := range []bool{false, true} {
		for _, across := range []bool{false, true} {
			t.Run(fmt.Sprintf("affinity=%t/across=%t", affinity, across), func(t *testing.T) {
				cfg := &config.Config{Routing: config.RoutingConfig{SessionAffinity: affinity, SessionAffinityAcrossPriorities: across}}
				service, err := NewBuilder().WithConfig(cfg).WithConfigPath(filepath.Join(t.TempDir(), "config.yaml")).Build()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(service.coreManager.StopAutoRefresh)
				t.Cleanup(service.proxyPoolManager.Stop)
				manager := service.coreManager
				manager.RegisterExecutor(&weightedRoutingEntryExecutor{})
				ctx := coreauth.WithSkipPersist(t.Context())
				for index, id := range []string{"high", "low"} {
					if _, err := manager.Register(ctx, &coreauth.Auth{ID: id, Provider: "weighted-test", Status: coreauth.StatusActive, Attributes: map[string]string{"priority": fmt.Sprint(10 - 10*index)}}); err != nil {
						t.Fatal(err)
					}
				}
				opts := core.Options{Headers: http.Header{"Session-Id": {"fixture"}}}
				assertPick := func(ctx context.Context, want string) {
					t.Helper()
					manager.RegisterExecutor(&weightedRoutingEntryExecutor{})
					got, err := manager.Execute(ctx, []string{"weighted-test"}, core.Request{}, opts)
					if err != nil || string(got.Payload) != want {
						t.Fatalf("expected %s, got %q: %v", want, got.Payload, err)
					}
				}
				setHighDisabled := func(disabled bool) {
					t.Helper()
					high, _ := manager.GetByID("high")
					high.Disabled = disabled
					if _, err := manager.Update(ctx, high); err != nil {
						t.Fatal(err)
					}
				}
				checkRecovery := func(affinity, across bool) {
					t.Helper()
					setHighDisabled(true)
					assertPick(ctx, "low")
					setHighDisabled(false)
					want := "high"
					if affinity && across {
						want = "low"
					}
					assertPick(ctx, want)
				}
				checkRecovery(affinity, across)
				for _, nextAcross := range []bool{!across, across} {
					frozen := manager.WithRoutingPolicySnapshot(ctx)
					oldWant := "high"
					if cfg.Routing.SessionAffinity && cfg.Routing.SessionAffinityAcrossPriorities {
						oldWant = "low"
						setHighDisabled(true)
						assertPick(frozen, "low")
						setHighDisabled(false)
					}
					next, _ := config.Clone(cfg)
					next.Routing.SessionAffinityAcrossPriorities = nextAcross
					result, err := service.ApplyRuntimeConfig(ctx, next)
					if err != nil || !result.Applied || result.RestartRequired {
						t.Fatalf("runtime policy was not applied: %v", err)
					}
					assertPick(frozen, oldWant)
					checkRecovery(affinity, nextAcross)
					cfg = next
				}
			})
		}
	}
}
