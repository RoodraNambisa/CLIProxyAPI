package auth

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexstate"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type stateRetryFixtureError struct{}

func (stateRetryFixtureError) Error() string        { return "local State unavailable" }
func (stateRetryFixtureError) StatusCode() int      { return 429 }
func (stateRetryFixtureError) SkipAuthResult() bool { return true }
func (stateRetryFixtureError) RetryOtherAuth() bool { return true }

func TestStateOutageHonorsAffinityAndBindsOnlyAfterSuccess(t *testing.T) {
	for _, operation := range []string{"execute", "stream"} {
		for _, failover := range []bool{true, false} {
			for _, hidden := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s/failover=%t/hidden=%t", operation, failover, hidden), func(t *testing.T) {
					selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: &FillFirstSelector{}, Failover: &failover})
					t.Cleanup(selector.Stop)
					manager := NewManager(nil, selector, nil)
					manager.SetConfigAndSelector(&config.Config{Routing: config.RoutingConfig{SessionAffinity: true, SessionAffinityFailover: &failover}}, selector)
					manager.SetRetryConfig(2, 0, 0)
					failures := map[string]error{}
					if !hidden {
						failures["state-a"] = stateRetryFixtureError{}
					}
					exec := &authFallbackExecutor{id: "codex", executeErrors: failures, streamFirstErrors: failures}
					manager.RegisterExecutor(exec)
					for _, item := range []struct{ id, priority string }{{"state-a", "3"}, {"state-b", "2"}} {
						registerFallbackAuthForModel(t, manager, &Auth{ID: item.id, Provider: "codex", Attributes: map[string]string{"priority": item.priority}}, "gpt-state")
					}
					reg := registry.GetGlobalRegistry()
					var view registry.ClientModelAvailability
					if hidden {
						view = registry.ClientModelAvailability{"state-a": {"gpt-state": {}}}
					}
					reg.SetClientModelAvailability(func() registry.ClientModelAvailability { return view })
					t.Cleanup(func() { reg.SetClientModelAvailability(codexstate.Default.Availability) })
					opts := core.Options{Headers: http.Header{"Session-Id": {"state-affinity-fixture"}}}
					selector.BindSession(t.Context(), "codex", "gpt-state", opts, "state-a")
					if failover {
						failures["state-b"] = stateRetryFixtureError{}
						if err := runCodexAvailabilityOperation(t.Context(), manager, operation, core.Request{Model: "gpt-state"}, opts); err == nil {
							t.Fatal("failed fallback unexpectedly succeeded")
						}
						if got := selector.cachedAuthID("codex", "gpt-state", opts); got != "state-a" {
							t.Fatal("failed fallback replaced the original binding")
						}
						delete(failures, "state-b")
					}
					err := runCodexAvailabilityOperation(t.Context(), manager, operation, core.Request{Model: "gpt-state"}, opts)
					want := "state-a"
					if failover {
						want = "state-b"
						if err != nil {
							t.Fatalf("State failover did not reach lower priority: %v", err)
						}
					} else if err == nil {
						t.Fatal("strict affinity unexpectedly switched credentials")
					}
					if got := selector.cachedAuthID("codex", "gpt-state", opts); got != want {
						t.Fatalf("binding=%s, want %s", got, want)
					}
					if hidden {
						view = registry.ClientModelAvailability{"state-a": {"gpt-state": time.Now().Add(time.Hour)}}
						if err := runCodexAvailabilityOperation(t.Context(), manager, operation, core.Request{Model: "gpt-state"}, opts); err != nil {
							t.Fatalf("recovery failed: %v", err)
						}
						if got := selector.cachedAuthID("codex", "gpt-state", opts); got != want {
							t.Fatalf("recovery forced rebinding: %s, want %s", got, want)
						}
					}
				})
			}
		}
	}
}

func TestStateAvailabilitySelectionAndRecovery(t *testing.T) {
	for _, strategy := range []string{"round-robin", "fill-first", "random", "weighted-round-robin"} {
		for _, path := range []string{"single", "mixed", "legacy-single", "legacy-mixed"} {
			t.Run(strategy+"/"+path, func(t *testing.T) {
				m := NewManager(nil, nil, nil)
				m.SetConfig(&config.Config{Routing: config.RoutingConfig{Strategy: strategy}})
				m.RegisterExecutor(schedulerTestExecutor{})
				reg := registry.GetGlobalRegistry()
				view := registry.ClientModelAvailability{"state-high": {"model": {}}}
				reg.SetClientModelAvailability(func() registry.ClientModelAvailability { return view })
				t.Cleanup(func() { reg.SetClientModelAvailability(codexstate.Default.Availability) })
				for _, pair := range []struct{ id, priority string }{{"state-high", "3"}, {"state-low", "0"}} {
					_, err := m.Register(WithSkipPersist(t.Context()), &Auth{ID: pair.id, Provider: "test", Attributes: map[string]string{"priority": pair.priority}})
					if err != nil {
						t.Fatal(err)
					}
					reg.RegisterClient(pair.id, "test", []*registry.ModelInfo{{ID: "model"}, {ID: "other"}})
					t.Cleanup(func() { reg.UnregisterClient(pair.id) })
				}
				pick := func(model string) *Auth {
					t.Helper()
					var selected *Auth
					var err error
					switch path {
					case "single":
						selected, _, err = m.pickNext(t.Context(), "test", model, core.Options{}, nil)
					case "mixed":
						selected, _, _, err = m.pickNextMixed(t.Context(), []string{"test", "unused"}, model, core.Options{}, nil)
					case "legacy-single":
						selected, _, err = m.pickNextLegacy(t.Context(), "test", model, core.Options{}, nil)
					case "legacy-mixed":
						selected, _, _, err = m.pickNextMixedLegacy(t.Context(), []string{"test", "unused"}, model, core.Options{}, nil, nil)
					}
					if err != nil || selected == nil {
						t.Fatalf("selection failed: %v", err)
					}
					return selected
				}
				if pick("model").ID != "state-low" || pick("other").ID != "state-high" {
					t.Fatal("missing State blocked the wrong model or prevented lower-priority fallback")
				}
				view = registry.ClientModelAvailability{"state-high": {"model": time.Now().Add(time.Hour)}}
				if pick("model").ID != "state-high" {
					t.Fatal("recovered State did not restore higher-priority selection")
				}
				view = registry.ClientModelAvailability{"state-high": {"model": time.Now().Add(-time.Second)}}
				if pick("model").ID != "state-low" {
					t.Fatal("expired State remained selectable")
				}
			})
		}
	}
}
