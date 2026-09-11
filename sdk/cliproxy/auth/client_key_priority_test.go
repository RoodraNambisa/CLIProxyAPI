package auth

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func clientPriorityContext(t *testing.T, key string) context.Context {
	t.Helper()
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Set("apiKey", key)
	return context.WithValue(t.Context(), "gin", c)
}

func clientPriorityConfig(allowed, excluded []int) *config.Config {
	return &config.Config{SDKConfig: config.SDKConfig{APIKeys: []string{"priority-fixture"}, APIKeyGroups: []config.APIKeyGroup{{APIKey: "priority-fixture", AllowedPriorities: allowed, ExcludedPriorities: excluded}}}}
}

func TestClientKeyPriorityFiltersEverySelectorBeforeReservation(t *testing.T) {
	for _, strategy := range []string{"round-robin", "fill-first", "random", "weighted", "custom"} {
		for _, path := range []string{"single", "mixed", "legacy-single", "legacy-mixed"} {
			for _, tc := range []struct {
				name              string
				allowed, excluded []int
				want              string
			}{
				{"unrestricted", nil, nil, "tier-10"},
				{"allow", []int{2}, nil, "tier-2"},
				{"exclude", nil, []int{10}, "tier-2"},
				{"exclude-wins", []int{0, 10}, []int{10}, "tier-0"},
				{"negative", []int{-1}, nil, "tier--1"},
				{"no-tier", []int{99}, nil, ""},
				{"deny-all", []int{2}, []int{2}, ""},
			} {
				t.Run(strategy+"/"+path+"/"+tc.name, func(t *testing.T) {
					var selector Selector = &RoundRobinSelector{}
					switch strategy {
					case "fill-first":
						selector = &FillFirstSelector{}
					case "random":
						selector = &RandomSelector{}
					case "weighted":
						selector = &WeightedRoundRobinSelector{}
					case "custom":
						selector = &trackingSelector{}
					}
					m := NewManager(nil, selector, nil)
					cfg := clientPriorityConfig(tc.allowed, tc.excluded)
					cfg.Routing.PerAuthRequestLimit = 1
					cfg.Routing.PerAuthRequestWindowMinutes = 1
					m.SetConfig(cfg)
					m.RegisterExecutor(schedulerTestExecutor{})
					for _, priority := range []int{-1, 0, 2, 10} {
						attributes := map[string]string{}
						if priority != 0 {
							attributes["priority"] = fmt.Sprint(priority)
						}
						if _, err := m.Register(WithSkipPersist(t.Context()), &Auth{ID: fmt.Sprintf("tier-%d", priority), Provider: "test", Attributes: attributes}); err != nil {
							t.Fatal(err)
						}
					}
					if !reflect.DeepEqual(m.ClientAPIKeyPriorityChoices(), []int{-1, 0, 2, 10}) {
						t.Fatal("credential priority options are incomplete")
					}
					ctx := m.WithRoutingPolicySnapshot(clientPriorityContext(t, "priority-fixture"))
					opts := core.Options{AuthRequestSlot: &core.AuthRequestSlot{}}
					defer opts.AuthRequestSlot.Release()
					var selected *Auth
					var err error
					switch path {
					case "single":
						selected, _, err = m.pickNext(ctx, "test", "", opts, nil)
					case "mixed":
						selected, _, _, err = m.pickNextMixed(ctx, []string{"test"}, "", opts, nil)
					case "legacy-single":
						selected, _, err = m.pickNextLegacy(ctx, "test", "", opts, nil)
					default:
						selected, _, _, err = m.pickNextMixedLegacy(ctx, []string{"test"}, "", opts, nil, nil)
					}
					if tc.want == "" {
						if err == nil || selected != nil {
							t.Fatal("restricted request escaped its allowed priorities")
						}
					} else if err != nil || selected == nil || selected.ID != tc.want {
						t.Fatalf("selection lost priority policy: %v, %v", selected, err)
					}
					for _, credential := range m.List() {
						if clientKeyPriorityAllowed(ctx, credential) {
							continue
						}
						if available, _ := m.authRequestLimiter().availableAt(credential.ID, m.routingAuthRequestLimitPolicyForAuth(credential), time.Now()); !available {
							t.Fatal("excluded credential consumed capacity")
						}
					}
				})
			}
		}
	}
}

func TestClientKeyPrioritySnapshotHotReloadAndPinnedAuth(t *testing.T) {
	m := NewManager(nil, &RoundRobinSelector{}, nil)
	m.RegisterExecutor(schedulerTestExecutor{})
	m.SetConfig(clientPriorityConfig([]int{1}, nil))
	for _, priority := range []int{1, 2} {
		if _, err := m.Register(WithSkipPersist(t.Context()), &Auth{ID: fmt.Sprint(priority), Provider: "test", Attributes: map[string]string{"priority": fmt.Sprint(priority)}}); err != nil {
			t.Fatal(err)
		}
	}
	base := clientPriorityContext(t, "priority-fixture")
	captured := m.WithRoutingPolicySnapshot(base)
	m.SetConfig(clientPriorityConfig([]int{2}, nil))
	for _, tc := range []struct {
		ctx  context.Context
		want string
	}{{captured, "1"}, {m.WithRoutingPolicySnapshot(base), "2"}} {
		picked, _, err := m.pickNext(tc.ctx, "test", "", core.Options{}, nil)
		if err != nil || picked.ID != tc.want {
			t.Fatal("retry changed its priority snapshot", err)
		}
	}
	opts := core.Options{Metadata: map[string]any{core.PinnedAuthMetadataKey: "1"}, Headers: http.Header{"Allowed-Priorities": {"1"}}}
	if selected, _, err := m.pickNext(base, "test", "", opts, nil); err == nil || selected != nil {
		t.Fatal("pinned credential or caller header bypassed priority access")
	}
	other := clientPriorityContext(t, "another-key")
	if selected, _, err := m.pickNext(other, "test", "", core.Options{}, nil); err != nil || selected.ID != "2" {
		t.Fatal("restriction leaked to another key")
	}
	ctx, cancel := context.WithCancel(base)
	cancel()
	if _, _, err := m.pickNext(ctx, "test", "", core.Options{}, nil); err != context.Canceled {
		t.Fatal("cancellation was lost", err)
	}
}

func TestClientKeyPriorityDerivedGrantKeepsIssuerScope(t *testing.T) {
	m := NewManager(nil, &RoundRobinSelector{}, nil)
	m.SetConfig(clientPriorityConfig([]int{1}, nil))
	scope := m.ClientAPIKeyPriorityScope(clientPriorityContext(t, "priority-fixture"))
	grant := clientPriorityContext(t, "temporary-principal")
	c := grant.Value("gin").(*gin.Context)
	c.Set(ClientAPIKeyScopeContextKey, scope)
	allowed := &Auth{Attributes: map[string]string{"priority": "1"}}
	excluded := &Auth{Attributes: map[string]string{"priority": "2"}}
	if ctx := m.WithRoutingPolicySnapshot(grant); !clientKeyPriorityAllowed(ctx, allowed) || clientKeyPriorityAllowed(ctx, excluded) {
		t.Fatal("temporary grant escaped issuer policy")
	}
	m.SetConfig(clientPriorityConfig([]int{2}, nil))
	if ctx := m.WithRoutingPolicySnapshot(grant); clientKeyPriorityAllowed(ctx, allowed) || !clientKeyPriorityAllowed(ctx, excluded) {
		t.Fatal("new grant request ignored issuer policy reload")
	}
	m.SetConfig(&config.Config{})
	if clientKeyPriorityAllowed(m.WithRoutingPolicySnapshot(grant), allowed) {
		t.Fatal("removed issuer became unrestricted")
	}
}
