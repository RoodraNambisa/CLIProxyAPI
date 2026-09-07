package auth

import (
	"fmt"
	"net/http"
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestSessionAffinityAcrossPrioritiesKeepsOptionalBinding(t *testing.T) {
	for _, strategy := range []string{"round-robin", "fill-first", "random", "weighted-round-robin"} {
		for _, across := range []bool{false, true} {
			for _, failover := range []bool{false, true} {
				for _, mixed := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/across=%t/failover=%t/mixed=%t", strategy, across, failover, mixed), func(t *testing.T) {
						manager, selector := newAcrossPriorityFixture(t, strategy, across, failover)
						opts := core.Options{Headers: http.Header{"Session-Id": {"fixture"}}}
						pick := func() (*Auth, error) {
							if mixed {
								auth, _, _, err := manager.pickNextMixed(t.Context(), []string{"test"}, "", opts, nil)
								return auth, err
							}
							auth, _, err := manager.pickNext(t.Context(), "test", "", opts, nil)
							return auth, err
						}
						cold, err := pick()
						if err != nil || cold.ID != "high" {
							t.Fatalf("cold pick = %v, %v", cold, err)
						}
						selector.BindSession(t.Context(), "test", "", opts, "low")
						bound, err := pick()
						if across {
							if err != nil || bound == nil || bound.ID != "low" {
								t.Fatalf("healthy low binding = %v, %v", bound, err)
							}
						} else if failover {
							if err != nil || bound == nil || bound.ID != "high" {
								t.Fatal("disabled mode changed priority failover")
							}
						} else if err == nil || bound != nil {
							t.Fatal("disabled mode changed strict binding")
						}
						low, _ := manager.GetByID("low")
						low.Disabled = true
						if _, err := manager.Update(WithSkipPersist(t.Context()), low); err != nil {
							t.Fatal(err)
						}
						selector.BindSession(t.Context(), "test", "", opts, "low")
						bound, err = pick()
						if failover && (err != nil || bound == nil || bound.ID != "high") || !failover && (err == nil || bound != nil) {
							t.Fatal("disabled binding bypassed failover rules")
						}
					})
				}
			}
		}
	}
}

func newAcrossPriorityFixture(t *testing.T, strategy string, across, failover bool) (*Manager, *SessionAffinitySelector) {
	t.Helper()
	var fallback Selector = &RoundRobinSelector{}
	switch strategy {
	case "fill-first":
		fallback = &FillFirstSelector{Range: 2}
	case "random":
		fallback = &RandomSelector{}
	case "weighted-round-robin":
		fallback = &WeightedRoundRobinSelector{}
	}
	selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: fallback, Failover: &failover, AcrossPriorities: across})
	t.Cleanup(selector.Stop)
	manager := NewManager(nil, selector, nil)
	manager.RegisterExecutor(schedulerTestExecutor{})
	manager.SetConfig(&internalconfig.Config{Routing: internalconfig.RoutingConfig{Strategy: strategy, FillFirstRange: 2}})
	for index, id := range []string{"high", "low"} {
		if _, err := manager.Register(WithSkipPersist(t.Context()), &Auth{ID: id, Provider: "test", Attributes: map[string]string{"priority": fmt.Sprint(10 - 10*index), "weight": "1"}}); err != nil {
			t.Fatal(err)
		}
	}
	return manager, selector
}

func TestSessionAffinityAcrossPrioritiesKeepsCapacityLimits(t *testing.T) {
	for _, rpm := range []bool{false, true} {
		for _, failover := range []bool{false, true} {
			t.Run(fmt.Sprintf("rpm=%t/failover=%t", rpm, failover), func(t *testing.T) {
				manager, selector := newAcrossPriorityFixture(t, "fill-first", true, failover)
				routing := internalconfig.RoutingConfig{Strategy: "fill-first", PerAuthRequestLimit: 1, PerAuthRequestWindowMinutes: 5}
				if rpm {
					routing.PerAuthRequestLimit, routing.FillFirstPerAuthRPM = 0, 1
				}
				manager.SetConfig(&internalconfig.Config{Routing: routing})
				opts := core.Options{Headers: http.Header{"Session-Id": {"fixture"}}}
				selector.BindSession(t.Context(), "test", "", opts, "low")
				first, _, err := manager.pickNext(t.Context(), "test", "", opts, nil)
				if err != nil || first == nil || first.ID != "low" {
					t.Fatalf("first bound pick = %v, %v", first, err)
				}
				second, _, err := manager.pickNext(t.Context(), "test", "", opts, nil)
				if failover && (err != nil || second == nil || second.ID != "high") || !failover && (err == nil || second != nil) {
					t.Fatalf("capacity-limited binding = %v, %v", second, err)
				}
			})
		}
	}
}

func TestSessionAffinityAcrossPrioritiesRespectsAttemptsAndWeights(t *testing.T) {
	manager, selector := newAcrossPriorityFixture(t, "round-robin", true, true)
	opts := core.Options{Headers: http.Header{"Session-Id": {"fixture"}}}
	selector.BindSession(t.Context(), "test", "", opts, "high")
	auth, _, err := manager.pickNext(t.Context(), "test", "", setSelectionAttemptMetadata(opts, 1), nil)
	if err != nil || auth == nil || auth.ID != "low" {
		t.Fatal("binding bypassed the retry priority range")
	}
	selector.BindSession(t.Context(), "test", "", opts, "low")
	auth, _, _, err = manager.pickNextMixedLegacy(t.Context(), []string{"test"}, "", opts, nil, func(auth *Auth) bool { return auth.ID != "low" })
	if err != nil || auth == nil || auth.ID != "high" {
		t.Fatal("binding bypassed the caller's candidate filter")
	}
	low, _ := manager.GetByID("low")
	low.Attributes["weight"] = "0"
	if _, err := manager.Update(WithSkipPersist(t.Context()), low); err != nil {
		t.Fatal(err)
	}
	manager.SetConfig(&internalconfig.Config{Routing: internalconfig.RoutingConfig{PriorityOverrides: []internalconfig.RoutingPriorityOverride{{Priority: 0, Strategy: "weighted-round-robin"}}}})
	selector.BindSession(t.Context(), "test", "", opts, "low")
	auth, _, err = manager.pickNext(t.Context(), "test", "", opts, nil)
	if err != nil || auth == nil || auth.ID != "high" {
		t.Fatal("binding bypassed the priority's disabled weight")
	}
}

func TestSessionAffinityAcrossPrioritiesDirectAndPinnedPolicy(t *testing.T) {
	manager, oldSelector := newAcrossPriorityFixture(t, "round-robin", false, true)
	opts := core.Options{Headers: http.Header{"Session-Id": {"fixture"}}}
	oldSelector.BindSession(t.Context(), "test", "", opts, "low")
	ctx := manager.WithRoutingPolicySnapshot(t.Context())
	next := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{AcrossPriorities: true})
	t.Cleanup(next.Stop)
	next.BindSession(t.Context(), "test", "", opts, "low")
	manager.SetSelector(next)
	oldPick, _, err := manager.pickNext(ctx, "test", "", opts, nil)
	if err != nil || oldPick == nil || oldPick.ID != "high" {
		t.Fatal("in-flight policy adopted the new affinity strategy")
	}
	newPick, _, err := manager.pickNext(t.Context(), "test", "", opts, nil)
	if err != nil || newPick == nil || newPick.ID != "low" {
		t.Fatal("new request ignored the installed affinity strategy")
	}
	high, _ := manager.GetByID("high")
	low, _ := manager.GetByID("low")
	direct, err := next.Pick(t.Context(), "test", "", opts, []*Auth{high, low})
	if err != nil || direct == nil || direct.ID != "low" {
		t.Fatal("direct selector differs from Manager selection")
	}
}
