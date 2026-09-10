package auth

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestRoutingPolicySnapshotRetainsChoicesAndManagerOwnership(t *testing.T) {
	manager := NewManager(nil, &WeightedRoundRobinSelector{}, nil)
	rangeValue := 3
	cfg := &config.Config{Routing: config.RoutingConfig{FillFirstRange: 2, PriorityOverrides: []config.RoutingPriorityOverride{{Priority: 7, Strategy: "random", FillFirstRange: &rangeValue}}}}
	manager.SetConfig(cfg)
	ctx := manager.WithRoutingPolicySnapshot(t.Context())
	old := routingPolicyFromContext(ctx)
	rangeValue = 9
	if old.strategyForPriority(0) != schedulerStrategyWeightedRoundRobin || old.strategyForPriority(7) != schedulerStrategyRandom || old.rangeForPriority(7) != 3 {
		t.Fatal("snapshot retained mutable configuration fields")
	}
	manager.SetSelector(&RoundRobinSelector{})
	manager.SetConfig(&config.Config{Routing: config.RoutingConfig{FillFirstRange: 5}})
	if manager.WithRoutingPolicySnapshot(ctx) != ctx || manager.selectionPolicy(ctx) != old || old.rangeForPriority(0) != 2 {
		t.Fatal("reload changed an existing logical request")
	}
	fresh := routingPolicyFromContext(manager.WithRoutingPolicySnapshot(t.Context()))
	if fresh == old || fresh.strategyForPriority(0) != schedulerStrategyRoundRobin || fresh.rangeForPriority(0) != 5 {
		t.Fatal("new request did not see updated policy")
	}
	other := NewManager(nil, &FillFirstSelector{}, nil)
	otherContext := other.WithRoutingPolicySnapshot(ctx)
	if policy := routingPolicyFromContext(otherContext); policy == old || policy.manager != other || other.selectionPolicy(ctx).manager != other {
		t.Fatal("one manager reused another manager's policy")
	}
}

func TestRoutingPolicySelectorInitializationWithoutConfig(t *testing.T) {
	var manager Manager
	manager.SetSelector(&RoundRobinSelector{})
	if p := routingPolicyFromContext(manager.WithRoutingPolicySnapshot(nil)); p == nil || p.strategy != schedulerStrategyRoundRobin {
		t.Fatal("selector initialization requires a preloaded config")
	}
}

func TestCodexQuotaObservationPolicySurvivesReloadAndSelectorChanges(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	oldContext := manager.WithRoutingPolicySnapshot(t.Context())
	if manager.selectionPolicy(oldContext).observeCodexQuota {
		t.Fatal("quota observation must default to disabled")
	}
	manager.SetConfig(&config.Config{Codex: config.CodexConfig{ObserveQuota: true}})
	onContext := manager.WithRoutingPolicySnapshot(t.Context())
	manager.SetSelector(&RandomSelector{})
	if !manager.selectionPolicy(onContext).observeCodexQuota || !manager.selectionPolicy().observeCodexQuota || manager.selectionPolicy(oldContext).observeCodexQuota {
		t.Fatal("selector change or reload drifted a quota policy snapshot")
	}
	manager.SetConfig(&config.Config{})
	if manager.selectionPolicy().observeCodexQuota || !manager.selectionPolicy(onContext).observeCodexQuota || manager.WithRoutingPolicySnapshot(onContext) != onContext {
		t.Fatal("disabling quota observation changed an in-flight request")
	}
	other := NewManager(nil, nil, nil)
	if other.selectionPolicy(other.WithRoutingPolicySnapshot(onContext)).observeCodexQuota {
		t.Fatal("a different manager inherited the quota observation setting")
	}
}

func TestRoutingPolicyMatchesEffectiveSchedulerRange(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{Range: 4}, nil)
	check := func(stage string) {
		t.Helper()
		policy := manager.selectionPolicy()
		manager.scheduler.mu.RLock()
		strategy, fillRange := manager.scheduler.strategy, manager.scheduler.globalFillFirstRange
		manager.scheduler.mu.RUnlock()
		if policy.strategy != strategy || policy.fillRange != fillRange {
			t.Errorf("%s: snapshot range=%d, scheduler range=%d", stage, policy.fillRange, fillRange)
		}
	}
	check("constructor")
	manager.SetConfig(&config.Config{Routing: config.RoutingConfig{FillFirstRange: 6}})
	check("config update")
	manager.SetSelector(&FillFirstSelector{Range: 3})
	check("selector update")
	manager.SetSelector(&RoundRobinSelector{})
	check("ordinary strategy")
}
