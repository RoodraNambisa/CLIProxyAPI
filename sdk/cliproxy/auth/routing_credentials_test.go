package auth

import (
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestCredentialRequestLimitMatchingAndInheritance(t *testing.T) {
	limit, window, specific := 10, 5, 2
	for _, provider := range []string{"codex", "xai", "claude", "chatgpt-web", "custom-compat"} {
		a := &Auth{ID: "auth-id", Index: "short-id", FileName: "File.json", Provider: provider, Attributes: map[string]string{"priority": "3"}}
		for _, identifier := range []string{a.ID, a.Index, a.FileName} {
			broad := config.RoutingSubscriptionOverride{Providers: []string{provider}, PerAuthRequestLimit: &limit, PerAuthRequestWindowMinutes: &window}
			narrow := config.RoutingSubscriptionOverride{Credentials: []string{identifier}, PerAuthRequestLimit: &specific}
			for _, rules := range [][]config.RoutingSubscriptionOverride{{broad, narrow}, {narrow, broad}} {
				cfg := &config.Config{Routing: config.RoutingConfig{PerAuthRequestLimit: 99, PriorityOverrides: []config.RoutingPriorityOverride{{Priority: 3, SubscriptionOverrides: rules}}}}
				m := NewManager(nil, nil, nil)
				m.SetConfig(cfg)
				for _, policy := range []authRequestLimitPolicy{m.scheduler.requestLimitPolicyForAuth(a), authRequestLimitPolicyForRoutingAuth(cfg.Routing, a)} {
					if policy.limit != 2 || policy.windowMinutes != 5 {
						t.Fatalf("wrong effective policy: %+v", policy)
					}
				}
				if summary := m.AuthRequestLimitSummary(a); summary.Source != "credential" || summary.Limit != 2 || summary.WindowMinutes != 5 {
					t.Fatalf("card disagrees: %+v", summary)
				}
				other := a.Clone()
				other.ID, other.Index, other.FileName = "other", "other-short", "Other.json"
				if p := m.scheduler.requestLimitPolicyForAuth(other); p.limit != 10 {
					t.Fatal("credential rule leaked")
				}
				other.Attributes["priority"] = "4"
				if p := m.scheduler.requestLimitPolicyForAuth(other); p.limit != 99 {
					t.Fatal("priority scope leaked")
				}
			}
		}
		rule := config.RoutingSubscriptionOverride{Credentials: []string{a.Index}, Providers: []string{provider}, PlanTypes: []string{"ChatGPTProPlan"}, PerAuthRequestLimit: &specific}
		if routingSubscriptionOverrideMatches(rule, a, "") {
			t.Fatal("unknown plan matched explicit plan")
		}
		if !routingSubscriptionOverrideMatches(rule, a, "pro") {
			t.Fatal("AND match failed")
		}
		rule.Providers = []string{"mismatch"}
		if routingSubscriptionOverrideMatches(rule, a, "pro") {
			t.Fatal("provider condition ignored")
		}
	}
	a := &Auth{ID: "A", Index: "short", FileName: "Upper.json"}
	if routingSubscriptionOverrideMatches(config.RoutingSubscriptionOverride{Credentials: []string{"upper.json"}}, a, "") || routingSubscriptionOverrideMatches(config.RoutingSubscriptionOverride{Credentials: []string{"Upper"}}, a, "") {
		t.Fatal("identifier matching was not exact")
	}
}

func TestSchedulerCredentialRequestLimitsAcrossStrategiesAndHotReload(t *testing.T) {
	for _, strategy := range []string{"fill-first", "random", "round-robin"} {
		t.Run(strategy, func(t *testing.T) {
			m := NewManager(nil, &RoundRobinSelector{}, nil)
			m.RegisterExecutor(schedulerTestExecutor{})
			a, err := m.Register(WithSkipPersist(t.Context()), &Auth{ID: "a", Provider: "test"})
			if err != nil {
				t.Fatal(err)
			}
			b, err := m.Register(WithSkipPersist(t.Context()), &Auth{ID: "b", Provider: "test"})
			if err != nil {
				t.Fatal(err)
			}
			general, special := 2, 1
			cfg := &config.Config{Routing: config.RoutingConfig{Strategy: strategy, PriorityOverrides: []config.RoutingPriorityOverride{{Priority: 0, SubscriptionOverrides: []config.RoutingSubscriptionOverride{
				{Providers: []string{"test"}, PerAuthRequestLimit: &general},
				{Credentials: []string{a.Index}, PerAuthRequestLimit: &special},
			}}}}}
			m.SetConfig(cfg)
			fixed := time.Date(2026, 9, 20, 12, 0, 10, 0, time.UTC)
			m.scheduler.requestLimiter.now = func() time.Time { return fixed }
			counts := map[string]int{}
			for range 3 {
				selected, _, err := m.pickNext(t.Context(), "test", "", core.Options{}, nil)
				if err != nil {
					t.Fatal(err)
				}
				counts[selected.ID]++
			}
			if counts[a.ID] != 1 || counts[b.ID] != 2 {
				t.Fatalf("limits not enforced: %v", counts)
			}
			if selected, _, err := m.pickNext(t.Context(), "test", "", core.Options{}, nil); selected != nil || !isAuthRequestLimitedError(err) {
				t.Fatal("exhausted limit allowed more requests")
			}
			cfg.Routing.PriorityOverrides[0].SubscriptionOverrides[1].Credentials[0] = b.Index
			m.SetConfig(cfg)
			if p := m.scheduler.requestLimitPolicyForAuth(a); p.limit != 2 {
				t.Fatal("hot reload kept old target")
			}
			if p := m.scheduler.requestLimitPolicyForAuth(b); p.limit != 1 {
				t.Fatal("hot reload lost new target")
			}
			cfg.Routing.PriorityOverrides[0].SubscriptionOverrides[1].Credentials[0] = "unrelated"
			if p := m.scheduler.requestLimitPolicyForAuth(b); p.limit != 1 {
				t.Fatal("scheduler aliases config slice")
			}
			zero := 0
			cfg.Routing.PriorityOverrides[0].SubscriptionOverrides[1].Credentials[0] = b.Index
			cfg.Routing.PriorityOverrides[0].SubscriptionOverrides[1].PerAuthRequestLimit = &zero
			m.SetConfig(cfg)
			if p := m.scheduler.requestLimitPolicyForAuth(b); p.limit != 0 {
				t.Fatal("explicit zero did not disable inherited limit")
			}
		})
	}
}
