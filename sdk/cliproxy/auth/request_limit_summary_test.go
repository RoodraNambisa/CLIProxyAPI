package auth

import (
	"fmt"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestRequestLimitSummaryIncludesIndependentConcurrencyPolicy(t *testing.T) {
	rpm, priorityConcurrency, planConcurrency, unlimited := 20, 4, 2, 0
	cfg := &config.Config{Routing: config.RoutingConfig{
		PerAuthRequestLimit: 100, PerAuthRequestWindowMinutes: 1, PerAuthConcurrency: 8,
		PriorityOverrides: []config.RoutingPriorityOverride{{
			Priority: 5, PerAuthRequestLimit: &rpm, PerAuthConcurrency: &priorityConcurrency,
			SubscriptionOverrides: []config.RoutingSubscriptionOverride{
				{Providers: []string{"codex"}, PlanTypes: []string{"pro"}, PerAuthConcurrency: &planConcurrency},
				{Credentials: []string{"selected"}, PerAuthConcurrency: &unlimited},
			},
		}},
	}}
	m := NewManager(nil, nil, nil)
	defer m.TrafficController().Close()
	m.SetConfig(cfg)
	for i, tc := range []struct {
		id, priority, provider, plan string
		concurrency, rpm             int
		concurrencySource, rpmSource string
		concurrencyRule              int
	}{
		{"global", "3", "codex", "pro", 8, 100, "global", "global", 0},
		{"priority", "5", "claude", "", 4, 20, "priority", "priority", 0},
		{"plan", "5", "codex", "pro", 2, 20, "subscription", "priority", 1},
		{"selected", "5", "codex", "pro", 0, 20, "credential", "priority", 2},
	} {
		t.Run(tc.id, func(t *testing.T) {
			a, err := m.Register(WithSkipPersist(t.Context()), &Auth{
				ID: tc.id, Index: fmt.Sprint("scope-", i), Provider: tc.provider,
				Attributes: map[string]string{"priority": tc.priority, "plan_type": tc.plan},
			})
			if err != nil {
				t.Fatal(err)
			}
			summary := m.AuthRequestLimitSummary(a)
			actual := m.routingAuthRequestLimitPolicyForAuth(a)
			if summary.ConcurrencyLimit != tc.concurrency || summary.ConcurrencySource != tc.concurrencySource ||
				summary.ConcurrencyRule != tc.concurrencyRule || summary.Limit != tc.rpm ||
				summary.Source != tc.rpmSource || summary.ConcurrencyLimit != actual.concurrency {
				t.Fatalf("summary does not match independent scheduler policies: %+v", summary)
			}
			snapshots := m.CredentialTraffic([]string{a.Index}, time.Now())
			if len(snapshots) != 1 || snapshots[0].Limit != tc.concurrency ||
				snapshots[0].ConcurrencySource != tc.concurrencySource || snapshots[0].Source != tc.rpmSource ||
				snapshots[0].Active != 0 {
				t.Fatalf("idle credential snapshot lost its configured limits: %+v", snapshots)
			}
		})
	}
	cfg.Routing.PriorityOverrides[0].PerAuthConcurrency = new(7)
	m.SetConfig(cfg)
	a, _ := m.GetByID("priority")
	if got := m.AuthRequestLimitSummary(a); got.ConcurrencyLimit != 7 || got.Limit != 20 {
		t.Fatalf("updated concurrency is stale or changed RPM: %+v", got)
	}
}

func TestProviderOnlyRequestLimitsApplyAcrossProviders(t *testing.T) {
	for _, provider := range []string{"codex", "chatgpt-web", "xai", "claude", "gemini", "gemini-interactions", "antigravity", "vertex", "aistudio", "kimi", "custom-compat"} {
		t.Run(provider, func(t *testing.T) {
			limit := 10
			rules, err := config.NormalizeRoutingPriorityOverrides([]config.RoutingPriorityOverride{{Priority: 0, SubscriptionOverrides: []config.RoutingSubscriptionOverride{{Providers: []string{provider}, PerAuthRequestLimit: &limit}}}})
			if err != nil {
				t.Fatal(err)
			}
			manager := NewManager(nil, nil, nil)
			manager.SetConfig(&config.Config{Routing: config.RoutingConfig{PriorityOverrides: rules}})
			for _, plan := range []string{"", "pro", "provider-specific-plan"} {
				auth := &Auth{Provider: provider, Metadata: map[string]any{"plan_type": plan}}
				if got := manager.AuthRequestLimitSummary(auth); got.Limit != 10 || got.Source != "subscription" {
					t.Fatalf("plan %q did not match: %+v", plan, got)
				}
			}
			if got := manager.AuthRequestLimitSummary(&Auth{Provider: "outside-scope"}); got.Limit != 0 {
				t.Fatal("provider-only rule escaped its provider scope")
			}
		})
	}
}

func TestRequestLimitSummaryMatchesProviderOnlySchedulerPolicy(t *testing.T) {
	limit, window := 10, 1
	cfg := &config.Config{Routing: config.RoutingConfig{PerAuthRequestLimit: 90, PerAuthRequestWindowMinutes: 5, PriorityOverrides: []config.RoutingPriorityOverride{{Priority: 3, SubscriptionOverrides: []config.RoutingSubscriptionOverride{{Providers: []string{"xai"}, PerAuthRequestLimit: &limit, PerAuthRequestWindowMinutes: &window}}}}}}
	manager := NewManager(nil, nil, nil)
	manager.SetConfig(cfg)
	for _, plan := range []string{"", "supergrok", "SuperGrok Heavy", "unknown"} {
		auth := &Auth{Provider: "xai", Attributes: map[string]string{"priority": "3", "plan_type": plan}}
		summary := manager.AuthRequestLimitSummary(auth)
		actual := manager.routingAuthRequestLimitPolicyForAuth(auth)
		if summary.Limit != 10 || summary.WindowMinutes != 1 || summary.Source != "subscription" || summary.Rule != 1 || actual.limit != summary.Limit || actual.windowMinutes != summary.WindowMinutes {
			t.Fatalf("plan %q: %+v actual=%+v", plan, summary, actual)
		}
	}
	for _, auth := range []*Auth{{Provider: "codex", Attributes: map[string]string{"priority": "3"}}, {Provider: "xai"}} {
		if got := manager.AuthRequestLimitSummary(auth); got.Limit != 90 || got.WindowMinutes != 5 || got.Source != "global" {
			t.Fatalf("provider or priority scope leaked: %+v", got)
		}
	}
	cfg.Routing.PerAuthRequestLimit = 0
	cfg.Routing.FillFirstPerAuthRPM = 7
	manager.SetConfigAndSelector(cfg, &FillFirstSelector{})
	if got := manager.AuthRequestLimitSummary(&Auth{Provider: "codex"}); got.Limit != 7 || got.WindowMinutes != 1 || got.Source != "fill_first" {
		t.Fatalf("legacy fill-first RPM missing: %+v", got)
	}
	manager.SetConfigAndSelector(cfg, &RoundRobinSelector{})
	if got := manager.AuthRequestLimitSummary(&Auth{Provider: "codex"}); got.Limit != 0 {
		t.Fatalf("inactive fill-first limit shown: %+v", got)
	}
}

func TestConcurrencyOnlyOverridePreservesRPMSource(t *testing.T) {
	concurrency := 2
	cfg := &config.Config{Routing: config.RoutingConfig{PerAuthRequestLimit: 50, PriorityOverrides: []config.RoutingPriorityOverride{{Priority: 5, SubscriptionOverrides: []config.RoutingSubscriptionOverride{{Credentials: []string{"selected"}, PerAuthConcurrency: &concurrency}}}}}}
	manager := NewManager(nil, nil, nil)
	manager.SetConfig(cfg)
	a := &Auth{ID: "selected", Provider: "codex", Attributes: map[string]string{"priority": "5"}}
	if summary := manager.AuthRequestLimitSummary(a); summary.Limit != 50 || summary.Source != "global" {
		t.Fatalf("concurrency override changed RPM source: %+v", summary)
	}
	limit := 3
	cfg.Routing.PriorityOverrides[0].SubscriptionOverrides[0].PerAuthRequestLimit = &limit
	manager.SetConfig(cfg)
	if summary := manager.AuthRequestLimitSummary(a); summary.Limit != 3 || summary.Source != "credential" {
		t.Fatalf("explicit RPM override lost source: %+v", summary)
	}
}
