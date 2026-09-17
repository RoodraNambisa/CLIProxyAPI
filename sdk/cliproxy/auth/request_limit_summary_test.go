package auth

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

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
