package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRoutingCredentialRulesNormalizeAndPersist(t *testing.T) {
	limit, zero := 20, 0
	rules, err := NormalizeRoutingPriorityOverrides([]RoutingPriorityOverride{{Priority: 3, SubscriptionOverrides: []RoutingSubscriptionOverride{
		{Providers: []string{" XAI "}, PerAuthRequestLimit: &limit},
		{Credentials: []string{" Short-ID ", "Short-ID", "CaseSensitive.json"}, PerAuthRequestLimit: &zero},
		{Credentials: []string{"different"}, PerAuthRequestLimit: &limit},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	selected := rules[0].SubscriptionOverrides[1]
	if len(selected.Credentials) != 2 || selected.Credentials[0] != "Short-ID" || selected.Credentials[1] != "CaseSensitive.json" {
		t.Fatalf("identifiers normalized incorrectly: %+v", selected)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err = os.WriteFile(path, []byte("port: 8317\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := &Config{Routing: RoutingConfig{PriorityOverrides: rules}}
	if err = SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	selected = loaded.Routing.PriorityOverrides[0].SubscriptionOverrides[1]
	if len(selected.Credentials) != 2 || selected.PerAuthRequestLimit == nil || *selected.PerAuthRequestLimit != 0 {
		t.Fatal("credential scope or explicit zero lost")
	}
	loaded.Routing.PriorityOverrides[0].SubscriptionOverrides[1].Credentials = nil
	loaded.Routing.PriorityOverrides[0].SubscriptionOverrides[1].Providers = []string{"claude"}
	if err = SaveConfigPreserveComments(path, loaded); err != nil {
		t.Fatal(err)
	}
	loaded, err = LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Routing.PriorityOverrides[0].SubscriptionOverrides[1].Credentials) > 0 {
		t.Fatal("cleared credential filter survived save")
	}
}

func TestRoutingCredentialRuleOverlapsAndValidation(t *testing.T) {
	limit := 10
	for _, tc := range []struct {
		left, right RoutingSubscriptionOverride
		valid       bool
	}{
		{RoutingSubscriptionOverride{Providers: []string{"xai"}}, RoutingSubscriptionOverride{Credentials: []string{"same"}}, true},
		{RoutingSubscriptionOverride{Credentials: []string{"same"}}, RoutingSubscriptionOverride{Credentials: []string{"other"}}, true},
		{RoutingSubscriptionOverride{Credentials: []string{"same"}}, RoutingSubscriptionOverride{Credentials: []string{"same"}}, false},
		{RoutingSubscriptionOverride{Credentials: []string{"same"}, Providers: []string{"codex"}}, RoutingSubscriptionOverride{Credentials: []string{"same"}, Providers: []string{"xai"}}, true},
		{RoutingSubscriptionOverride{Credentials: []string{"same"}, PlanTypes: []string{"pro"}}, RoutingSubscriptionOverride{Credentials: []string{"same"}, PlanTypes: []string{"plus"}}, true},
		{RoutingSubscriptionOverride{Credentials: []string{"same"}}, RoutingSubscriptionOverride{Credentials: []string{"same"}, PlanTypes: []string{"plus"}}, false},
	} {
		tc.left.PerAuthRequestLimit = &limit
		tc.right.PerAuthRequestLimit = &limit
		for _, sub := range [][]RoutingSubscriptionOverride{{tc.left, tc.right}, {tc.right, tc.left}} {
			_, err := NormalizeRoutingPriorityOverrides([]RoutingPriorityOverride{{SubscriptionOverrides: sub}})
			if (err == nil) != tc.valid {
				t.Fatalf("unexpected overlap result: %v", err)
			}
		}
	}
	for _, ids := range [][]string{{""}, {"bad\x00id"}, {strings.Repeat("x", 513)}, make([]string, 1025)} {
		_, err := NormalizeRoutingPriorityOverrides([]RoutingPriorityOverride{{SubscriptionOverrides: []RoutingSubscriptionOverride{{Credentials: ids, PerAuthRequestLimit: &limit}}}})
		if err == nil {
			t.Fatal("invalid selector accepted")
		}
	}
}
