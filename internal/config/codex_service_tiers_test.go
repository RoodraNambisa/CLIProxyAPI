package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCodexServiceTierRulesValidationAndSnapshots(t *testing.T) {
	cfg := &Config{Codex: CodexConfig{ServiceTierRules: []CodexServiceTierRule{
		{Models: []string{"*"}, Tiers: []string{"flex"}},
		{Models: []string{"client-alias"}, Tiers: []string{" fast ", "ultrafast"}, Default: "fast"},
	}}}
	if err := cfg.ValidateCodexServiceTiers(); err != nil {
		t.Fatal(err)
	}
	selected := CodexServiceTierRuleForModel(cfg.Codex.ServiceTierRules, "client-alias")
	if selected.Default != "priority" || selected.Tiers[0] != "priority" || selected.AllowsClientTier("flex") || !selected.AllowsClientTier("fast") || !selected.AllowsClientTier("default") || selected.AllowsClientTier("auto") {
		t.Fatal("incorrect rule selection or allowed tier set")
	}
	selected.Tiers[0] = "changed"
	cloned := CloneCodexServiceTierRules(cfg.Codex.ServiceTierRules)
	cloned[0].Models[0] = "changed"
	if cfg.Codex.ServiceTierRules[0].Models[0] != "*" || cfg.Codex.ServiceTierRules[1].Tiers[0] != " fast " {
		t.Fatal("rule snapshot mutated config")
	}
	if got := CodexServiceTierRuleForModel(cfg.Codex.ServiceTierRules, "upstream-model"); len(got.Tiers) != 1 || got.Tiers[0] != "flex" {
		t.Fatal("wildcard fallback not selected")
	}
	if CodexServiceTierRuleForModel(nil, "model") != nil {
		t.Fatal("missing rule changed inheritance")
	}
}

func TestCodexServiceTierRulesLoadSaveAndInvalidOptional(t *testing.T) {
	valid := "codex:\n  allow-client-service-tier: true\n  service-tier-rules:\n    - models: [alias]\n      tiers: [priority, ultrafast]\n      default: priority\n      future-field: keep\n"
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(valid), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	clone, err := Clone(cfg)
	if err != nil {
		t.Fatal(err)
	}
	clone.Codex.ServiceTierRules[0].Tiers = nil
	clone.Codex.ServiceTierRules[0].Default = "default"
	if err := SaveConfigPreserveComments(p, clone); err != nil {
		t.Fatal(err)
	}
	saved, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(saved), "future-field: keep") {
		t.Fatal("unknown field lost")
	}
	roundtrip, err := LoadConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(roundtrip.Codex.ServiceTierRules) != 1 || len(roundtrip.Codex.ServiceTierRules[0].Tiers) != 0 {
		t.Fatal("standard-only rule lost")
	}
	for _, rules := range []string{
		"true", "{}", "[{models: [], tiers: []}]", "[{models: [x], tiers: [auto]}]",
		"[{models: [x], tiers: [fast, priority]}]", "[{models: [x], tiers: [], default: ultrafast}]",
		"[{models: [x], tiers: []}, {models: [x], tiers: [flex]}]", "[{models: ['gpt-*'], tiers: []}]",
	} {
		for _, optional := range []bool{false, true} {
			if err := os.WriteFile(p, []byte("codex:\n  service-tier-rules: "+rules+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if cfg, err := LoadConfigOptional(p, optional); err == nil || cfg != nil || !strings.Contains(err.Error(), "service-tier-rules") {
				t.Fatalf("accepted invalid rule %s with optional=%t: %v", rules, optional, err)
			}
		}
	}
}

func TestCodexServiceTierRulesClearDefaultAndRules(t *testing.T) {
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte("codex:\n  service-tier-rules:\n    - models: [m]\n      tiers: [priority]\n      default: priority\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Codex.ServiceTierRules[0].Default = ""
	if err := SaveConfigPreserveComments(p, cfg); err != nil {
		t.Fatal(err)
	}
	cfg, err = LoadConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Codex.ServiceTierRules[0].Default != "" {
		t.Fatal("cleared default remained sticky")
	}
	cfg.Codex.ServiceTierRules = nil
	if err := SaveConfigPreserveComments(p, cfg); err != nil {
		t.Fatal(err)
	}
	cfg, err = LoadConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Codex.ServiceTierRules) != 0 {
		t.Fatal("removed rules remained sticky")
	}
}
