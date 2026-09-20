package config

import (
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestStateRetryRoundDefaultsValidationAndInheritance(t *testing.T) {
	if p := (CodexStateOverrideConfig{}).Resolved(); p.MaxRetryRounds != 0 || p.RetryRoundIntervalMinutes != 30 {
		t.Fatal("new defaults enabled retries")
	}
	for _, tc := range []struct {
		rounds, interval int
		valid            bool
	}{{0, 30, true}, {2, 60, true}, {10, 1440, true}, {-1, 30, false}, {11, 30, false}, {2, -1, false}, {2, 1441, false}} {
		validationConfig := &Config{}
		validationConfig.Codex.StateOverride = CodexStateOverrideConfig{MaxRetryRounds: tc.rounds, RetryRoundIntervalMinutes: tc.interval}
		if (validationConfig.ValidateCodexStateOverride() == nil) != tc.valid {
			t.Fatalf("unexpected validation for %+v", tc)
		}
	}
	rule := CodexStateRule{
		ID:             "main",
		Settings:       CodexStateRuleSettings{RetryRoundIntervalMinutes: ruleValue(60)},
		ModelOverrides: []CodexStateRuleModelOverride{{ID: "sol", Models: []string{"sol"}, Settings: CodexStateRuleSettings{MaxRetryRounds: ruleValue(0)}}},
	}
	cfg := &Config{Codex: CodexConfig{StateOverride: CodexStateOverrideConfig{Enabled: true, MaxRetryRounds: 2, RetryRoundIntervalMinutes: 30, Rules: &[]CodexStateRule{rule}}}}

	if err := cfg.ValidateCodexStateOverride(); err != nil {
		t.Fatal(err)
	}
	p, match, _ := cfg.Codex.StateOverride.PolicyFor(CodexStateScope{Model: "sol"})
	if p.MaxRetryRounds != 0 || p.RetryRoundIntervalMinutes != 60 || match.Sources["max-retry-rounds"] != "model-override" || match.Sources["retry-round-interval-minutes"] != "rule" {
		t.Fatal("round settings lost inheritance")
	}
	p, _, _ = cfg.Codex.StateOverride.PolicyFor(CodexStateScope{Model: "astra"})
	if p.MaxRetryRounds != 2 {
		t.Fatal("model override leaked to other models")
	}
	copy, err := Clone(cfg)
	if err != nil {
		t.Fatal(err)
	}
	*(*copy.Codex.StateOverride.Rules)[0].ModelOverrides[0].Settings.MaxRetryRounds = 3
	if *(*cfg.Codex.StateOverride.Rules)[0].ModelOverrides[0].Settings.MaxRetryRounds != 0 {
		t.Fatal("clone aliases round settings")
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("port: 8317\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	var got Config
	if err := yaml.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	item := (*got.Codex.StateOverride.Rules)[0].ModelOverrides[0]
	if item.Settings.MaxRetryRounds == nil || *item.Settings.MaxRetryRounds != 0 {
		t.Fatalf("explicit zero removed: %s", data)
	}
	(*got.Codex.StateOverride.Rules)[0].ModelOverrides[0].Settings.MaxRetryRounds = nil
	if err := SaveConfigPreserveComments(path, &got); err != nil {
		t.Fatal(err)
	}
	data, _ = os.ReadFile(path)
	var cleared Config
	_ = yaml.Unmarshal(data, &cleared)
	p, _, _ = cleared.Codex.StateOverride.PolicyFor(CodexStateScope{Model: "sol"})
	if p.MaxRetryRounds != 2 {
		t.Fatal("restoring inheritance retained old zero")
	}
}
