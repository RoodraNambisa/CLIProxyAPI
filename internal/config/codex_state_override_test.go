package config

import "testing"

func TestCodexStateOverrideDefaultsAndValidation(t *testing.T) {
	cfg := &Config{}
	resolved := cfg.Codex.StateOverride.Resolved()
	if resolved.Enabled || resolved.MaxAttempts != 3 || resolved.TTLMinutes != 60 || resolved.RefreshBeforeMinutes != 5 || !*resolved.MatchModel || resolved.Lengths[0] != 292 {
		t.Fatalf("defaults: %+v", resolved)
	}
	for _, change := range []func(*CodexStateOverrideConfig){
		func(c *CodexStateOverrideConfig) { c.MaxAttempts = -1 },
		func(c *CodexStateOverrideConfig) { c.TTLMinutes = 5; c.RefreshBeforeMinutes = 5 },
		func(c *CodexStateOverrideConfig) { c.ProxyMode = "custom"; c.ProxyURL = "http://u-{0}:p@proxy:80" },
		func(c *CodexStateOverrideConfig) { c.Lengths = []int{-1} },
		func(c *CodexStateOverrideConfig) { c.MissingPolicy = "unknown" },
		func(c *CodexStateOverrideConfig) { c.IncludedCredentials = []string{""} },
		func(c *CodexStateOverrideConfig) { c.IncludedCredentials = []string{"bad\x00id"} },
		func(c *CodexStateOverrideConfig) { c.IncludedCredentials = make([]string, 1025) },
	} {
		cfg.Codex.StateOverride = CodexStateOverrideConfig{}
		change(&cfg.Codex.StateOverride)
		if cfg.ValidateCodexStateOverride() == nil {
			t.Fatal("invalid config accepted")
		}
	}
	cfg.Codex.StateOverride = CodexStateOverrideConfig{Enabled: true, ProxyMode: "custom", ProxyURL: "http://u-{12}:p@proxy:80"}
	if err := cfg.ValidateCodexStateOverride(); err != nil {
		t.Fatal(err)
	}
	cfg.Codex.TurnStatePolicy = CodexTurnStatePolicyStrip
	if cfg.ValidateCodexStateOverride() == nil {
		t.Fatal("contradictory strip accepted")
	}
}

func TestCodexStateIncludedCredentialsSurviveClone(t *testing.T) {
	cfg := &Config{Codex: CodexConfig{StateOverride: CodexStateOverrideConfig{
		Priorities: []int{3}, IncludedCredentials: []string{"priority-zero-id"},
	}}}
	copyConfig, err := Clone(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := copyConfig.ValidateCodexStateOverride(); err != nil {
		t.Fatal(err)
	}
	resolved := copyConfig.Codex.StateOverride.Resolved()
	if len(resolved.IncludedCredentials) != 1 || resolved.IncludedCredentials[0] != "priority-zero-id" {
		t.Fatalf("included credentials lost in clone: %+v", resolved.IncludedCredentials)
	}
	resolved.IncludedCredentials[0] = "changed"
	if cfg.Codex.StateOverride.IncludedCredentials[0] != "priority-zero-id" || copyConfig.Codex.StateOverride.IncludedCredentials[0] != "priority-zero-id" {
		t.Fatal("resolved configuration aliases credential selectors")
	}
}
