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
