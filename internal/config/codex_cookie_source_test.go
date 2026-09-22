package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCookieSourceAndGroupInheritance(t *testing.T) {
	cfg := &Config{Codex: CodexConfig{StateOverride: CodexStateOverrideConfig{Enabled: true, Strategy: "cookie-only", CookieAcquisitionModel: "base", CookiePoolGroup: "default", Rules: &[]CodexStateRule{{ID: "clear", Settings: CodexStateRuleSettings{CodexStateStrategySettings: CodexStateStrategySettings{CookieAcquisitionModel: new(""), CookiePoolGroup: new("")}}, ModelOverrides: []CodexStateRuleModelOverride{{ID: "specific", Models: []string{"special"}, Settings: CodexStateRuleSettings{CodexStateStrategySettings: CodexStateStrategySettings{CookieAcquisitionModel: new("alternate"), CookiePoolGroup: new("custom")}}}}}}}}}
	if err := cfg.ValidateCodexStateOverride(); err != nil {
		t.Fatal(err)
	}
	cloned, err := Clone(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p, _, _ := cloned.Codex.StateOverride.PolicyFor(CodexStateScope{Model: "normal"})
	if p.CookieAcquisitionModel != "" || p.CookiePoolGroup != "" || p.CookiePoolMode != "auto" {
		t.Fatal("explicit empty did not clear inherited source/group")
	}
	p, _, _ = cloned.Codex.StateOverride.PolicyFor(CodexStateScope{Model: "special"})
	if p.CookieAcquisitionModel != "alternate" || p.CookiePoolGroup != "custom" {
		t.Fatal("model override ignored")
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err = os.WriteFile(path, []byte("codex:\n  state-override:\n    extension: keep\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = SaveConfigPreserveComments(path, cloned); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `cookie-acquisition-model: ""`) || !strings.Contains(string(data), `cookie-pool-group: ""`) {
		t.Fatalf("empty overrides lost: %s", data)
	}
}

func TestCookieSourceRejectsAmbiguousModelsAndGroups(t *testing.T) {
	for _, model := range []string{"*", "two models", "line\nbreak", strings.Repeat("a", 257)} {
		if (CodexStateStrategySettings{CookieAcquisitionModel: &model}).validateExplicit() == nil {
			t.Fatalf("invalid source accepted: %q", model)
		}
	}
	c := CodexStateOverrideConfig{Strategy: "cookie-only", CookiePoolMode: "shared"}.Resolved()
	if c.validateStateStrategy() == nil {
		t.Fatal("shared mode allowed an unnamed group")
	}
	c.CookiePoolGroup = "group"
	if err := c.validateStateStrategy(); err != nil {
		t.Fatal(err)
	}
}
