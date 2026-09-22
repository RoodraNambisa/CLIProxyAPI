package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestCodexStateCookieStrategyInheritance(t *testing.T) {
	var cfg Config
	err := yaml.Unmarshal([]byte(`codex:
  turn-state-policy: strip
  state-override:
    enabled: true
    strategy: cookie-only
    ttl-seconds: 120
    refresh-before-seconds: 10
    cookie-verify-after-acquire: true
    cookie-max-age-seconds: 240
    rules:
      - id: one
        settings:
          ttl-minutes: 1
          cookie-max-age-seconds: 0
          cookie-verify-after-acquire: false
        model-overrides:
          - id: child
            models: [astra]
            settings:
              ttl-seconds: 45
              refresh-before-seconds: 0
              missing-returned-state: reject
`), &cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = cfg.ValidateCodexStateOverride(); err != nil {
		t.Fatal(err)
	}
	cloned, err := Clone(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []*Config{&cfg, cloned} {
		p, _, ok := c.Codex.StateOverride.PolicyFor(CodexStateScope{Model: "sol"})
		if !ok || p.StateTTL() != time.Minute || p.StateRefreshBefore() != 10*time.Second || p.CookieMaxAgeSeconds != 0 || p.CookieVerifyAfterAcquire || !p.CookieOnly() {
			t.Fatalf("bad rule inheritance: %+v", p)
		}
		p, _, _ = c.Codex.StateOverride.PolicyFor(CodexStateScope{Model: "astra"})
		if p.StateTTL() != 45*time.Second || p.StateRefreshBefore() != 0 || p.MissingReturnedState != "reject" {
			t.Fatalf("bad model inheritance: %+v", p)
		}
	}
	data, err := json.Marshal(cloned)
	if err != nil {
		t.Fatal(err)
	}
	var round Config
	if err = json.Unmarshal(data, &round); err != nil {
		t.Fatal(err)
	}
	p, _, _ := round.Codex.StateOverride.PolicyFor(CodexStateScope{Model: "astra"})
	if p.RefreshBeforeSeconds == nil || *p.RefreshBeforeSeconds != 0 {
		t.Fatal("explicit zero lost")
	}
}

func TestCodexStateCookieSettingsPersistExplicitZerosAndClear(t *testing.T) {
	cfg := &Config{Codex: CodexConfig{StateOverride: CodexStateOverrideConfig{Strategy: "cookie-only", RefreshBeforeSeconds: new(0), Rules: &[]CodexStateRule{{ID: "one", Settings: CodexStateRuleSettings{CodexStateStrategySettings: CodexStateStrategySettings{CookieMaxAgeSeconds: new(0), CookieVerifyAfterAcquire: new(false)}}}}}}}
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("codex:\n  state-override:\n    extension: preserved\n"), 0600); err != nil {
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
	setting := (*got.Codex.StateOverride.Rules)[0].Settings
	if got.Codex.StateOverride.RefreshBeforeSeconds == nil || *got.Codex.StateOverride.RefreshBeforeSeconds != 0 || setting.CookieMaxAgeSeconds == nil || *setting.CookieMaxAgeSeconds != 0 || setting.CookieVerifyAfterAcquire == nil || *setting.CookieVerifyAfterAcquire {
		t.Fatalf("explicit override lost: %s", data)
	}
	if !strings.Contains(string(data), "extension: preserved") {
		t.Fatal("extension lost")
	}
	got.Codex.StateOverride.RefreshBeforeSeconds = nil
	(*got.Codex.StateOverride.Rules)[0].Settings.CookieMaxAgeSeconds = nil
	if err := SaveConfigPreserveComments(path, &got); err != nil {
		t.Fatal(err)
	}
	data, _ = os.ReadFile(path)
	var cleared Config
	_ = yaml.Unmarshal(data, &cleared)
	if cleared.Codex.StateOverride.RefreshBeforeSeconds != nil || (*cleared.Codex.StateOverride.Rules)[0].Settings.CookieMaxAgeSeconds != nil {
		t.Fatal("restoring inheritance retained old override")
	}
}

func TestCodexStateCookieStrategyRejectsInvalidOverrides(t *testing.T) {
	for _, s := range []CodexStateStrategySettings{{Strategy: new("")}, {Strategy: new("invalid")}, {TTLSeconds: new(0)}, {CookieMaxAgeSeconds: new(-1)}, {MissingReturnedState: new("")}} {
		cfg := &Config{Codex: CodexConfig{StateOverride: CodexStateOverrideConfig{ModelOverrides: []CodexStateModelOverride{{Model: "model", CodexStateStrategySettings: s}}}}}
		if cfg.ValidateCodexStateOverride() == nil {
			t.Fatalf("bad legacy settings accepted: %+v", s)
		}
		cfg.Codex.StateOverride.ModelOverrides = nil
		cfg.Codex.StateOverride.Rules = &[]CodexStateRule{{ID: "one", Settings: CodexStateRuleSettings{CodexStateStrategySettings: s}}}
		if cfg.ValidateCodexStateOverride() == nil {
			t.Fatalf("bad rule settings accepted: %+v", s)
		}
	}
}
