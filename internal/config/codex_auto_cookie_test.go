package config

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestAutoCookieConfigAndConflicts(t *testing.T) {
	for _, tc := range []struct{ name, yaml, conflict string }{
		{"off", "codex: {state-override: {enabled: true, strategy: cookie-only}}", ""},
		{"disabled", "codex: {auto-cookie: true, state-override: {enabled: false, strategy: cookie-only}}", ""},
		{"state", "codex: {auto-cookie: true, state-override: {enabled: true, strategy: state}}", ""},
		{"cookie", "codex: {auto-cookie: true, state-override: {enabled: true, strategy: cookie-only}}", "strategy"},
		{"model", "codex: {auto-cookie: true, state-override: {enabled: true, strategy: state, model-overrides: [{model: sol, strategy: cookie-only}]}}", "model-overrides"},
		{"skip", "codex: {auto-cookie: true, state-override: {enabled: true, strategy: cookie-only, rules: [{id: skip, action: skip}, {id: disabled, enabled: false}]}}", ""},
		{"override", "codex: {auto-cookie: true, state-override: {enabled: true, strategy: cookie-only, rules: [{id: state, settings: {strategy: state}}]}}", ""},
		{"nested", "codex: {auto-cookie: true, state-override: {enabled: true, rules: [{id: scoped, settings: {strategy: state}, model-overrides: [{id: cookie, models: [sol], settings: {strategy: cookie-only}}]}]}}", "scoped"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cfg Config
			if err := yaml.Unmarshal([]byte(tc.yaml), &cfg); err != nil {
				t.Fatal(err)
			}
			err := cfg.ValidateCodexAutoCookie()
			if tc.conflict == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.conflict) {
				t.Fatal(err)
			}
		})
	}
	var cfg Config
	if err := yaml.Unmarshal([]byte("codex: {auto-cookie: true, auto-cookie-override: false}"), &cfg); err != nil {
		t.Fatal(err)
	}
	if !cfg.Codex.AutoCookie || cfg.Codex.OverridesAutoCookie() {
		t.Fatal("explicit false lost")
	}
	copy, err := Clone(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	if copy.Codex.OverridesAutoCookie() {
		t.Fatal("clone changed policy")
	}
	*copy.Codex.AutoCookieOverride = true
	if cfg.Codex.OverridesAutoCookie() {
		t.Fatal("pointer aliased")
	}
	if !(CodexConfig{}).OverridesAutoCookie() {
		t.Fatal("override default must be true")
	}
}
