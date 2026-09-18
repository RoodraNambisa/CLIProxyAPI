package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCodexQuotaAutoDisableConfigValidationAndRoundTrip(t *testing.T) {
	for _, test := range []struct {
		threshold string
		valid     bool
	}{
		{"10", true}, {"0", true}, {"100", true}, {"10.5", true},
		{"null", false}, {"-1", false}, {"101", false}, {".nan", false}, {".inf", false}, {"oops", false},
	} {
		t.Run(test.threshold, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			source := "codex:\n  observe-quota: true\n  quota-auto-disable:\n    enabled: true\n    rules:\n      - providers: [codex]\n        auth-priorities: [0, 3]\n        weekly-remaining-percent: " + test.threshold + "\n"
			if err := os.WriteFile(path, []byte(source), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfig(path)
			if (err == nil) != test.valid {
				t.Fatalf("valid=%t, error=%v", test.valid, err)
			}
			if !test.valid {
				return
			}
			if cfg.Codex.QuotaAutoDisable.Rules[0].FiveHourRemainingPercent != nil {
				t.Fatal("blank window became an active threshold")
			}
			// Setting a threshold to null must clear the prior YAML value.
			cfg.Codex.QuotaAutoDisable.Rules[0].FiveHourRemainingPercent = cfg.Codex.QuotaAutoDisable.Rules[0].WeeklyRemainingPercent
			cfg.Codex.QuotaAutoDisable.Rules[0].WeeklyRemainingPercent = nil
			if err := SaveConfigPreserveComments(path, cfg); err != nil {
				t.Fatal(err)
			}
			reloaded, err := LoadConfig(path)
			if err != nil || reloaded.Codex.QuotaAutoDisable.Rules[0].WeeklyRemainingPercent != nil || reloaded.Codex.QuotaAutoDisable.Rules[0].FiveHourRemainingPercent == nil {
				t.Fatalf("threshold clear did not survive reload: %v", err)
			}
			cfg.Codex.QuotaAutoDisable = CodexQuotaAutoDisableConfig{}
			if err := SaveConfigPreserveComments(path, cfg); err != nil {
				t.Fatal(err)
			}
			reloaded, err = LoadConfig(path)
			if err != nil || reloaded.Codex.QuotaAutoDisable.Enabled || len(reloaded.Codex.QuotaAutoDisable.Rules) != 0 {
				t.Fatal("cleared rules survived reload")
			}
		})
	}
	for _, rule := range []string{
		"{weekly-remaining-percent: 10, auth-priorities: [1.5]}",
		"{weekly-remaining-percent: 10, providers: ['']}",
		"{weekly-remaining-percent: 10, credential-ids: ['']}",
		"{weekly-remaining-percent: 10, auth-priorities: [9007199254740992]}",
	} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte("codex:\n  quota-auto-disable:\n    rules: ["+rule+"]\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadConfig(path); err == nil {
			t.Fatalf("accepted invalid rule %s", rule)
		}
	}
}

func TestCodexQuotaAutoDisableDefaultAndBounds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("codex:\n  observe-quota: true\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil || cfg.Codex.QuotaAutoDisable.Enabled || len(cfg.Codex.QuotaAutoDisable.Rules) != 0 {
		t.Fatal("upgraded config enabled automatic disable")
	}
	cfg.Codex.QuotaAutoDisable.Rules = make([]CodexQuotaAutoDisableRule, 129)
	if err = cfg.ValidateCodexQuotaAutoDisable(); err == nil || !strings.Contains(err.Error(), "128") {
		t.Fatal("rule limit not enforced")
	}
}
