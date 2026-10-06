package config

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func TestCodexQuotaCreditsConfig(t *testing.T) {
	for _, tc := range []struct {
		value string
		valid bool
	}{
		{"{}", true}, {"{enabled: false}", true}, {"{enabled: true, minimum-balance: null}", true},
		{"{enabled: true, minimum-balance: 0}", true}, {"{enabled: true, minimum-balance: 62500.5}", true},
		{"{enabled: true, minimum-balance: -1}", false}, {"{enabled: true, minimum-balance: .nan}", false},
		{"{enabled: true, minimum-balance: .inf}", false}, {"{enabled: 'true'}", false},
	} {
		t.Run(tc.value, func(t *testing.T) {
			var cfg Config
			err := yaml.Unmarshal([]byte("codex: {quota-auto-disable: {rules: [{weekly-remaining-percent: 10, credits: "+tc.value+"}]}}"), &cfg)
			if err == nil {
				err = cfg.ValidateCodexQuotaAutoDisable()
			}
			if (err == nil) != tc.valid {
				t.Fatal("unexpected credit validation", err)
			}
			if !tc.valid {
				return
			}
			copy, errClone := Clone(&cfg)
			if errClone != nil {
				t.Fatal(errClone)
			}
			got, want := copy.Codex.QuotaAutoDisable.Rules[0].Credits, cfg.Codex.QuotaAutoDisable.Rules[0].Credits
			if got.Enabled != want.Enabled || (got.MinimumBalance == nil) != (want.MinimumBalance == nil) {
				t.Fatal("credit configuration changed on clone")
			}
			if want.MinimumBalance != nil && *got.MinimumBalance != *want.MinimumBalance {
				t.Fatal("credit threshold changed on clone")
			}
		})
	}
	var cfg Config
	if err := yaml.Unmarshal([]byte("codex: {quota-auto-disable: {rules: [{credits: {enabled: true, minimum-balance: 100}}]}}"), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.ValidateCodexQuotaAutoDisable() == nil {
		t.Fatal("accepted a credit-only rule without a main quota threshold")
	}
}
