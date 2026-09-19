package config

import (
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestReviewStateRulesSurviveConfigPersistence(t *testing.T) {
	for _, tc := range []struct {
		name  string
		rules []CodexStateRule
	}{
		{name: "explicit empty rules", rules: []CodexStateRule{}},
		{name: "disabled and priority zero", rules: []CodexStateRule{{ID: "zero", Enabled: ruleValue(false), Priorities: []int{0}, Settings: CodexStateRuleSettings{Lengths: ruleValue([]int{}), MatchModel: ruleValue(false), ResponseContains: ruleValue("")}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte("port: 8317\n"), 0600); err != nil {
				t.Fatal(err)
			}
			cfg := &Config{Codex: CodexConfig{StateOverride: CodexStateOverrideConfig{Enabled: true, Rules: &tc.rules}}}
			if err := SaveConfigPreserveComments(path, cfg); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var got Config
			if err := yaml.Unmarshal(data, &got); err != nil {
				t.Fatal(err)
			}
			rules := got.Codex.StateOverride.Rules
			if rules == nil || len(*rules) != len(tc.rules) {
				t.Fatalf("rules scope changed: %s", data)
			}
			if len(*rules) > 0 {
				r := (*rules)[0]
				if r.Enabled == nil || *r.Enabled || len(r.Priorities) != 1 || r.Priorities[0] != 0 || r.Settings.Lengths == nil || len(*r.Settings.Lengths) != 0 || r.Settings.MatchModel == nil || *r.Settings.MatchModel || r.Settings.ResponseContains == nil || *r.Settings.ResponseContains != "" {
					t.Fatalf("explicit overrides were lost: %s", data)
				}
			}
		})
	}
}
func TestReviewStateRuleInheritanceAndOrderPersist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	original := `codex:
  state-override:
    enabled: true
    rules:
      - id: special
        settings: {lengths: [332], match-model: false}
      - id: common
        settings: {lengths: [292]}
`
	if err := os.WriteFile(path, []byte(original), 0600); err != nil {
		t.Fatal(err)
	}
	var cfg Config
	if err := yaml.Unmarshal([]byte(original), &cfg); err != nil {
		t.Fatal(err)
	}
	cfg.Codex.StateOverride.Rules = ruleValue([]CodexStateRule{{ID: "common"}, {ID: "special"}})
	if err := SaveConfigPreserveComments(path, &cfg); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	var got Config
	if err := yaml.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	for _, rule := range *got.Codex.StateOverride.Rules {
		if rule.Settings.Lengths != nil || rule.Settings.MatchModel != nil {
			t.Fatalf("inherited settings retained old overrides after reorder: %s", data)
		}
	}
}

func TestReviewStateRuleSaveNullClearAndExtensions(t *testing.T) {
	for _, original := range []string{"codex: {state-override: {rules: null}}\n", "codex: {state-override: {rules: [{id: old}]}}\n", "defaults: &state {rules: [{id: inherited}]}\ncodex: {state-override: {<<: *state}}\n"} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte(original), 0600); err != nil {
			t.Fatal(err)
		}
		var cfg Config
		if err := yaml.Unmarshal([]byte(original), &cfg); err != nil {
			t.Fatal(err)
		}
		cfg.Codex.StateOverride.Rules = ruleValue([]CodexStateRule{})
		for _, empty := range []bool{true, false} {
			if !empty {
				cfg.Codex.StateOverride.Rules = nil
			}
			if err := SaveConfigPreserveComments(path, &cfg); err != nil {
				t.Fatal(err)
			}
			data, _ := os.ReadFile(path)
			var got Config
			if err := yaml.Unmarshal(data, &got); err != nil {
				t.Fatal(err)
			}
			if (got.Codex.StateOverride.Rules != nil) != empty {
				t.Fatalf("nil/empty rule meaning changed: %s", data)
			}
		}
	}
	original := `codex:
  state-override:
    rules:
      - id: a
        note: keep-a
        settings: {lengths: [332], future-setting: keep-setting}
      - id: b
        note: keep-b
`
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(original), 0600); err != nil {
		t.Fatal(err)
	}
	var cfg Config
	if err := yaml.Unmarshal([]byte(original), &cfg); err != nil {
		t.Fatal(err)
	}
	cfg.Codex.StateOverride.Rules = ruleValue([]CodexStateRule{{ID: "b"}, {ID: "a"}})
	if err := SaveConfigPreserveComments(path, &cfg); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	var doc struct {
		Codex struct {
			State struct {
				Rules []map[string]any `yaml:"rules"`
			} `yaml:"state-override"`
		} `yaml:"codex"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Codex.State.Rules[0]["note"] != "keep-b" || doc.Codex.State.Rules[1]["note"] != "keep-a" {
		t.Fatal("reorder lost extension identity")
	}
	settings := doc.Codex.State.Rules[1]["settings"].(map[string]any)
	if settings["future-setting"] != "keep-setting" || settings["lengths"] != nil {
		t.Fatal("clearing known overrides damaged unknown extensions")
	}
}

func TestReviewStateConfigClonePreservesInheritedLengths(t *testing.T) {
	cfg := &Config{Codex: CodexConfig{StateOverride: CodexStateOverrideConfig{Enabled: true, ModelOverrides: []CodexStateModelOverride{{Model: "model", MatchModel: ruleValue(false)}}}}}
	for _, lengths := range [][]int{nil, {}} {
		cfg.Codex.StateOverride.Lengths = lengths
		copyConfig, err := Clone(cfg)
		if err != nil {
			t.Fatal(err)
		}
		got := copyConfig.Codex.StateOverride
		if (got.Lengths == nil) != (lengths == nil) || got.ModelOverrides[0].Lengths != nil {
			t.Fatal("clone converted inherited lengths into an unlimited override")
		}
		if lengths == nil && got.ForModel("model").Lengths[0] != 292 {
			t.Fatal("cloned model stopped inheriting default lengths")
		}
		*got.ModelOverrides[0].MatchModel = true
		if *cfg.Codex.StateOverride.ModelOverrides[0].MatchModel {
			t.Fatal("cloned validation pointer aliases source")
		}
	}
}
