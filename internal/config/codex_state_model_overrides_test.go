package config

import (
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestStateRuleModelOverridesIndependentFlagsAndInheritance(t *testing.T) {
	for _, matchModel := range []bool{false, true} {
		for _, invalidate := range []bool{false, true} {
			c := CodexStateOverrideConfig{Enabled: true, Lengths: []int{292, 332}, InvalidateOnModelMismatch: true, Rules: &[]CodexStateRule{{ID: "priority-3", Priorities: []int{3}, Models: []string{"astra", "sol", "terra"}, Settings: CodexStateRuleSettings{TTLMinutes: ruleValue(90)}, ModelOverrides: []CodexStateRuleModelOverride{{ID: "sol", Models: []string{"sol"}, Settings: CodexStateRuleSettings{MatchModel: &matchModel, InvalidateOnModelMismatch: &invalidate}}}}}}
			for _, model := range []string{"astra", "sol", "terra"} {
				policy, match, ok := c.PolicyFor(CodexStateScope{Priority: 3, Model: model})
				if !ok || len(policy.Lengths) != 2 || policy.TTLMinutes != 90 || match.Sources["ttl-minutes"] != "rule" || match.Sources["lengths"] != "default" {
					t.Fatalf("inheritance failed: %+v %+v", policy, match)
				}
				wantMatch, wantInvalidate := true, true
				if model == "sol" {
					wantMatch, wantInvalidate = matchModel, invalidate
					if match.ModelOverrideID != "sol" || match.Sources["match-model"] != "model-override" {
						t.Fatal("override provenance missing")
					}
				}
				if *policy.MatchModel != wantMatch || policy.InvalidateOnModelMismatch != wantInvalidate {
					t.Fatal("independent flags changed")
				}
			}
			for _, scope := range []CodexStateScope{{Priority: 4, Model: "sol"}, {Priority: 3, Model: "luna"}} {
				if _, _, ok := c.PolicyFor(scope); ok {
					t.Fatal("override expanded scope")
				}
			}
			c.Lengths = []int{400}
			p, _, _ := c.PolicyFor(CodexStateScope{Priority: 3, Model: "sol"})
			if p.Lengths[0] != 400 {
				t.Fatal("Sol stopped following defaults")
			}
		}
	}
}

func TestStateRuleModelOverrideAliasesDefaultsAndConflicts(t *testing.T) {
	c := CodexStateOverrideConfig{Enabled: true, ModelOverrides: []CodexStateModelOverride{{Model: "sol", MatchModel: ruleValue(false), Lengths: []int{292}}}, PlanLengths: []CodexStatePlanLengths{{PlanTypes: []string{"team"}, Lengths: []int{332}}}, Rules: &[]CodexStateRule{{ID: "rule", Models: []string{"alias"}, ModelOverrides: []CodexStateRuleModelOverride{{ID: "special", Models: []string{"sol"}, Settings: CodexStateRuleSettings{InvalidateOnModelMismatch: ruleValue(true)}}}}}}
	scope := CodexStateScope{Model: "sol", Aliases: []string{"alias"}, Plan: "business"}
	p, m, ok := c.PolicyFor(scope)
	if !ok || *p.MatchModel || p.Lengths[0] != 332 || m.Sources["lengths"] != "plan" || m.Sources["match-model"] != "global-model" || !p.InvalidateOnModelMismatch {
		t.Fatalf("lost layered defaults: %+v %+v", p, m)
	}
	(*c.Rules)[0].ModelOverrides = append((*c.Rules)[0].ModelOverrides, CodexStateRuleModelOverride{ID: "collision", Models: []string{"alias"}})
	_, m, ok = c.PolicyFor(scope)
	if ok || m.Action != "conflict" || len(m.Conflicts) != 2 {
		t.Fatal("alias collision silently chose a policy")
	}
	(*c.Rules)[0].ModelOverrides[1].Enabled = ruleValue(false)
	if _, _, ok = c.PolicyFor(scope); !ok {
		t.Fatal("disabled override causes conflict")
	}
	(*c.Rules)[0].Action = "skip"
	if _, _, ok = c.PolicyFor(scope); ok {
		t.Fatal("override bypassed skip")
	}
}

func TestStateRuleModelOverrideCloneAndPersistence(t *testing.T) {
	original := `codex:
  state-override:
    rules:
      - id: main
        model-overrides:
          - id: a
            models: [sol]
            note: keep-a
            settings: {match-model: false, invalidate-on-model-mismatch: false, lengths: [], response-contains: "", future: keep}
          - id: b
            models: [terra]
            note: keep-b
            settings: {match-model: true}
`
	var cfg Config
	if err := yaml.Unmarshal([]byte(original), &cfg); err != nil {
		t.Fatal(err)
	}
	copy, err := Clone(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	(*(*copy.Codex.StateOverride.Rules)[0].ModelOverrides[0].Settings.MatchModel) = true
	if *(*cfg.Codex.StateOverride.Rules)[0].ModelOverrides[0].Settings.MatchModel {
		t.Fatal("clone aliases override")
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(original), 0600); err != nil {
		t.Fatal(err)
	}
	items := (*cfg.Codex.StateOverride.Rules)[0].ModelOverrides
	items[1].Settings = CodexStateRuleSettings{}
	(*cfg.Codex.StateOverride.Rules)[0].ModelOverrides = []CodexStateRuleModelOverride{items[1], items[0]}
	if err := SaveConfigPreserveComments(path, &cfg); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	var got Config
	if err := yaml.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	out := (*got.Codex.StateOverride.Rules)[0].ModelOverrides
	if out[0].ID != "b" || out[0].Settings.MatchModel != nil || out[1].Settings.MatchModel == nil || *out[1].Settings.MatchModel || out[1].Settings.Lengths == nil || len(*out[1].Settings.Lengths) != 0 || out[1].Settings.InvalidateOnModelMismatch == nil || *out[1].Settings.InvalidateOnModelMismatch {
		t.Fatalf("save changed overrides: %s", data)
	}
	var doc map[string]any
	_ = yaml.Unmarshal(data, &doc)
	rules := doc["codex"].(map[string]any)["state-override"].(map[string]any)["rules"].([]any)
	nested := rules[0].(map[string]any)["model-overrides"].([]any)
	if nested[0].(map[string]any)["note"] != "keep-b" || nested[1].(map[string]any)["settings"].(map[string]any)["future"] != "keep" {
		t.Fatal("extensions lost identity")
	}
	(*got.Codex.StateOverride.Rules)[0].ModelOverrides = nil
	if err := SaveConfigPreserveComments(path, &got); err != nil {
		t.Fatal(err)
	}
	data, _ = os.ReadFile(path)
	var cleared Config
	_ = yaml.Unmarshal(data, &cleared)
	if len((*cleared.Codex.StateOverride.Rules)[0].ModelOverrides) != 0 {
		t.Fatal("deleted overrides remained")
	}
}

func TestStateRuleModelOverrideValidation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		items   []CodexStateRuleModelOverride
		invalid bool
	}{
		{"valid", []CodexStateRuleModelOverride{{ID: "a", Models: []string{"sol"}, Settings: CodexStateRuleSettings{MatchModel: ruleValue(false)}}}, false},
		{"missing models", []CodexStateRuleModelOverride{{ID: "a"}}, true},
		{"duplicate", []CodexStateRuleModelOverride{{ID: "a", Models: []string{"sol"}}, {ID: "b", Models: []string{"sol"}}}, true},
		{"wildcard", []CodexStateRuleModelOverride{{ID: "a", Models: []string{"gpt*"}}}, true},
		{"invalid length", []CodexStateRuleModelOverride{{ID: "a", Models: []string{"sol"}, Settings: CodexStateRuleSettings{Lengths: ruleValue([]int{-1})}}}, true},
		{"effective lifetime", []CodexStateRuleModelOverride{{ID: "a", Models: []string{"sol"}, Settings: CodexStateRuleSettings{RefreshBeforeMinutes: ruleValue(95)}}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{Codex: CodexConfig{StateOverride: CodexStateOverrideConfig{Rules: &[]CodexStateRule{{ID: "rule", Settings: CodexStateRuleSettings{TTLMinutes: ruleValue(90)}, ModelOverrides: tc.items}}}}}
			if (cfg.ValidateCodexStateOverride() != nil) != tc.invalid {
				t.Fatal("unexpected validation result")
			}
		})
	}
}
