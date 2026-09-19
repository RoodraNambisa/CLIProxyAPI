package config

import (
	"testing"
)

func ruleValue[T any](v T) *T { return &v }
func TestCodexStateRulesPriorityCredentialAndFirstMatch(t *testing.T) {
	c := CodexStateOverrideConfig{Enabled: true, Lengths: []int{200}, Rules: &[]CodexStateRule{
		{ID: "special", Name: "Specific accounts", Credentials: []string{"short-a"}, Models: []string{"alias"}, Settings: CodexStateRuleSettings{Lengths: ruleValue([]int{332}), MatchModel: ruleValue(false)}},
		{ID: "skip", Credentials: []string{"b"}, Models: []string{"model"}, Action: "skip"},
		{ID: "priority", Priorities: []int{4}, Models: []string{"model"}, Settings: CodexStateRuleSettings{Lengths: ruleValue([]int{292})}},
	}}
	for _, tc := range []struct {
		id, short string
		priority  int
		model     string
		want      string
		length    int
		managed   bool
	}{
		{"a", "short-a", 4, "model", "special", 332, true}, {"b", "", 4, "model", "skip", 0, false},
		{"c", "", 4, "model", "priority", 292, true}, {"c", "", 3, "model", "", 0, false}, {"a", "short-a", 4, "other", "", 0, false},
	} {
		p, m, ok := c.PolicyFor(CodexStateScope{ID: tc.id, ShortID: tc.short, Priority: tc.priority, Model: tc.model, Aliases: []string{tc.model + "-client"}})
		if tc.id == "a" && tc.model == "model" {
			p, m, ok = c.PolicyFor(CodexStateScope{ID: tc.id, ShortID: tc.short, Priority: tc.priority, Model: tc.model, Aliases: []string{"alias"}})
		}
		if ok != tc.managed || m.RuleID != tc.want || ok && (len(p.Lengths) != 1 || p.Lengths[0] != tc.length) {
			t.Fatalf("%+v: policy=%+v match=%+v", tc, p, m)
		}
	}
	(*c.Rules)[0].Priorities = []int{3}
	if _, match, _ := c.PolicyFor(CodexStateScope{ID: "a", ShortID: "short-a", Priority: 4, Model: "model", Aliases: []string{"alias"}}); match.RuleID != "priority" {
		t.Fatal("rule selectors did not use AND")
	}
	(*c.Rules)[0].Priorities = nil
	(*c.Rules)[0].Enabled = ruleValue(false)
	if _, match, _ := c.PolicyFor(CodexStateScope{ID: "a", ShortID: "short-a", Priority: 4, Model: "model", Aliases: []string{"alias"}}); match.RuleID != "priority" {
		t.Fatal("disabled rule stopped matching")
	}
}
func TestCodexStateRuleInheritanceAndClone(t *testing.T) {
	for _, rules := range []*[]CodexStateRule{nil, ruleValue([]CodexStateRule{}), ruleValue([]CodexStateRule{{ID: "all", Settings: CodexStateRuleSettings{Lengths: ruleValue([]int{}), MatchModel: ruleValue(false), ResponseContains: ruleValue("")}}})} {
		c := &Config{Codex: CodexConfig{StateOverride: CodexStateOverrideConfig{Enabled: true, Lengths: []int{292}, Rules: rules, ResponseContains: "OK"}}}
		copyConfig, err := Clone(c)
		if err != nil {
			t.Fatal(err)
		}
		if (rules == nil) != (copyConfig.Codex.StateOverride.Rules == nil) || rules != nil && len(*rules) != len(*copyConfig.Codex.StateOverride.Rules) {
			t.Fatal("clone changed absent/empty rules semantics")
		}
		p, _, managed := copyConfig.Codex.StateOverride.PolicyFor(CodexStateScope{ID: "x", Model: "m"})
		if rules == nil {
			if !managed || p.Lengths[0] != 292 {
				t.Fatal("legacy defaults changed")
			}
			continue
		}
		if len(*rules) == 0 {
			if managed {
				t.Fatal("empty rules managed all accounts")
			}
			continue
		}
		if !managed || len(p.Lengths) != 0 || *p.MatchModel || p.ResponseContains != "" {
			t.Fatal("explicit empty or false inherited defaults")
		}
		resolved := copyConfig.Codex.StateOverride.Resolved()
		*(*resolved.Rules)[0].Settings.MatchModel = true
		if *(*copyConfig.Codex.StateOverride.Rules)[0].Settings.MatchModel {
			t.Fatal("resolved settings alias source")
		}
	}
}
func TestCodexStateRuleValidation(t *testing.T) {
	for _, settings := range []CodexStateRuleSettings{{RetrySeconds: ruleValue(0)}, {MaxAttempts: ruleValue(11)}, {TTLMinutes: ruleValue(4)}, {Mode: ruleValue("")}, {Lengths: ruleValue([]int{-1})}, {ProxyMode: ruleValue("custom"), ProxyURL: ruleValue("file:///etc/passwd")}} {
		c := &Config{Codex: CodexConfig{StateOverride: CodexStateOverrideConfig{Rules: ruleValue([]CodexStateRule{{ID: "rule", Settings: settings}})}}}
		if c.ValidateCodexStateOverride() == nil {
			t.Fatalf("invalid settings accepted: %+v", settings)
		}
	}
}

func TestCodexStateRulesPlanExclusionAndDefaultPrecedence(t *testing.T) {
	c := CodexStateOverrideConfig{Enabled: true, Lengths: []int{200}, ModelOverrides: []CodexStateModelOverride{{Model: "model", Lengths: []int{292}}}, PlanLengths: []CodexStatePlanLengths{{PlanTypes: []string{"team"}, Lengths: []int{332}}}, Rules: ruleValue([]CodexStateRule{
		{ID: "specific", Priorities: []int{4}, PlanTypes: []string{"business"}, ExcludedCredentials: []string{"excluded"}, Settings: CodexStateRuleSettings{Lengths: ruleValue([]int{})}},
		{ID: "fallback", Settings: CodexStateRuleSettings{MatchModel: ruleValue(false)}},
	})}
	p, match, ok := c.PolicyFor(CodexStateScope{ID: "account", Priority: 4, Plan: "ChatGPTBusinessPlan", Model: "model"})
	if !ok || match.RuleID != "specific" || len(p.Lengths) != 0 {
		t.Fatal("plan matching or explicit unlimited lengths lost")
	}
	p, match, ok = c.PolicyFor(CodexStateScope{ID: "excluded", Priority: 4, Plan: "team", Model: "model"})
	if !ok || match.RuleID != "fallback" || p.Lengths[0] != 332 || *p.MatchModel {
		t.Fatal("exclusion did not continue to the next rule with resolved defaults")
	}
	cfg := &Config{Codex: CodexConfig{StateOverride: c}}
	(*cfg.Codex.StateOverride.Rules)[1].ID = "specific"
	if cfg.ValidateCodexStateOverride() == nil {
		t.Fatal("duplicate rule ID accepted")
	}
}
