package config

import (
	"reflect"
	"testing"
)

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

func TestCodexStateSubscriptionLengthPrecedence(t *testing.T) {
	policy := CodexStateOverrideConfig{
		Lengths:        []int{200},
		ModelOverrides: []CodexStateModelOverride{{Model: "model-a", Lengths: []int{292}, Prompt: "model prompt"}},
		PlanLengths: []CodexStatePlanLengths{
			{PlanTypes: []string{"plus", "pro"}, Lengths: []int{292}},
			{PlanTypes: []string{"business"}, Lengths: []int{332}},
			{PlanTypes: []string{"team"}, Models: []string{"model-a"}, Lengths: []int{312}},
			{PlanTypes: []string{"free"}, Lengths: []int{}},
			{PlanTypes: []string{"TEAM"}, Lengths: []int{444}},
		},
	}
	for _, tc := range []struct {
		plan, model string
		lengths     []int
	}{
		{"plus", "other", []int{292}}, {"ChatGPTProPlan", "other", []int{292}},
		{"business", "other", []int{332}}, {"team", "other", []int{332}},
		{"ChatGPTBusinessPlan", "model-a", []int{312}}, {"business", "model-a", []int{312}},
		{"enterprise", "model-a", []int{292}}, {"", "other", []int{200}},
		{"free", "model-a", []int{}},
	} {
		t.Run(tc.plan+"/"+tc.model, func(t *testing.T) {
			got := policy.ForCredential(tc.plan, tc.model)
			if !reflect.DeepEqual(got.Lengths, tc.lengths) {
				t.Fatalf("lengths=%v, want %v", got.Lengths, tc.lengths)
			}
			if tc.model == "model-a" && got.Prompt != "model prompt" {
				t.Fatal("plan length changed another model setting")
			}
		})
	}
	cfg := &Config{Codex: CodexConfig{StateOverride: policy}}
	if err := cfg.ValidateCodexStateOverride(); err != nil {
		t.Fatal(err)
	}
	cloned, err := Clone(cfg)
	if err != nil {
		t.Fatal(err)
	}
	resolved := cloned.Codex.StateOverride.Resolved()
	resolved.PlanLengths[0].PlanTypes[0] = "changed"
	resolved.PlanLengths[0].Lengths[0] = 999
	if cloned.Codex.StateOverride.PlanLengths[0].PlanTypes[0] != "plus" || policy.PlanLengths[0].Lengths[0] != 292 {
		t.Fatal("plan length policy aliases its source")
	}
	for _, invalid := range []CodexStatePlanLengths{
		{Lengths: []int{292}}, {PlanTypes: []string{"pro"}},
		{PlanTypes: []string{"pro"}, Lengths: []int{-1}},
		{PlanTypes: []string{"bad\x00plan"}, Lengths: []int{292}},
	} {
		cfg.Codex.StateOverride.PlanLengths = []CodexStatePlanLengths{invalid}
		if cfg.ValidateCodexStateOverride() == nil {
			t.Fatal("invalid plan length rule accepted")
		}
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
