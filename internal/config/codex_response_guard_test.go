package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestResponseGuardIndependentRulesAndAcceptance(t *testing.T) {
	c := CodexConfig{ResponseGuard: CodexResponseGuardConfig{Enabled: true, CodexResponseGuardSettings: CodexResponseGuardSettings{Mode: new("enforce"), LengthMode: new("allow"), Lengths: new([]int{292})}, Rules: &[]CodexResponseGuardRule{
		{ID: "specific", Credentials: []string{"short"}, PlanTypes: []string{"pro"}, Models: []string{"sol"}, Settings: CodexResponseGuardSettings{AllowedReturnedModels: new([]string{"6-sol"})}, ModelOverrides: []CodexResponseGuardModelOverride{{ID: "sol", Models: []string{"sol"}, Settings: CodexResponseGuardSettings{Lengths: new([]int{292, 312})}}}},
		{ID: "fallback", Priorities: []int{0}},
	}}}
	if c.StateOverride.Enabled {
		t.Fatal("fixture must have management disabled")
	}
	p := c.ResponseGuard.PolicyFor(CodexStateScope{ShortID: "short", Plan: "pro", Model: "sol"})
	if p.Mode != "enforce" || p.Rule != 1 || !p.Evaluate("sol", CodexResponseEvidence{Model: "6-sol", StatePresent: true, StateLength: 312}, true).Accepted() {
		t.Fatalf("independent policy: %+v", p)
	}
	for _, scope := range []CodexStateScope{{ShortID: "other", Plan: "pro", Model: "sol"}, {ShortID: "short", Plan: "plus", Model: "sol"}, {ShortID: "short", Plan: "pro", Model: "astra"}} {
		policy := c.ResponseGuard.PolicyFor(scope)
		if policy.Rule != 2 || policy.Evaluate(scope.Model, CodexResponseEvidence{Model: "6-sol", StatePresent: true, StateLength: 312}, true).Accepted() {
			t.Fatalf("scope leaked: %+v", policy)
		}
	}
	state := c.ManagedStateConfig().ApplyResponseAcceptance(CodexStateScope{ShortID: "short", Plan: "pro", Model: "sol"}, CodexStateOverrideConfig{Lengths: []int{111}, InvalidateOnModelMismatch: true})
	if state.Enabled || !state.InvalidateOnModelMismatch || !reflect.DeepEqual(state.Lengths, []int{292, 312}) || !CodexReturnedModelAccepted("sol", "6-sol", state.AcceptedReturnedModels) {
		t.Fatalf("resource acceptance: %+v", state)
	}
	c.ResponseGuard.Enabled = false
	if c.ResponseGuard.PolicyFor(CodexStateScope{Model: "sol"}).Mode != "off" {
		t.Fatal("master disable must override all rules")
	}
}

func TestResponseGuardDimensionsAndMissing(t *testing.T) {
	c := CodexResponseGuardConfig{Enabled: true, CodexResponseGuardSettings: CodexResponseGuardSettings{Mode: new("enforce"), AllowedReturnedModels: new([]string{"alias"}), LengthMode: new("allow"), Lengths: new([]int{292})}}
	p := c.PolicyFor(CodexStateScope{Model: "requested"})
	for _, test := range []struct {
		model    string
		length   int
		accepted bool
	}{{"requested", 292, true}, {"alias", 292, true}, {"alias", 312, false}, {"other", 292, false}, {"other", 312, false}, {"", 0, true}} {
		v := p.Evaluate("requested", CodexResponseEvidence{Model: test.model, StatePresent: test.length > 0, StateLength: test.length}, true)
		if v.Accepted() != test.accepted {
			t.Fatalf("%+v: %+v", test, v)
		}
	}
	p.MissingModel, p.MissingState = "reject", "reject"
	if !p.Evaluate("requested", CodexResponseEvidence{}, false).Accepted() || len(p.Evaluate("requested", CodexResponseEvidence{}, true).Reasons) != 2 {
		t.Fatal("missing values must wait for admission")
	}
	p.LengthMode = "deny"
	p.Lengths = []int{312}
	if p.Evaluate("requested", CodexResponseEvidence{Model: "requested", StatePresent: true, StateLength: 312}, true).Accepted() || !p.Evaluate("requested", CodexResponseEvidence{Model: "requested", StatePresent: true, StateLength: 292}, true).Accepted() {
		t.Fatal("deny semantics")
	}
}

func TestResponseGuardSavePreservesUnknownAndExplicitEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	initial := "codex:\n  response-guard:\n    enabled: true\n    mode: enforce\n    future-option: retained\n    rules:\n      - id: r\n        future-rule: retained\n        settings:\n          match-model: true\n          lengths: [292]\n          future-setting: retained\n"
	if err := os.WriteFile(path, []byte(initial), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	c.Codex.ResponseGuard.Rules = &[]CodexResponseGuardRule{{ID: "r", Settings: CodexResponseGuardSettings{MatchModel: new(false), Lengths: new([]int{})}}}
	if err := SaveConfigPreserveComments(path, c); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	p := loaded.Codex.ResponseGuard.PolicyFor(CodexStateScope{Model: "m"})
	if p.MatchModel || p.Lengths == nil || len(p.Lengths) != 0 {
		t.Fatalf("lost explicit overrides: %+v", p)
	}
	b, _ := os.ReadFile(path)
	for _, value := range []string{"future-option: retained", "future-rule: retained", "future-setting: retained"} {
		if !strings.Contains(string(b), value) {
			t.Fatalf("lost %s: %s", value, b)
		}
	}
}
