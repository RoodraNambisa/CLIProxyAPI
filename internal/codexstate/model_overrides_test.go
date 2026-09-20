package codexstate

import (
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"testing"
	"time"
)

func TestModelOverrideMergeAndHotUpdatePreserveOtherModels(t *testing.T) {
	m := New()
	sol := Credential{ID: "a", Owner: "a", Instance: "a", Model: "model", Priority: 3}
	astra := sol
	astra.Model = "astra"
	special := config.CodexStateRuleSettings{MatchModel: rulePtr(false), InvalidateOnModelMismatch: rulePtr(false)}
	cfg := config.CodexStateOverrideConfig{Enabled: true, Acquisition: "manual", Rules: &[]config.CodexStateRule{{ID: "special", Models: []string{"model"}, Settings: special}, {ID: "common"}}}
	m.Sync(cfg, []Credential{sol, astra})
	now := time.Now()
	finishFixture(m, sol, good(), nil, now)
	result := good()
	result.Model = "astra"
	finishFixture(m, astra, result, nil, now)
	_, _, version, _ := m.PickVersion(sol, "", now)
	cfg.Rules = &[]config.CodexStateRule{{ID: "merged", ModelOverrides: []config.CodexStateRuleModelOverride{{ID: "special", Models: []string{"model"}, Settings: special}}}}
	m.Sync(cfg, []Credential{sol, astra})
	v, _, v2, _ := m.PickVersion(sol, "", now)
	if v == "" || version != v2 {
		t.Fatal("equivalent merge cleared State")
	}
	(*cfg.Rules)[0].ModelOverrides[0].Settings.MatchModel = rulePtr(true)
	m.Sync(cfg, []Credential{sol, astra})
	if v, _, _ := m.Pick(sol, "", now); v != "" {
		t.Fatal("changed acquisition validation reused State")
	}
	if v, _, _ := m.Pick(astra, "", now); v == "" {
		t.Fatal("unrelated model cleared")
	}
}

func TestObservationUsesRequestPolicyWithoutInvalidatingNewVersion(t *testing.T) {
	m, c, cfg := fixture()
	cfg.Acquisition = "manual"
	m.Sync(cfg, []Credential{c})
	now := time.Now()
	finishFixture(m, c, good(), nil, now)
	value, _, version, _ := m.PickVersion(c, "", now)
	enabled := cfg
	enabled.InvalidateOnModelMismatch = true
	if reason := m.ObserveResponseForPolicy(c, version, value, "other", 0, cfg); reason != "" {
		t.Fatal("disabled request check invalidated State")
	}
	m.Sync(enabled, []Credential{c})
	if reason := m.ObserveResponseForPolicy(c, version, value, "other", 0, cfg); reason != "" {
		t.Fatal("new config changed in-flight request policy")
	}
	finishFixture(m, c, good(), nil, now)
	m.ObserveResponseForPolicy(c, version, value, "other", 0, enabled)
	if v, _, _ := m.Pick(c, "", now); v == "" {
		t.Fatal("old response cleared replacement State")
	}
	value, _, version, _ = m.PickVersion(c, "", now)
	m.Sync(cfg, []Credential{c})
	if reason := m.ObserveResponseForPolicy(c, version, value, "other", 0, enabled); reason != "response_model_mismatch" {
		t.Fatal("original request policy was not respected")
	}
}
