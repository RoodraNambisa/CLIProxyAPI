package auth

import (
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func authWithRequestScopedRules(action string) *Auth {
	return &Auth{ID: "rule-lifecycle", Provider: "codex", Metadata: map[string]any{
		"request_scoped_errors": []config.RequestScopedErrorRule{{Status: 400, Match: []string{"fixture"}, Action: action}},
	}}
}

func TestRequestScopedErrorCredentialUpdatesAndSnapshots(t *testing.T) {
	store := &countingStore{}
	m := NewManager(store, nil, nil)
	source := authWithRequestScopedRules("stop")
	installed, err := m.Register(WithSkipPersist(t.Context()), source)
	if err != nil {
		t.Fatal(err)
	}
	original := installed.requestScopedErrorRules
	source.Metadata["request_scoped_errors"].([]config.RequestScopedErrorRule)[0].Match[0] = "changed"
	current, _ := m.GetByID(source.ID)
	if current.Metadata["request_scoped_errors"].([]config.RequestScopedErrorRule)[0].Match[0] != "fixture" {
		t.Fatal("registered rules shared input metadata")
	}
	if err := m.persistWithoutLock(WithSkipPersist(t.Context()), current, false); err != nil || current.requestScopedErrorRules != original {
		t.Fatal("unchanged result persistence recompiled rules")
	}
	for _, update := range []string{"register", "update", "conditional"} {
		bad := authWithRequestScopedRules("invalid")
		switch update {
		case "register":
			_, err = m.Register(t.Context(), bad)
		case "update":
			_, err = m.Update(t.Context(), bad)
		case "conditional":
			_, _, err = m.UpdateIfCurrent(t.Context(), current, bad)
		}
		if err == nil {
			t.Fatal("invalid rules replaced credential")
		}
		next, _ := m.GetByID(source.ID)
		if next.RuntimeInstanceID() != current.RuntimeInstanceID() || next.requestScopedErrorRules != original {
			t.Fatal("rejected rules changed installed instance")
		}
		if store.saveCount.Load() != 0 {
			t.Fatal("invalid rules reached persistence")
		}
	}
	edited := current.Clone()
	edited.Metadata["request_scoped_errors"].([]config.RequestScopedErrorRule)[0].Action = "continue"
	next, err := m.Update(WithSkipPersist(t.Context()), edited)
	if err != nil {
		t.Fatal(err)
	}
	if action, _ := next.requestScopedErrorRules.rules.Match(400, "fixture"); action != config.RequestScopedActionContinue {
		t.Fatal("edited rules were not published")
	}
	if action, _ := original.rules.Match(400, "fixture"); action != config.RequestScopedActionStop {
		t.Fatal("edit changed an in-flight attempt")
	}
	delete(next.Metadata, "request_scoped_errors")
	next, err = m.Update(WithSkipPersist(t.Context()), next)
	if err != nil || next.requestScopedErrorRules != nil {
		t.Fatal("rule removal retained compiled policy")
	}
}

func TestRequestScopedErrorCredentialRefreshKeepsLocalRules(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		m := NewManager(nil, nil, nil)
		current, err := m.Register(WithSkipPersist(t.Context()), authWithRequestScopedRules("stop"))
		if err != nil {
			t.Fatal(err)
		}
		baseline := current.Clone()
		updated := authWithRequestScopedRules("continue-and-cooldown")
		var installed *Auth
		if prepared {
			installed, err = m.installPreparedRequestAuth(WithSkipPersist(t.Context()), current, updated, true)
		} else {
			installed, err = m.applyRefreshedAuth(WithSkipPersist(t.Context()), current, baseline, updated, time.Time{})
		}
		if err != nil || installed == nil {
			t.Fatalf("refresh: %v", err)
		}
		if installed.requestScopedErrorRules != current.requestScopedErrorRules {
			t.Fatal("refresh replaced compiled local rules")
		}
		if action, _ := installed.requestScopedErrorRules.rules.Match(400, "fixture"); action != config.RequestScopedActionStop {
			t.Fatal("refresh learned upstream rules")
		}
		edit := installed.Clone()
		edit.Metadata["request_scoped_errors"].([]config.RequestScopedErrorRule)[0].Action = "continue"
		edit, err = m.Update(WithSkipPersist(t.Context()), edit)
		if err != nil {
			t.Fatal(err)
		}
		// A refresh begun before the edit must never reinstall its old policy.
		_, _ = m.applyRefreshedAuth(WithSkipPersist(t.Context()), installed, installed, updated, time.Time{})
		after, _ := m.GetByID(edit.ID)
		if action, _ := after.requestScopedErrorRules.rules.Match(400, "fixture"); action != config.RequestScopedActionContinue {
			t.Fatal("stale refresh overwrote local edit")
		}
	}
}
