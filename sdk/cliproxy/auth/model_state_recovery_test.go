package auth

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
)

type suffixStateResultHook struct {
	NoopHook
	model string
}

func (h *suffixStateResultHook) OnResult(_ context.Context, result Result) { h.model = result.Model }

func TestModelSuffixStateWritesAndRecovery(t *testing.T) {
	previous := quotaCooldownDisabled.Load()
	quotaCooldownDisabled.Store(false)
	t.Cleanup(func() { quotaCooldownDisabled.Store(previous) })
	hook := &suffixStateResultHook{}
	manager := NewManager(nil, &RoundRobinSelector{}, hook)
	manager.SetConfig(&config.Config{FixedErrorCooldowns: []config.FixedErrorCooldownRule{{StatusCode: 429, CooldownSeconds: 180, Scope: "model"}}})
	auth := &Auth{ID: "suffix-state-result", Provider: "codex"}
	registerFallbackAuthForModel(t, manager, auth, "suffix-state-model")
	for _, model := range []string{"suffix-state-model(high)", "suffix-state-model(low)"} {
		manager.MarkResult(t.Context(), Result{AuthID: auth.ID, Provider: "codex", Model: model, Error: &Error{HTTPStatus: 429, Message: "fixture quota"}})
		if hook.model != model {
			t.Fatal("state normalization changed the hook's original model")
		}
	}
	current, _ := manager.GetByID(auth.ID)
	state := current.ModelStates["suffix-state-model"]
	if len(current.ModelStates) != 1 || state == nil || state.Quota.StrikeCount != 2 {
		t.Fatalf("suffix writes did not share quota accounting: %+v", current.ModelStates)
	}
	if registry.GetGlobalRegistry().GetModelCount("suffix-state-model") != 0 {
		t.Fatal("suffix quota did not suspend the registered base model")
	}
	manager.MarkResult(t.Context(), Result{AuthID: auth.ID, Provider: "codex", Model: "suffix-state-model(max)", Success: true})
	current, _ = manager.GetByID(auth.ID)
	if !modelStateIsClean(current.ModelStates["suffix-state-model"]) || current.Unavailable {
		t.Fatal("successful sibling did not clear the shared model cooldown")
	}
	if registry.GetGlobalRegistry().GetModelCount("suffix-state-model") != 1 {
		t.Fatal("successful sibling did not resume the registered model")
	}
}

func TestModelSuffixStateInstallationAndLegacyRecovery(t *testing.T) {
	for _, entry := range []string{"register", "update", "load", "refresh"} {
		t.Run(entry, func(t *testing.T) {
			auth := &Auth{ID: "suffix-install-" + entry, Provider: "codex", ModelStates: map[string]*ModelState{
				"model(high)": {Status: StatusError, Unavailable: true, NextRetryAfter: time.Now().Add(time.Hour), Quota: QuotaState{Exceeded: true, StrikeCount: 3}},
				"other":       {Status: StatusError, Unavailable: true, NextRetryAfter: time.Now().Add(time.Hour)},
			}}
			before := auth.Clone()
			manager := NewManager(nil, &RoundRobinSelector{}, nil)
			var err error
			switch entry {
			case "register":
				_, err = manager.Register(t.Context(), auth)
			case "load":
				manager.SetStore(&fileProjectionTestStore{auths: []*Auth{auth}})
				err = manager.Load(t.Context())
			case "update", "refresh":
				installed, errRegister := manager.Register(t.Context(), &Auth{ID: auth.ID, Provider: auth.Provider})
				if errRegister != nil {
					t.Fatal(errRegister)
				}
				if entry == "update" {
					_, err = manager.Update(t.Context(), auth)
				} else {
					// Simulate a legacy installed state. Refresh must carry runtime
					// availability forward instead of accepting new token metadata as state.
					manager.mu.Lock()
					manager.auths[auth.ID].ModelStates = auth.Clone().ModelStates
					manager.mu.Unlock()
					installed, _ = manager.GetByID(auth.ID)
					_, _, err = manager.UpdateRefreshedIfCurrent(t.Context(), installed, auth)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before.ModelStates, auth.ModelStates) {
				t.Fatal("installation mutated caller-owned model states")
			}
			current, _ := manager.GetByID(auth.ID)
			if current.ModelStates["model"] == nil || len(current.ModelStates) != 2 {
				t.Fatal("legacy suffix state was not normalized on installation")
			}
			manager.MarkResult(t.Context(), Result{AuthID: auth.ID, Provider: "codex", Model: "model(low)", Success: true})
			current, _ = manager.GetByID(auth.ID)
			if blocked, _, _ := isAuthBlockedForModel(current, "model(high)", time.Now()); blocked {
				t.Fatal("legacy suffix still blocks after sibling success")
			}
			if blocked, _, _ := isAuthBlockedForModel(current, "other", time.Now()); !blocked {
				t.Fatal("sibling success cleared another model's cooldown")
			}
		})
	}
}

func TestModelSuffixNormalizationPreservesLocalStateContracts(t *testing.T) {
	now := time.Now()
	deadline := now.Add(time.Hour)
	for _, fixture := range []struct {
		name    string
		states  map[string]*ModelState
		blocked bool
		reason  blockReason
	}{
		{"zero and inactive", map[string]*ModelState{
			"model(high)": {Status: StatusError, Unavailable: true, Quota: QuotaState{Exceeded: true, NextRecoverAt: deadline}},
			"model(low)":  {Status: StatusActive, NextRetryAfter: deadline},
		}, false, blockReasonNone},
		{"active and newer clean", map[string]*ModelState{
			"model(high)": {Status: StatusError, Unavailable: true, NextRetryAfter: deadline},
			"model(low)":  {Status: StatusActive, UpdatedAt: now},
		}, true, blockReasonOther},
		{"disabled and newer clean", map[string]*ModelState{
			"model(high)": {Status: StatusDisabled},
			"model(low)":  {Status: StatusActive, UpdatedAt: now},
		}, true, blockReasonDisabled},
		{"quota cannot mask other block", map[string]*ModelState{
			"model(high)": {Status: StatusError, Unavailable: true, NextRetryAfter: deadline},
			"model(low)":  {Status: StatusError, Unavailable: true, NextRetryAfter: deadline.Add(time.Hour), Quota: QuotaState{Exceeded: true}},
		}, true, blockReasonOther},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			auth := &Auth{ModelStates: fixture.states}
			normalizeModelStates(auth)
			if len(auth.ModelStates) != 1 {
				t.Fatal("sibling states did not merge")
			}
			blocked, reason, _ := isAuthBlockedForModel(auth, "model(max)", now)
			if blocked != fixture.blocked || reason != fixture.reason {
				t.Fatalf("merged availability = %v/%v", blocked, reason)
			}
			before := auth.Clone()
			normalizeModelStates(auth)
			if !reflect.DeepEqual(before, auth) {
				t.Fatal("normalization is not idempotent")
			}
		})
	}
	for range 30 {
		original := &Auth{ModelStates: map[string]*ModelState{
			"model(high)": {Status: StatusError, Unavailable: true, NextRetryAfter: deadline.Add(time.Hour), LastError: &Error{Code: "older"}, Quota: QuotaState{Exceeded: true, BackoffLevel: 4, StrikeCount: 8}},
			"model(low)":  {Status: StatusError, Unavailable: true, NextRetryAfter: deadline, UpdatedAt: now, LastError: &Error{Code: "newer"}, Quota: QuotaState{Exceeded: true, BackoffLevel: 2, StrikeCount: 3}},
		}}
		auth := &Auth{ModelStates: original.ModelStates}
		normalizeModelStates(auth)
		state := auth.ModelStates["model"]
		if !state.NextRetryAfter.Equal(deadline.Add(time.Hour)) || state.Quota.BackoffLevel != 4 || state.Quota.StrikeCount != 8 || state.LastError.Code != "newer" {
			t.Fatal("merge lost the strongest deadline, local counters, or latest diagnostic")
		}
		state.LastError.Code = "changed"
		if original.ModelStates["model(low)"].LastError.Code != "newer" {
			t.Fatal("merged error aliases caller-owned state")
		}
	}
}
