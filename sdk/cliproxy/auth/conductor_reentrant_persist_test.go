package auth

import (
	"context"
	"testing"
	"time"
)

type reconcileModelStatesHook struct {
	manager *Manager
}

func (reconcileModelStatesHook) OnAuthRegistered(context.Context, *Auth) {}

func (hook reconcileModelStatesHook) OnAuthUpdated(ctx context.Context, auth *Auth) {
	if hook.manager != nil && auth != nil {
		hook.manager.PruneRegistryModelStates(ctx, auth.ID)
	}
}

func (reconcileModelStatesHook) OnResult(context.Context, Result) {}

type reentrantPersistUpdateResult struct {
	auth    *Auth
	current bool
	err     error
}

func TestUpdateIfCurrentSourceHashReusesOuterMutationLockDuringModelStateReconciliation(t *testing.T) {
	const authID = "reentrant-model-state.json"

	store := newChatGPTWebDependencyTestStore()
	hook := &reconcileModelStatesHook{}
	manager := NewManager(store, nil, hook)
	hook.manager = manager

	registered, errRegister := manager.Register(t.Context(), &Auth{
		ID:       authID,
		FileName: authID,
		Provider: "codex",
		Status:   StatusActive,
		Attributes: map[string]string{
			"priority": "9",
		},
		Metadata: map[string]any{
			"type":     "codex",
			"priority": 9,
		},
		ModelStates: map[string]*ModelState{
			"removed-model": {
				Unavailable:    true,
				NextRetryAfter: time.Now().Add(time.Hour),
			},
		},
	})
	if errRegister != nil {
		t.Fatalf("Register() error: %v", errRegister)
	}

	lockedCtx, unlockOuter, errLock := manager.LockAuthMutation(t.Context(), registered)
	if errLock != nil {
		t.Fatalf("LockAuthMutation() error: %v", errLock)
	}
	outerLocked := true
	defer func() {
		if outerLocked {
			unlockOuter()
		}
	}()

	candidate := registered.Clone()
	candidate.Attributes["priority"] = "0"
	candidate.Metadata["priority"] = 0

	updateDone := make(chan reentrantPersistUpdateResult, 1)
	go func() {
		updated, current, errUpdate := manager.UpdateIfCurrentSourceHash(lockedCtx, registered, candidate)
		updateDone <- reentrantPersistUpdateResult{auth: updated, current: current, err: errUpdate}
	}()

	var result reentrantPersistUpdateResult
	select {
	case result = <-updateDone:
		unlockOuter()
		outerLocked = false
	case <-time.After(500 * time.Millisecond):
		unlockOuter()
		outerLocked = false
		select {
		case <-updateDone:
		case <-time.After(time.Second):
		}
		t.Fatal("UpdateIfCurrentSourceHash() deadlocked while reconciling model state")
	}

	if result.err != nil {
		t.Fatalf("UpdateIfCurrentSourceHash() error: %v", result.err)
	}
	if !result.current || result.auth == nil {
		t.Fatalf("UpdateIfCurrentSourceHash() result = (%#v, %v), want current auth", result.auth, result.current)
	}

	current, ok := manager.GetByID(authID)
	if !ok || current == nil {
		t.Fatal("updated auth is missing from manager")
	}
	if got := current.Attributes["priority"]; got != "0" {
		t.Fatalf("priority attribute = %q, want 0", got)
	}
	if len(current.ModelStates) != 0 {
		t.Fatalf("runtime model states = %#v, want none", current.ModelStates)
	}

	persisted, errList := store.List(t.Context())
	if errList != nil {
		t.Fatalf("List() error: %v", errList)
	}
	if len(persisted) != 1 {
		t.Fatalf("persisted auth count = %d, want 1", len(persisted))
	}
	if got := persisted[0].Attributes["priority"]; got != "0" {
		t.Fatalf("persisted priority attribute = %q, want 0", got)
	}
	if len(persisted[0].ModelStates) != 0 {
		t.Fatalf("persisted model states = %#v, want none", persisted[0].ModelStates)
	}

	nextCtx, cancelNext := context.WithTimeout(t.Context(), time.Second)
	defer cancelNext()
	_, unlockNext, errNext := manager.LockAuthMutation(nextCtx, current)
	if errNext != nil {
		t.Fatalf("subsequent LockAuthMutation() error: %v", errNext)
	}
	unlockNext()
}
