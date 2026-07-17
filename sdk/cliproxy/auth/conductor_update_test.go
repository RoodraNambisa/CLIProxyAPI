package auth

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type countingHook struct {
	results int
}

type orderedAuthUpdateHook struct {
	NoopHook
	mu     sync.Mutex
	labels []string
}

func (h *orderedAuthUpdateHook) OnAuthRegistered(_ context.Context, auth *Auth) {
	h.record(auth)
}

func (h *orderedAuthUpdateHook) OnAuthUpdated(_ context.Context, auth *Auth) {
	h.record(auth)
}

func (h *orderedAuthUpdateHook) record(auth *Auth) {
	if h == nil || auth == nil {
		return
	}
	h.mu.Lock()
	h.labels = append(h.labels, auth.Label)
	h.mu.Unlock()
}

func (h *orderedAuthUpdateHook) Labels() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.labels...)
}

type blockingAuthCleanupExecutor struct {
	schedulerProviderTestExecutor
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (e *blockingAuthCleanupExecutor) CloseAuthInstanceExecutionSessions(string, string, string) {
	e.once.Do(func() { close(e.started) })
	<-e.release
}

func (h *countingHook) OnAuthRegistered(context.Context, *Auth) {}

func (h *countingHook) OnAuthUpdated(context.Context, *Auth) {}

func (h *countingHook) OnResult(context.Context, Result) {
	h.results++
}

type reentrantDeleteHook struct {
	mgr           *Manager
	deleteOnEvent string
	triggered     atomic.Bool
	done          chan error
}

func (h *reentrantDeleteHook) OnAuthRegistered(ctx context.Context, auth *Auth) {
	if h == nil || h.deleteOnEvent != "register" || auth == nil {
		return
	}
	h.triggerDelete(ctx, auth.ID)
}

func (h *reentrantDeleteHook) OnAuthUpdated(ctx context.Context, auth *Auth) {
	if h == nil || h.deleteOnEvent != "update" || auth == nil {
		return
	}
	h.triggerDelete(ctx, auth.ID)
}

func (h *reentrantDeleteHook) OnResult(context.Context, Result) {}

func (h *reentrantDeleteHook) triggerDelete(ctx context.Context, id string) {
	if h == nil || h.mgr == nil || !h.triggered.CompareAndSwap(false, true) {
		return
	}
	err := h.mgr.Delete(ctx, id)
	select {
	case h.done <- err:
	default:
	}
}

func TestManager_Update_PreservesModelStates(t *testing.T) {
	m := NewManager(nil, nil, nil)

	model := "test-model"
	backoffLevel := 7

	if _, errRegister := m.Register(context.Background(), &Auth{
		ID:       "auth-1",
		Provider: "claude",
		Attributes: map[string]string{
			SourceHashAttributeKey: "same-hash",
		},
		Metadata: map[string]any{"k": "v"},
		ModelStates: map[string]*ModelState{
			model: {
				Quota: QuotaState{BackoffLevel: backoffLevel},
			},
		},
	}); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	if _, errUpdate := m.Update(context.Background(), &Auth{
		ID:       "auth-1",
		Provider: "claude",
		Attributes: map[string]string{
			SourceHashAttributeKey: "same-hash",
		},
		Metadata: map[string]any{"k": "v2"},
	}); errUpdate != nil {
		t.Fatalf("update auth: %v", errUpdate)
	}

	updated, ok := m.GetByID("auth-1")
	if !ok || updated == nil {
		t.Fatalf("expected auth to be present")
	}
	if len(updated.ModelStates) == 0 {
		t.Fatalf("expected ModelStates to be preserved")
	}
	state := updated.ModelStates[model]
	if state == nil {
		t.Fatalf("expected model state to be present")
	}
	if state.Quota.BackoffLevel != backoffLevel {
		t.Fatalf("expected BackoffLevel to be %d, got %d", backoffLevel, state.Quota.BackoffLevel)
	}
}

func TestManager_Update_WithSkipStateCarryForwardClearsRuntimeState(t *testing.T) {
	m := NewManager(nil, nil, nil)
	retryAt := time.Now().Add(5 * time.Minute).UTC()

	if _, errRegister := m.Register(context.Background(), &Auth{
		ID:          "auth-skip-carry-forward",
		Provider:    "claude",
		Status:      StatusError,
		Unavailable: true,
		Attributes: map[string]string{
			SourceHashAttributeKey: "same-hash",
		},
		Metadata:       map[string]any{"k": "v"},
		LastError:      &Error{HTTPStatus: 429, Message: "quota exhausted"},
		StatusMessage:  "quota exhausted",
		NextRetryAfter: retryAt,
		Quota: QuotaState{
			Exceeded:      true,
			NextRecoverAt: retryAt,
		},
		ModelStates: map[string]*ModelState{
			"test-model": {
				Status:         StatusError,
				StatusMessage:  "quota exhausted",
				Unavailable:    true,
				LastError:      &Error{HTTPStatus: 429, Message: "quota exhausted"},
				NextRetryAfter: retryAt,
				Quota: QuotaState{
					Exceeded:      true,
					NextRecoverAt: retryAt,
				},
			},
		},
	}); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	if _, errUpdate := m.Update(WithSkipStateCarryForward(context.Background()), &Auth{
		ID:       "auth-skip-carry-forward",
		Provider: "claude",
		Status:   StatusActive,
		Attributes: map[string]string{
			SourceHashAttributeKey: "same-hash",
		},
		Metadata: map[string]any{"k": "v2"},
		ModelStates: map[string]*ModelState{
			"test-model": {
				Status: StatusActive,
			},
		},
	}); errUpdate != nil {
		t.Fatalf("update auth: %v", errUpdate)
	}

	updated, ok := m.GetByID("auth-skip-carry-forward")
	if !ok || updated == nil {
		t.Fatal("expected auth to be present")
	}
	if updated.Unavailable {
		t.Fatal("expected unavailable flag to be cleared")
	}
	if updated.Status != StatusActive {
		t.Fatalf("status = %q, want %q", updated.Status, StatusActive)
	}
	if updated.LastError != nil {
		t.Fatalf("last error = %#v, want nil", updated.LastError)
	}
	if updated.StatusMessage != "" {
		t.Fatalf("status message = %q, want empty", updated.StatusMessage)
	}
	if !updated.NextRetryAfter.IsZero() {
		t.Fatalf("next retry after = %v, want zero", updated.NextRetryAfter)
	}
	if updated.Quota.Exceeded {
		t.Fatalf("quota = %#v, want zero state", updated.Quota)
	}
	state := updated.ModelStates["test-model"]
	if state == nil {
		t.Fatal("expected model state to remain present")
	}
	if state.Status != StatusActive {
		t.Fatalf("model status = %q, want %q", state.Status, StatusActive)
	}
	if state.StatusMessage != "" {
		t.Fatalf("model status message = %q, want empty", state.StatusMessage)
	}
	if state.Unavailable {
		t.Fatal("expected model unavailable flag to be cleared")
	}
	if state.LastError != nil {
		t.Fatalf("model last error = %#v, want nil", state.LastError)
	}
	if !state.NextRetryAfter.IsZero() {
		t.Fatalf("model next retry after = %v, want zero", state.NextRetryAfter)
	}
	if state.Quota.Exceeded {
		t.Fatalf("model quota = %#v, want zero state", state.Quota)
	}
}

func TestManager_Update_WithForceRuntimeReplacementRetiresOldInstance(t *testing.T) {
	m := NewManager(nil, nil, nil)
	registered, err := m.Register(context.Background(), &Auth{
		ID:       "auth-force-runtime-replacement",
		Provider: "chatgpt-web",
		Status:   StatusActive,
		Attributes: map[string]string{
			SourceHashAttributeKey: "same-hash",
		},
		Metadata: map[string]any{"account_id": "account-a"},
	})
	if err != nil {
		t.Fatalf("register auth: %v", err)
	}

	updated, err := m.Update(WithForceRuntimeReplacement(context.Background()), &Auth{
		ID:       registered.ID,
		Provider: registered.Provider,
		Status:   StatusActive,
		Attributes: map[string]string{
			SourceHashAttributeKey: "same-hash",
		},
		Metadata: map[string]any{"account_id": "account-b"},
	})
	if err != nil {
		t.Fatalf("update auth: %v", err)
	}
	if updated.RuntimeInstanceID() == registered.RuntimeInstanceID() {
		t.Fatal("forced replacement reused the old runtime instance")
	}
}

func TestManager_Update_DisabledExistingDoesNotInheritModelStates(t *testing.T) {
	m := NewManager(nil, nil, nil)

	// Register a disabled auth with existing ModelStates.
	if _, err := m.Register(context.Background(), &Auth{
		ID:       "auth-disabled",
		Provider: "claude",
		Disabled: true,
		Status:   StatusDisabled,
		ModelStates: map[string]*ModelState{
			"stale-model": {
				Quota: QuotaState{BackoffLevel: 5},
			},
		},
	}); err != nil {
		t.Fatalf("register auth: %v", err)
	}

	// Update with empty ModelStates — should NOT inherit stale states.
	if _, err := m.Update(context.Background(), &Auth{
		ID:       "auth-disabled",
		Provider: "claude",
		Disabled: true,
		Status:   StatusDisabled,
	}); err != nil {
		t.Fatalf("update auth: %v", err)
	}

	updated, ok := m.GetByID("auth-disabled")
	if !ok || updated == nil {
		t.Fatalf("expected auth to be present")
	}
	if len(updated.ModelStates) != 0 {
		t.Fatalf("expected disabled auth NOT to inherit ModelStates, got %d entries", len(updated.ModelStates))
	}
}

func TestManager_Update_ActiveToDisabledDoesNotInheritModelStates(t *testing.T) {
	m := NewManager(nil, nil, nil)

	// Register an active auth with ModelStates (simulates existing live auth).
	if _, err := m.Register(context.Background(), &Auth{
		ID:       "auth-a2d",
		Provider: "claude",
		Status:   StatusActive,
		ModelStates: map[string]*ModelState{
			"stale-model": {
				Quota: QuotaState{BackoffLevel: 9},
			},
		},
	}); err != nil {
		t.Fatalf("register auth: %v", err)
	}

	// File watcher deletes config → synthesizes Disabled=true auth → Update.
	// Even though existing is active, incoming auth is disabled → skip inheritance.
	if _, err := m.Update(context.Background(), &Auth{
		ID:       "auth-a2d",
		Provider: "claude",
		Disabled: true,
		Status:   StatusDisabled,
	}); err != nil {
		t.Fatalf("update auth: %v", err)
	}

	updated, ok := m.GetByID("auth-a2d")
	if !ok || updated == nil {
		t.Fatalf("expected auth to be present")
	}
	if len(updated.ModelStates) != 0 {
		t.Fatalf("expected active→disabled transition NOT to inherit ModelStates, got %d entries", len(updated.ModelStates))
	}
}

func TestManager_Update_DisabledToActiveDoesNotInheritStaleModelStates(t *testing.T) {
	m := NewManager(nil, nil, nil)

	// Register a disabled auth with stale ModelStates.
	if _, err := m.Register(context.Background(), &Auth{
		ID:       "auth-d2a",
		Provider: "claude",
		Disabled: true,
		Status:   StatusDisabled,
		ModelStates: map[string]*ModelState{
			"stale-model": {
				Quota: QuotaState{BackoffLevel: 4},
			},
		},
	}); err != nil {
		t.Fatalf("register auth: %v", err)
	}

	// Re-enable: incoming auth is active, existing is disabled → skip inheritance.
	if _, err := m.Update(context.Background(), &Auth{
		ID:       "auth-d2a",
		Provider: "claude",
		Status:   StatusActive,
	}); err != nil {
		t.Fatalf("update auth: %v", err)
	}

	updated, ok := m.GetByID("auth-d2a")
	if !ok || updated == nil {
		t.Fatalf("expected auth to be present")
	}
	if len(updated.ModelStates) != 0 {
		t.Fatalf("expected disabled→active transition NOT to inherit stale ModelStates, got %d entries", len(updated.ModelStates))
	}
}

func TestManager_Update_ActiveInheritsModelStates(t *testing.T) {
	m := NewManager(nil, nil, nil)

	model := "active-model"
	backoffLevel := 3

	// Register an active auth with ModelStates.
	if _, err := m.Register(context.Background(), &Auth{
		ID:       "auth-active",
		Provider: "claude",
		Status:   StatusActive,
		Attributes: map[string]string{
			SourceHashAttributeKey: "same-hash",
		},
		ModelStates: map[string]*ModelState{
			model: {
				Quota: QuotaState{BackoffLevel: backoffLevel},
			},
		},
	}); err != nil {
		t.Fatalf("register auth: %v", err)
	}

	// Update with empty ModelStates — both sides active → SHOULD inherit.
	if _, err := m.Update(context.Background(), &Auth{
		ID:       "auth-active",
		Provider: "claude",
		Status:   StatusActive,
		Attributes: map[string]string{
			SourceHashAttributeKey: "same-hash",
		},
	}); err != nil {
		t.Fatalf("update auth: %v", err)
	}

	updated, ok := m.GetByID("auth-active")
	if !ok || updated == nil {
		t.Fatalf("expected auth to be present")
	}
	if len(updated.ModelStates) == 0 {
		t.Fatalf("expected active auth to inherit ModelStates")
	}
	state := updated.ModelStates[model]
	if state == nil {
		t.Fatalf("expected model state to be present")
	}
	if state.Quota.BackoffLevel != backoffLevel {
		t.Fatalf("expected BackoffLevel to be %d, got %d", backoffLevel, state.Quota.BackoffLevel)
	}
}

func TestManager_MarkResultTracksQuotaStrikeCountAndClearsOnSuccess(t *testing.T) {
	m := NewManager(nil, nil, nil)

	if _, err := m.Register(context.Background(), &Auth{
		ID:       "auth-429",
		Provider: "claude",
		Status:   StatusActive,
		Metadata: map[string]any{"k": "v"},
	}); err != nil {
		t.Fatalf("register auth: %v", err)
	}

	m.MarkResult(context.Background(), Result{
		AuthID:  "auth-429",
		Success: false,
		Error:   &Error{HTTPStatus: 429, Message: "quota exhausted"},
	})

	auth, ok := m.GetByID("auth-429")
	if !ok || auth == nil {
		t.Fatalf("expected auth to exist")
	}
	if auth.Quota.StrikeCount != 1 {
		t.Fatalf("StrikeCount = %d, want 1", auth.Quota.StrikeCount)
	}
	if !auth.Quota.Exceeded {
		t.Fatal("expected quota exceeded after 429")
	}

	m.MarkResult(context.Background(), Result{
		AuthID:  "auth-429",
		Success: true,
	})

	auth, ok = m.GetByID("auth-429")
	if !ok || auth == nil {
		t.Fatalf("expected auth to exist after success")
	}
	if auth.Quota.StrikeCount != 0 {
		t.Fatalf("StrikeCount after success = %d, want 0", auth.Quota.StrikeCount)
	}
	if auth.Quota.Exceeded {
		t.Fatal("expected quota exceeded to clear after success")
	}
}

func TestManager_AddHookKeepsExistingHook(t *testing.T) {
	m := NewManager(nil, nil, nil)
	first := &countingHook{}
	second := &countingHook{}

	m.SetHook(first)
	m.AddHook(second)

	if _, err := m.Register(context.Background(), &Auth{
		ID:       "auth-hook",
		Provider: "claude",
		Status:   StatusActive,
		Metadata: map[string]any{"k": "v"},
	}); err != nil {
		t.Fatalf("register auth: %v", err)
	}

	m.MarkResult(context.Background(), Result{
		AuthID:  "auth-hook",
		Success: true,
	})

	if first.results != 1 || second.results != 1 {
		t.Fatalf("hook counts = (%d, %d), want (1, 1)", first.results, second.results)
	}
}

func TestManager_Register_ReentrantHookDoesNotDeadlock(t *testing.T) {
	m := NewManager(nil, nil, nil)
	hook := &reentrantDeleteHook{
		mgr:           m,
		deleteOnEvent: "register",
		done:          make(chan error, 1),
	}
	m.SetHook(hook)

	registerDone := make(chan error, 1)
	go func() {
		_, err := m.Register(context.Background(), &Auth{
			ID:       "auth-register-hook",
			Provider: "claude",
			Metadata: map[string]any{"type": "claude"},
		})
		registerDone <- err
	}()

	select {
	case err := <-registerDone:
		if err != nil {
			t.Fatalf("Register returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Register deadlocked while hook re-entered Delete")
	}

	select {
	case err := <-hook.done:
		if err != nil {
			t.Fatalf("hook Delete returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("hook Delete did not complete")
	}
}

func TestManager_Update_ReentrantHookDoesNotDeadlock(t *testing.T) {
	m := NewManager(nil, nil, nil)
	if _, err := m.Register(context.Background(), &Auth{
		ID:       "auth-update-hook",
		Provider: "claude",
		Metadata: map[string]any{"type": "claude"},
	}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	hook := &reentrantDeleteHook{
		mgr:           m,
		deleteOnEvent: "update",
		done:          make(chan error, 1),
	}
	m.SetHook(hook)

	updateDone := make(chan error, 1)
	go func() {
		_, err := m.Update(context.Background(), &Auth{
			ID:       "auth-update-hook",
			Provider: "claude",
			Metadata: map[string]any{"type": "claude", "updated": true},
		})
		updateDone <- err
	}()

	select {
	case err := <-updateDone:
		if err != nil {
			t.Fatalf("Update returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Update deadlocked while hook re-entered Delete")
	}

	select {
	case err := <-hook.done:
		if err != nil {
			t.Fatalf("hook Delete returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("hook Delete did not complete")
	}
}

func TestManagerReplacementSkipsStaleLifecycleNotification(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		replace func(*Manager, context.Context, *Auth) (*Auth, error)
	}{
		{name: "register", replace: (*Manager).Register},
		{name: "update", replace: (*Manager).Update},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			const authID = "stale-hook-auth"
			manager := NewManager(nil, nil, nil)
			blocker := &blockingAuthCleanupExecutor{
				schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "initial"},
				started:                       make(chan struct{}),
				release:                       make(chan struct{}),
			}
			manager.RegisterExecutor(blocker)
			manager.RegisterExecutor(schedulerProviderTestExecutor{provider: "middle"})
			manager.RegisterExecutor(schedulerProviderTestExecutor{provider: "latest"})
			if _, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: "initial", Label: "initial"}); errRegister != nil {
				t.Fatalf("Register() error = %v", errRegister)
			}

			hook := &orderedAuthUpdateHook{}
			manager.SetHook(hook)
			middleDone := make(chan error, 1)
			go func() {
				_, errReplace := testCase.replace(manager, t.Context(), &Auth{ID: authID, Provider: "middle", Label: "middle"})
				middleDone <- errReplace
			}()
			select {
			case <-blocker.started:
			case <-time.After(5 * time.Second):
				t.Fatal("middle replacement did not enter cleanup")
			}

			if _, errReplace := testCase.replace(manager, t.Context(), &Auth{ID: authID, Provider: "latest", Label: "latest"}); errReplace != nil {
				t.Fatalf("latest replacement error = %v", errReplace)
			}
			close(blocker.release)
			select {
			case errReplace := <-middleDone:
				if errReplace != nil {
					t.Fatalf("middle replacement error = %v", errReplace)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("middle replacement remained blocked")
			}

			labels := hook.Labels()
			if len(labels) != 1 || labels[0] != "latest" {
				t.Fatalf("update hook labels = %v, want [latest]", labels)
			}
		})
	}
}

func TestManagerReplacementNotificationSurvivesRuntimeMetadataMutation(t *testing.T) {
	const authID = "metadata-during-cleanup-hook-auth"
	manager := NewManager(nil, nil, nil)
	blocker := &blockingAuthCleanupExecutor{
		schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "initial"},
		started:                       make(chan struct{}),
		release:                       make(chan struct{}),
	}
	manager.RegisterExecutor(blocker)
	manager.RegisterExecutor(schedulerProviderTestExecutor{provider: "middle"})
	if _, err := manager.Register(t.Context(), &Auth{
		ID:       authID,
		Provider: "initial",
		Label:    "initial",
		Metadata: map[string]any{"session_id": "initial"},
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	hook := &orderedAuthUpdateHook{}
	manager.SetHook(hook)
	updateDone := make(chan error, 1)
	go func() {
		_, err := manager.Update(t.Context(), &Auth{
			ID:       authID,
			Provider: "middle",
			Label:    "middle",
			Metadata: map[string]any{"session_id": "before"},
		})
		updateDone <- err
	}()
	select {
	case <-blocker.started:
	case <-time.After(5 * time.Second):
		t.Fatal("replacement did not enter cleanup")
	}

	current, ok := manager.GetByID(authID)
	if !ok {
		t.Fatal("replacement missing during cleanup")
	}
	if _, stillCurrent, err := manager.UpdateRuntimeMetadataIfCurrent(t.Context(), current, map[string]any{
		"session_id": "after",
	}); err != nil || !stillCurrent {
		t.Fatalf("UpdateRuntimeMetadataIfCurrent() = current %v, err %v", stillCurrent, err)
	}

	close(blocker.release)
	select {
	case err := <-updateDone:
		if err != nil {
			t.Fatalf("Update() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("replacement remained blocked")
	}

	labels := hook.Labels()
	if len(labels) != 1 || labels[0] != "middle" {
		t.Fatalf("update hook labels = %v, want [middle]", labels)
	}
}
