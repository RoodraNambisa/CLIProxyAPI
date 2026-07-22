package auth

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type codexAgentIdentityRecoveryExecutor struct {
	recoveryCalls atomic.Int32
	prepareCalls  atomic.Int32
	closeCalls    atomic.Int32
	started       chan struct{}
	gate          chan struct{}
	startOnce     sync.Once
	closeMu       sync.Mutex
	closeAuthID   string
	closeReason   string
}

func (*codexAgentIdentityRecoveryExecutor) Identifier() string { return "codex" }

func (*codexAgentIdentityRecoveryExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, errors.New("not implemented")
}

func (*codexAgentIdentityRecoveryExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, errors.New("not implemented")
}

func (*codexAgentIdentityRecoveryExecutor) Refresh(context.Context, *Auth) (*Auth, error) {
	return nil, errors.New("not implemented")
}

func (*codexAgentIdentityRecoveryExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, errors.New("not implemented")
}

func (*codexAgentIdentityRecoveryExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (*codexAgentIdentityRecoveryExecutor) ShouldRecoverUnauthorized(auth *Auth, err error) bool {
	status, ok := err.(interface{ StatusCode() int })
	return auth != nil && status != nil && ok && status.StatusCode() == http.StatusUnauthorized && auth.Metadata["task_id"] == "expired-task"
}

func (*codexAgentIdentityRecoveryExecutor) ShouldPrepareRequestAuth(auth *Auth) bool {
	return auth != nil && auth.Metadata != nil && auth.Metadata["task_id"] == ""
}

func (executor *codexAgentIdentityRecoveryExecutor) PrepareRequestAuth(_ context.Context, auth *Auth) (*Auth, error) {
	executor.prepareCalls.Add(1)
	updated := auth.Clone()
	updated.Metadata["task_id"] = "prepared-task"
	return updated, nil
}

func (executor *codexAgentIdentityRecoveryExecutor) RecoverUnauthorized(ctx context.Context, auth *Auth) (*Auth, error) {
	executor.recoveryCalls.Add(1)
	executor.startOnce.Do(func() { close(executor.started) })
	select {
	case <-executor.gate:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	updated := auth.Clone()
	updated.Metadata["task_id"] = "replacement-task"
	return updated, nil
}

func (executor *codexAgentIdentityRecoveryExecutor) CloseAuthExecutionSessions(authID, reason string) {
	executor.closeCalls.Add(1)
	executor.closeMu.Lock()
	executor.closeAuthID = authID
	executor.closeReason = reason
	executor.closeMu.Unlock()
}

func TestCodexAgentIdentityUnauthorizedRecoveryIsSingleflightAndPreservesRuntimeState(t *testing.T) {
	store := &requestPrepareStore{}
	executor := &codexAgentIdentityRecoveryExecutor{started: make(chan struct{}), gate: make(chan struct{})}
	manager := NewManager(store, nil, nil)
	manager.RegisterExecutor(executor)
	nextRetry := time.Now().Add(10 * time.Minute).Round(time.Second)
	auth := &Auth{
		ID:             "codex-agent",
		Provider:       "codex",
		Status:         StatusError,
		Unavailable:    true,
		NextRetryAfter: nextRetry,
		Quota: QuotaState{
			Exceeded:      true,
			Reason:        "quota",
			NextRecoverAt: nextRetry,
		},
		Metadata: map[string]any{
			"auth_mode":         "agentIdentity",
			"agent_runtime_id":  "runtime-id",
			"agent_private_key": "private-key",
			"task_id":           "expired-task",
		},
		ModelStates: map[string]*ModelState{
			"gpt-5.4": {
				Status:         StatusError,
				Unavailable:    true,
				NextRetryAfter: nextRetry,
				Quota:          QuotaState{Exceeded: true, Reason: "model quota", NextRecoverAt: nextRetry},
			},
		},
	}
	if _, errRegister := manager.Register(t.Context(), auth); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	current, ok := manager.GetByID(auth.ID)
	if !ok {
		t.Fatal("registered auth not found")
	}
	baselineSaves := store.saveCount.Load()

	const callers = 16
	start := make(chan struct{})
	ready := make(chan struct{}, callers)
	errorsCh := make(chan error, callers)
	var callersWG sync.WaitGroup
	callersWG.Add(callers)
	for range callers {
		go func() {
			defer callersWG.Done()
			ready <- struct{}{}
			<-start
			recovered, attempted, errRecover := manager.tryRefreshAfterUnauthorized(t.Context(), executor, current.Clone(), &Error{HTTPStatus: http.StatusUnauthorized, Code: "task_expired", Message: "task_expired"}, false)
			if errRecover != nil {
				errorsCh <- errRecover
				return
			}
			if !attempted || recovered == nil || recovered.Metadata["task_id"] != "replacement-task" {
				errorsCh <- errors.New("recovery did not return replacement task")
			}
		}()
	}
	for range callers {
		<-ready
	}
	close(start)
	select {
	case <-executor.started:
	case <-time.After(2 * time.Second):
		t.Fatal("recovery did not start")
	}
	time.Sleep(20 * time.Millisecond)
	close(executor.gate)
	callersWG.Wait()
	close(errorsCh)
	for errRecover := range errorsCh {
		t.Errorf("concurrent recovery: %v", errRecover)
	}

	if got := executor.recoveryCalls.Load(); got != 1 {
		t.Fatalf("RecoverUnauthorized() calls = %d, want 1", got)
	}
	if got := store.saveCount.Load() - baselineSaves; got != 1 {
		t.Fatalf("recovery persistence saves = %d, want 1", got)
	}
	if got := executor.closeCalls.Load(); got != 1 {
		t.Fatalf("CloseAuthExecutionSessions() calls = %d, want 1", got)
	}
	lateRecovered, attempted, errLate := manager.tryRefreshAfterUnauthorized(t.Context(), executor, current.Clone(), &Error{HTTPStatus: http.StatusUnauthorized, Code: "task_expired", Message: "task_expired"}, false)
	if errLate != nil {
		t.Fatalf("late stale recovery error = %v", errLate)
	}
	if !attempted || lateRecovered == nil || lateRecovered.Metadata["task_id"] != "replacement-task" {
		t.Fatalf("late stale recovery = %#v, attempted = %v", lateRecovered, attempted)
	}
	if got := executor.recoveryCalls.Load(); got != 1 {
		t.Fatalf("RecoverUnauthorized() calls after stale recovery = %d, want 1", got)
	}
	if got := store.saveCount.Load() - baselineSaves; got != 1 {
		t.Fatalf("recovery persistence saves after stale recovery = %d, want 1", got)
	}
	if got := executor.closeCalls.Load(); got != 1 {
		t.Fatalf("CloseAuthExecutionSessions() calls after stale recovery = %d, want 1", got)
	}
	executor.closeMu.Lock()
	if executor.closeAuthID != auth.ID || executor.closeReason != "agent_task_rotated" {
		t.Fatalf("closed session = (%q, %q), want (%q, agent_task_rotated)", executor.closeAuthID, executor.closeReason, auth.ID)
	}
	executor.closeMu.Unlock()

	installed, ok := manager.GetByID(auth.ID)
	if !ok || installed.Metadata["task_id"] != "replacement-task" {
		t.Fatalf("installed auth = %#v, want replacement task", installed)
	}
	if !installed.Unavailable || !installed.NextRetryAfter.Equal(nextRetry) || !installed.Quota.Exceeded {
		t.Fatalf("auth runtime state was cleared: %#v", installed)
	}
	modelState := installed.ModelStates["gpt-5.4"]
	if modelState == nil || !modelState.Unavailable || !modelState.NextRetryAfter.Equal(nextRetry) || !modelState.Quota.Exceeded {
		t.Fatalf("model runtime state was cleared: %#v", modelState)
	}
}

func TestCodexAgentIdentityMissingTaskPreparationReusesAdvancedTask(t *testing.T) {
	store := &requestPrepareStore{}
	executor := &codexAgentIdentityRecoveryExecutor{}
	manager := NewManager(store, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &Auth{
		ID:       "codex-agent-missing-task",
		Provider: "codex",
		Metadata: map[string]any{
			"auth_mode":         "agentIdentity",
			"agent_runtime_id":  "runtime-id",
			"agent_private_key": "private-key",
			"task_id":           "",
		},
	}
	if _, errRegister := manager.Register(t.Context(), auth); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	stale, ok := manager.GetByID(auth.ID)
	if !ok {
		t.Fatal("registered auth not found")
	}
	baselineSaves := store.saveCount.Load()

	prepared, errPrepare := manager.prepareRequestAuth(t.Context(), executor, stale.Clone())
	if errPrepare != nil {
		t.Fatalf("first prepareRequestAuth() error = %v", errPrepare)
	}
	if prepared == nil || prepared.Metadata["task_id"] != "prepared-task" {
		t.Fatalf("first prepared auth = %#v", prepared)
	}

	reused, errReuse := manager.prepareRequestAuth(t.Context(), executor, stale.Clone())
	if errReuse != nil {
		t.Fatalf("stale prepareRequestAuth() error = %v", errReuse)
	}
	if reused == nil || reused.Metadata["task_id"] != "prepared-task" {
		t.Fatalf("stale prepared auth = %#v", reused)
	}
	if got := executor.prepareCalls.Load(); got != 1 {
		t.Fatalf("PrepareRequestAuth() calls = %d, want 1", got)
	}
	if got := store.saveCount.Load() - baselineSaves; got != 1 {
		t.Fatalf("preparation persistence saves = %d, want 1", got)
	}
}

func TestAgentIdentityTaskAlreadyAdvancedRequiresSigningMaterial(t *testing.T) {
	current := &Auth{
		ID:       "codex-agent",
		Provider: "codex",
		Metadata: map[string]any{"auth_mode": "agentIdentity", "task_id": "new-task"},
	}
	expected := &Auth{
		ID:       current.ID,
		Provider: current.Provider,
		Metadata: map[string]any{"auth_mode": "agentIdentity", "task_id": "old-task"},
	}
	if agentIdentityTaskAlreadyAdvanced(current, expected) {
		t.Fatal("task advancement accepted credentials without signing material")
	}
}
