package auth

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type retireOnSuccessfulExecutionExecutor struct {
	schedulerProviderTestExecutor
	manager      *Manager
	executeCalls atomic.Int32
	countCalls   atomic.Int32
	updateMu     sync.Mutex
	updateErr    error
}

func (e *retireOnSuccessfulExecutionExecutor) Execute(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	if e.executeCalls.Add(1) == 1 {
		e.rotateAuth(auth)
	}
	return cliproxyexecutor.Response{Payload: []byte("execute-success")}, nil
}

func (e *retireOnSuccessfulExecutionExecutor) CountTokens(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	if e.countCalls.Add(1) == 1 {
		e.rotateAuth(auth)
	}
	return cliproxyexecutor.Response{Payload: []byte("count-success")}, nil
}

func (e *retireOnSuccessfulExecutionExecutor) rotateAuth(auth *Auth) {
	replacement := auth.Clone()
	if replacement.Attributes == nil {
		replacement.Attributes = make(map[string]string)
	}
	replacement.Attributes["api_key"] = "rotated-key"
	_, errUpdate := e.manager.Update(WithSkipPersist(context.Background()), replacement)
	e.updateMu.Lock()
	e.updateErr = errUpdate
	e.updateMu.Unlock()
}

func (e *retireOnSuccessfulExecutionExecutor) UpdateError() error {
	e.updateMu.Lock()
	defer e.updateMu.Unlock()
	return e.updateErr
}

type retirementSuccessSelector struct {
	bindCalls atomic.Int32
}

func (*retirementSuccessSelector) Pick(_ context.Context, _, _ string, _ cliproxyexecutor.Options, auths []*Auth) (*Auth, error) {
	if len(auths) == 0 {
		return nil, fmt.Errorf("no auths")
	}
	return auths[0], nil
}

func (s *retirementSuccessSelector) BindSession(context.Context, string, string, cliproxyexecutor.Options, string) {
	s.bindCalls.Add(1)
}

type retirementSuccessHook struct {
	NoopHook
	results atomic.Int32
}

func (h *retirementSuccessHook) OnResult(context.Context, Result) {
	h.results.Add(1)
}

func TestManagerExecuteReturnsSuccessfulResponseWhenSelectedInstanceRetires(t *testing.T) {
	manager, executor, selector, hook, authID, model := newRetireOnSuccessManager(t, "execute")
	originalInstance := managerAuthInstanceID(t, manager, authID)

	response, errExecute := manager.Execute(t.Context(), []string{"retire-success"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if errExecute != nil {
		t.Fatalf("Execute() error = %v", errExecute)
	}
	if string(response.Payload) != "execute-success" {
		t.Fatalf("Execute() payload = %q", response.Payload)
	}
	if calls := executor.executeCalls.Load(); calls != 1 {
		t.Fatalf("Execute() calls = %d, want 1", calls)
	}
	assertRetiredSuccessDidNotWriteBack(t, manager, executor, selector, hook, authID, originalInstance)
}

func TestManagerExecuteCountReturnsSuccessfulResponseWhenSelectedInstanceRetires(t *testing.T) {
	manager, executor, selector, hook, authID, model := newRetireOnSuccessManager(t, "count")
	originalInstance := managerAuthInstanceID(t, manager, authID)

	response, errCount := manager.ExecuteCount(t.Context(), []string{"retire-success"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if errCount != nil {
		t.Fatalf("ExecuteCount() error = %v", errCount)
	}
	if string(response.Payload) != "count-success" {
		t.Fatalf("ExecuteCount() payload = %q", response.Payload)
	}
	if calls := executor.countCalls.Load(); calls != 1 {
		t.Fatalf("ExecuteCount() calls = %d, want 1", calls)
	}
	assertRetiredSuccessDidNotWriteBack(t, manager, executor, selector, hook, authID, originalInstance)
}

func TestAntigravityCreditsReturnsSuccessfulResponseWhenSelectedInstanceRetires(t *testing.T) {
	const (
		authID = "credits-retire-success"
		model  = "claude-credits-retire-success"
	)
	selector := &retirementSuccessSelector{}
	hook := &retirementSuccessHook{}
	manager := NewManager(nil, selector, hook)
	executor := &retireOnSuccessfulExecutionExecutor{
		schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "antigravity"},
		manager:                       manager,
	}
	manager.RegisterExecutor(executor)
	registerSchedulerModels(t, "antigravity", model, authID)
	if _, errRegister := manager.Register(t.Context(), hashlessConfigTestAuth(authID, "antigravity")); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	originalInstance := managerAuthInstanceID(t, manager, authID)

	response, ok := manager.tryAntigravityCreditsExecute(t.Context(), cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if !ok {
		t.Fatal("tryAntigravityCreditsExecute() did not return the successful response")
	}
	if string(response.Payload) != "execute-success" {
		t.Fatalf("credits payload = %q", response.Payload)
	}
	if calls := executor.executeCalls.Load(); calls != 1 {
		t.Fatalf("credits Execute() calls = %d, want 1", calls)
	}
	assertRetiredSuccessDidNotWriteBack(t, manager, executor, selector, hook, authID, originalInstance)
}

func newRetireOnSuccessManager(t *testing.T, suffix string) (*Manager, *retireOnSuccessfulExecutionExecutor, *retirementSuccessSelector, *retirementSuccessHook, string, string) {
	t.Helper()
	authID := "retire-success-" + suffix
	model := "retire-success-model-" + suffix
	selector := &retirementSuccessSelector{}
	hook := &retirementSuccessHook{}
	manager := NewManager(nil, selector, hook)
	executor := &retireOnSuccessfulExecutionExecutor{
		schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "retire-success"},
		manager:                       manager,
	}
	manager.RegisterExecutor(executor)
	registerSchedulerModels(t, "retire-success", model, authID)
	if _, errRegister := manager.Register(t.Context(), hashlessConfigTestAuth(authID, "retire-success")); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	return manager, executor, selector, hook, authID, model
}

func assertRetiredSuccessDidNotWriteBack(t *testing.T, manager *Manager, executor *retireOnSuccessfulExecutionExecutor, selector *retirementSuccessSelector, hook *retirementSuccessHook, authID, originalInstance string) {
	t.Helper()
	if errUpdate := executor.UpdateError(); errUpdate != nil {
		t.Fatalf("retiring Update() error = %v", errUpdate)
	}
	if currentInstance := managerAuthInstanceID(t, manager, authID); currentInstance == originalInstance {
		t.Fatalf("execution did not retire instance %q", originalInstance)
	}
	if results := hook.results.Load(); results != 0 {
		t.Fatalf("result writebacks = %d, want 0", results)
	}
	if binds := selector.bindCalls.Load(); binds != 0 {
		t.Fatalf("session affinity writes = %d, want 0", binds)
	}
}

func TestManagerUpdateRetainsHashlessConfigRuntimeInstanceWithoutCarryingCooldown(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	auth := hashlessConfigTestAuth("hashless-config-unchanged", "claude")
	if _, errRegister := manager.Register(t.Context(), auth); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	originalInstance := managerAuthInstanceID(t, manager, auth.ID)
	retryAfter := time.Minute
	manager.MarkResult(t.Context(), Result{
		AuthID:     auth.ID,
		Provider:   auth.Provider,
		Model:      "test-model",
		Error:      &Error{HTTPStatus: http.StatusTooManyRequests, Message: "rate limited"},
		RetryAfter: &retryAfter,
	})

	reloaded := hashlessConfigTestAuth(auth.ID, auth.Provider)
	reloaded.CreatedAt = time.Now().Add(time.Hour)
	reloaded.UpdatedAt = time.Now().Add(2 * time.Hour)
	if _, errUpdate := manager.Update(WithSkipPersist(t.Context()), reloaded); errUpdate != nil {
		t.Fatalf("Update() error = %v", errUpdate)
	}
	if currentInstance := managerAuthInstanceID(t, manager, auth.ID); currentInstance != originalInstance {
		t.Fatalf("unchanged config instance changed from %q to %q", originalInstance, currentInstance)
	}
	current, ok := manager.GetByID(auth.ID)
	if !ok || current == nil {
		t.Fatal("updated auth not found")
	}
	if current.LastError != nil || current.Unavailable || !current.NextRetryAfter.IsZero() {
		t.Fatalf("hashless config inherited cooldown state: %#v", current)
	}
}

func TestManagerUpdateRotatesHashlessConfigRuntimeInstanceOnEffectiveChanges(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Auth)
	}{
		{
			name: "api key",
			change: func(auth *Auth) {
				auth.Attributes["api_key"] = "changed-key"
			},
		},
		{
			name: "proxy",
			change: func(auth *Auth) {
				auth.ProxyURL = "http://changed-proxy.example"
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			manager := NewManager(nil, nil, nil)
			auth := hashlessConfigTestAuth("hashless-config-changed-"+testCase.name, "claude")
			if _, errRegister := manager.Register(t.Context(), auth); errRegister != nil {
				t.Fatalf("Register() error = %v", errRegister)
			}
			originalInstance := managerAuthInstanceID(t, manager, auth.ID)
			reloaded := hashlessConfigTestAuth(auth.ID, auth.Provider)
			testCase.change(reloaded)
			if _, errUpdate := manager.Update(WithSkipPersist(t.Context()), reloaded); errUpdate != nil {
				t.Fatalf("Update() error = %v", errUpdate)
			}
			if currentInstance := managerAuthInstanceID(t, manager, auth.ID); currentInstance == originalInstance {
				t.Fatalf("effective config change retained instance %q", currentInstance)
			}
		})
	}
}

func hashlessConfigTestAuth(id, provider string) *Auth {
	return &Auth{
		ID:       id,
		Provider: provider,
		Label:    provider + "-apikey",
		Prefix:   "test-prefix",
		Status:   StatusActive,
		ProxyURL: "http://proxy.example",
		Attributes: map[string]string{
			"source":        "config:" + provider + "[stable]",
			"auth_kind":     "apikey",
			"api_key":       "stable-key",
			"base_url":      "https://upstream.example",
			"header:X-Test": "stable-header",
			"models_hash":   "stable-models",
		},
	}
}

type legacyAuthSessionCloserExecutor struct {
	schedulerProviderTestExecutor
	legacyCalls atomic.Int32
}

func (e *legacyAuthSessionCloserExecutor) CloseAuthExecutionSessions(string, string) {
	e.legacyCalls.Add(1)
}

type dualAuthSessionCloserExecutor struct {
	schedulerProviderTestExecutor
	legacyCalls   atomic.Int32
	instanceCalls atomic.Int32
	instanceID    string
}

func (e *dualAuthSessionCloserExecutor) CloseAuthExecutionSessions(string, string) {
	e.legacyCalls.Add(1)
}

func (e *dualAuthSessionCloserExecutor) CloseAuthInstanceExecutionSessions(_ string, instanceID string, _ string) {
	e.instanceID = instanceID
	e.instanceCalls.Add(1)
}

var _ AuthExecutionSessionCloser = (*legacyAuthSessionCloserExecutor)(nil)
var _ AuthExecutionSessionCloser = (*dualAuthSessionCloserExecutor)(nil)
var _ AuthInstanceExecutionSessionCloser = (*dualAuthSessionCloserExecutor)(nil)

func TestManagerAuthSessionCleanupFallsBackToV6Closer(t *testing.T) {
	executor := &legacyAuthSessionCloserExecutor{schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "legacy-closer"}}
	manager := NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	const authID = "legacy-closer-auth"
	if _, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: "legacy-closer"}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	if _, errUpdate := manager.Update(t.Context(), &Auth{ID: authID, Provider: "replacement"}); errUpdate != nil {
		t.Fatalf("Update() error = %v", errUpdate)
	}
	if calls := executor.legacyCalls.Load(); calls != 1 {
		t.Fatalf("legacy closer calls = %d, want 1", calls)
	}
}

func TestManagerAuthSessionCleanupPrefersInstanceCloser(t *testing.T) {
	executor := &dualAuthSessionCloserExecutor{schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "dual-closer"}}
	manager := NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	const authID = "dual-closer-auth"
	if _, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: "dual-closer"}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	originalInstance := managerAuthInstanceID(t, manager, authID)
	if _, errUpdate := manager.Update(t.Context(), &Auth{ID: authID, Provider: "replacement"}); errUpdate != nil {
		t.Fatalf("Update() error = %v", errUpdate)
	}
	if calls := executor.instanceCalls.Load(); calls != 1 {
		t.Fatalf("instance closer calls = %d, want 1", calls)
	}
	if calls := executor.legacyCalls.Load(); calls != 0 {
		t.Fatalf("legacy closer calls = %d, want 0", calls)
	}
	if executor.instanceID != originalInstance {
		t.Fatalf("closed instance = %q, want %q", executor.instanceID, originalInstance)
	}
}
