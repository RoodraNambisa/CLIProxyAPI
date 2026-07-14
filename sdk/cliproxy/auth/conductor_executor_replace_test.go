package auth

import (
	"context"
	"net/http"
	"sync"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type replaceAwareExecutor struct {
	id string

	mu                     sync.Mutex
	executeCalls           int
	closedSessionIDs       []string
	closedAuthInstanceCall []authInstanceCloseCall
}

type nonComparableReplaceAwareExecutor struct {
	*replaceAwareExecutor
	marker []byte
}

type authInstanceCloseCall struct {
	authID     string
	instanceID string
	reason     string
}

func (e *replaceAwareExecutor) Identifier() string {
	return e.id
}

func (e *replaceAwareExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.mu.Lock()
	e.executeCalls++
	e.mu.Unlock()
	return cliproxyexecutor.Response{}, nil
}

func (e *replaceAwareExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	ch := make(chan cliproxyexecutor.StreamChunk)
	close(ch)
	return &cliproxyexecutor.StreamResult{Chunks: ch}, nil
}

func (e *replaceAwareExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	return auth, nil
}

func (e *replaceAwareExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}

func (e *replaceAwareExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

func (e *replaceAwareExecutor) CloseExecutionSession(sessionID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closedSessionIDs = append(e.closedSessionIDs, sessionID)
}

func (e *replaceAwareExecutor) CloseAuthInstanceExecutionSessions(authID string, instanceID string, reason string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closedAuthInstanceCall = append(e.closedAuthInstanceCall, authInstanceCloseCall{
		authID:     authID,
		instanceID: instanceID,
		reason:     reason,
	})
}

func (e *replaceAwareExecutor) ClosedSessionIDs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]string, len(e.closedSessionIDs))
	copy(out, e.closedSessionIDs)
	return out
}

func (e *replaceAwareExecutor) ExecuteCalls() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.executeCalls
}

func (e *replaceAwareExecutor) ClosedAuthInstanceCalls() []authInstanceCloseCall {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]authInstanceCloseCall, len(e.closedAuthInstanceCall))
	copy(out, e.closedAuthInstanceCall)
	return out
}

func TestManagerRegisterExecutorClosesReplacedExecutionSessions(t *testing.T) {
	t.Parallel()

	manager := NewManager(nil, nil, nil)
	replaced := &replaceAwareExecutor{id: "codex"}
	current := &replaceAwareExecutor{id: "codex"}

	manager.RegisterExecutor(replaced)
	manager.RegisterExecutor(current)

	closed := replaced.ClosedSessionIDs()
	if len(closed) != 1 {
		t.Fatalf("expected replaced executor close calls = 1, got %d", len(closed))
	}
	if closed[0] != CloseAllExecutionSessionsID {
		t.Fatalf("expected close marker %q, got %q", CloseAllExecutionSessionsID, closed[0])
	}
	if len(current.ClosedSessionIDs()) != 0 {
		t.Fatalf("expected current executor to stay open")
	}
}

func TestManagerExecutorReturnsRegisteredExecutor(t *testing.T) {
	t.Parallel()

	manager := NewManager(nil, nil, nil)
	current := &replaceAwareExecutor{id: "codex"}
	manager.RegisterExecutor(current)

	resolved, okResolved := manager.Executor("CODEX")
	if !okResolved {
		t.Fatal("expected registered executor to be found")
	}
	resolvedExecutor, okResolvedExecutor := resolved.(*replaceAwareExecutor)
	if !okResolvedExecutor {
		t.Fatalf("expected resolved executor type %T, got %T", current, resolved)
	}
	if resolvedExecutor != current {
		t.Fatal("expected resolved executor to match registered executor")
	}

	_, okMissing := manager.Executor("unknown")
	if okMissing {
		t.Fatal("expected unknown provider lookup to fail")
	}
}

func TestManagerAuthInstanceCleanupUsesExecutorOwnerAfterReplacement(t *testing.T) {
	tests := []struct {
		name              string
		registerAuthFirst bool
	}{
		{name: "executor before auth"},
		{name: "auth before executor", registerAuthFirst: true},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			manager := NewManager(nil, nil, nil)
			ownerState := &replaceAwareExecutor{id: "codex"}
			replacementState := &replaceAwareExecutor{id: "codex"}
			var owner ProviderExecutor = nonComparableReplaceAwareExecutor{replaceAwareExecutor: ownerState, marker: []byte("owner")}
			var replacement ProviderExecutor = nonComparableReplaceAwareExecutor{replaceAwareExecutor: replacementState, marker: []byte("replacement")}
			auth := &Auth{
				ID:       "owner-test",
				Provider: "codex",
				Status:   StatusActive,
				Attributes: map[string]string{
					"api_key": "test-key",
					"source":  "config:test",
				},
			}
			registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "test-model"}})
			t.Cleanup(func() {
				registry.GetGlobalRegistry().UnregisterClient(auth.ID)
			})

			var registered *Auth
			var errRegister error
			if testCase.registerAuthFirst {
				registered, errRegister = manager.Register(t.Context(), auth)
				manager.RegisterExecutor(owner)
			} else {
				manager.RegisterExecutor(owner)
				registered, errRegister = manager.Register(t.Context(), auth)
			}
			if errRegister != nil {
				t.Fatalf("register auth: %v", errRegister)
			}
			if _, errExecute := manager.Execute(t.Context(), []string{"codex"}, cliproxyexecutor.Request{Model: "test-model"}, cliproxyexecutor.Options{}); errExecute != nil {
				t.Fatalf("execute with owner: %v", errExecute)
			}

			manager.RegisterExecutor(replacement)
			if _, errExecute := manager.Execute(t.Context(), []string{"codex"}, cliproxyexecutor.Request{Model: "test-model"}, cliproxyexecutor.Options{}); errExecute != nil {
				t.Fatalf("execute with replacement: %v", errExecute)
			}
			if _, errUpdate := manager.Update(t.Context(), &Auth{ID: auth.ID, Provider: auth.Provider, Label: "replacement"}); errUpdate != nil {
				t.Fatalf("replace auth: %v", errUpdate)
			}

			if ownerState.ExecuteCalls() != 1 {
				t.Fatalf("owner execute calls = %d, want 1", ownerState.ExecuteCalls())
			}
			if replacementState.ExecuteCalls() != 1 {
				t.Fatalf("replacement execute calls = %d, want 1", replacementState.ExecuteCalls())
			}

			ownerCalls := ownerState.ClosedAuthInstanceCalls()
			if len(ownerCalls) != 1 {
				t.Fatalf("owner cleanup calls = %d, want 1", len(ownerCalls))
			}
			wantCall := authInstanceCloseCall{authID: auth.ID, instanceID: registered.RuntimeInstanceID(), reason: "auth_replaced"}
			if ownerCalls[0] != wantCall {
				t.Fatalf("owner cleanup call = %#v, want %#v", ownerCalls[0], wantCall)
			}
			replacementCalls := replacementState.ClosedAuthInstanceCalls()
			if len(replacementCalls) != 1 {
				t.Fatalf("replacement cleanup calls = %d, want 1", len(replacementCalls))
			}
			if replacementCalls[0] != wantCall {
				t.Fatalf("replacement cleanup call = %#v, want %#v", replacementCalls[0], wantCall)
			}
		})
	}
}

func TestManagerAuthInstanceDoesNotRetainUnusedExecutorReplacement(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	owner := &replaceAwareExecutor{id: "codex"}
	replacement := &replaceAwareExecutor{id: "codex"}
	manager.RegisterExecutor(owner)
	registered, errRegister := manager.Register(t.Context(), &Auth{ID: "unused-replacement", Provider: "codex"})
	if errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	manager.RegisterExecutor(replacement)
	if _, errUpdate := manager.Update(t.Context(), &Auth{ID: registered.ID, Provider: registered.Provider, Label: "replacement"}); errUpdate != nil {
		t.Fatalf("replace auth: %v", errUpdate)
	}
	if calls := owner.ClosedAuthInstanceCalls(); len(calls) != 1 || calls[0].instanceID != registered.RuntimeInstanceID() {
		t.Fatalf("owner cleanup calls = %#v", calls)
	}
	if calls := replacement.ClosedAuthInstanceCalls(); len(calls) != 0 {
		t.Fatalf("unused replacement cleanup calls = %#v, want none", calls)
	}
}

func TestManagerAuthInstanceOwnersConcurrentBind(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	registered, errRegister := manager.Register(t.Context(), &Auth{ID: "concurrent-bind", Provider: "codex"})
	if errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	ownerState := &replaceAwareExecutor{id: "codex"}
	replacementState := &replaceAwareExecutor{id: "codex"}
	var owner ProviderExecutor = nonComparableReplaceAwareExecutor{replaceAwareExecutor: ownerState, marker: []byte("owner")}
	var replacement ProviderExecutor = nonComparableReplaceAwareExecutor{replaceAwareExecutor: replacementState, marker: []byte("replacement")}
	executors := []ProviderExecutor{owner, replacement}

	start := make(chan struct{})
	var wait sync.WaitGroup
	for worker := 0; worker < 32; worker++ {
		wait.Add(1)
		go func(offset int) {
			defer wait.Done()
			<-start
			for iteration := 0; iteration < 100; iteration++ {
				registered.bindExecutorOwner(executors[(offset+iteration)%len(executors)])
				_ = registered.executorOwners()
			}
		}(worker)
	}
	close(start)
	wait.Wait()

	if _, errUpdate := manager.Update(t.Context(), &Auth{ID: registered.ID, Provider: registered.Provider, Label: "replacement"}); errUpdate != nil {
		t.Fatalf("replace auth: %v", errUpdate)
	}
	wantCall := authInstanceCloseCall{authID: registered.ID, instanceID: registered.RuntimeInstanceID(), reason: "auth_replaced"}
	for name, executor := range map[string]*replaceAwareExecutor{"owner": ownerState, "replacement": replacementState} {
		calls := executor.ClosedAuthInstanceCalls()
		if len(calls) != 1 || calls[0] != wantCall {
			t.Fatalf("%s cleanup calls = %#v, want [%#v]", name, calls, wantCall)
		}
	}
}
