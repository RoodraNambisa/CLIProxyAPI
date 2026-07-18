package auth

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

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

type lifecycleReplaceAwareExecutor struct {
	*replaceAwareExecutor
	mu         sync.Mutex
	closeCalls int
}

type blockingLifecycleReplaceAwareExecutor struct {
	*replaceAwareExecutor
	closeStart chan struct{}
	closeGate  chan struct{}
	closeOnce  sync.Once
	closeErr   error
}

func (e *blockingLifecycleReplaceAwareExecutor) Close() error {
	e.closeOnce.Do(func() { close(e.closeStart) })
	<-e.closeGate
	return e.closeErr
}

func (e *lifecycleReplaceAwareExecutor) Close() error {
	e.mu.Lock()
	e.closeCalls++
	e.mu.Unlock()
	return nil
}

func (e *lifecycleReplaceAwareExecutor) CloseCalls() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.closeCalls
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

func TestManagerRegisterExecutorClosesReplacedLifecycle(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	replaced := &lifecycleReplaceAwareExecutor{replaceAwareExecutor: &replaceAwareExecutor{id: "antigravity"}}
	current := &lifecycleReplaceAwareExecutor{replaceAwareExecutor: &replaceAwareExecutor{id: "antigravity"}}

	manager.RegisterExecutor(replaced)
	manager.RegisterExecutor(current)

	if got := replaced.CloseCalls(); got != 1 {
		t.Fatalf("replaced Close() calls = %d, want 1", got)
	}
	if got := current.CloseCalls(); got != 0 {
		t.Fatalf("current Close() calls = %d, want 0", got)
	}
}

func TestManagerUnregisterExecutorClosesLifecycle(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	executor := &lifecycleReplaceAwareExecutor{replaceAwareExecutor: &replaceAwareExecutor{id: "antigravity"}}
	manager.RegisterExecutor(executor)

	manager.UnregisterExecutor("antigravity")

	if got := executor.CloseCalls(); got != 1 {
		t.Fatalf("unregistered Close() calls = %d, want 1", got)
	}
}

func TestManagerUnregisterExecutorAfterShutdownRemovesClosedExecutor(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	executor := &lifecycleReplaceAwareExecutor{replaceAwareExecutor: &replaceAwareExecutor{id: "antigravity"}}
	manager.RegisterExecutor(executor)
	if errClose := manager.CloseExecutors(); errClose != nil {
		t.Fatalf("CloseExecutors() error: %v", errClose)
	}

	manager.UnregisterExecutor("antigravity")

	if _, ok := manager.Executor("antigravity"); ok {
		t.Fatal("Executor() returned a closed executor after unregister")
	}
	if got := executor.CloseCalls(); got != 1 {
		t.Fatalf("executor Close() calls = %d, want 1", got)
	}
}

func TestManagerCloseExecutorsRejectsLateRegistration(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	current := &lifecycleReplaceAwareExecutor{replaceAwareExecutor: &replaceAwareExecutor{id: "antigravity"}}
	late := &lifecycleReplaceAwareExecutor{replaceAwareExecutor: &replaceAwareExecutor{id: "antigravity"}}
	manager.RegisterExecutor(current)

	if errClose := manager.CloseExecutors(); errClose != nil {
		t.Fatalf("CloseExecutors() error: %v", errClose)
	}
	manager.RegisterExecutor(late)

	if got := current.CloseCalls(); got != 1 {
		t.Fatalf("current Close() calls = %d, want 1", got)
	}
	if got := late.CloseCalls(); got != 1 {
		t.Fatalf("late Close() calls = %d, want 1", got)
	}
	resolved, ok := manager.Executor("antigravity")
	if !ok || resolved != current {
		t.Fatalf("registered executor = %T %p, want current %p", resolved, resolved, current)
	}
}

func TestManagerCloseExecutorsIgnoresLateRegistrationOfOwnedExecutor(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	current := &lifecycleReplaceAwareExecutor{replaceAwareExecutor: &replaceAwareExecutor{id: "antigravity"}}
	manager.RegisterExecutor(current)
	if errClose := manager.CloseExecutors(); errClose != nil {
		t.Fatalf("CloseExecutors() error: %v", errClose)
	}

	manager.RegisterExecutor(current)
	if got := current.CloseCalls(); got != 1 {
		t.Fatalf("owned executor Close() calls = %d, want 1", got)
	}
}

func TestManagerCloseExecutorsWaitsForReplacementClose(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	replaced := &blockingLifecycleReplaceAwareExecutor{
		replaceAwareExecutor: &replaceAwareExecutor{id: "antigravity"},
		closeStart:           make(chan struct{}),
		closeGate:            make(chan struct{}),
	}
	var releaseOnce sync.Once
	releaseClose := func() { releaseOnce.Do(func() { close(replaced.closeGate) }) }
	t.Cleanup(releaseClose)
	current := &lifecycleReplaceAwareExecutor{replaceAwareExecutor: &replaceAwareExecutor{id: "antigravity"}}
	manager.RegisterExecutor(replaced)

	registerDone := make(chan struct{})
	go func() {
		manager.RegisterExecutor(current)
		close(registerDone)
	}()
	select {
	case <-replaced.closeStart:
	case <-time.After(time.Second):
		t.Fatal("replacement close did not start")
	}

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- manager.CloseExecutors() }()
	select {
	case errClose := <-shutdownDone:
		t.Fatalf("CloseExecutors() returned before replacement close completed: %v", errClose)
	case <-time.After(20 * time.Millisecond):
	}
	if got := current.CloseCalls(); got != 1 {
		t.Fatalf("current Close() calls before old replacement release = %d, want 1", got)
	}
	releaseClose()
	select {
	case <-registerDone:
	case <-time.After(time.Second):
		t.Fatal("RegisterExecutor() did not finish")
	}
	select {
	case errClose := <-shutdownDone:
		if errClose != nil {
			t.Fatalf("CloseExecutors() error: %v", errClose)
		}
	case <-time.After(time.Second):
		t.Fatal("CloseExecutors() did not finish")
	}
	if got := current.CloseCalls(); got != 1 {
		t.Fatalf("current Close() calls = %d, want 1", got)
	}
}

func TestManagerConcurrentCloseExecutorsWaitsAndReturnsSameError(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	closeErr := errors.New("close failed")
	executor := &blockingLifecycleReplaceAwareExecutor{
		replaceAwareExecutor: &replaceAwareExecutor{id: "antigravity"},
		closeStart:           make(chan struct{}),
		closeGate:            make(chan struct{}),
		closeErr:             closeErr,
	}
	var releaseOnce sync.Once
	releaseClose := func() { releaseOnce.Do(func() { close(executor.closeGate) }) }
	t.Cleanup(releaseClose)
	manager.RegisterExecutor(executor)

	firstDone := make(chan error, 1)
	go func() { firstDone <- manager.CloseExecutors() }()
	select {
	case <-executor.closeStart:
	case <-time.After(time.Second):
		t.Fatal("executor close did not start")
	}
	secondDone := make(chan error, 1)
	go func() { secondDone <- manager.CloseExecutors() }()
	select {
	case errSecond := <-secondDone:
		t.Fatalf("second CloseExecutors() returned before first completed: %v", errSecond)
	case <-time.After(20 * time.Millisecond):
	}
	releaseClose()
	for index, done := range []<-chan error{firstDone, secondDone} {
		select {
		case errClose := <-done:
			if !errors.Is(errClose, closeErr) {
				t.Fatalf("CloseExecutors() result %d = %v, want %v", index, errClose, closeErr)
			}
		case <-time.After(time.Second):
			t.Fatalf("CloseExecutors() result %d did not finish", index)
		}
	}
}

func TestManagerCloseExecutorsContextTracksCloseAfterDeadline(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	executor := &blockingLifecycleReplaceAwareExecutor{
		replaceAwareExecutor: &replaceAwareExecutor{id: "antigravity"},
		closeStart:           make(chan struct{}),
		closeGate:            make(chan struct{}),
	}
	var releaseOnce sync.Once
	releaseClose := func() { releaseOnce.Do(func() { close(executor.closeGate) }) }
	t.Cleanup(releaseClose)
	manager.RegisterExecutor(executor)

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if errClose := manager.CloseExecutorsContext(ctx); !errors.Is(errClose, context.DeadlineExceeded) {
		t.Fatalf("CloseExecutorsContext() error = %v, want deadline exceeded", errClose)
	}

	late := &lifecycleReplaceAwareExecutor{replaceAwareExecutor: &replaceAwareExecutor{id: "antigravity"}}
	registerDone := make(chan struct{})
	go func() {
		manager.RegisterExecutor(late)
		close(registerDone)
	}()
	select {
	case <-registerDone:
	case <-time.After(time.Second):
		t.Fatal("late registration blocked behind timed-out executor close")
	}
	if got := late.CloseCalls(); got != 1 {
		t.Fatalf("late executor Close() calls = %d, want 1", got)
	}

	releaseClose()
	if errClose := manager.CloseExecutors(); errClose != nil {
		t.Fatalf("tracked CloseExecutors() error: %v", errClose)
	}
}

func TestManagerCloseExecutorsWaitsForRejectedLateRegistrationClose(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	current := &blockingLifecycleReplaceAwareExecutor{
		replaceAwareExecutor: &replaceAwareExecutor{id: "antigravity"},
		closeStart:           make(chan struct{}),
		closeGate:            make(chan struct{}),
	}
	late := &blockingLifecycleReplaceAwareExecutor{
		replaceAwareExecutor: &replaceAwareExecutor{id: "antigravity"},
		closeStart:           make(chan struct{}),
		closeGate:            make(chan struct{}),
		closeErr:             errors.New("late close failed"),
	}
	manager.RegisterExecutor(current)

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- manager.CloseExecutors() }()
	select {
	case <-current.closeStart:
	case <-time.After(time.Second):
		t.Fatal("registered executor close did not start")
	}

	lateDone := make(chan struct{})
	go func() {
		manager.RegisterExecutor(late)
		close(lateDone)
	}()
	select {
	case <-late.closeStart:
	case <-time.After(time.Second):
		t.Fatal("late executor close did not start")
	}
	close(current.closeGate)
	select {
	case errClose := <-shutdownDone:
		t.Fatalf("CloseExecutors() returned before late close completed: %v", errClose)
	case <-time.After(20 * time.Millisecond):
	}
	close(late.closeGate)
	select {
	case <-lateDone:
	case <-time.After(time.Second):
		t.Fatal("late RegisterExecutor() did not finish")
	}
	select {
	case errClose := <-shutdownDone:
		if !errors.Is(errClose, late.closeErr) {
			t.Fatalf("CloseExecutors() error = %v, want %v", errClose, late.closeErr)
		}
	case <-time.After(time.Second):
		t.Fatal("CloseExecutors() did not finish after late close")
	}
}

func TestManagerCloseExecutorsWaitsForRegistrationAfterCloseSeal(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	done := make(chan struct{})
	manager.executorLifecycleMu.Lock()
	manager.executorsClosed = true
	manager.executorCloseSealed = true
	manager.executorsCloseDone = done
	manager.executorLifecycleMu.Unlock()
	manager.executorCloseWG.Done()

	late := &blockingLifecycleReplaceAwareExecutor{
		replaceAwareExecutor: &replaceAwareExecutor{id: "antigravity"},
		closeStart:           make(chan struct{}),
		closeGate:            make(chan struct{}),
		closeErr:             errors.New("sealed late close failed"),
	}
	registerDone := make(chan struct{})
	go func() {
		manager.RegisterExecutor(late)
		close(registerDone)
	}()
	select {
	case <-late.closeStart:
	case <-time.After(time.Second):
		t.Fatal("late executor close did not start")
	}

	go manager.closeExecutors(nil, done)
	select {
	case <-done:
		t.Fatal("executor shutdown completed before sealed late close")
	case <-time.After(20 * time.Millisecond):
	}
	close(late.closeGate)
	select {
	case <-registerDone:
	case <-time.After(time.Second):
		t.Fatal("late registration did not finish")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("executor shutdown did not finish after sealed late close")
	}
	manager.executorLifecycleMu.Lock()
	closeErr := manager.executorsCloseErr
	manager.executorLifecycleMu.Unlock()
	if !errors.Is(closeErr, late.closeErr) {
		t.Fatalf("executor shutdown error = %v, want %v", closeErr, late.closeErr)
	}
}

func TestManagerCloseExecutorsClosesRejectedExecutorOnlyOnce(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	manager.RegisterExecutor(&lifecycleReplaceAwareExecutor{replaceAwareExecutor: &replaceAwareExecutor{id: "antigravity"}})
	if errClose := manager.CloseExecutors(); errClose != nil {
		t.Fatalf("CloseExecutors() error = %v", errClose)
	}

	rejected := &lifecycleReplaceAwareExecutor{replaceAwareExecutor: &replaceAwareExecutor{id: "antigravity"}}
	manager.RegisterExecutor(rejected)
	manager.RegisterExecutor(rejected)
	if got := rejected.CloseCalls(); got != 1 {
		t.Fatalf("rejected executor Close() calls = %d, want 1", got)
	}
}

func TestManagerUnregisterDuringExecutorShutdownDoesNotCloseTwice(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	executor := &blockingLifecycleReplaceAwareExecutor{
		replaceAwareExecutor: &replaceAwareExecutor{id: "antigravity"},
		closeStart:           make(chan struct{}),
		closeGate:            make(chan struct{}),
	}
	var releaseOnce sync.Once
	releaseClose := func() { releaseOnce.Do(func() { close(executor.closeGate) }) }
	t.Cleanup(releaseClose)
	manager.RegisterExecutor(executor)

	closeDone := make(chan error, 1)
	go func() { closeDone <- manager.CloseExecutors() }()
	select {
	case <-executor.closeStart:
	case <-time.After(time.Second):
		t.Fatal("executor close did not start")
	}

	manager.UnregisterExecutor("antigravity")
	releaseClose()
	select {
	case errClose := <-closeDone:
		if errClose != nil {
			t.Fatalf("CloseExecutors() error: %v", errClose)
		}
	case <-time.After(time.Second):
		t.Fatal("CloseExecutors() did not finish")
	}
	if got := len(executor.ClosedSessionIDs()); got != 1 {
		t.Fatalf("executor close session calls = %d, want 1", got)
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
