package auth

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type chatGPTWebAutoRefreshCloseExecutor struct {
	*chatGPTWebUnauthorizedRefreshExecutor
	closeOnce sync.Once
	closed    chan struct{}
}

type chatGPTWebRolledBackRefreshStore struct{}

func (*chatGPTWebRolledBackRefreshStore) List(context.Context) ([]*Auth, error) {
	return nil, nil
}

func (*chatGPTWebRolledBackRefreshStore) Save(context.Context, *Auth) (string, error) {
	return "", NewSaveOutcomeError(SaveOutcomeRolledBack, errors.New("source generation changed"))
}

func (*chatGPTWebRolledBackRefreshStore) Delete(context.Context, string) error {
	return nil
}

func (executor *chatGPTWebAutoRefreshCloseExecutor) Close() error {
	executor.closeOnce.Do(func() {
		close(executor.closed)
	})
	return nil
}

func (executor *chatGPTWebAutoRefreshCloseExecutor) RefreshToCompletion(ctx context.Context, auth *Auth) (*Auth, error) {
	return executor.Refresh(context.WithoutCancel(ctx), auth)
}

func TestManagerLoadReleasesDependencyLockWhenPersistBarrierWaitIsCanceled(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	unlockPersist, errLock := manager.lockPersistKeyContext(t.Context(), "load-barrier-holder")
	if errLock != nil {
		t.Fatal(errLock)
	}
	persistLocked := true
	defer func() {
		if persistLocked {
			unlockPersist()
		}
	}()

	loadCtx, cancelLoad := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancelLoad()
	loadResult := make(chan error, 1)
	go func() {
		loadResult <- manager.Load(loadCtx)
	}()
	var errLoad error
	select {
	case errLoad = <-loadResult:
	case <-time.After(time.Second):
		unlockPersist()
		persistLocked = false
		select {
		case errLoad = <-loadResult:
			t.Fatalf("Load did not honor cancellation; completed only after blocker release: %v", errLoad)
		case <-time.After(time.Second):
			t.Fatal("Load remained blocked after the persistence lock was released")
		}
	}
	if !errors.Is(errLoad, context.DeadlineExceeded) {
		t.Fatalf("Load error = %v, want deadline exceeded", errLoad)
	}

	dependencyCtx, cancelDependency := context.WithTimeout(t.Context(), time.Second)
	defer cancelDependency()
	if errDependency := manager.chatGPTWebDependencyMutation.lock(dependencyCtx); errDependency != nil {
		t.Fatalf("dependency lock remained held after Load cancellation: %v", errDependency)
	}
	manager.chatGPTWebDependencyMutation.unlock()

	unlockPersist()
	persistLocked = false
	writerCtx, cancelWriter := context.WithTimeout(t.Context(), time.Second)
	defer cancelWriter()
	unlockWriter, errWriter := manager.lockPersistBarrierWrite(writerCtx)
	if errWriter != nil {
		t.Fatalf("persist barrier remained held after Load cancellation: %v", errWriter)
	}
	unlockWriter()
}

func TestApplyRefreshedAuthHonorsCanceledPersistLockWait(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	installed, errRegister := manager.Register(WithSkipPersist(t.Context()), &Auth{
		ID:       "refresh-persist-cancel",
		Provider: "chatgpt-web",
		Status:   StatusActive,
		Metadata: map[string]any{
			"access_token":    "old-token",
			"account_id":      "account",
			"lifecycle_state": LifecycleStateActive,
		},
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}

	unlockPersist, errLock := manager.lockPersistKeyContext(t.Context(), installed.ID)
	if errLock != nil {
		t.Fatal(errLock)
	}
	persistLocked := true
	defer func() {
		if persistLocked {
			unlockPersist()
		}
	}()

	updated := installed.Clone()
	updated.Metadata["access_token"] = "new-token"
	applyCtx, cancelApply := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancelApply()
	applyResult := make(chan error, 1)
	go func() {
		_, errApply := manager.applyRefreshedAuth(applyCtx, installed, installed.Clone(), updated, time.Time{})
		applyResult <- errApply
	}()
	var errApply error
	select {
	case errApply = <-applyResult:
	case <-time.After(time.Second):
		unlockPersist()
		persistLocked = false
		select {
		case errApply = <-applyResult:
			t.Fatalf("applyRefreshedAuth did not honor cancellation; completed only after blocker release: %v", errApply)
		case <-time.After(time.Second):
			t.Fatal("applyRefreshedAuth remained blocked after the persistence lock was released")
		}
	}
	if !errors.Is(errApply, context.DeadlineExceeded) {
		t.Fatalf("applyRefreshedAuth error = %v, want deadline exceeded", errApply)
	}

	unlockPersist()
	persistLocked = false
	writerCtx, cancelWriter := context.WithTimeout(t.Context(), time.Second)
	defer cancelWriter()
	unlockWriter, errWriter := manager.lockPersistBarrierWrite(writerCtx)
	if errWriter != nil {
		t.Fatalf("persist barrier remained held after apply cancellation: %v", errWriter)
	}
	unlockWriter()
}

func TestPersistBarrierWriterIntentBlocksNewReaders(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	unlockInitializedTurnstile, errInitialize := manager.lockPersistBarrierTurnstile(t.Context())
	if errInitialize != nil {
		t.Fatal(errInitialize)
	}
	unlockInitializedTurnstile()

	unlockFirstReader, errFirstReader := manager.lockPersistKeyContext(t.Context(), "first-reader")
	if errFirstReader != nil {
		t.Fatal(errFirstReader)
	}

	type writerResult struct {
		unlock func()
		err    error
	}
	writerAcquired := make(chan writerResult, 1)
	go func() {
		unlock, errLock := manager.lockPersistBarrierWrite(t.Context())
		writerAcquired <- writerResult{unlock: unlock, err: errLock}
	}()
	deadline := time.Now().Add(time.Second)
	for len(manager.persistBarrierTurnstile) != 0 {
		if time.Now().After(deadline) {
			unlockFirstReader()
			t.Fatal("writer did not acquire the persistence turnstile")
		}
		time.Sleep(time.Millisecond)
	}

	lateReaderWaiting := make(chan struct{}, 1)
	manager.persistBarrierReadObserved = func() {
		select {
		case lateReaderWaiting <- struct{}{}:
		default:
		}
	}
	lateReaderAcquired := make(chan func(), 1)
	go func() {
		unlock, errLock := manager.lockPersistKeyContext(t.Context(), "late-reader")
		if errLock == nil {
			lateReaderAcquired <- unlock
		}
	}()
	select {
	case <-lateReaderWaiting:
	case <-time.After(time.Second):
		unlockFirstReader()
		t.Fatal("late reader did not reach the persistence turnstile")
	}
	select {
	case unlock := <-lateReaderAcquired:
		unlock()
		unlockFirstReader()
		t.Fatal("late reader bypassed pending writer intent")
	default:
	}

	unlockFirstReader()
	var writer writerResult
	select {
	case writer = <-writerAcquired:
		if writer.err != nil {
			t.Fatalf("writer failed to acquire barrier: %v", writer.err)
		}
	case <-time.After(time.Second):
		t.Fatal("writer did not acquire barrier after existing reader released")
	}
	select {
	case unlock := <-lateReaderAcquired:
		unlock()
		writer.unlock()
		t.Fatal("late reader acquired before pending writer")
	default:
	}
	writer.unlock()

	select {
	case unlock := <-lateReaderAcquired:
		unlock()
	case <-time.After(time.Second):
		t.Fatal("late reader did not proceed after writer released the turnstile")
	}
}

func TestPersistBarrierReadWaitHonorsCancellation(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	unlockWriter, errWriter := manager.lockPersistBarrierWrite(t.Context())
	if errWriter != nil {
		t.Fatal(errWriter)
	}
	defer unlockWriter()

	readCtx, cancelRead := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancelRead()
	startedAt := time.Now()
	errRead := manager.lockPersistBarrierRead(readCtx)
	if !errors.Is(errRead, context.DeadlineExceeded) {
		if errRead == nil {
			manager.persistBarrier.RUnlock()
		}
		t.Fatalf("persist barrier read error = %v, want deadline exceeded", errRead)
	}
	if time.Since(startedAt) > time.Second {
		t.Fatal("persist barrier read did not honor cancellation")
	}
}

func TestChatGPTWebRefreshPersistenceOutlivesAcquisitionDeadline(t *testing.T) {
	store := &chatGPTWebRefreshPersistenceStore{}
	manager := NewManager(store, nil, nil)
	executor := &chatGPTWebUnauthorizedRefreshExecutor{}
	refreshCompleted := make(chan struct{})
	executor.refreshHook = func(_, updated *Auth) {
		updated.Metadata["refresh_token"] = "rotated-refresh-token"
		close(refreshCompleted)
	}
	manager.RegisterExecutor(executor)
	installed, errRegister := manager.Register(t.Context(), &Auth{
		ID:       "refresh-fresh-persistence-deadline",
		Provider: "chatgpt-web",
		Status:   StatusActive,
		Metadata: map[string]any{
			"access_token":    "old-token",
			"refresh_token":   "refresh-token",
			"lifecycle_state": LifecycleStateActive,
		},
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}

	unlockPersist, errLock := manager.lockPersistKeyContext(t.Context(), installed.ID)
	if errLock != nil {
		t.Fatal(errLock)
	}
	persistLocked := true
	defer func() {
		if persistLocked {
			unlockPersist()
		}
	}()

	refreshCtx, cancelRefresh := context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer cancelRefresh()
	type refreshResult struct {
		auth *Auth
		err  error
	}
	result := make(chan refreshResult, 1)
	go func() {
		refreshed, errRefresh := manager.refreshProviderForRequest(
			refreshCtx,
			installed.ID,
			authAccessToken(installed),
			"chatgpt-web",
			installed,
		)
		result <- refreshResult{auth: refreshed, err: errRefresh}
	}()

	select {
	case <-refreshCompleted:
	case <-time.After(time.Second):
		t.Fatal("provider refresh did not complete")
	}
	select {
	case completed := <-result:
		if !errors.Is(completed.err, context.DeadlineExceeded) {
			t.Fatalf("request result error = %v, want deadline exceeded", completed.err)
		}
	case <-time.After(time.Second):
		t.Fatal("request caller did not honor the acquisition deadline")
	}

	closeCtx, cancelClose := context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer cancelClose()
	if errClose := manager.CloseExecutorsContext(closeCtx); !errors.Is(errClose, context.DeadlineExceeded) {
		t.Fatalf("CloseExecutorsContext error = %v, want deadline exceeded while refresh commit is blocked", errClose)
	}

	unlockPersist()
	persistLocked = false

	deadline := time.Now().Add(time.Second)
	for {
		saved := store.snapshot()
		if saved != nil &&
			authAccessToken(saved) == "fresh" &&
			chatGPTWebIdentityMetadataString(saved.Metadata, "refresh_token") == "rotated-refresh-token" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("rotated credential was not persisted after caller cancellation: %#v", saved)
		}
		time.Sleep(time.Millisecond)
	}
	if errClose := manager.CloseExecutorsContext(t.Context()); errClose != nil {
		t.Fatalf("manager close did not complete after refresh persistence: %v", errClose)
	}
	current, ok := manager.GetByID(installed.ID)
	if !ok || current == nil || authAccessToken(current) != "fresh" {
		t.Fatalf("runtime credential was not updated after caller cancellation: %#v", current)
	}
}

func TestChatGPTWebRefreshCommitRetriesAfterPersistLockContention(t *testing.T) {
	store := &chatGPTWebRefreshPersistenceStore{}
	manager := NewManager(store, nil, nil)
	manager.refreshCommitAttemptTimeout = 25 * time.Millisecond
	manager.refreshCommitMaxAttempts = 3

	installed, errRegister := manager.Register(t.Context(), &Auth{
		ID:       "refresh-commit-retry",
		Provider: "chatgpt-web",
		Status:   StatusActive,
		Metadata: map[string]any{
			"access_token":    "old-token",
			"refresh_token":   "old-refresh-token",
			"lifecycle_state": LifecycleStateActive,
		},
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	unlockPersist, errLock := manager.lockPersistKeyContext(t.Context(), installed.ID)
	if errLock != nil {
		t.Fatal(errLock)
	}
	persistLocked := true
	defer func() {
		if persistLocked {
			unlockPersist()
		}
	}()

	firstRetry := make(chan struct{}, 1)
	manager.refreshCommitRetryObserved = func(id string, attempt int) {
		if id == installed.ID && attempt == 1 {
			firstRetry <- struct{}{}
		}
	}
	updated := installed.Clone()
	updated.Metadata["access_token"] = "fresh-token"
	updated.Metadata["refresh_token"] = "rotated-refresh-token"

	type applyResult struct {
		auth *Auth
		err  error
	}
	result := make(chan applyResult, 1)
	go func() {
		saved, errApply := manager.applyRefreshedAuthDurably(
			t.Context(),
			"chatgpt-web",
			installed,
			installed.Clone(),
			updated,
			time.Time{},
		)
		result <- applyResult{auth: saved, err: errApply}
	}()

	select {
	case <-firstRetry:
	case <-time.After(time.Second):
		t.Fatal("durable refresh commit did not reach its retry")
	}
	unlockPersist()
	persistLocked = false

	select {
	case completed := <-result:
		if completed.err != nil || completed.auth == nil {
			t.Fatalf("durable refresh commit = (%v, %v)", completed.auth, completed.err)
		}
	case <-time.After(time.Second):
		t.Fatal("durable refresh commit did not finish after lock release")
	}
	saved := store.snapshot()
	if saved == nil ||
		authAccessToken(saved) != "fresh-token" ||
		chatGPTWebIdentityMetadataString(saved.Metadata, "refresh_token") != "rotated-refresh-token" {
		t.Fatalf("rotated credential was not persisted by retry: %#v", saved)
	}
}

func TestChatGPTWebRefreshCommitFailureMarksCredentialUnavailable(t *testing.T) {
	store := &chatGPTWebRefreshPersistenceStore{}
	manager := NewManager(store, nil, nil)
	manager.refreshCommitAttemptTimeout = 20 * time.Millisecond
	manager.refreshCommitMaxAttempts = 2

	installed, errRegister := manager.Register(t.Context(), &Auth{
		ID:       "refresh-commit-failure",
		Provider: "chatgpt-web",
		Status:   StatusActive,
		Metadata: map[string]any{
			"access_token":    "old-token",
			"refresh_token":   "old-refresh-token",
			"lifecycle_state": LifecycleStateActive,
		},
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	unlockPersist, errLock := manager.lockPersistKeyContext(t.Context(), installed.ID)
	if errLock != nil {
		t.Fatal(errLock)
	}
	defer unlockPersist()

	updated := installed.Clone()
	updated.Metadata["access_token"] = "fresh-token"
	updated.Metadata["refresh_token"] = "rotated-refresh-token"
	startedAt := time.Now()
	_, errApply := manager.applyRefreshedAuthDurably(
		nil,
		"chatgpt-web",
		installed,
		installed.Clone(),
		updated,
		time.Time{},
	)
	if errApply == nil {
		t.Fatal("durable refresh commit returned no error after retry exhaustion")
	}
	if time.Since(startedAt) > time.Second {
		t.Fatal("durable refresh commit did not stop after its finite retry budget")
	}
	var authErr *Error
	if !errors.As(errApply, &authErr) || authErr.Code != "refresh_persist_failed" {
		t.Fatalf("durable refresh commit error = %v", errApply)
	}
	current, ok := manager.GetByID(installed.ID)
	if !ok || current == nil {
		t.Fatal("credential disappeared after durable commit failure")
	}
	if current.LifecycleState() != LifecycleStateReauthRequired ||
		chatGPTWebIdentityMetadataString(current.Metadata, "lifecycle_reason") != "refresh_persist_failed" {
		t.Fatalf("credential lifecycle after commit failure = %q / %q",
			current.LifecycleState(),
			chatGPTWebIdentityMetadataString(current.Metadata, "lifecycle_reason"),
		)
	}
	if current.LastError == nil || current.LastError.Code != "refresh_persist_failed" || current.LastError.Retryable {
		t.Fatalf("credential error after commit failure = %#v", current.LastError)
	}
}

func TestChatGPTWebCodexSourceCommitFailureRemainsRetryable(t *testing.T) {
	store := &chatGPTWebRefreshPersistenceStore{}
	manager := NewManager(store, nil, nil)
	manager.refreshCommitAttemptTimeout = 20 * time.Millisecond
	manager.refreshCommitMaxAttempts = 2

	installed, errRegister := manager.Register(t.Context(), &Auth{
		ID:       "refresh-commit-codex-source",
		Provider: "chatgpt-web",
		Status:   StatusActive,
		Metadata: map[string]any{
			"access_token":          "old-token",
			"refresh_strategy":      "codex_source",
			"source_auth_id":        "codex-source.json",
			"source_credential_uid": "codex-source-uid",
			"lifecycle_state":       LifecycleStateActive,
		},
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	unlockPersist, errLock := manager.lockPersistKeyContext(t.Context(), installed.ID)
	if errLock != nil {
		t.Fatal(errLock)
	}
	defer unlockPersist()

	updated := installed.Clone()
	updated.Metadata["access_token"] = "fresh-token"
	_, errApply := manager.applyRefreshedAuthDurably(
		t.Context(),
		"chatgpt-web",
		installed,
		installed.Clone(),
		updated,
		time.Time{},
	)
	var authErr *Error
	if !errors.As(errApply, &authErr) || authErr.Code != "refresh_persist_failed" || !authErr.Retryable {
		t.Fatalf("durable refresh commit error = %#v", errApply)
	}
	current, ok := manager.GetByID(installed.ID)
	if !ok || current == nil {
		t.Fatal("credential disappeared after durable commit failure")
	}
	if current.LifecycleState() != LifecycleStateActive || current.Unavailable {
		t.Fatalf("recoverable source credential became unavailable: %#v", current)
	}
	if current.LastError == nil || current.LastError.Code != "refresh_persist_failed" || !current.LastError.Retryable {
		t.Fatalf("recoverable source credential error = %#v", current.LastError)
	}
	if !current.NextRefreshAfter.After(time.Now()) {
		t.Fatalf("recoverable source credential next refresh = %v", current.NextRefreshAfter)
	}
}

func TestChatGPTWebRolledBackRefreshStopsRotatedOAuthCredential(t *testing.T) {
	manager := NewManager(&chatGPTWebRolledBackRefreshStore{}, nil, nil)
	installed, errRegister := manager.Register(WithSkipPersist(t.Context()), &Auth{
		ID:       "refresh-commit-rolled-back",
		Provider: "chatgpt-web",
		Status:   StatusActive,
		Metadata: map[string]any{
			"access_token":     "old-token",
			"refresh_token":    "old-refresh-token",
			"refresh_strategy": "web_oauth_rt",
			"lifecycle_state":  LifecycleStateActive,
		},
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	updated := installed.Clone()
	updated.Metadata["access_token"] = "fresh-token"
	updated.Metadata["refresh_token"] = "rotated-refresh-token"

	_, errApply := manager.applyRefreshedAuthDurably(
		t.Context(),
		"chatgpt-web",
		installed,
		installed.Clone(),
		updated,
		time.Time{},
	)
	var authErr *Error
	if !errors.As(errApply, &authErr) || authErr.Code != "refresh_persist_failed" {
		t.Fatalf("rolled-back refresh error = %#v", errApply)
	}
	current, ok := manager.GetByID(installed.ID)
	if !ok || current == nil || current.LifecycleState() != LifecycleStateReauthRequired {
		t.Fatalf("rolled-back OAuth credential remained selectable: %#v", current)
	}
}

func TestChatGPTWebAutoRefreshPersistsRotatedTokenAfterLoopCancellation(t *testing.T) {
	store := &chatGPTWebRefreshPersistenceStore{}
	manager := NewManager(store, nil, nil)
	exchangeStarted := make(chan struct{})
	releaseExchange := make(chan struct{})
	var releaseExchangeOnce sync.Once
	release := func() {
		releaseExchangeOnce.Do(func() {
			close(releaseExchange)
		})
	}
	t.Cleanup(release)
	executor := &chatGPTWebAutoRefreshCloseExecutor{
		chatGPTWebUnauthorizedRefreshExecutor: &chatGPTWebUnauthorizedRefreshExecutor{},
		closed:                                make(chan struct{}),
	}
	executor.refreshContextHook = func(ctx context.Context) error {
		close(exchangeStarted)
		select {
		case <-releaseExchange:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	executor.refreshHook = func(_, updated *Auth) {
		updated.Metadata["refresh_token"] = "rotated-background-refresh-token"
	}
	manager.RegisterExecutor(executor)
	installed, errRegister := manager.Register(t.Context(), &Auth{
		ID:       "background-refresh-durable-commit",
		Provider: "chatgpt-web",
		Status:   StatusActive,
		Metadata: map[string]any{
			"access_token":    "old-background-token",
			"refresh_token":   "old-background-refresh-token",
			"lifecycle_state": LifecycleStateActive,
		},
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}

	job, marked := manager.markRefreshPending(installed.ID, time.Now())
	if !marked {
		t.Fatal("markRefreshPending() = false")
	}

	loopCtx, cancelLoop := context.WithCancel(context.Background())
	loop := newAuthAutoRefreshLoop(manager, time.Hour, 1)
	loop.jobs <- job
	go loop.run(loopCtx)

	select {
	case <-exchangeStarted:
	case <-time.After(time.Second):
		cancelLoop()
		release()
		t.Fatal("background refresh did not start the token exchange")
	}
	cancelLoop()

	closeDone := make(chan error, 1)
	closeWaiting := make(chan struct{})
	manager.refreshFlightWaitObserved = func() {
		close(closeWaiting)
	}
	go func() {
		closeDone <- manager.CloseExecutors()
	}()
	select {
	case <-closeWaiting:
	case <-time.After(time.Second):
		release()
		t.Fatal("manager close did not reach the refresh flight barrier")
	}
	select {
	case <-executor.closed:
		release()
		t.Fatal("executor closed before the in-flight token exchange was committed")
	case errClose := <-closeDone:
		release()
		t.Fatalf("manager closed before the in-flight token exchange was committed: %v", errClose)
	default:
	}
	release()

	if !loop.wait(time.Second) {
		t.Fatal("background refresh loop did not finish after the token exchange")
	}
	select {
	case errClose := <-closeDone:
		if errClose != nil {
			t.Fatalf("CloseExecutors() error: %v", errClose)
		}
	case <-time.After(time.Second):
		t.Fatal("manager did not close after the background refresh committed")
	}
	select {
	case <-executor.closed:
	default:
		t.Fatal("executor was not closed after the background refresh committed")
	}
	saved := store.snapshot()
	if saved == nil ||
		authAccessToken(saved) != "fresh" ||
		chatGPTWebIdentityMetadataString(saved.Metadata, "refresh_token") != "rotated-background-refresh-token" {
		t.Fatalf("background refresh did not persist rotated tokens after loop cancellation: %#v", saved)
	}
}
