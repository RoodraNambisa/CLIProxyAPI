package executor

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestChatGPTWebRuntimePermanentAccountErrorPersistsDeadLifecycle(t *testing.T) {
	store := &chatGPTWebLifecycleCaptureStore{}
	manager := cliproxyauth.NewManager(store, nil, nil)
	auth := chatGPTWebTestAuth("runtime-account-deactivated")
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatal(errRegister)
	}
	current, ok := manager.GetByID(auth.ID)
	if !ok {
		t.Fatalf("registered auth %q not found", auth.ID)
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	t.Cleanup(func() { _ = executor.Close() })

	upstream := newChatGPTWebStatusError(
		http.StatusForbidden,
		"/backend-api/conversation",
		[]byte(`{"error":{"code":"account_deactivated","message":"account unavailable"}}`),
		nil,
	)
	handled := executor.handleChatGPTWebRuntimeLifecycleError(t.Context(), current, upstream)
	var skip interface{ SkipAuthResult() bool }
	var retry interface{ RetryOtherAuth() bool }
	if !errors.As(handled, &skip) || !skip.SkipAuthResult() ||
		!errors.As(handled, &retry) || !retry.RetryOtherAuth() {
		t.Fatalf("handled error routing semantics = %v", handled)
	}

	updated, ok := manager.GetByID(auth.ID)
	if !ok {
		t.Fatalf("updated auth %q not found", auth.ID)
	}
	if state := updated.LifecycleState(); state != cliproxyauth.LifecycleStateDead {
		t.Fatalf("lifecycle state = %q, want dead", state)
	}
	if reason := updated.Metadata["lifecycle_reason"]; reason != "account_deactivated" {
		t.Fatalf("lifecycle reason = %v", reason)
	}
	if updated.LifecycleSelectable() {
		t.Fatal("dead credential remains selectable")
	}
	if updated.Unavailable || !updated.NextRetryAfter.IsZero() {
		t.Fatalf("dead lifecycle was converted to ordinary cooldown: unavailable=%v retry=%v", updated.Unavailable, updated.NextRetryAfter)
	}
	persisted := store.Saved()
	if persisted == nil || persisted.LifecycleState() != cliproxyauth.LifecycleStateDead ||
		persisted.Metadata["lifecycle_reason"] != "account_deactivated" {
		t.Fatalf("persisted lifecycle = %#v", persisted)
	}
}

func TestChatGPTWebAutoReloginExhaustionIsFinite(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "finite-background-relogin")
	maxRetries := 2
	jitterPercent := 0
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(context.Context, chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		return nil, &chatgptwebauth.AuthError{Code: "network_error", Retryable: true}
	}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
		AutoRelogin:              true,
		AutoReloginMaxRetries:    &maxRetries,
		AutoReloginJitterPercent: &jitterPercent,
	}}, manager)
	executor.authService = fake
	executor.reloginBackoff = func(int) time.Duration { return 0 }
	t.Cleanup(func() { _ = executor.Close() })

	executor.TriggerBackgroundRelogin(expected)
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		current, ok := manager.GetByID(expected.ID)
		return ok && current.LifecycleState() == cliproxyauth.LifecycleStateReauthRequired
	})
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		snapshot := executor.BackgroundReloginSnapshot()
		return snapshot.Queued == 0 && snapshot.Delayed == 0 && snapshot.Running == 0
	})
	if got := fake.loginCalls.Load(); got != 3 {
		t.Fatalf("login calls = %d, want initial plus 2 retries", got)
	}
	current, _ := manager.GetByID(expected.ID)
	if reason := current.Metadata["lifecycle_reason"]; reason != "auto_relogin_exhausted" {
		t.Fatalf("lifecycle reason = %v", reason)
	}
	time.Sleep(20 * time.Millisecond)
	if got := fake.loginCalls.Load(); got != 3 {
		t.Fatalf("login calls after exhaustion = %d, want 3", got)
	}
}

func TestChatGPTWebReloginJitterIsBounded(t *testing.T) {
	const delay = 100 * time.Millisecond
	minimum := chatGPTWebJitterDuration(delay, 20, bytes.NewReader(make([]byte, 8)))
	if minimum != 80*time.Millisecond {
		t.Fatalf("minimum jitter = %v, want 80ms", minimum)
	}
	if unchanged := chatGPTWebJitterDuration(delay, 20, bytes.NewReader(nil)); unchanged != delay {
		t.Fatalf("failed random read jitter = %v, want %v", unchanged, delay)
	}
	if disabled := chatGPTWebJitterDuration(delay, 0, bytes.NewReader(make([]byte, 8))); disabled != delay {
		t.Fatalf("disabled jitter = %v, want %v", disabled, delay)
	}
}

type chatGPTWebLifecycleCaptureStore struct {
	mu    sync.Mutex
	saved *cliproxyauth.Auth
}

func (*chatGPTWebLifecycleCaptureStore) List(context.Context) ([]*cliproxyauth.Auth, error) {
	return nil, nil
}

func (store *chatGPTWebLifecycleCaptureStore) Save(_ context.Context, auth *cliproxyauth.Auth) (string, error) {
	store.mu.Lock()
	store.saved = auth.Clone()
	store.mu.Unlock()
	return auth.ID, nil
}

func (*chatGPTWebLifecycleCaptureStore) Delete(context.Context, string) error {
	return nil
}

func (store *chatGPTWebLifecycleCaptureStore) Saved() *cliproxyauth.Auth {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.saved == nil {
		return nil
	}
	return store.saved.Clone()
}
