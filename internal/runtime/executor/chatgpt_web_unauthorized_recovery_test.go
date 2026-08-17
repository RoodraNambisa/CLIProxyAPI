package executor

import (
	"context"
	"sync"
	"testing"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func registerChatGPTWebUnauthorizedRecoveryAuth(
	t *testing.T,
	manager *cliproxyauth.Manager,
	id string,
	method chatgptwebauth.LoginMethod,
	reason string,
) *cliproxyauth.Auth {
	t.Helper()
	credential := &chatgptwebauth.Credential{
		Type:            chatgptwebauth.Provider,
		Email:           id + "@example.com",
		AccessToken:     "stale-token",
		LoginMethod:     method,
		API798URL:       "https://api798.com/get_code?email=" + id + "%40example.com&auth_code=opaque",
		Persona:         chatgptwebauth.DefaultPersona(),
		LifecycleState:  chatgptwebauth.LifecycleReauthRequired,
		LifecycleReason: reason,
	}
	metadata := make(map[string]any)
	credential.ApplyToMetadata(metadata)
	auth := &cliproxyauth.Auth{
		ID:          "chatgpt-web-" + id,
		Provider:    chatgptwebauth.Provider,
		Status:      cliproxyauth.StatusError,
		Unavailable: true,
		Attributes:  map[string]string{cliproxyauth.SourceHashAttributeKey: "source-" + id},
		Metadata:    metadata,
	}
	installed, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth)
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	return installed
}

func TestChatGPTWebUnauthorizedHistoricalRecoveryRespectsResolvedLoginMethod(t *testing.T) {
	tests := []struct {
		name            string
		method          chatgptwebauth.LoginMethod
		allowAutoAPI798 bool
		wantEligible    bool
	}{
		{name: "explicit api798 ignores legacy fallback switch", method: chatgptwebauth.LoginMethodAPI798, wantEligible: true},
		{name: "legacy auto remains blocked when fallback is disabled", method: chatgptwebauth.LoginMethodAuto, wantEligible: false},
		{name: "legacy auto is eligible when fallback is enabled", method: chatgptwebauth.LoginMethodAuto, allowAutoAPI798: true, wantEligible: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manager := cliproxyauth.NewManager(nil, nil, nil)
			auth := registerChatGPTWebUnauthorizedRecoveryAuth(t, manager, "historical-method", test.method, "session_expired")
			started := make(chan struct{})
			release := make(chan struct{})
			var startedOnce sync.Once
			var releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
			fake := &fakeChatGPTWebAuthService{loginFn: func(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
				startedOnce.Do(func() { close(started) })
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				credential := *input.Credential
				credential.AccessToken = "recovered-token"
				credential.LifecycleState = chatgptwebauth.LifecycleActive
				credential.LifecycleReason = ""
				return &credential, nil
			}}
			executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
				AutoRelogin:            true,
				API798AutoLoginEnabled: test.allowAutoAPI798,
			}}, manager)
			executor.authService = fake
			t.Cleanup(func() { _ = executor.Close() })

			if got := executor.SyncUnauthorizedRecovery(auth); got != test.wantEligible {
				t.Fatalf("SyncUnauthorizedRecovery() = %t, want %t", got, test.wantEligible)
			}
			if test.wantEligible {
				select {
				case <-started:
				case <-time.After(time.Second):
					t.Fatal("eligible historical credential did not start re-login")
				}
				current, _ := manager.GetByID(auth.ID)
				if current.LifecycleState() != cliproxyauth.LifecycleStateReloginPending {
					t.Fatalf("eligible lifecycle = %q", current.LifecycleState())
				}
				releaseOnce.Do(func() { close(release) })
				return
			}
			select {
			case <-started:
				t.Fatal("blocked historical credential started re-login")
			case <-time.After(30 * time.Millisecond):
			}
			current, _ := manager.GetByID(auth.ID)
			if current.LifecycleState() != cliproxyauth.LifecycleStateReauthRequired {
				t.Fatalf("blocked lifecycle = %q", current.LifecycleState())
			}
		})
	}
}

func TestChatGPTWebUnauthorizedRecoveryNeverRevivesExhaustedWithoutManualAction(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := registerChatGPTWebUnauthorizedRecoveryAuth(
		t,
		manager,
		"historical-exhausted",
		chatgptwebauth.LoginMethodAPI798,
		"auto_relogin_exhausted",
	)
	fake := &fakeChatGPTWebAuthService{loginFn: func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		credential := *input.Credential
		credential.AccessToken = "manually-recovered-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		credential.LifecycleReason = ""
		return &credential, nil
	}}
	cfg := &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}
	executor := NewChatGPTWebExecutor(cfg, manager)
	executor.authService = fake
	t.Cleanup(func() { _ = executor.Close() })

	if executor.SyncUnauthorizedRecovery(auth) {
		t.Fatal("exhausted credential was accepted by direct historical recovery")
	}
	executor.ScheduleUnauthorizedRecoveryReconcile()
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		return executor.BackgroundReloginSnapshot().HistoricalExhausted == 1
	})
	executor.UpdateConfig(cfg)
	time.Sleep(30 * time.Millisecond)
	if got := fake.loginCalls.Load(); got != 0 {
		t.Fatalf("automatic login calls = %d, want zero", got)
	}
	current, _ := manager.GetByID(auth.ID)
	if current.LifecycleState() != cliproxyauth.LifecycleStateReauthRequired || chatGPTWebLifecycleReason(current) != "auto_relogin_exhausted" {
		t.Fatalf("exhausted lifecycle changed: %#v", current)
	}

	updated, installed, errRelogin := executor.ReloginCurrent(t.Context(), current)
	if errRelogin != nil || !installed || updated == nil || updated.LifecycleState() != cliproxyauth.LifecycleStateActive {
		t.Fatalf("manual ReloginCurrent() = (%#v, %t, %v)", updated, installed, errRelogin)
	}
	if got := fake.loginCalls.Load(); got != 1 {
		t.Fatalf("manual login calls = %d, want one", got)
	}
}

func TestChatGPTWebUnauthorizedRecoveryHotUpdateReconcilesExistingSessionExpired(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := registerChatGPTWebUnauthorizedRecoveryAuth(
		t,
		manager,
		"historical-hot-update",
		chatgptwebauth.LoginMethodAPI798,
		"session_expired",
	)
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	fake := &fakeChatGPTWebAuthService{loginFn: func(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		startedOnce.Do(func() { close(started) })
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		credential := *input.Credential
		credential.AccessToken = "hot-update-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return &credential, nil
	}}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.authService = fake
	t.Cleanup(func() { _ = executor.Close() })

	executor.UpdateConfig(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("hot update did not reconcile existing session-expired credential")
	}
	current, _ := manager.GetByID(auth.ID)
	if current.LifecycleState() != cliproxyauth.LifecycleStateReloginPending {
		t.Fatalf("hot-update lifecycle = %q", current.LifecycleState())
	}
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		return executor.BackgroundReloginSnapshot().HistoricalEligible == 0
	})
	releaseOnce.Do(func() { close(release) })
}

func TestChatGPTWebUnauthorizedRecoveryBlockedPendingCredentialDoesNotBlockLaterEligibleCredential(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	blocked := registerChatGPTWebUnauthorizedRecoveryAuth(
		t,
		manager,
		"a-blocked-pending",
		chatgptwebauth.LoginMethodAuto,
		"session_expired",
	)
	eligible := registerChatGPTWebUnauthorizedRecoveryAuth(
		t,
		manager,
		"b-eligible-pending",
		chatgptwebauth.LoginMethodAPI798,
		"session_expired",
	)
	for _, auth := range []*cliproxyauth.Auth{blocked, eligible} {
		updated := auth.Clone()
		setChatGPTWebLifecycle(updated, cliproxyauth.LifecycleStateReloginPending, "session_expired", testNow())
		installed, current, errUpdate := manager.UpdateIfCurrent(cliproxyauth.WithSkipPersist(t.Context()), auth, updated)
		if errUpdate != nil || !current || installed == nil {
			t.Fatalf("UpdateIfCurrent(%q) = (%#v, %t, %v)", auth.ID, installed, current, errUpdate)
		}
	}

	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	fake := &fakeChatGPTWebAuthService{loginFn: func(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		startedOnce.Do(func() { close(started) })
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		credential := *input.Credential
		credential.AccessToken = "eligible-recovered-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return &credential, nil
	}}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
	executor.authService = fake
	t.Cleanup(func() { _ = executor.Close() })

	executor.ScheduleUnauthorizedRecoveryReconcile()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("eligible credential behind a blocked pending credential was not scheduled")
	}
	blockedCurrent, _ := manager.GetByID(blocked.ID)
	if blockedCurrent.LifecycleState() != cliproxyauth.LifecycleStateReloginPending {
		t.Fatalf("blocked credential lifecycle = %q", blockedCurrent.LifecycleState())
	}
	if got := executor.BackgroundReloginSnapshot().HistoricalBlockedByMethod; got != 1 {
		t.Fatalf("historical blocked-by-method count = %d, want 1", got)
	}
	releaseOnce.Do(func() { close(release) })
}
