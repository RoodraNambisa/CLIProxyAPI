package executor

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"net/http"
	"testing"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestChatGPTWebExecutorPersistsPasskeyReloginStateForCurrentInstance(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebPasskeyTestAuth(t, "persist-passkey-state", 7)
	registered, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth)
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	t.Cleanup(func() { _ = executor.Close() })

	credential, errCredential := chatgptwebauth.ParseCredential(registered.Metadata)
	if errCredential != nil {
		t.Fatal(errCredential)
	}
	updated := clonePasskeyTestCredential(credential.WebAuthn)
	updated.SignCount++
	persisted, errPersist := executor.persistWebAuthnReloginState(t.Context(), registered, updated)
	if errPersist != nil {
		t.Fatal(errPersist)
	}
	if persisted.SignCount != 8 {
		t.Fatalf("persisted sign count = %d, want 8", persisted.SignCount)
	}

	updated = clonePasskeyTestCredential(&persisted)
	updated.LastUsedAt = "2026-08-05T02:00:00Z"
	persisted, errPersist = executor.persistWebAuthnReloginState(t.Context(), registered, updated)
	if errPersist != nil {
		t.Fatal(errPersist)
	}
	if persisted.SignCount != 8 || persisted.LastUsedAt != updated.LastUsedAt {
		t.Fatalf("persisted Passkey state = %+v", persisted)
	}

	latest, ok := manager.GetByID(registered.ID)
	if !ok {
		t.Fatal("persisted credential was not installed")
	}
	latestCredential, errLatest := chatgptwebauth.ParseCredential(latest.Metadata)
	if errLatest != nil {
		t.Fatal(errLatest)
	}
	if latestCredential.WebAuthn.SignCount != 8 || latestCredential.WebAuthn.LastUsedAt != updated.LastUsedAt {
		t.Fatalf("installed Passkey state = %+v", latestCredential.WebAuthn)
	}

	regressed := clonePasskeyTestCredential(latestCredential.WebAuthn)
	regressed.SignCount++
	regressed.LastUsedAt = "2026-08-05T01:00:00Z"
	if _, errPersist = executor.persistWebAuthnReloginState(t.Context(), registered, regressed); errPersist == nil {
		t.Fatal("persistWebAuthnReloginState() accepted a regressed last-used timestamp")
	}
}

func TestChatGPTWebExecutorRejectsPasskeyStateFromReplacedInstance(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebPasskeyTestAuth(t, "replace-passkey-state", 3)
	registered, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth)
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	t.Cleanup(func() { _ = executor.Close() })

	replacement := registered.Clone()
	replacement.Metadata["password"] = "management-replacement"
	if _, errUpdate := manager.Update(cliproxyauth.WithSkipPersist(t.Context()), replacement); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	credential, errCredential := chatgptwebauth.ParseCredential(registered.Metadata)
	if errCredential != nil {
		t.Fatal(errCredential)
	}
	updated := clonePasskeyTestCredential(credential.WebAuthn)
	updated.SignCount++
	if _, errPersist := executor.persistWebAuthnReloginState(t.Context(), registered, updated); !errors.Is(errPersist, chatgptwebauth.ErrCredentialSuperseded) {
		t.Fatalf("persistWebAuthnReloginState() error = %v, want superseded", errPersist)
	}
}

func TestChatGPTWebExecutorReloginMergesPersistedPasskeyCounter(t *testing.T) {
	manager := cliproxyauth.NewManager(&chatGPTWebReloginSourceHashStore{}, nil, nil)
	auth := chatGPTWebPasskeyTestAuth(t, "merge-passkey-state", 11)
	auth.Metadata["lifecycle_state"] = cliproxyauth.LifecycleStateReloginPending
	if _, errRegister := manager.Register(t.Context(), auth); errRegister != nil {
		t.Fatal(errRegister)
	}
	expected, ok := manager.GetByID(auth.ID)
	if !ok {
		t.Fatal("registered credential not found")
	}

	const lastUsedAt = "2026-08-05T03:00:00Z"
	fake := &fakeChatGPTWebAuthService{loginFn: func(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		if input.PersistWebAuthn == nil || input.Credential == nil || input.Credential.WebAuthn == nil {
			t.Fatal("Passkey re-login did not receive its persistence callback")
		}
		counter := clonePasskeyTestCredential(input.Credential.WebAuthn)
		counter.SignCount++
		persisted, errPersist := input.PersistWebAuthn(ctx, counter)
		if errPersist != nil {
			return nil, errPersist
		}
		persisted.LastUsedAt = lastUsedAt
		if _, errPersist = input.PersistWebAuthn(ctx, persisted); errPersist != nil {
			return nil, errPersist
		}

		result := cloneChatGPTWebCredential(input.Credential)
		result.AccessToken = "fresh-passkey-access"
		result.RefreshToken = ""
		result.RefreshStrategy = chatgptwebauth.RefreshStrategyChatGPTSession
		result.LifecycleState = chatgptwebauth.LifecycleActive
		return result, nil
	}}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.authService = fake
	t.Cleanup(func() { _ = executor.Close() })

	installed, current, errRelogin := executor.ReloginCurrent(t.Context(), expected)
	if errRelogin != nil || !current || installed == nil {
		t.Fatalf("ReloginCurrent() = (%v, %v, %v)", installed, current, errRelogin)
	}
	credential, errCredential := chatgptwebauth.ParseCredential(installed.Metadata)
	if errCredential != nil {
		t.Fatal(errCredential)
	}
	if credential.WebAuthn == nil || credential.WebAuthn.SignCount != 12 || credential.WebAuthn.LastUsedAt != lastUsedAt {
		t.Fatalf("installed Passkey state = %+v", credential.WebAuthn)
	}
	if credential.RefreshStrategy != chatgptwebauth.RefreshStrategyChatGPTSession || credential.RefreshToken != "" {
		t.Fatalf("refresh state = %q/%q", credential.RefreshStrategy, credential.RefreshToken)
	}
}

func TestChatGPTWebExecutorPersistsAdvancedSecurityReloginStateForCurrentInstance(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebAdvancedSecurityTestAuth(t, "persist-advanced-security-state", 6, 4)
	registered, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth)
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	t.Cleanup(func() { _ = executor.Close() })

	credential, errCredential := chatgptwebauth.ParseCredential(registered.Metadata)
	if errCredential != nil {
		t.Fatal(errCredential)
	}
	updated := chatgptwebauth.CloneAdvancedAccountSecurityCredential(credential.AdvancedAccountSecurity)
	updated.Passkeys[1].Credential.SignCount++
	persisted, errPersist := executor.persistAdvancedAccountSecurityReloginState(t.Context(), registered, *updated)
	if errPersist != nil {
		t.Fatal(errPersist)
	}
	if persisted.Passkeys[0].Credential.SignCount != 6 || persisted.Passkeys[1].Credential.SignCount != 5 {
		t.Fatalf("persisted counters = %d/%d, want 6/5", persisted.Passkeys[0].Credential.SignCount, persisted.Passkeys[1].Credential.SignCount)
	}

	const lastUsedAt = "2026-08-05T04:00:00Z"
	updated = chatgptwebauth.CloneAdvancedAccountSecurityCredential(&persisted)
	updated.Passkeys[1].Credential.LastUsedAt = lastUsedAt
	persisted, errPersist = executor.persistAdvancedAccountSecurityReloginState(t.Context(), registered, *updated)
	if errPersist != nil {
		t.Fatal(errPersist)
	}
	if persisted.Passkeys[1].Credential.LastUsedAt != lastUsedAt ||
		persisted.RecoveryKeys[0].RecoveryKey != credential.AdvancedAccountSecurity.RecoveryKeys[0].RecoveryKey {
		t.Fatalf("persisted advanced security state = %#v", persisted)
	}

	tampered := chatgptwebauth.CloneAdvancedAccountSecurityCredential(&persisted)
	tampered.RecoveryKeys[0].RecoveryKey = "replacement-recovery-key"
	if _, errPersist = executor.persistAdvancedAccountSecurityReloginState(t.Context(), registered, *tampered); !errors.Is(errPersist, chatgptwebauth.ErrCredentialSuperseded) {
		t.Fatalf("tampered advanced security state error = %v, want superseded", errPersist)
	}
}

func TestChatGPTWebExecutorRejectsAdvancedSecurityStateFromReplacedInstance(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebAdvancedSecurityTestAuth(t, "replace-advanced-security-state", 6, 4)
	registered, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth)
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	t.Cleanup(func() { _ = executor.Close() })

	replacement := registered.Clone()
	replacement.Metadata["password"] = "management-replacement"
	if _, errUpdate := manager.Update(cliproxyauth.WithSkipPersist(t.Context()), replacement); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	credential, errCredential := chatgptwebauth.ParseCredential(registered.Metadata)
	if errCredential != nil {
		t.Fatal(errCredential)
	}
	updated := chatgptwebauth.CloneAdvancedAccountSecurityCredential(credential.AdvancedAccountSecurity)
	updated.Passkeys[0].Credential.SignCount++
	if _, errPersist := executor.persistAdvancedAccountSecurityReloginState(t.Context(), registered, *updated); !errors.Is(errPersist, chatgptwebauth.ErrCredentialSuperseded) {
		t.Fatalf("persistAdvancedAccountSecurityReloginState() error = %v, want superseded", errPersist)
	}
}

func TestChatGPTWebExecutorReloginMergesPersistedAdvancedSecurityState(t *testing.T) {
	manager := cliproxyauth.NewManager(&chatGPTWebReloginSourceHashStore{}, nil, nil)
	auth := chatGPTWebAdvancedSecurityTestAuth(t, "merge-advanced-security-state", 6, 4)
	auth.Metadata["lifecycle_state"] = cliproxyauth.LifecycleStateReloginPending
	if _, errRegister := manager.Register(t.Context(), auth); errRegister != nil {
		t.Fatal(errRegister)
	}
	expected, ok := manager.GetByID(auth.ID)
	if !ok {
		t.Fatal("registered credential not found")
	}

	const lastUsedAt = "2026-08-05T05:00:00Z"
	fake := &fakeChatGPTWebAuthService{loginFn: func(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		if input.PersistAdvancedAccountSecurity == nil || input.Credential == nil || input.Credential.AdvancedAccountSecurity == nil {
			t.Fatal("advanced security re-login did not receive its persistence callback")
		}
		updated := chatgptwebauth.CloneAdvancedAccountSecurityCredential(input.Credential.AdvancedAccountSecurity)
		updated.Passkeys[1].Credential.SignCount++
		persisted, errPersist := input.PersistAdvancedAccountSecurity(ctx, *updated)
		if errPersist != nil {
			return nil, errPersist
		}
		persisted.Passkeys[1].Credential.LastUsedAt = lastUsedAt
		if _, errPersist = input.PersistAdvancedAccountSecurity(ctx, persisted); errPersist != nil {
			return nil, errPersist
		}

		result := cloneChatGPTWebCredential(input.Credential)
		result.AccessToken = "fresh-advanced-security-access"
		result.RefreshToken = ""
		result.RefreshStrategy = chatgptwebauth.RefreshStrategyChatGPTSession
		result.LifecycleState = chatgptwebauth.LifecycleActive
		return result, nil
	}}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.authService = fake
	t.Cleanup(func() { _ = executor.Close() })

	installed, current, errRelogin := executor.ReloginCurrent(t.Context(), expected)
	if errRelogin != nil || !current || installed == nil {
		t.Fatalf("ReloginCurrent() = (%v, %v, %v)", installed, current, errRelogin)
	}
	credential, errCredential := chatgptwebauth.ParseCredential(installed.Metadata)
	if errCredential != nil {
		t.Fatal(errCredential)
	}
	if credential.AdvancedAccountSecurity == nil ||
		credential.AdvancedAccountSecurity.Passkeys[0].Credential.SignCount != 6 ||
		credential.AdvancedAccountSecurity.Passkeys[1].Credential.SignCount != 5 ||
		credential.AdvancedAccountSecurity.Passkeys[1].Credential.LastUsedAt != lastUsedAt {
		t.Fatalf("installed advanced security state = %#v", credential.AdvancedAccountSecurity)
	}
	if credential.AdvancedAccountSecurity.RecoveryKeys[0].RecoveryKey != "recovery-a" {
		t.Fatalf("recovery material changed: %#v", credential.AdvancedAccountSecurity.RecoveryKeys[0])
	}
}

func TestChatGPTWebExecutorTokenOnlyPasskeySchedulesAutomaticRelogin(t *testing.T) {
	auth := chatGPTWebPasskeyTestAuth(t, "token-only-passkey", 0)
	credential, errCredential := chatgptwebauth.ParseCredential(auth.Metadata)
	if errCredential != nil {
		t.Fatal(errCredential)
	}
	credential.RefreshStrategy = chatgptwebauth.RefreshStrategyTokenOnly
	credential.Cookies = nil
	credential.SessionToken = ""
	credential.ApplyToMetadata(auth.Metadata)

	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, nil)
	t.Cleanup(func() { _ = executor.Close() })
	updated, errRefresh := executor.Refresh(t.Context(), auth)
	if errRefresh == nil || updated == nil {
		t.Fatalf("Refresh() = (%v, %v), want lifecycle update and error", updated, errRefresh)
	}
	if state := updated.LifecycleState(); state != cliproxyauth.LifecycleStateReloginPending {
		t.Fatalf("lifecycle = %q, want %q", state, cliproxyauth.LifecycleStateReloginPending)
	}
}

func TestChatGPTWebExecutorPromotesExhaustedInvalidPasskeyResponseToDead(t *testing.T) {
	flowAttempts := 2
	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebPasskeyOnlyReloginAuth(t, "invalid-passkey-dead")
	auth.Unavailable = true
	auth.NextRetryAfter = time.Now().Add(30 * 24 * time.Hour)
	auth.CooldownScope = "auth"
	auth.ModelStates = map[string]*cliproxyauth.ModelState{
		"gpt-5": {
			Status:         cliproxyauth.StatusError,
			Unavailable:    true,
			NextRetryAfter: time.Now().Add(30 * 24 * time.Hour),
			LastError:      &cliproxyauth.Error{Code: "unauthorized", HTTPStatus: http.StatusUnauthorized, Message: "unauthorized"},
		},
	}
	registered, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth)
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	fake := &fakeChatGPTWebAuthService{loginFn: invalidPasskeyResponseLogin}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
		InvalidPasskeyResponseAsDead: true,
		LoginProxy: config.ChatGPTWebLoginProxyConfig{
			Enabled:      true,
			URLTemplate:  "http://proxy.example:8080",
			FlowAttempts: &flowAttempts,
		},
	}}, manager)
	executor.authService = fake
	t.Cleanup(func() { _ = executor.Close() })

	installed, current, errRelogin := executor.ReloginCurrent(t.Context(), registered)
	if errRelogin == nil || !current || installed == nil {
		t.Fatalf("ReloginCurrent() = (%v, %v, %v)", installed, current, errRelogin)
	}
	if installed.LifecycleState() != cliproxyauth.LifecycleStateDead || chatGPTWebLifecycleReason(installed) != "invalid_passkey_response" {
		t.Fatalf("installed lifecycle = %q/%q", installed.LifecycleState(), chatGPTWebLifecycleReason(installed))
	}
	if installed.Unavailable || !installed.NextRetryAfter.IsZero() || installed.CooldownScope != "" {
		t.Fatalf("auth cooldown was not cleared: unavailable=%v next=%v scope=%q", installed.Unavailable, installed.NextRetryAfter, installed.CooldownScope)
	}
	if state := installed.ModelStates["gpt-5"]; state == nil || state.Unavailable || !state.NextRetryAfter.IsZero() || state.LastError != nil {
		t.Fatalf("model cooldown was not cleared: %#v", state)
	}
	authError, ok := chatgptwebauth.AsAuthError(errRelogin)
	if !ok || authError.State != chatgptwebauth.LifecycleDead {
		t.Fatalf("re-login error = %#v", errRelogin)
	}
}

func TestChatGPTWebExecutorInvalidPasskeyResponseDeadGuards(t *testing.T) {
	tests := []struct {
		name       string
		configure  func(*config.ChatGPTWebConfig)
		mutateAuth func(*cliproxyauth.Auth, *chatgptwebauth.Credential)
		mutateErr  func(*chatgptwebauth.AuthError)
		proxy      chatgptwebauth.LoginProxyConfig
	}{
		{name: "disabled", configure: func(cfg *config.ChatGPTWebConfig) { cfg.InvalidPasskeyResponseAsDead = false }},
		{name: "password remains", mutateAuth: func(_ *cliproxyauth.Auth, credential *chatgptwebauth.Credential) { credential.Password = "recoverable" }},
		{name: "totp remains", mutateAuth: func(_ *cliproxyauth.Auth, credential *chatgptwebauth.Credential) {
			credential.TOTPSecret = "recoverable"
		}},
		{name: "linked source remains", mutateAuth: func(_ *cliproxyauth.Auth, credential *chatgptwebauth.Credential) {
			credential.SourceAuthID = "codex-source"
			credential.SourceCredentialUID = "uid"
		}},
		{name: "unverified oauth refresh remains", mutateAuth: func(_ *cliproxyauth.Auth, credential *chatgptwebauth.Credential) {
			credential.RefreshStrategy = chatgptwebauth.RefreshStrategyWebOAuthRT
			credential.RefreshToken = "recoverable"
		}},
		{name: "unverified session cookie remains", mutateAuth: func(_ *cliproxyauth.Auth, credential *chatgptwebauth.Credential) {
			credential.RefreshStrategy = chatgptwebauth.RefreshStrategyTokenOnly
			credential.Cookies = []chatgptwebauth.Cookie{{
				Name: "__Secure-next-auth.session-token", Value: "recoverable", Domain: "chatgpt.com", Path: "/", Secure: true,
			}}
		}},
		{name: "network failure", mutateErr: func(authError *chatgptwebauth.AuthError) {
			authError.Status = http.StatusServiceUnavailable
			authError.StatusCode = http.StatusServiceUnavailable
			authError.DiagnosticCode = ""
			authError.Retryable = true
			authError.Terminal = false
		}},
		{name: "flow attempts remain", proxy: chatgptwebauth.LoginProxyConfig{Enabled: true, FlowAttempts: 2}},
		{name: "active lifecycle", mutateAuth: func(auth *cliproxyauth.Auth, _ *chatgptwebauth.Credential) {
			auth.Metadata["lifecycle_state"] = cliproxyauth.LifecycleStateActive
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := config.ChatGPTWebConfig{InvalidPasskeyResponseAsDead: true}
			if test.configure != nil {
				test.configure(&cfg)
			}
			executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: cfg}, nil)
			t.Cleanup(func() { _ = executor.Close() })
			auth := chatGPTWebPasskeyOnlyReloginAuth(t, "guard-"+test.name)
			credential, errCredential := chatgptwebauth.ParseCredential(auth.Metadata)
			if errCredential != nil {
				t.Fatal(errCredential)
			}
			if test.mutateAuth != nil {
				test.mutateAuth(auth, credential)
			}
			authError := invalidPasskeyResponseError()
			if test.mutateErr != nil {
				test.mutateErr(authError)
			}
			promoted, promotedErr := promoteExhaustedInvalidPasskeyResponse(
				cfg.InvalidPasskeyResponseAsDead,
				auth,
				credential,
				authError,
				test.proxy,
				time.Now(),
			)
			if promoted.LifecycleState == chatgptwebauth.LifecycleDead {
				t.Fatalf("guard promoted credential to dead: %#v / %v", promoted, promotedErr)
			}
		})
	}
}

func TestChatGPTWebPasskeyRecoveryExhaustedRequiresEvidenceForEveryRefreshMaterial(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*cliproxyauth.Auth, *chatgptwebauth.Credential)
		want      bool
	}{
		{
			name: "invalid oauth refresh token",
			configure: func(auth *cliproxyauth.Auth, credential *chatgptwebauth.Credential) {
				credential.RefreshStrategy = chatgptwebauth.RefreshStrategyWebOAuthRT
				credential.RefreshToken = "rejected"
				auth.Metadata["lifecycle_reason"] = "invalid_grant"
			},
			want: true,
		},
		{
			name: "expired session cookie",
			configure: func(auth *cliproxyauth.Auth, credential *chatgptwebauth.Credential) {
				credential.RefreshStrategy = chatgptwebauth.RefreshStrategyChatGPTSession
				credential.Cookies = []chatgptwebauth.Cookie{{
					Name: "__Secure-next-auth.session-token", Value: "rejected", Domain: "chatgpt.com", Path: "/", Secure: true,
				}}
				auth.Metadata["lifecycle_reason"] = "session_expired"
			},
			want: true,
		},
		{
			name: "session failed but oauth refresh remains",
			configure: func(auth *cliproxyauth.Auth, credential *chatgptwebauth.Credential) {
				credential.RefreshStrategy = chatgptwebauth.RefreshStrategyWebOAuthRT
				credential.RefreshToken = "unverified"
				credential.Cookies = []chatgptwebauth.Cookie{{
					Name: "__Secure-next-auth.session-token", Value: "rejected", Domain: "chatgpt.com", Path: "/", Secure: true,
				}}
				auth.Metadata["lifecycle_reason"] = "session_expired"
			},
			want: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			auth := chatGPTWebPasskeyOnlyReloginAuth(t, "refresh-evidence-"+test.name)
			credential, errCredential := chatgptwebauth.ParseCredential(auth.Metadata)
			if errCredential != nil {
				t.Fatal(errCredential)
			}
			test.configure(auth, credential)
			if got := chatGPTWebPasskeyRecoveryExhausted(auth, credential); got != test.want {
				t.Fatalf("chatGPTWebPasskeyRecoveryExhausted() = %t, want %t", got, test.want)
			}
		})
	}
}

func chatGPTWebPasskeyOnlyReloginAuth(t *testing.T, id string) *cliproxyauth.Auth {
	t.Helper()
	auth := chatGPTWebPasskeyTestAuth(t, id, 0)
	credential, errCredential := chatgptwebauth.ParseCredential(auth.Metadata)
	if errCredential != nil {
		t.Fatal(errCredential)
	}
	credential.Password = ""
	credential.TOTPSecret = ""
	credential.RefreshToken = ""
	credential.RefreshStrategy = chatgptwebauth.RefreshStrategyTokenOnly
	credential.Cookies = nil
	credential.SessionToken = ""
	credential.LifecycleState = chatgptwebauth.LifecycleReloginPending
	credential.LifecycleReason = "token_only_expired"
	credential.ApplyToMetadata(auth.Metadata)
	auth.Status = cliproxyauth.StatusPending
	return auth
}

func invalidPasskeyResponseLogin(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
	credential := cloneChatGPTWebCredential(input.Credential)
	credential.LifecycleState = chatgptwebauth.LifecycleReauthRequired
	credential.LifecycleReason = "passkey_verification_failed"
	authError := invalidPasskeyResponseError()
	if input.RetryInvalidPasskeyResponse {
		authError.Attempts = max(1, input.LoginProxy.FlowAttempts)
	}
	return credential, authError
}

func invalidPasskeyResponseError() *chatgptwebauth.AuthError {
	return &chatgptwebauth.AuthError{
		Code:           "passkey_verification_failed",
		DiagnosticCode: "invalid_passkey_response",
		State:          chatgptwebauth.LifecycleReauthRequired,
		LifecycleState: chatgptwebauth.LifecycleReauthRequired,
		Status:         http.StatusBadRequest,
		StatusCode:     http.StatusBadRequest,
		FailureStage:   "passkey_verify",
		Attempts:       1,
		Message:        "Passkey credential was rejected",
		Terminal:       true,
	}
}

func chatGPTWebPasskeyTestAuth(t *testing.T, id string, signCount uint32) *cliproxyauth.Auth {
	t.Helper()
	auth := chatGPTWebTestAuth(id)
	credential, errCredential := chatgptwebauth.ParseCredential(auth.Metadata)
	if errCredential != nil {
		t.Fatal(errCredential)
	}
	privateKey, errGenerate := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if errGenerate != nil {
		t.Fatal(errGenerate)
	}
	privateDER, errMarshal := x509.MarshalPKCS8PrivateKey(privateKey)
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	credential.CredentialSchemaVersion = chatgptwebauth.CredentialSchemaVersionWebAuthn
	credential.WebAuthn = &chatgptwebauth.WebAuthnCredential{
		Version:         chatgptwebauth.WebAuthnCredentialVersion,
		CredentialID:    base64.RawURLEncoding.EncodeToString([]byte("credential-" + id)),
		UserHandle:      base64.RawURLEncoding.EncodeToString([]byte("user-" + id)),
		RPID:            chatgptwebauth.WebAuthnRPID,
		Origin:          chatgptwebauth.WebAuthnOrigin,
		Algorithm:       chatgptwebauth.WebAuthnES256Algorithm,
		PrivateKeyPKCS8: base64.StdEncoding.EncodeToString(privateDER),
		SignCount:       signCount,
		MFAFactorID:     "factor-" + id,
		Transports:      []string{"internal"},
		UserPresent:     true,
		UserVerified:    true,
		CreatedAt:       "2026-08-05T00:00:00Z",
		LastUsedAt:      "2026-08-05T00:00:00Z",
	}
	credential.ApplyToMetadata(auth.Metadata)
	return auth
}

func chatGPTWebAdvancedSecurityTestAuth(t *testing.T, id string, passkeyCount, securityKeyCount uint32) *cliproxyauth.Auth {
	t.Helper()
	auth := chatGPTWebPasskeyTestAuth(t, id+"-passkey", passkeyCount)
	auth.ID = id
	credential, errCredential := chatgptwebauth.ParseCredential(auth.Metadata)
	if errCredential != nil {
		t.Fatal(errCredential)
	}
	passkey := clonePasskeyTestCredential(credential.WebAuthn)
	passkey.BackupEligible = true
	passkey.BackupState = true
	passkey.Transports = []string{"hybrid"}

	securityAuth := chatGPTWebPasskeyTestAuth(t, id+"-security-key", securityKeyCount)
	securityCredential, errSecurityCredential := chatgptwebauth.ParseCredential(securityAuth.Metadata)
	if errSecurityCredential != nil {
		t.Fatal(errSecurityCredential)
	}
	securityKey := clonePasskeyTestCredential(securityCredential.WebAuthn)
	securityKey.BackupEligible = false
	securityKey.BackupState = false
	securityKey.Transports = []string{"usb"}

	recoveryKeys := make([]chatgptwebauth.AdvancedAccountRecoveryKey, 5)
	for index := range recoveryKeys {
		suffix := string(rune('a' + index))
		recoveryKeys[index] = chatgptwebauth.AdvancedAccountRecoveryKey{
			RecoveryKey:                "recovery-" + suffix,
			AccountRecoveryCode:        "account-" + suffix,
			XWingPublicKeyBase64:       base64.StdEncoding.EncodeToString([]byte{byte(index + 1), 1}),
			AuthenticationSecretBase64: base64.StdEncoding.EncodeToString([]byte{byte(index + 1), 2}),
		}
	}
	credential.CredentialSchemaVersion = chatgptwebauth.CredentialSchemaVersionAdvancedAccountSecurity
	credential.WebAuthn = nil
	credential.LoginMethod = chatgptwebauth.LoginMethodAdvancedSecurityPasskey
	credential.AdvancedAccountSecurity = &chatgptwebauth.AdvancedAccountSecurityCredential{
		Version: chatgptwebauth.AdvancedAccountSecurityCredentialVersion,
		Enabled: true,
		Passkeys: []chatgptwebauth.AdvancedAccountSecurityPasskeyCredential{
			{Kind: "passkey", IsNonDeviceBound: true, Credential: passkey},
			{Kind: "security-key", IsSecurityKey: true, Credential: securityKey},
		},
		RecoveryKeys: recoveryKeys,
		EnrolledAt:   "2026-08-05T00:00:00Z",
		VerifiedAt:   "2026-08-05T01:00:00Z",
		LoginMethod:  "passkey",
	}
	credential.ApplyToMetadata(auth.Metadata)
	return auth
}

func clonePasskeyTestCredential(source *chatgptwebauth.WebAuthnCredential) chatgptwebauth.WebAuthnCredential {
	clone := *source
	clone.Transports = append([]string(nil), source.Transports...)
	return clone
}
