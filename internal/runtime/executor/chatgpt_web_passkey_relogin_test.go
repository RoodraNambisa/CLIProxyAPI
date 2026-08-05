package executor

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"testing"

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

func clonePasskeyTestCredential(source *chatgptwebauth.WebAuthnCredential) chatgptwebauth.WebAuthnCredential {
	clone := *source
	clone.Transports = append([]string(nil), source.Transports...)
	return clone
}
