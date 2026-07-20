package auth

import (
	"context"
	"testing"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
)

func TestLockCredentialRefreshSerializesCallers(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	releaseFirst, errLock := manager.LockCredentialRefresh(t.Context(), "web.json")
	if errLock != nil {
		t.Fatal(errLock)
	}
	acquired := make(chan struct{})
	go func() {
		releaseSecond, errSecond := manager.LockCredentialRefresh(context.Background(), "web.json")
		if errSecond == nil {
			close(acquired)
			releaseSecond()
		}
	}()
	select {
	case <-acquired:
		t.Fatal("second credential refresh lock acquired before the first was released")
	case <-time.After(50 * time.Millisecond):
	}
	releaseFirst()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("second credential refresh lock did not acquire after release")
	}
}

func TestCarryForwardConcurrentRefreshMetadataPreservesCurrentOnlyCookies(t *testing.T) {
	baseline := chatGPTWebRefreshMergeAuth([]chatgptwebauth.Cookie{{Name: "session", Value: "baseline"}})
	current := baseline.Clone()
	currentCredential, errParse := chatgptwebauth.ParseCredential(current.Metadata)
	if errParse != nil {
		t.Fatal(errParse)
	}
	currentCredential.Cookies = append(currentCredential.Cookies, chatgptwebauth.Cookie{Name: "runtime", Value: "current"})
	currentCredential.ApplyToMetadata(current.Metadata)
	next := baseline.Clone()

	carryForwardConcurrentRefreshMetadata(baseline, current, next)
	merged, errParse := chatgptwebauth.ParseCredential(next.Metadata)
	if errParse != nil {
		t.Fatal(errParse)
	}
	if len(merged.Cookies) != 2 || merged.Cookies[1].Name != "runtime" || merged.Cookies[1].Value != "current" {
		t.Fatalf("cookies = %+v", merged.Cookies)
	}
}

func TestApplyRefreshedAuthRejectsExternalSourceReplacement(t *testing.T) {
	store := newChatGPTWebDependencyTestStore()
	manager := NewManager(store, nil, nil)
	expected := registerDependencyTestAuth(t, manager, dependencyTestCodexAuth("source", "uid-a"))
	updated := expected.Clone()
	updated.Metadata["access_token"] = "rotated-access"
	store.replaceOutsideManager(expected.ID, func(replacement *Auth) {
		replacement.Metadata["access_token"] = "external-access"
	})

	installed, errRefresh := manager.applyRefreshedAuth(t.Context(), expected, expected, updated, time.Time{})
	if errRefresh == nil || installed != nil {
		t.Fatalf("apply refreshed auth = %#v, %v; want source-generation conflict", installed, errRefresh)
	}
	if outcome, explicit := SaveOutcomeFromError(errRefresh); !explicit || outcome != SaveOutcomeRolledBack {
		t.Fatalf("refresh outcome = %q, explicit=%v, err=%v", outcome, explicit, errRefresh)
	}
	store.mu.Lock()
	persisted := store.records[expected.ID].Clone()
	store.mu.Unlock()
	if got := chatGPTWebIdentityMetadataString(persisted.Metadata, "access_token"); got != "external-access" {
		t.Fatalf("persisted access token = %q, want external replacement", got)
	}
}

func chatGPTWebRefreshMergeAuth(cookies []chatgptwebauth.Cookie) *Auth {
	credential := &chatgptwebauth.Credential{
		Type: chatgptwebauth.Provider, Email: "person@example.com", AccessToken: "access",
		RefreshStrategy: chatgptwebauth.RefreshStrategyChatGPTSession, Cookies: cookies,
		LifecycleState: chatgptwebauth.LifecycleActive,
	}
	metadata := make(map[string]any)
	credential.ApplyToMetadata(metadata)
	return &Auth{ID: "web.json", Provider: chatgptwebauth.Provider, Metadata: metadata}
}
