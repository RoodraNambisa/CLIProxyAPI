package auth

import (
	"context"
	"errors"
	"testing"
)

type linkedProxyTestResolver struct {
	resolved      ResolvedProxy
	existing      ResolvedProxy
	existingFound bool
	existingUID   string
	requestedUID  string
	resolveCalls  int
	existingCalls int
}

func (resolver *linkedProxyTestResolver) Resolve(context.Context, *Auth) (ResolvedProxy, error) {
	resolver.resolveCalls++
	return resolver.resolved, nil
}

func (*linkedProxyTestResolver) ReportFailure(context.Context, *Auth, error) error { return nil }

func (resolver *linkedProxyTestResolver) ResolveExistingBinding(context.Context, string) (ResolvedProxy, bool, error) {
	resolver.existingCalls++
	return resolver.existing, resolver.existingFound, nil
}

func (resolver *linkedProxyTestResolver) ResolveExistingBindingForCredential(_ context.Context, _, credentialUID string) (ResolvedProxy, bool, error) {
	resolver.existingCalls++
	resolver.requestedUID = credentialUID
	if resolver.existingFound && resolver.existingUID != credentialUID {
		return ResolvedProxy{}, false, nil
	}
	return resolver.existing, resolver.existingFound, nil
}

func TestResolveProxyAuthRejectsLinkedSourceIdentityChange(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	source, web := linkedProxyTestAuths(t, manager)
	resolver := &linkedProxyTestResolver{resolved: ResolvedProxy{URL: "http://source.example", BindingID: "binding-a"}}
	manager.SetProxyResolver(resolver)

	changed := source.Clone()
	changed.Metadata["account_id"] = "account-b"
	if _, errUpdate := manager.Update(WithSkipPersist(t.Context()), changed); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	_, errResolve := manager.ResolveProxyAuth(t.Context(), web)
	if errResolve == nil {
		t.Fatal("ResolveProxyAuth() accepted a changed linked source identity")
	}
	var coded interface{ ChatGPTWebErrorCode() string }
	if !errors.As(errResolve, &coded) || coded.ChatGPTWebErrorCode() != "source_identity_changed" {
		t.Fatalf("ResolveProxyAuth() error = %#v", errResolve)
	}
	if resolver.resolveCalls != 0 || resolver.existingCalls != 0 {
		t.Fatalf("resolver calls = %d/%d", resolver.resolveCalls, resolver.existingCalls)
	}
}

func TestResolveProxyAuthReusesLinkedSourceBindingAfterExternalRemoval(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	source, web := linkedProxyTestAuths(t, manager)
	resolver := &linkedProxyTestResolver{
		existing:      ResolvedProxy{URL: "http://stable.example", BindingID: "binding-a"},
		existingFound: true,
		existingUID:   "uid-a",
	}
	manager.SetProxyResolver(resolver)
	if errDelete := manager.Delete(WithSkipPersist(t.Context()), source.ID); errDelete != nil {
		t.Fatal(errDelete)
	}

	resolved, errResolve := manager.ResolveProxyAuth(t.Context(), web)
	if errResolve != nil {
		t.Fatal(errResolve)
	}
	if resolved.EffectiveProxyURL() != "http://stable.example" || resolved.EffectiveProxyBindingID() != "binding-a" || resolved.EffectiveProxyAuthID() != source.ID {
		t.Fatalf("resolved proxy = %#v", resolved)
	}
	if resolver.existingCalls != 1 || resolver.resolveCalls != 0 || resolver.requestedUID != "uid-a" {
		t.Fatalf("resolver calls = %d/%d", resolver.resolveCalls, resolver.existingCalls)
	}
}

func TestResolveProxyAuthUsesPersistedSourceProxySnapshotAfterRemoval(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	source, web := linkedProxyTestAuths(t, manager)
	web.Metadata["source_proxy_url"] = "socks5h://stable.example:1080"
	if errDelete := manager.Delete(WithSkipPersist(t.Context()), source.ID); errDelete != nil {
		t.Fatal(errDelete)
	}

	resolved, errResolve := manager.ResolveProxyAuth(t.Context(), web)
	if errResolve != nil {
		t.Fatal(errResolve)
	}
	if resolved.EffectiveProxyURL() != "socks5h://stable.example:1080" || resolved.EffectiveProxyAuthID() != source.ID {
		t.Fatalf("resolved proxy = %#v", resolved)
	}
}

func TestResolveProxyAuthRejectsBindingFromDifferentSourceGeneration(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	source, web := linkedProxyTestAuths(t, manager)
	resolver := &linkedProxyTestResolver{
		existing:      ResolvedProxy{URL: "http://replacement.example", BindingID: "binding-b"},
		existingFound: true,
		existingUID:   "uid-b",
	}
	manager.SetProxyResolver(resolver)
	if errDelete := manager.Delete(WithSkipPersist(t.Context()), source.ID); errDelete != nil {
		t.Fatal(errDelete)
	}

	_, errResolve := manager.ResolveProxyAuth(t.Context(), web)
	if errResolve == nil {
		t.Fatal("ResolveProxyAuth() reused a binding from a different source generation")
	}
	if resolver.requestedUID != "uid-a" {
		t.Fatalf("requested UID = %q, want uid-a", resolver.requestedUID)
	}
}

func TestResolveProxyAuthFailsClosedWhenLinkedSourceProxyCannotBeRecovered(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	source, web := linkedProxyTestAuths(t, manager)
	resolver := &linkedProxyTestResolver{}
	manager.SetProxyResolver(resolver)
	if errDelete := manager.Delete(WithSkipPersist(t.Context()), source.ID); errDelete != nil {
		t.Fatal(errDelete)
	}

	_, errResolve := manager.ResolveProxyAuth(t.Context(), web)
	if errResolve == nil {
		t.Fatal("ResolveProxyAuth() silently changed proxy after source removal")
	}
	var status interface{ StatusCode() int }
	if !errors.As(errResolve, &status) || status.StatusCode() != 503 {
		t.Fatalf("ResolveProxyAuth() error = %#v", errResolve)
	}
}

func TestResolveProxyAuthWithoutResolverNeverUsesLinkedWebProxyOverride(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	source, web := linkedProxyTestAuths(t, manager)
	web.ProxyURL = "http://web-override.example"

	resolved, errResolve := manager.ResolveProxyAuth(t.Context(), web)
	if errResolve != nil {
		t.Fatal(errResolve)
	}
	if resolved == web || resolved.ProxyURL != "" || resolved.EffectiveProxyURL() != "" || resolved.EffectiveProxyAuthID() != source.ID {
		t.Fatalf("resolved proxy = %#v, want source-owned default proxy resolution", resolved)
	}
}

func TestResolveProxyAuthUsesLinkedSourceExplicitProxyBeforeResolver(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	source, web := linkedProxyTestAuths(t, manager)
	updatedSource := source.Clone()
	updatedSource.ProxyURL = "socks5h://source-explicit.example:1080"
	if _, errUpdate := manager.Update(WithSkipPersist(t.Context()), updatedSource); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	resolver := &linkedProxyTestResolver{resolved: ResolvedProxy{URL: "http://pool.example", BindingID: "pool-binding"}}
	manager.SetProxyResolver(resolver)

	resolved, errResolve := manager.ResolveProxyAuth(t.Context(), web)
	if errResolve != nil {
		t.Fatal(errResolve)
	}
	if resolved.EffectiveProxyURL() != updatedSource.ProxyURL || resolved.EffectiveProxyAuthID() != source.ID || resolved.EffectiveProxyBindingID() != "" {
		t.Fatalf("resolved proxy = %#v", resolved)
	}
	if resolver.resolveCalls != 0 {
		t.Fatalf("resolver calls = %d, want 0", resolver.resolveCalls)
	}
}

func linkedProxyTestAuths(t *testing.T, manager *Manager) (*Auth, *Auth) {
	t.Helper()
	source := &Auth{ID: "codex-source.json", FileName: "codex-source.json", Provider: "codex", Status: StatusActive, Metadata: map[string]any{
		"type": "codex", "credential_uid": "uid-a", "account_id": "account-a", "user_id": "user-a", "email": "person@example.com",
	}}
	installedSource, errRegister := manager.Register(WithSkipPersist(t.Context()), source)
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	identitySource := installedSource.Clone()
	identitySource.Provider = "chatgpt-web"
	web := &Auth{ID: "web.json", FileName: "web.json", Provider: "chatgpt-web", Status: StatusActive, Metadata: map[string]any{
		"type": "chatgpt-web", "refresh_strategy": "codex_source", "source_auth_id": installedSource.ID,
		"source_credential_uid": "uid-a", "source_identity": ChatGPTWebCredentialReferenceValue(identitySource), "access_token": "token",
	}}
	installedWeb, errRegister := manager.Register(WithSkipPersist(t.Context()), web)
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	return installedSource, installedWeb
}
