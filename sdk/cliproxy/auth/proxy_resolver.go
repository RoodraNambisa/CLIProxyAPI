package auth

import (
	"context"
	"net/http"
	"strings"
)

// ResolvedProxy is the request-time proxy decision for one credential.
type ResolvedProxy struct {
	URL       string
	Source    string
	BindingID string
}

// ProxyResolver resolves structured proxy rules and records proxy-only
// failures without changing credential availability.
type ProxyResolver interface {
	Resolve(ctx context.Context, auth *Auth) (ResolvedProxy, error)
	ReportFailure(ctx context.Context, auth *Auth, err error) error
}

// ProxyBindingLeaser keeps a binding alive while a credential is being
// acquired but has not yet been registered with the runtime auth source.
type ProxyBindingLeaser interface {
	HoldBinding(authID string) func()
}

type credentialProxyBindingResolver interface {
	ResolveExistingBindingForCredential(ctx context.Context, authID, credentialUID string) (ResolvedProxy, bool, error)
}

// SetProxyResolver installs the request-time structured proxy resolver.
func (m *Manager) SetProxyResolver(resolver ProxyResolver) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.proxyResolver = resolver
	m.mu.Unlock()
}

// ResolveProxyAuth returns an execution clone carrying the resolved pool URL.
// The persisted ProxyURL remains unchanged.
func (m *Manager) ResolveProxyAuth(ctx context.Context, auth *Auth) (*Auth, error) {
	if auth == nil {
		return nil, nil
	}
	m.mu.RLock()
	resolver := m.proxyResolver
	m.mu.RUnlock()
	resolverAuth, bindingOwner, linkedErr := m.linkedProxySourceAuth(auth)
	if linkedErr != nil {
		if missing, ok := linkedErr.(*linkedSourceProxyError); ok && missing.sourceMissing {
			if existingResolver, supported := resolver.(credentialProxyBindingResolver); supported {
				resolved, found, errExisting := existingResolver.ResolveExistingBindingForCredential(ctx, bindingOwner, ChatGPTWebLinkedSourceUID(auth))
				if errExisting != nil {
					return auth, errExisting
				}
				if found {
					clone := auth.Clone()
					clone.ProxyURL = ""
					clone.RuntimeProxyAuthID = bindingOwner
					clone.RuntimeProxyURL = strings.TrimSpace(resolved.URL)
					clone.RuntimeProxyBindingID = strings.TrimSpace(resolved.BindingID)
					clone.runtimeProxyResolved = true
					return clone, nil
				}
			}
			if snapshotURL, exists := linkedSourceProxySnapshot(auth); exists {
				clone := auth.Clone()
				clone.ProxyURL = ""
				clone.RuntimeProxyAuthID = bindingOwner
				clone.RuntimeProxyURL = snapshotURL
				clone.RuntimeProxyBindingID = ""
				clone.runtimeProxyResolved = true
				return clone, nil
			}
		}
		return auth, linkedErr
	}
	if resolver == nil {
		if resolverAuth != auth {
			clone := auth.Clone()
			clone.ProxyURL = ""
			clone.RuntimeProxyURL = strings.TrimSpace(resolverAuth.ProxyURL)
			clone.RuntimeProxyAuthID = bindingOwner
			clone.runtimeProxyResolved = true
			return clone, nil
		}
		return auth, nil
	}
	if resolverAuth != auth && strings.TrimSpace(resolverAuth.ProxyURL) != "" {
		clone := auth.Clone()
		clone.ProxyURL = ""
		clone.RuntimeProxyAuthID = bindingOwner
		clone.RuntimeProxyURL = strings.TrimSpace(resolverAuth.ProxyURL)
		clone.RuntimeProxyBindingID = ""
		clone.runtimeProxyResolved = true
		return clone, nil
	}
	resolved, errResolve := resolver.Resolve(ctx, resolverAuth)
	if errResolve != nil {
		return auth, errResolve
	}
	clone := auth.Clone()
	if clone == nil {
		return clone, nil
	}
	clone.runtimeProxyResolved = true
	if resolverAuth == auth && strings.TrimSpace(clone.ProxyURL) != "" {
		return clone, nil
	}
	if resolverAuth != auth {
		clone.ProxyURL = ""
		clone.RuntimeProxyAuthID = bindingOwner
	}
	clone.RuntimeProxyURL = strings.TrimSpace(resolved.URL)
	clone.RuntimeProxyBindingID = strings.TrimSpace(resolved.BindingID)
	return clone, nil
}

func linkedSourceProxySnapshot(auth *Auth) (string, bool) {
	if auth == nil || auth.Metadata == nil {
		return "", false
	}
	value, exists := auth.Metadata[chatGPTWebSourceProxyURLKey]
	if !exists {
		return "", false
	}
	proxyURL, ok := value.(string)
	if !ok {
		return "", false
	}
	return strings.TrimSpace(proxyURL), true
}

func (m *Manager) linkedProxySourceAuth(auth *Auth) (*Auth, string, error) {
	if m == nil || auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), "chatgpt-web") ||
		!strings.EqualFold(chatGPTWebIdentityMetadataString(auth.Metadata, "refresh_strategy"), "codex_source") {
		return auth, "", nil
	}
	sourceID := chatGPTWebIdentityMetadataString(auth.Metadata, "source_auth_id")
	sourceUID := chatGPTWebIdentityMetadataString(auth.Metadata, "source_credential_uid")
	if sourceID == "" || sourceUID == "" {
		return nil, sourceID, &linkedSourceProxyError{code: "source_auth_missing", sourceMissing: true}
	}
	m.mu.RLock()
	source := m.auths[sourceID]
	if source != nil {
		source = source.Clone()
	}
	m.mu.RUnlock()
	if source == nil {
		return nil, sourceID, &linkedSourceProxyError{code: "source_auth_missing", sourceMissing: true}
	}
	if !strings.EqualFold(strings.TrimSpace(source.Provider), "codex") ||
		chatGPTWebIdentityMetadataString(source.Metadata, "credential_uid") != sourceUID {
		return nil, sourceID, &linkedSourceProxyError{code: "source_identity_changed"}
	}
	if reference := chatGPTWebIdentityMetadataString(auth.Metadata, "source_identity"); reference != "" {
		identitySource := source.Clone()
		identitySource.Provider = "chatgpt-web"
		if !ChatGPTWebCredentialReferenceMatches(reference, identitySource) {
			return nil, sourceID, &linkedSourceProxyError{code: "source_identity_changed"}
		}
	}
	return source, source.ID, nil
}

type linkedSourceProxyError struct {
	code          string
	sourceMissing bool
}

func (err *linkedSourceProxyError) Error() string {
	if err != nil && err.code == "source_identity_changed" {
		return "linked Codex credential identity changed before proxy resolution"
	}
	return "linked Codex credential proxy is unavailable"
}

func (*linkedSourceProxyError) StatusCode() int      { return http.StatusServiceUnavailable }
func (*linkedSourceProxyError) SkipAuthResult() bool { return true }
func (*linkedSourceProxyError) RetryOtherAuth() bool { return true }

func (err *linkedSourceProxyError) ChatGPTWebErrorCode() string {
	if err == nil {
		return "source_auth_missing"
	}
	return err.code
}

// HoldProxyBinding prevents background pruning from removing a pending
// credential's binding. Resolvers without lease support require no cleanup.
func (m *Manager) HoldProxyBinding(authID string) func() {
	if m == nil {
		return func() {}
	}
	m.mu.RLock()
	resolver := m.proxyResolver
	m.mu.RUnlock()
	leaser, ok := resolver.(ProxyBindingLeaser)
	if !ok || leaser == nil {
		return func() {}
	}
	release := leaser.HoldBinding(strings.TrimSpace(authID))
	if release == nil {
		return func() {}
	}
	return release
}

func (m *Manager) reportProxyFailure(ctx context.Context, auth *Auth, err error) error {
	if m == nil || auth == nil || err == nil || auth.EffectiveProxyBindingID() == "" {
		return err
	}
	m.mu.RLock()
	resolver := m.proxyResolver
	m.mu.RUnlock()
	if resolver == nil {
		return err
	}
	return resolver.ReportFailure(ctx, auth, err)
}

// ReportProxyFailure lets management and background paths apply the same
// proxy-only failure semantics as normal request execution.
func (m *Manager) ReportProxyFailure(ctx context.Context, auth *Auth, err error) error {
	return m.reportProxyFailure(ctx, auth, err)
}

func carryRuntimeProxy(from, to *Auth) {
	if from == nil || to == nil || strings.TrimSpace(to.ProxyURL) != "" || to.runtimeProxyResolved || strings.TrimSpace(to.RuntimeProxyURL) != "" || strings.TrimSpace(to.RuntimeProxyBindingID) != "" {
		return
	}
	to.RuntimeProxyURL = from.RuntimeProxyURL
	to.RuntimeProxyBindingID = from.RuntimeProxyBindingID
	to.RuntimeProxyAuthID = from.RuntimeProxyAuthID
	to.runtimeProxyResolved = from.runtimeProxyResolved || strings.TrimSpace(from.RuntimeProxyURL) != "" || strings.TrimSpace(from.RuntimeProxyBindingID) != ""
}

func clearRuntimeProxy(auth *Auth) {
	if auth == nil {
		return
	}
	auth.RuntimeProxyURL = ""
	auth.RuntimeProxyBindingID = ""
	auth.RuntimeProxyAuthID = ""
	auth.runtimeProxyResolved = false
}
