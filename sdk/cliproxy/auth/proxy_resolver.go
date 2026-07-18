package auth

import (
	"context"
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
	if resolver == nil {
		return auth, nil
	}
	resolved, errResolve := resolver.Resolve(ctx, auth)
	if errResolve != nil {
		return auth, errResolve
	}
	clone := auth.Clone()
	if clone == nil {
		return clone, nil
	}
	clone.runtimeProxyResolved = true
	if strings.TrimSpace(clone.ProxyURL) != "" {
		return clone, nil
	}
	clone.RuntimeProxyURL = strings.TrimSpace(resolved.URL)
	clone.RuntimeProxyBindingID = strings.TrimSpace(resolved.BindingID)
	return clone, nil
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
	to.runtimeProxyResolved = from.runtimeProxyResolved || strings.TrimSpace(from.RuntimeProxyURL) != "" || strings.TrimSpace(from.RuntimeProxyBindingID) != ""
}

func clearRuntimeProxy(auth *Auth) {
	if auth == nil {
		return
	}
	auth.RuntimeProxyURL = ""
	auth.RuntimeProxyBindingID = ""
	auth.runtimeProxyResolved = false
}
