package auth

import "strings"

// ProxyPreview describes configured routing without allocating or probing a node.
// URL is internal transport data and must be sanitized before exposing it.
type ProxyPreview struct {
	ResolvedProxy
	Pending bool
	Linked  bool
}

type proxyPreviewer interface {
	PreviewProxy(*Auth) (ProxyPreview, error)
	PreviewExistingProxy(authID, credentialUID string) (ProxyPreview, bool)
}

// PreviewProxyAuth follows the same linked-credential ownership as execution.
func (m *Manager) PreviewProxyAuth(auth *Auth) (ProxyPreview, error) {
	if auth == nil || m == nil {
		return ProxyPreview{ResolvedProxy: ResolvedProxy{Source: "inherit"}}, nil
	}
	if strings.EqualFold(strings.TrimSpace(auth.Provider), "aistudio") {
		return ProxyPreview{ResolvedProxy: ResolvedProxy{Source: "relay"}}, nil
	}
	m.mu.RLock()
	resolver := m.proxyResolver
	m.mu.RUnlock()
	previewer, supported := resolver.(proxyPreviewer)
	source, owner, err := m.linkedProxySourceAuth(auth)
	if err != nil {
		if missing, ok := err.(*linkedSourceProxyError); ok && missing.sourceMissing {
			if supported {
				if preview, found := previewer.PreviewExistingProxy(owner, ChatGPTWebLinkedSourceUID(auth)); found {
					preview.Linked = true
					return preview, nil
				}
			}
			if snapshot, exists := linkedSourceProxySnapshot(auth); exists {
				return ProxyPreview{ResolvedProxy: ResolvedProxy{URL: snapshot, Source: "source_snapshot"}, Linked: true}, nil
			}
		}
		return ProxyPreview{}, err
	}
	if supported {
		preview, errPreview := previewer.PreviewProxy(source)
		preview.Linked = source != auth
		return preview, errPreview
	}
	if raw := strings.TrimSpace(source.ProxyURL); raw != "" {
		return ProxyPreview{ResolvedProxy: ResolvedProxy{URL: raw, Source: "auth"}, Linked: source != auth}, nil
	}
	return ProxyPreview{ResolvedProxy: ResolvedProxy{Source: "inherit"}, Pending: resolver != nil, Linked: source != auth}, nil
}
