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

// ProxyCredentialSummary contains only fields needed to search region consumers.
// It deliberately excludes proxy URLs, tokens, model maps, and auth metadata.
type ProxyCredentialSummary struct {
	ID             string `json:"auth_id"`
	Index          string `json:"auth_index"`
	Name           string `json:"name"`
	Provider       string `json:"provider"`
	Disabled       bool   `json:"disabled"`
	HasCustomProxy bool   `json:"-"`
	BindingOwnerID string `json:"-"`
}

// ProxyCredentialSummaries limits bound-only searches to the supplied IDs and
// their linked Web consumers, using the dependency index rather than a full scan.
// A nil ID list requests all credentials; an empty non-nil list requests none.
func (m *Manager) ProxyCredentialSummaries(ids []string) []ProxyCredentialSummary {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if ids == nil {
		ids = make([]string, 0, len(m.auths))
		for id := range m.auths {
			ids = append(ids, id)
		}
	} else {
		selected := make(map[string]struct{}, len(ids))
		for _, id := range ids {
			selected[id] = struct{}{}
			source := m.auths[id]
			for dependentID := range m.dependencyDependentIDs[ChatGPTWebCredentialUID(source)] {
				if ChatGPTWebLinkedSourceMatches(m.auths[dependentID], source) {
					selected[dependentID] = struct{}{}
				}
			}
		}
		ids = make([]string, 0, len(selected))
		for id := range selected {
			ids = append(ids, id)
		}
	}
	result := make([]ProxyCredentialSummary, 0, len(ids))
	for _, id := range ids {
		a := m.auths[id]
		if a == nil {
			continue
		}
		name := a.FileName
		if name == "" {
			name = a.Label
		}
		if name == "" {
			name = a.ID
		}
		owner := a
		if sourceID := ChatGPTWebLinkedSourceID(a); sourceID != "" {
			owner = m.auths[sourceID]
			if !ChatGPTWebLinkedSourceMatches(a, owner) {
				owner = nil
			}
		}
		summary := ProxyCredentialSummary{ID: a.ID, Index: a.Index, Name: name, Provider: a.Provider, Disabled: a.Disabled}
		if owner != nil {
			summary.BindingOwnerID = owner.ID
			summary.HasCustomProxy = strings.TrimSpace(owner.ProxyURL) != ""
		}
		result = append(result, summary)
	}
	return result
}
