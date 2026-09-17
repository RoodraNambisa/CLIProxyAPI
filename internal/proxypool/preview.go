package proxypool

import (
	"strings"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
)

// PreviewProxy reads the current rule and binding without network or disk writes.
func (m *Manager) PreviewProxy(auth *coreauth.Auth) (coreauth.ProxyPreview, error) {
	result := coreauth.ProxyPreview{ResolvedProxy: coreauth.ResolvedProxy{Source: "inherit"}}
	if auth == nil {
		return result, nil
	}
	if strings.EqualFold(strings.TrimSpace(auth.Provider), "aistudio") {
		result.Source = "relay"
		return result, nil
	}
	if raw := strings.TrimSpace(auth.ProxyURL); raw != "" {
		result.URL, result.Source = raw, "auth"
		_, err := proxyutil.Parse(raw)
		return result, err
	}
	snapshot := m.snapshot()
	if snapshot == nil {
		return result, nil
	}
	targets, matched := internalconfig.MatchProxyRuleTargets(snapshot.rules, auth.Provider, authPriority(auth))
	if !matched {
		if snapshot.globalURL != "" {
			result.URL, result.Source = snapshot.globalURL, "global"
		}
		return result, nil
	}
	m.mu.RLock()
	binding, found := m.bindings[auth.ID]
	m.mu.RUnlock()
	if found && bindingCredentialGenerationMatches(binding.CredentialUID, coreauth.ChatGPTWebCredentialUID(auth)) && bindingMatchesRuleTargets(binding, targets) {
		if raw, valid := m.bindingURL(snapshot, binding); valid {
			result.ResolvedProxy = resolvedProxy(binding, raw)
			return result, nil
		}
	}
	if len(targets) == 1 && targets[0].Direct {
		result.URL, result.Source = "direct", "direct"
		return result, nil
	}
	result.Source, result.Pending = "pool", true
	return result, nil
}

// PreviewExistingProxy is used for linked accounts whose source was removed.
func (m *Manager) PreviewExistingProxy(authID, credentialUID string) (coreauth.ProxyPreview, bool) {
	snapshot := m.snapshot()
	if snapshot == nil || strings.TrimSpace(credentialUID) == "" {
		return coreauth.ProxyPreview{}, false
	}
	m.mu.RLock()
	binding, found := m.bindings[authID]
	m.mu.RUnlock()
	if !found || strings.TrimSpace(binding.CredentialUID) != strings.TrimSpace(credentialUID) {
		return coreauth.ProxyPreview{}, false
	}
	raw, valid := m.bindingURL(snapshot, binding)
	return coreauth.ProxyPreview{ResolvedProxy: resolvedProxy(binding, raw)}, valid
}
