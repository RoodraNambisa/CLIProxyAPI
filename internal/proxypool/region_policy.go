package proxypool

import (
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/proxyregion"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func (m *Manager) manualRegion(r proxyregion.Route) (config.ProxyRegionConfig, string) {
	cfg := m.regionConfig.Load()
	if cfg == nil {
		return config.ProxyRegionConfig{}, ""
	}
	if r.BindingID != "" {
		snapshot := m.snapshot()
		owner, _, valid := m.exits.binding(r, snapshot)
		if !valid {
			return config.ProxyRegionConfig{}, ""
		}
		m.mu.RLock()
		b := m.bindings[owner]
		m.mu.RUnlock()
		pool := snapshot.pools[strings.ToLower(b.Pool)]
		entry := pool.byID[strings.ToLower(b.Entry)]
		return pool.config.Region.Overlay(entry.config.Region), "proxy_pool"
	}
	m.mu.RLock()
	source := m.auths
	m.mu.RUnlock()
	if source != nil && r.OwnerID != "" {
		if a, ok := source.GetByID(r.OwnerID); ok {
			if strings.TrimSpace(a.ProxyURL) != "" {
				if strings.TrimSpace(a.ProxyURL) == strings.TrimSpace(r.ProxyURL) {
					v, e := config.ReadProxyRegion(a.Metadata[config.ProxyRegionMetadataKey])
					if e == nil {
						return v, "credential_proxy"
					}
				}
				return config.ProxyRegionConfig{}, ""
			}
		}
	}
	if cfg.ProxyURL != "" && strings.TrimSpace(cfg.ProxyURL) == strings.TrimSpace(r.ProxyURL) {
		return cfg.ProxyRegion, "global_proxy"
	}
	return config.ProxyRegionConfig{}, ""
}
func (m *Manager) ReconcileRegionAuth(id string) {
	m.mu.RLock()
	source := m.auths
	m.mu.RUnlock()
	if source == nil {
		return
	}
	a, ok := source.GetByID(id)
	if !ok || !m.regionOwnerActive(id) {
		m.regions.ForgetOwner(id)
		return
	}
	cfg := m.regionConfig.Load()
	if cfg == nil {
		return
	}
	preview, err := m.PreviewProxy(a)
	if extended, ok := source.(interface {
		PreviewProxyAuth(*coreauth.Auth) (coreauth.ProxyPreview, error)
	}); ok {
		preview, err = extended.PreviewProxyAuth(a)
	}
	if err != nil || preview.Pending {
		m.regions.ForgetOwner(id)
		return
	}
	target := proxyregion.ChatGPTScope
	if a.Provider == "codex" {
		target = helps.ResolveCodexUpstream(a, cfg).BaseURL
	}
	routed := a.Clone()
	routed.ProxyURL = preview.URL
	routed.RuntimeProxyURL = ""
	r := proxyregion.Route{ProxyURL: helps.ResolvedRequestProxy(cfg, routed, target), BindingID: preview.BindingID, OwnerID: id, Scope: proxyregion.ScopeForTarget(target)}
	manual, _ := m.manualRegion(r)
	needed := false
	if a.Provider == "codex" {
		needed = manual.Timezone == ""
	} else {
		policy, e := config.ApplyWebRegionOverride(cfg.ChatGPTWeb, a.Metadata[config.WebRegionMetadataKey])
		needed = e == nil && ((policy.BrowserRegion.AutoTimezone && manual.Timezone == "") || (policy.BrowserRegion.AutoLanguage && manual.Language == ""))
	}
	if !needed {
		m.regions.ForgetOwner(id)
		return
	}
	m.regions.Register(r)
}
func (m *Manager) ReconcileRegions() {
	m.mu.RLock()
	source := m.auths
	m.mu.RUnlock()
	if source == nil {
		return
	}
	for _, a := range source.List() {
		if a != nil {
			m.ReconcileRegionAuth(a.ID)
		}
	}
	m.regions.PruneOwners(m.regionOwnerActive)
}

func (m *Manager) NotifyRegionConsumer(id string) {
	m.regionChanges.Store(id, true)
	m.regions.NotifyConsumers()
}
func (m *Manager) notifyAllRegions() { m.regionAll.Store(true); m.regions.NotifyConsumers() }
func (m *Manager) reconcileRegionChanges() {
	if m.regionAll.Swap(false) {
		m.ReconcileRegions()
		return
	}
	m.regionChanges.Range(func(key, value any) bool {
		m.regionChanges.Delete(key)
		m.ReconcileRegionAuth(key.(string))
		return true
	})
}
