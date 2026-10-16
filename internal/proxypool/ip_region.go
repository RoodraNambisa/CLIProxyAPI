package proxypool

import (
	"context"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/proxyregion"
)

func (m *Manager) regionOwnerActive(id string) bool {
	if id == "" {
		return true
	}
	m.mu.RLock()
	source := m.auths
	m.mu.RUnlock()
	cfg := m.regionConfig.Load()
	if source == nil || cfg == nil {
		return false
	}
	auth, ok := source.GetByID(id)
	if !ok || auth.Disabled {
		return false
	}
	switch auth.Provider {
	case "codex":
		p, e := config.EffectiveCodexTimeContext(cfg.Codex.TimeContext, auth.Metadata[config.CodexTimeContextMetadataKey])
		return e == nil && p.Enabled && p.Mode == "proxy"
	case "chatgpt-web":
		p, e := config.ApplyWebRegionOverride(cfg.ChatGPTWeb, auth.Metadata[config.WebRegionMetadataKey])
		return e == nil && (p.BrowserRegion.AutoTimezone || p.BrowserRegion.AutoLanguage)
	}
	return false
}
func (m *Manager) regionObservation(route proxyregion.Route) proxyregion.Observation {
	return m.exits.snapshot(route)
}
func (m *Manager) ensureRegionObservation(ctx context.Context, route proxyregion.Route) proxyregion.Observation {
	return m.exits.ensure(ctx, route)
}
func (m *Manager) demandRegionObservation(route proxyregion.Route) { m.exits.request(route) }

// RecordUnboundObservation imports explicit checks through the same detector.
func (m *Manager) RecordUnboundObservation(proxy string, result TraceResult) {
	if m == nil || m.exits == nil || !result.OK {
		return
	}
	snapshot := m.snapshot()
	r := proxyregion.Route{ProxyURL: proxy, Scope: result.Endpoint}
	m.exits.publish(&exitTask{route: r, snapshot: snapshot, ctx: m.exits.ctx}, result)
}
