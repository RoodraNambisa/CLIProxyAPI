package codexstate

import (
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func (m *Manager) StateExpiry(c Credential, version uint64) time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e := m.entries[key(c)]; e != nil && e.valueVersion == version {
		return e.ExpiresAt
	}
	return time.Time{}
}
func (m *Manager) StateSelectionValid(c Credential, value string, version uint64, expires, now time.Time, p config.CodexStateOverrideConfig) bool {
	if value == "" || version == 0 || !now.Before(expires) {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.entries[key(c)]
	return m.cfg.Enabled && e != nil && !e.paused && e.credential.Instance == c.Instance && e.credential.Plan == c.Plan && e.invalidStateVersion < version && sameStateValidation(e.policy, p)
}
func (m *Manager) CookieSelectionValid(c Credential, selected CookieSelection, now time.Time, p config.CodexStateOverrideConfig) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.cookies[cookiePoolKey(c.ID, selected.Pool)]
	if !m.cfg.Enabled || g == nil || g.owner != c.Owner || g.work.paused || selected.Version == 0 || selected.Version != g.version || g.main == nil {
		return false
	}
	configured, _, _ := m.resolvePolicy(m.cfg, c)
	if CookiePool(c, configured) != selected.Pool || !sameCookieValidation(configured, p) {
		return false
	}
	current := g.main.Select(selected.URL, now, p)
	return selected.Header != "" && selected.Header == current.Header
}

// CookieConnectionValid checks route ownership at the next turn boundary.
// Auxiliary-cookie deltas do not require closing an established connection.
func (m *Manager) CookieConnectionValid(c Credential, selected CookieSelection, now time.Time, p config.CodexStateOverrideConfig) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.cookies[cookiePoolKey(c.ID, selected.Pool)]
	configured, _, _ := m.resolvePolicy(m.cfg, c)
	return CookiePool(c, configured) == selected.Pool && m.cfg.Enabled && g != nil && g.owner == c.Owner && !g.work.paused && selected.Version != 0 && selected.Version == g.version && g.main.Select(selected.URL, now, p).Header != ""
}
