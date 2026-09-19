package codexstate

import (
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
)

func init() {
	registry.GetGlobalRegistry().SetClientModelAvailability(Default.Availability)
}

// Availability never takes the manager lock: registry and scheduler readers
// can inspect a published snapshot without introducing a lock-order dependency.
func (m *Manager) Availability() registry.ClientModelAvailability {
	if snapshot := m.availability.Load(); snapshot != nil {
		return *snapshot
	}
	return nil
}

func (m *Manager) publishAvailabilityLocked() {
	var result registry.ClientModelAvailability
	if m.cfg.Enabled && !m.diagnostic {
		for _, e := range m.entries {
			if e.policy.MissingPolicy != "hide" || e.ManualOnly {
				continue
			}
			if result == nil {
				result = registry.ClientModelAvailability{}
			}
			routes := result[e.credential.ID]
			if routes == nil {
				routes = map[string]time.Time{}
				result[e.credential.ID] = routes
			}
			var expires time.Time
			if !e.paused && e.state != "" {
				expires = e.ExpiresAt
			}
			for _, route := range append([]string{e.Model, e.credential.Route}, e.credential.Aliases...) {
				if route != "" {
					routes[route] = expires
				}
			}
		}
	}
	if len(result) == 0 {
		m.availability.Store(nil)
	} else {
		m.availability.Store(&result)
	}
}
