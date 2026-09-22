package codexstate

import (
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
)

// CookiePool defines sharing independently of the acquisition model.
func CookiePool(c Credential, p config.CodexStateOverrideConfig) string {
	switch p.CookiePoolMode {
	case "credential":
		return ""
	case "shared":
		return "shared:" + p.CookiePoolGroup
	case "model":
		return "model:" + c.Model
	default:
		if p.CookiePoolGroup != "" {
			return "shared:" + p.CookiePoolGroup
		}
		if p.CookieAcquisitionModel != "" {
			return "source:" + p.CookieAcquisitionModel
		}
		return "model:" + c.Model
	}
}

func cookiePoolKey(id, source string) string {
	if source == "" {
		return id
	}
	return id + "\x00" + source
}

// CookieAcquisition keeps scheduling and business policy on the triggering
// model, while resolving the probe model's own acceptance rules independently.
// Explicit source selection does not enable that model for business routing.
func CookieAcquisition(cfg config.CodexStateOverrideConfig, c Credential, p config.CodexStateOverrideConfig) (Credential, config.CodexStateOverrideConfig) {
	if !p.CookieOnly() || p.CookieAcquisitionModel == "" {
		return c, p
	}
	c.Route = p.CookieAcquisitionModel
	c.Model = thinking.ParseSuffix(c.Route).ModelName
	c.Aliases = []string{c.Route}
	for _, model := range registry.GetGlobalRegistry().GetModelsForClient(c.ID) {
		if model != nil && thinking.ParseSuffix(model.ID).ModelName == c.Model {
			if model.UpstreamID != "" {
				c.Model = thinking.ParseSuffix(model.UpstreamID).ModelName
			}
			break
		}
	}
	acceptance, _ := DiagnosticPolicy(cfg, c)
	p.MatchModel = cloneCookieBool(acceptance.MatchModel)
	p.Lengths = slices.Clone(acceptance.Lengths)
	p.AcceptedReturnedModels = slices.Clone(acceptance.AcceptedReturnedModels)
	p.ReturnedLengthMode = acceptance.ReturnedLengthMode
	p.MissingReturnedState = acceptance.MissingReturnedState
	return c, p
}

func cloneCookieBool(value *bool) *bool {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func (m *Manager) cookieKeysLocked(id, model string) []string {
	keys := map[string]bool{}
	if model != "" {
		for _, e := range m.entries {
			if e.credential.ID == id && e.Model == model && e.policy.CookieOnly() {
				keys[cookiePoolKey(id, CookiePool(e.credential, e.policy))] = true
			}
		}
	} else {
		for key, group := range m.cookies {
			if group.work.credential.ID == id {
				keys[key] = true
			}
		}
	}
	var result []string
	for key := range keys {
		if m.cookies[key] != nil {
			result = append(result, key)
		}
	}
	sort.Strings(result)
	return result
}

// CookieSnapshots exposes every configured source pool without secret values.
func (m *Manager) CookieSnapshots(id string, now time.Time) []*CookieSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := []*CookieSnapshot{}
	for _, key := range m.cookieKeysLocked(id, "") {
		result = append(result, m.cookieSnapshotLocked(key, now))
	}
	return result
}

// CookieSnapshot preserves the original single-pool field for older clients.
// Model-scoped diagnostic polling can select the corresponding source pool.
func (m *Manager) CookieSnapshot(id string, now time.Time, models ...string) *CookieSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	model := ""
	if len(models) > 0 {
		model = strings.TrimSpace(models[0])
	}
	keys := m.cookieKeysLocked(id, model)
	if len(keys) == 0 {
		return nil
	}
	return m.cookieSnapshotLocked(keys[0], now)
}

func (m *Manager) CookieAction(id, model, action string, pools ...string) (bool, uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	defer m.publishAvailabilityLocked()
	matched, previous := false, uint64(0)
	for _, key := range m.cookieKeysLocked(id, model) {
		if len(pools) > 0 && key != cookiePoolKey(id, pools[0]) {
			continue
		}
		ok, count := m.cookieActionLocked(key, model, action)
		matched = matched || ok
		previous += count
	}
	return matched, previous
}
