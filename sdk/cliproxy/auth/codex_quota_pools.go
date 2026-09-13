package auth

import (
	"maps"
	"net/http"
	"sort"
	"strings"
	"time"
)

const maxCodexQuotaPools = 8

// CodexQuotaPool keeps an independently observed pool. Signals use the default
// window prefix within this explicit scope; they never inherit another pool's fields.
type CodexQuotaPool struct {
	ID         string            `json:"id"`
	Name       string            `json:"name,omitempty"`
	ObservedAt time.Time         `json:"observed_at"`
	Source     string            `json:"source"`
	Signals    map[string]string `json:"signals"`
}

func mergeCodexQuotaPools(previous, current *CodexQuotaObservation) ([]CodexQuotaPool, bool) {
	pools := make(map[string]CodexQuotaPool)
	updated := false
	if previous != nil {
		for _, pool := range previous.Pools {
			pools[pool.ID] = pool
		}
	}
	for _, pool := range extractCodexQuotaPools(current) {
		if old, exists := pools[pool.ID]; exists && old.ObservedAt.After(pool.ObservedAt) {
			continue
		}
		// Replace the entire pool, including missing windows and reset hints.
		// A name is identity metadata, so a later unnamed event may retain it.
		if pool.Name == "" {
			pool.Name = pools[pool.ID].Name
		}
		pools[pool.ID] = pool
		updated = true
	}
	result := make([]CodexQuotaPool, 0, len(pools))
	for _, pool := range pools {
		pool.Signals = maps.Clone(pool.Signals)
		result = append(result, pool)
	}
	sort.Slice(result, func(i, j int) bool {
		if !result[i].ObservedAt.Equal(result[j].ObservedAt) {
			return result[i].ObservedAt.After(result[j].ObservedAt)
		}
		return result[i].ID < result[j].ID
	})
	if len(result) > maxCodexQuotaPools {
		result = result[:maxCodexQuotaPools]
	}
	return result, updated
}

func extractCodexQuotaPools(observation *CodexQuotaObservation) []CodexQuotaPool {
	groups := make(map[string]map[string]string)
	for rawKey, value := range observation.Signals {
		key := strings.ToLower(rawKey)
		if !strings.HasPrefix(key, "x-codex-") {
			continue
		}
		for _, kind := range []string{"primary", "secondary"} {
			marker := "-" + kind + "-"
			index := strings.LastIndex(key, marker)
			if index < len("x-codex") {
				continue
			}
			field := key[index+len(marker):]
			if field != "used-percent" && field != "window-minutes" && field != "reset-at" && field != "reset-after-seconds" {
				continue
			}
			prefix := key[:index]
			if groups[prefix] == nil {
				groups[prefix] = make(map[string]string)
			}
			groups[prefix][http.CanonicalHeaderKey("x-codex-"+kind+"-"+field)] = value
		}
	}
	// Explicit named families are authoritative when the active/default family
	// is an alias of the same id. Values alone never establish pool identity.
	prefixes := make([]string, 0, len(groups))
	for prefix := range groups {
		prefixes = append(prefixes, prefix)
	}
	sort.Strings(prefixes)
	pools := make(map[string]CodexQuotaPool)
	for _, prefix := range prefixes {
		id := normalizeCodexQuotaPoolID(strings.TrimPrefix(prefix, "x-"))
		if prefix == "x-codex" {
			if active := quotaPoolSignal(observation, "x-codex-active-limit"); active != "" {
				id = normalizeCodexQuotaPoolID(active)
			}
		} else if explicitID := quotaPoolSignal(observation, prefix+"-limit-id"); explicitID != "" {
			id = normalizeCodexQuotaPoolID(explicitID)
		}
		if id == "" {
			continue
		}
		name := quotaPoolSignal(observation, prefix+"-limit-name")
		if name == "" {
			name = pools[id].Name
		}
		pools[id] = CodexQuotaPool{
			ID: id, Name: name,
			ObservedAt: observation.ObservedAt, Source: observation.Source, Signals: groups[prefix],
		}
	}
	result := make([]CodexQuotaPool, 0, len(pools))
	for _, pool := range pools {
		result = append(result, pool)
	}
	return result
}

func quotaPoolSignal(observation *CodexQuotaObservation, key string) string {
	return observation.Signals[http.CanonicalHeaderKey(key)]
}

func normalizeCodexQuotaPoolID(id string) string {
	id = strings.ToLower(strings.TrimSpace(id))
	if !validCodexQuotaSignalKey(id) || len(id) > 128 {
		return ""
	}
	// The default HTTP header family uses premium for the shared Codex quota.
	if id == "premium" {
		return "codex"
	}
	return strings.ReplaceAll(id, "-", "_")
}
