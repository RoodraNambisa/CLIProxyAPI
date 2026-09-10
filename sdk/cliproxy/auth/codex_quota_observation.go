package auth

import (
	"maps"
	"net/http"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	maxCodexQuotaSignals    = 64
	maxCodexQuotaSignalKey  = 256
	maxCodexQuotaSignalText = 512
)

// CodexQuotaObservation is passive upstream information, never scheduler state.
// It must not be serialized into credential or cooldown storage.
type CodexQuotaObservation struct {
	ObservedAt time.Time         `json:"observed_at"`
	Source     string            `json:"source"`
	Signals    map[string]string `json:"signals"`
}

// Clone detaches the signal map before a snapshot leaves its owner.
func (q *CodexQuotaObservation) Clone() *CodexQuotaObservation {
	if q == nil {
		return nil
	}
	copy := *q
	copy.Signals = maps.Clone(q.Signals)
	return &copy
}

// CodexQuotaSnapshot returns detached passive information for management views.
func (a *Auth) CodexQuotaSnapshot() *CodexQuotaObservation {
	if a == nil {
		return nil
	}
	return a.codexQuotaObservation.Clone()
}

// recordCodexQuotaObservation accepts only the currently installed credential
// instance. It deliberately bypasses persistence, hooks and scheduler updates.
func (m *Manager) recordCodexQuotaObservation(authID, instanceID, source string, headers http.Header, observedAt time.Time) bool {
	if m == nil || authID == "" || instanceID == "" {
		return false
	}
	observation := newCodexQuotaObservation(headers, source, observedAt)
	if observation == nil || observedAt.IsZero() {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	auth := m.auths[strings.TrimSpace(authID)]
	if auth == nil || auth.instanceID != instanceID || auth.RuntimeInstanceRetired() || !strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
		return false
	}
	if previous := auth.codexQuotaObservation; previous != nil && observedAt.Before(previous.ObservedAt) {
		return false
	}
	auth.codexQuotaObservation = observation
	return true
}

func newCodexQuotaObservation(headers http.Header, source string, observedAt time.Time) *CodexQuotaObservation {
	if source != "http" && source != "websocket" {
		return nil
	}
	signals := collectCodexQuotaSignals(headers)
	if len(signals) == 0 {
		return nil
	}
	return &CodexQuotaObservation{ObservedAt: observedAt, Source: source, Signals: signals}
}

func collectCodexQuotaSignals(headers http.Header) map[string]string {
	keys := make([]string, 0, len(headers))
	for key, values := range headers {
		if len(values) == 0 || !validCodexQuotaSignalKey(key) || !isCodexQuotaSignalHeader(key) {
			continue
		}
		keys = append(keys, key)
	}
	// Raw maps can contain differently cased duplicate keys. Prefer canonical
	// spelling and then lexical order so map iteration never chooses a value.
	sort.Slice(keys, func(i, j int) bool {
		iCanonical := keys[i] == http.CanonicalHeaderKey(keys[i])
		jCanonical := keys[j] == http.CanonicalHeaderKey(keys[j])
		if iCanonical != jCanonical {
			return iCanonical
		}
		return keys[i] < keys[j]
	})
	values := make(map[string]string, len(keys))
	names := make([]string, 0, len(keys))
	for _, key := range keys {
		canonical := http.CanonicalHeaderKey(key)
		if _, exists := values[canonical]; exists {
			continue
		}
		headerValues := headers[key]
		value := headerValues[len(headerValues)-1]
		if !validCodexQuotaSignalText(value) {
			continue
		}
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		names = append(names, canonical)
		values[canonical] = strings.Clone(value)
	}
	if len(names) == 0 {
		return nil
	}
	sort.Slice(names, func(i, j int) bool {
		left, right := codexQuotaSignalRank(names[i]), codexQuotaSignalRank(names[j])
		if left != right {
			return left < right
		}
		return names[i] < names[j]
	})
	if len(names) > maxCodexQuotaSignals {
		names = names[:maxCodexQuotaSignals]
	}
	signals := make(map[string]string, len(names))
	for _, name := range names {
		signals[strings.Clone(name)] = values[name]
	}
	return signals
}

func validCodexQuotaSignalKey(key string) bool {
	if key == "" || len(key) > maxCodexQuotaSignalKey {
		return false
	}
	for _, char := range key {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '-' || char == '_' || char == '.' {
			continue
		}
		return false
	}
	return true
}

func validCodexQuotaSignalText(value string) bool {
	if value == "" || len(value) > maxCodexQuotaSignalText || !utf8.ValidString(value) {
		return false
	}
	for _, char := range value {
		if char < 0x20 || char == 0x7f {
			return false
		}
	}
	return true
}

func isCodexQuotaSignalHeader(name string) bool {
	lower := strings.ToLower(name)
	if lower == "retry-after" || strings.HasPrefix(lower, "x-ratelimit-") {
		return true
	}
	switch lower {
	case "x-codex-plan-type", "x-codex-active-limit", "x-codex-credits-has-credits", "x-codex-credits-unlimited", "x-codex-credits-balance":
		return true
	}
	if !strings.HasPrefix(lower, "x-codex-") {
		return false
	}
	for _, suffix := range []string{"-allowed", "-limit-reached", "-limit-name", "-used-percent", "-window-minutes", "-reset-after-seconds", "-reset-at", "-over-secondary-limit-percent"} {
		if strings.HasSuffix(lower, suffix) {
			return true
		}
	}
	return false
}

func codexQuotaSignalRank(name string) int {
	lower := strings.ToLower(name)
	switch {
	case lower == "retry-after":
		return 0
	case lower == "x-codex-plan-type", lower == "x-codex-active-limit", strings.HasPrefix(lower, "x-codex-credits-"):
		return 1
	case lower == "x-codex-allowed", lower == "x-codex-limit-reached", strings.HasPrefix(lower, "x-codex-primary-"), strings.HasPrefix(lower, "x-codex-secondary-"):
		return 2
	case strings.HasPrefix(lower, "x-codex-code-review-"):
		return 3
	case strings.HasPrefix(lower, "x-codex-additional-"):
		return 5
	case strings.HasPrefix(lower, "x-codex-"):
		return 4
	default:
		return 6
	}
}
