package config

import (
	"fmt"
	"sort"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
)

const (
	DefaultProxyPoolCheckIntervalSeconds = 300
	DefaultProxyPoolBindAttempts         = 3
	MaxProxyPoolBindAttempts             = 20
)

// NormalizeProxyConfiguration validates and canonicalizes proxy pools and rules.
func NormalizeProxyConfiguration(pools []ProxyPoolConfig, rules []ProxyRuleConfig) ([]ProxyPoolConfig, []ProxyRuleConfig, error) {
	normalizedPools := make([]ProxyPoolConfig, 0, len(pools))
	poolNames := make(map[string]string, len(pools))
	for index, rawPool := range pools {
		pool := rawPool
		pool.Name = strings.TrimSpace(pool.Name)
		if pool.Name == "" {
			return nil, nil, fmt.Errorf("proxy-pools[%d].name is required", index)
		}
		if !validProxyPoolName(pool.Name) {
			return nil, nil, fmt.Errorf("proxy-pools[%d].name contains unsupported path characters", index)
		}
		poolKey := strings.ToLower(pool.Name)
		if _, duplicate := poolNames[poolKey]; duplicate {
			return nil, nil, fmt.Errorf("proxy-pools contains duplicate name %q", pool.Name)
		}
		poolNames[poolKey] = pool.Name

		charset, errCharset := proxyutil.NormalizePlaceholderCharset(pool.PlaceholderCharset)
		if errCharset != nil {
			return nil, nil, fmt.Errorf("proxy-pools[%d].placeholder-charset: %w", index, errCharset)
		}
		pool.PlaceholderCharset = charset
		if pool.CheckIntervalSeconds < 0 {
			return nil, nil, fmt.Errorf("proxy-pools[%d].check-interval-seconds cannot be negative", index)
		}
		if pool.CheckIntervalSeconds == 0 {
			pool.CheckIntervalSeconds = DefaultProxyPoolCheckIntervalSeconds
		}
		if pool.BindAttempts < 0 {
			return nil, nil, fmt.Errorf("proxy-pools[%d].bind-attempts cannot be negative", index)
		}
		if pool.BindAttempts == 0 {
			pool.BindAttempts = DefaultProxyPoolBindAttempts
		}
		if pool.BindAttempts > MaxProxyPoolBindAttempts {
			return nil, nil, fmt.Errorf("proxy-pools[%d].bind-attempts must be at most %d", index, MaxProxyPoolBindAttempts)
		}
		if len(pool.Entries) == 0 {
			return nil, nil, fmt.Errorf("proxy-pools[%d].entries cannot be empty", index)
		}
		entries := make([]ProxyPoolEntryConfig, 0, len(pool.Entries))
		entryIDs := make(map[string]struct{}, len(pool.Entries))
		for entryIndex, rawEntry := range pool.Entries {
			entry := rawEntry
			entry.ID = strings.TrimSpace(entry.ID)
			if entry.ID == "" {
				return nil, nil, fmt.Errorf("proxy-pools[%d].entries[%d].id is required", index, entryIndex)
			}
			entryKey := strings.ToLower(entry.ID)
			if _, duplicate := entryIDs[entryKey]; duplicate {
				return nil, nil, fmt.Errorf("proxy-pools[%d] contains duplicate entry id %q", index, entry.ID)
			}
			entryIDs[entryKey] = struct{}{}
			urlTemplate, ports, errTemplate := proxyutil.ValidateURLTemplate(entry.URLTemplate, entry.Ports, pool.PlaceholderCharset)
			if errTemplate != nil {
				return nil, nil, fmt.Errorf("proxy-pools[%d].entries[%d]: %w", index, entryIndex, errTemplate)
			}
			entry.URLTemplate = urlTemplate
			entry.Ports = ports
			entries = append(entries, entry)
		}
		pool.Entries = entries
		normalizedPools = append(normalizedPools, pool)
	}

	normalizedRules := make([]ProxyRuleConfig, 0, len(rules))
	ruleNames := make(map[string]struct{}, len(rules))
	for index, rawRule := range rules {
		rule := rawRule
		rule.Name = strings.TrimSpace(rule.Name)
		if rule.Name == "" {
			return nil, nil, fmt.Errorf("proxy-rules[%d].name is required", index)
		}
		ruleKey := strings.ToLower(rule.Name)
		if _, duplicate := ruleNames[ruleKey]; duplicate {
			return nil, nil, fmt.Errorf("proxy-rules contains duplicate name %q", rule.Name)
		}
		ruleNames[ruleKey] = struct{}{}
		poolKey := strings.ToLower(strings.TrimSpace(rule.Pool))
		canonicalPool, exists := poolNames[poolKey]
		if !exists {
			return nil, nil, fmt.Errorf("proxy-rules[%d] references unknown pool %q", index, rule.Pool)
		}
		rule.Pool = canonicalPool
		rawProviderCount := len(rule.Providers)
		rule.Providers = normalizeLowerStrings(rule.Providers)
		if rawProviderCount > 0 && len(rule.Providers) == 0 {
			return nil, nil, fmt.Errorf("proxy-rules[%d].providers cannot contain only empty values", index)
		}
		rule.Priorities = normalizePriorities(rule.Priorities)
		normalizedRules = append(normalizedRules, rule)
	}
	return normalizedPools, normalizedRules, nil
}

func validProxyPoolName(name string) bool {
	if len(name) > 128 || strings.ContainsAny(name, "/?#") {
		return false
	}
	for _, ch := range name {
		if ch < 0x20 || ch == 0x7f {
			return false
		}
	}
	return true
}

// NormalizeProxyConfiguration validates and stores proxy pools and rules.
func (cfg *Config) NormalizeProxyConfiguration() error {
	if cfg == nil {
		return nil
	}
	pools, rules, err := NormalizeProxyConfiguration(cfg.ProxyPools, cfg.ProxyRules)
	if err != nil {
		return err
	}
	cfg.ProxyPools = pools
	cfg.ProxyRules = rules
	return nil
}

// MatchProxyRule returns the first configured pool matching provider and priority.
func MatchProxyRule(rules []ProxyRuleConfig, provider string, priority int) (string, bool) {
	provider = strings.ToLower(strings.TrimSpace(provider))
	for _, rule := range rules {
		if len(rule.Providers) > 0 && !containsString(rule.Providers, provider) {
			continue
		}
		if len(rule.Priorities) > 0 && !containsInt(rule.Priorities, priority) {
			continue
		}
		return rule.Pool, true
	}
	return "", false
}

func normalizeLowerStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, raw := range values {
		value := strings.ToLower(strings.TrimSpace(raw))
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func normalizePriorities(values []int) []int {
	seen := make(map[int]struct{}, len(values))
	out := make([]int, 0, len(values))
	for _, value := range values {
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(out)))
	return out
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func containsInt(values []int, target int) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
