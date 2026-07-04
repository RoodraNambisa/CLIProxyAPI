package auth

import (
	"strconv"
	"strings"

	internalcodex "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

// AuthModelExclusionRuleMatches reports whether a rule matches non-secret auth metadata.
func AuthModelExclusionRuleMatches(rule internalconfig.AuthModelExclusionRule, auth *Auth, provider string) bool {
	if auth == nil || (len(rule.Models) == 0 && !rule.DisableImageGeneration) {
		return false
	}
	if len(rule.Providers) > 0 {
		providerKey := strings.ToLower(strings.TrimSpace(provider))
		authProvider := strings.ToLower(strings.TrimSpace(auth.Provider))
		matched := false
		for _, item := range rule.Providers {
			item = strings.ToLower(strings.TrimSpace(item))
			if item != "" && (item == providerKey || item == authProvider) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	if len(rule.Priorities) > 0 {
		priority, ok := authPriorityForModelExclusion(auth)
		if !ok {
			return false
		}
		matched := false
		for _, item := range rule.Priorities {
			if item == priority {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	if len(rule.KeywordContains) > 0 {
		haystack := authModelExclusionKeywordHaystack(auth)
		if haystack == "" {
			return false
		}
		matched := false
		for _, keyword := range rule.KeywordContains {
			keyword = strings.ToLower(strings.TrimSpace(keyword))
			if keyword != "" && strings.Contains(haystack, keyword) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return len(rule.Providers) > 0 || len(rule.Priorities) > 0 || len(rule.KeywordContains) > 0
}

// AuthDisablesImageGeneration reports whether image_generation should be disabled for this Codex auth.
func AuthDisablesImageGeneration(cfg *internalconfig.Config, auth *Auth, provider string) bool {
	if cfg == nil || auth == nil || len(cfg.AuthModelExclusions) == 0 {
		return false
	}
	if !isCodexProvider(auth, provider) {
		return false
	}
	for _, rule := range cfg.AuthModelExclusions {
		if rule.DisableImageGeneration && AuthModelExclusionRuleMatches(rule, auth, provider) {
			return true
		}
	}
	return false
}

func isCodexProvider(auth *Auth, provider string) bool {
	if strings.EqualFold(strings.TrimSpace(provider), "codex") {
		return true
	}
	return auth != nil && strings.EqualFold(strings.TrimSpace(auth.Provider), "codex")
}

func authPriorityForModelExclusion(auth *Auth) (int, bool) {
	if auth == nil {
		return 0, false
	}
	if auth.Attributes != nil {
		if value := strings.TrimSpace(auth.Attributes["priority"]); value != "" {
			if parsed, err := strconv.Atoi(value); err == nil {
				return parsed, true
			}
		}
	}
	if auth.Metadata == nil {
		return 0, false
	}
	switch value := auth.Metadata["priority"].(type) {
	case int:
		return value, true
	case int64:
		return int(value), true
	case float64:
		return int(value), true
	case string:
		parsed, err := strconv.Atoi(strings.TrimSpace(value))
		if err == nil {
			return parsed, true
		}
	}
	return 0, false
}

func authModelExclusionKeywordHaystack(auth *Auth) string {
	if auth == nil {
		return ""
	}
	values := []string{auth.ID, auth.FileName, auth.Provider, auth.Label}
	if priority, ok := authPriorityForModelExclusion(auth); ok {
		values = append(values, strconv.Itoa(priority))
	}
	if auth.Attributes != nil {
		for _, key := range []string{"email", "account", "plan_type", "path", "priority"} {
			values = append(values, auth.Attributes[key])
		}
	}
	if auth.Metadata != nil {
		for _, key := range []string{"email", "account", "plan_type", "path", "priority"} {
			switch value := auth.Metadata[key].(type) {
			case string:
				values = append(values, value)
			case float64:
				values = append(values, strconv.Itoa(int(value)))
			case int:
				values = append(values, strconv.Itoa(value))
			case int64:
				values = append(values, strconv.Itoa(int(value)))
			}
		}
		if planType := strings.TrimSpace(internalcodex.EffectivePlanType(auth.Metadata)); planType != "" {
			values = append(values, planType)
		}
	}
	if accountType, account := auth.AccountInfo(); !strings.EqualFold(accountType, "api_key") {
		values = append(values, accountType, account)
	}
	var b strings.Builder
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(value)
	}
	return b.String()
}
