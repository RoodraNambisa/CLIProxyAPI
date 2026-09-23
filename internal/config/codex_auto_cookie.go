package config

import (
	"fmt"
	"slices"
)

func (c CodexConfig) OverridesAutoCookie() bool {
	return c.AutoCookieOverride == nil || *c.AutoCookieOverride
}

// CookieOnlyConflict returns the first enabled strategy layer that can own a
// managed Cookie pool. Disabled and skip rules never conflict with passive cookies.
func (c CodexStateOverrideConfig) CookieOnlyConflict() string {
	if !c.Enabled {
		return ""
	}
	global := func(model string) string {
		strategy := c.Strategy
		for _, item := range c.ModelOverrides {
			if item.Model == model && item.Strategy != nil {
				strategy = *item.Strategy
				break
			}
		}
		return strategy
	}
	models := func(scope []string) []string {
		out := slices.Clone(scope)
		for _, item := range c.ModelOverrides {
			if !slices.Contains(out, item.Model) {
				out = append(out, item.Model)
			}
		}
		if len(scope) == 0 {
			out = append(out, "")
		}
		return out
	}
	if c.Rules == nil {
		for _, model := range models(c.Models) {
			if global(model) == "cookie-only" {
				if model != "" {
					return "model-overrides (" + model + ")"
				}
				return "strategy"
			}
		}
		return ""
	}
	for i, rule := range *c.Rules {
		if rule.Enabled != nil && !*rule.Enabled || rule.Action == "skip" {
			continue
		}
		candidates := models(rule.Models)
		for _, item := range rule.ModelOverrides {
			if item.Enabled == nil || *item.Enabled {
				candidates = append(candidates, item.Models...)
			}
		}
		for _, model := range candidates {
			strategy := global(model)
			if rule.Settings.Strategy != nil {
				strategy = *rule.Settings.Strategy
			}
			for _, item := range rule.ModelOverrides {
				if item.Enabled != nil && !*item.Enabled || !slices.Contains(item.Models, model) {
					continue
				}
				if item.Settings.Strategy != nil {
					strategy = *item.Settings.Strategy
				}
				break
			}
			if strategy == "cookie-only" {
				return fmt.Sprintf("rules[%d] (%s)", i, rule.ID)
			}
		}
	}
	return ""
}

func (cfg *Config) ValidateCodexAutoCookie() error {
	if cfg != nil && cfg.Codex.AutoCookie {
		if source := cfg.Codex.StateOverride.CookieOnlyConflict(); source != "" {
			return fmt.Errorf("codex.auto-cookie conflicts with codex.state-override.%s: disable Cookie-only management before enabling automatic cookies", source)
		}
	}
	return nil
}
