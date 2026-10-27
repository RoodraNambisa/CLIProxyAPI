package config

import (
	"fmt"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
)

// CodexServiceTierRule overrides client-visible choices for exact model IDs.
// A single wildcard model provides a fallback; exact model rules take precedence.
type CodexServiceTierRule struct {
	Models  []string `yaml:"models" json:"models"`
	Tiers   []string `yaml:"tiers" json:"tiers"`
	Default string   `yaml:"default" json:"default"`
}

// NormalizeCodexServiceTier keeps Fast compatible with the official Codex client.
func NormalizeCodexServiceTier(tier string) string {
	tier = strings.ToLower(strings.TrimSpace(tier))
	if tier == "fast" {
		return "priority"
	}
	return tier
}

func (cfg *Config) ValidateCodexServiceTiers() error {
	if cfg == nil {
		return nil
	}
	if len(cfg.Codex.ServiceTierRules) > 64 {
		return fmt.Errorf("codex.service-tier-rules supports at most 64 rules")
	}
	seen := make(map[string]bool)
	for index, rule := range cfg.Codex.ServiceTierRules {
		field := fmt.Sprintf("codex.service-tier-rules[%d]", index)
		if len(rule.Models) == 0 || len(rule.Models) > 64 {
			return fmt.Errorf("%s.models must contain 1–64 model IDs", field)
		}
		for _, raw := range rule.Models {
			model := strings.TrimSpace(raw)
			if model == "" || len(model) > 256 || strings.ContainsAny(model, " \t\r\n\x00?[]\\") || (strings.Contains(model, "*") && model != "*") || seen[model] {
				return fmt.Errorf("%s.models must contain unique exact model IDs or *", field)
			}
			seen[model] = true
		}
		tiers := make(map[string]bool)
		for _, raw := range rule.Tiers {
			tier := NormalizeCodexServiceTier(raw)
			if (tier != "priority" && tier != "ultrafast" && tier != "flex") || tiers[tier] {
				return fmt.Errorf("%s.tiers must contain unique priority, ultrafast or flex values", field)
			}
			tiers[tier] = true
		}
		defaultTier := NormalizeCodexServiceTier(rule.Default)
		if defaultTier != "" && defaultTier != "default" && !tiers[defaultTier] {
			return fmt.Errorf("%s.default must be default or one of the configured tiers", field)
		}
	}
	return nil
}

// CloneCodexServiceTierRules isolates handler configuration from hot updates.
func CloneCodexServiceTierRules(rules []CodexServiceTierRule) []CodexServiceTierRule {
	if rules == nil {
		return nil
	}
	out := make([]CodexServiceTierRule, len(rules))
	for index, rule := range rules {
		out[index] = rule
		out[index].Models = slices.Clone(rule.Models)
		out[index].Tiers = slices.Clone(rule.Tiers)
	}
	return out
}

// CodexServiceTierRuleForModel matches the client-visible model, not an alias target.
func CodexServiceTierRuleForModel(rules []CodexServiceTierRule, model string) *CodexServiceTierRule {
	model = strings.TrimSpace(model)
	var selected *CodexServiceTierRule
	for index := range rules {
		for _, pattern := range rules[index].Models {
			pattern = strings.TrimSpace(pattern)
			if pattern == model {
				return cloneCodexServiceTierRule(rules[index])
			}
			if pattern == "*" {
				selected = &rules[index]
			}
		}
	}
	if selected == nil {
		return nil
	}
	return cloneCodexServiceTierRule(*selected)
}

func cloneCodexServiceTierRule(rule CodexServiceTierRule) *CodexServiceTierRule {
	rule.Models = slices.Clone(rule.Models)
	rule.Tiers = slices.Clone(rule.Tiers)
	for index := range rule.Tiers {
		rule.Tiers[index] = NormalizeCodexServiceTier(rule.Tiers[index])
	}
	rule.Default = NormalizeCodexServiceTier(rule.Default)
	return &rule
}

// AllowsClientTier always permits explicit Standard. Auto cannot bypass an explicit allowlist.
func (rule *CodexServiceTierRule) AllowsClientTier(tier string) bool {
	if rule == nil {
		return true
	}
	tier = NormalizeCodexServiceTier(tier)
	return tier == "default" || slices.Contains(rule.Tiers, tier)
}

// Reject malformed policy types even when the caller uses optional config loading.
func validateCodexServiceTiersYAML(data []byte) error {
	var document yaml.Node
	if err := yaml.Unmarshal(data, &document); err != nil || len(document.Content) == 0 {
		return nil
	}
	codex, err := credentialYAMLField(document.Content[0], "codex", make(map[*yaml.Node]bool))
	if err != nil {
		return err
	}
	rules, err := credentialYAMLField(codex, "service-tier-rules", make(map[*yaml.Node]bool))
	if err != nil || rules == nil {
		return err
	}
	cfg := &Config{}
	if err := rules.Decode(&cfg.Codex.ServiceTierRules); err != nil {
		return fmt.Errorf("codex.service-tier-rules must be a list of model tier rules: %w", err)
	}
	return cfg.ValidateCodexServiceTiers()
}

// Match by model scope so editing a tier/default preserves the rule's comments and extensions.
func matchCodexServiceTierYAMLRule(original []*yaml.Node, used []bool, target *yaml.Node) int {
	identity := func(node *yaml.Node) string {
		var rule CodexServiceTierRule
		if node == nil || node.Decode(&rule) != nil || len(rule.Models) == 0 {
			return ""
		}
		models := slices.Clone(rule.Models)
		for i := range models {
			models[i] = strings.TrimSpace(models[i])
		}
		slices.Sort(models)
		return strings.Join(models, "\x00")
	}
	wanted := identity(target)
	for index, node := range original {
		if !used[index] && wanted != "" && identity(node) == wanted {
			return index
		}
	}
	return -1
}
