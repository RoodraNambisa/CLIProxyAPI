package config

import (
	"fmt"
	"math"
	"strings"

	"gopkg.in/yaml.v3"
)

// CodexQuotaAutoDisableConfig acts only on newly observed main-pool quota.
type CodexQuotaAutoDisableConfig struct {
	Enabled bool                        `yaml:"enabled" json:"enabled"`
	Rules   []CodexQuotaAutoDisableRule `yaml:"rules" json:"rules"`
}

type CodexQuotaAutoDisableRule struct {
	Providers                []string           `yaml:"providers" json:"providers,omitempty"`
	AuthPriorities           APIKeyPriorityList `yaml:"auth-priorities" json:"auth-priorities,omitempty"`
	CredentialIDs            []string           `yaml:"credential-ids" json:"credential-ids,omitempty"`
	WeeklyRemainingPercent   *float64           `yaml:"weekly-remaining-percent" json:"weekly-remaining-percent"`
	FiveHourRemainingPercent *float64           `yaml:"five-hour-remaining-percent" json:"five-hour-remaining-percent"`
}

func (cfg *CodexQuotaAutoDisableConfig) UnmarshalYAML(node *yaml.Node) error {
	value, err := credentialYAMLField(node, "enabled", make(map[*yaml.Node]bool))
	if err != nil {
		return err
	}
	if value != nil && value.Tag != "!!null" && (value.Kind != yaml.ScalarNode || value.Tag != "!!bool") {
		return fmt.Errorf("codex.quota-auto-disable.enabled must be a boolean")
	}
	type plain CodexQuotaAutoDisableConfig
	var decoded plain
	if err := node.Decode(&decoded); err != nil {
		return err
	}
	*cfg = CodexQuotaAutoDisableConfig(decoded)
	return nil
}

func (cfg *Config) ValidateCodexQuotaAutoDisable() error {
	if cfg == nil {
		return nil
	}
	if len(cfg.Codex.QuotaAutoDisable.Rules) > 128 {
		return fmt.Errorf("codex.quota-auto-disable supports at most 128 rules")
	}
	for i, rule := range cfg.Codex.QuotaAutoDisable.Rules {
		invalid := func(detail string) error {
			return fmt.Errorf("codex.quota-auto-disable rule %d: %s", i+1, detail)
		}
		if rule.WeeklyRemainingPercent == nil && rule.FiveHourRemainingPercent == nil {
			return invalid("at least one remaining-percent threshold is required")
		}
		for _, threshold := range []*float64{rule.WeeklyRemainingPercent, rule.FiveHourRemainingPercent} {
			if threshold != nil && (math.IsNaN(*threshold) || math.IsInf(*threshold, 0) || *threshold < 0 || *threshold > 100) {
				return invalid("remaining-percent must be between 0 and 100")
			}
		}
		for _, list := range [][]string{rule.Providers, rule.CredentialIDs} {
			if len(list) > 128 {
				return invalid("too many matchers")
			}
			for _, value := range list {
				if strings.TrimSpace(value) == "" || len(value) > 256 || strings.ContainsAny(value, "\r\n\x00") {
					return invalid("invalid matcher")
				}
			}
		}
		if len(rule.AuthPriorities) > 128 {
			return invalid("too many priorities")
		}
		for _, priority := range rule.AuthPriorities {
			if int64(priority) < -APIKeyPriorityLimit || int64(priority) > APIKeyPriorityLimit {
				return invalid("invalid priority")
			}
		}
	}
	return nil
}
