package config

import (
	"fmt"
	"strings"
)

type ResponseModelRewriteConfig struct {
	Enabled bool                       `yaml:"enabled" json:"enabled"`
	Rules   []ResponseModelRewriteRule `yaml:"rules" json:"rules"`
}

type ResponseModelRewriteRule struct {
	Providers      []string `yaml:"providers" json:"providers,omitempty"`
	AuthPriorities []int    `yaml:"auth-priorities" json:"auth-priorities,omitempty"`
	CredentialIDs  []string `yaml:"credential-ids" json:"credential-ids,omitempty"`
	RequestModels  []string `yaml:"request-models" json:"request-models,omitempty"`
}

func (cfg *Config) ValidateResponseModelRewrite() error {
	if cfg == nil {
		return nil
	}
	if len(cfg.ResponseModelRewrite.Rules) > 128 {
		return fmt.Errorf("response-model-rewrite supports at most 128 rules")
	}
	for i, rule := range cfg.ResponseModelRewrite.Rules {
		if len(rule.AuthPriorities) > 128 {
			return fmt.Errorf("response-model-rewrite rule %d has too many priorities", i+1)
		}
		for _, list := range [][]string{rule.Providers, rule.CredentialIDs, rule.RequestModels} {
			if len(list) > 128 {
				return fmt.Errorf("response-model-rewrite rule %d has too many matchers", i+1)
			}
			for _, value := range list {
				if strings.TrimSpace(value) == "" || len(value) > 256 || strings.ContainsAny(value, "\r\n\x00") {
					return fmt.Errorf("response-model-rewrite rule %d has an invalid matcher", i+1)
				}
			}
		}
		for _, priority := range rule.AuthPriorities {
			if int64(priority) < -APIKeyPriorityLimit || int64(priority) > APIKeyPriorityLimit {
				return fmt.Errorf("response-model-rewrite rule %d has an invalid priority", i+1)
			}
		}
	}
	return nil
}
