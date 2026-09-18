package config

import (
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// CodexStateOverrideConfig manages short-lived state in memory only.
type CodexStateOverrideConfig struct {
	Enabled              bool                      `yaml:"enabled" json:"enabled"`
	Priorities           APIKeyPriorityList        `yaml:"priorities" json:"priorities"`
	ExcludedCredentials  []string                  `yaml:"excluded-credentials" json:"excluded-credentials"`
	Models               []string                  `yaml:"models" json:"models"`
	Mode                 string                    `yaml:"mode" json:"mode"`
	MissingPolicy        string                    `yaml:"missing-policy" json:"missing-policy"`
	Acquisition          string                    `yaml:"acquisition" json:"acquisition"`
	ActiveMinutes        int                       `yaml:"active-minutes" json:"active-minutes"`
	TTLMinutes           int                       `yaml:"ttl-minutes" json:"ttl-minutes"`
	RefreshBeforeMinutes int                       `yaml:"refresh-before-minutes" json:"refresh-before-minutes"`
	Concurrency          int                       `yaml:"concurrency" json:"concurrency"`
	RetrySeconds         int                       `yaml:"retry-seconds" json:"retry-seconds"`
	MaxAttempts          int                       `yaml:"max-attempts" json:"max-attempts"`
	ProxyMode            string                    `yaml:"proxy-mode" json:"proxy-mode"`
	ProxyURL             string                    `yaml:"proxy-url" json:"proxy-url"`
	Lengths              []int                     `yaml:"lengths" json:"lengths"`
	MatchModel           *bool                     `yaml:"match-model,omitempty" json:"match-model,omitempty"`
	Prompt               string                    `yaml:"prompt" json:"prompt"`
	ResponseContains     string                    `yaml:"response-contains" json:"response-contains"`
	ErrorType            string                    `yaml:"error-type" json:"error-type"`
	ErrorCode            string                    `yaml:"error-code" json:"error-code"`
	ErrorMessage         string                    `yaml:"error-message" json:"error-message"`
	ModelOverrides       []CodexStateModelOverride `yaml:"model-overrides" json:"model-overrides"`
}

type CodexStateModelOverride struct {
	Model            string  `yaml:"model" json:"model"`
	Lengths          []int   `yaml:"lengths" json:"lengths"`
	MatchModel       *bool   `yaml:"match-model,omitempty" json:"match-model,omitempty"`
	Prompt           string  `yaml:"prompt" json:"prompt"`
	ResponseContains *string `yaml:"response-contains,omitempty" json:"response-contains,omitempty"`
}

// ForModel applies only validation/probe overrides; selection is still catalog-scoped.
func (c CodexStateOverrideConfig) ForModel(model string) CodexStateOverrideConfig {
	c = c.Resolved()
	for _, v := range c.ModelOverrides {
		if v.Model == model {
			if v.Lengths != nil {
				c.Lengths = slices.Clone(v.Lengths)
			}
			if v.MatchModel != nil {
				yes := *v.MatchModel
				c.MatchModel = &yes
			}
			if v.Prompt != "" {
				c.Prompt = v.Prompt
			}
			if v.ResponseContains != nil {
				c.ResponseContains = *v.ResponseContains
			}
			break
		}
	}
	return c
}

func (c CodexStateOverrideConfig) Resolved() CodexStateOverrideConfig {
	c.ModelOverrides = slices.Clone(c.ModelOverrides)
	for i, v := range c.ModelOverrides {
		c.ModelOverrides[i].Lengths = slices.Clone(v.Lengths)
		if v.MatchModel != nil {
			b := *v.MatchModel
			c.ModelOverrides[i].MatchModel = &b
		}
		if v.ResponseContains != nil {
			s := *v.ResponseContains
			c.ModelOverrides[i].ResponseContains = &s
		}
	}
	c.Priorities = slices.Clone(c.Priorities)
	c.Models = slices.Clone(c.Models)
	c.ExcludedCredentials = slices.Clone(c.ExcludedCredentials)
	c.Lengths = slices.Clone(c.Lengths)
	if c.Mode == "" {
		c.Mode = "override"
	}
	if c.MissingPolicy == "" {
		c.MissingPolicy = "continue"
	}
	if c.Acquisition == "" {
		c.Acquisition = "active"
	}
	if c.ActiveMinutes == 0 {
		c.ActiveMinutes = 60
	}
	if c.TTLMinutes == 0 {
		c.TTLMinutes = 60
	}
	if c.RefreshBeforeMinutes == 0 {
		c.RefreshBeforeMinutes = 5
	}
	if c.Concurrency == 0 {
		c.Concurrency = 1
	}
	if c.RetrySeconds == 0 {
		c.RetrySeconds = 60
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = 3
	}
	if c.ProxyMode == "" {
		c.ProxyMode = "inherit"
	}
	if c.Lengths == nil {
		c.Lengths = []int{292}
	}
	if c.MatchModel == nil {
		yes := true
		c.MatchModel = &yes
	} else {
		yes := *c.MatchModel
		c.MatchModel = &yes
	}
	if c.Prompt == "" {
		c.Prompt = "Reply with exactly OK."
	}
	if c.ErrorType == "" {
		c.ErrorType = "rate_limit_exceeded"
	}
	if c.ErrorCode == "" {
		c.ErrorCode = "rate_limit_exceeded"
	}
	if c.ErrorMessage == "" {
		c.ErrorMessage = "Rate limit exceeded for image_generation. Please try again later."
	}
	return c
}

var CodexStateProxyPlaceholder = regexp.MustCompile(`\{([0-9]+)\}`)

func (cfg *Config) ValidateCodexStateOverride() error {
	if cfg == nil {
		return nil
	}
	c := cfg.Codex.StateOverride.Resolved()
	invalid := func(message string) error { return fmt.Errorf("codex.state-override: %s", message) }
	if len(c.ModelOverrides) > 256 {
		return invalid("too many model overrides")
	}
	seen := map[string]bool{}
	for _, v := range c.ModelOverrides {
		if strings.TrimSpace(v.Model) == "" || seen[v.Model] || len(v.Model) > 256 || strings.ContainsAny(v.Model, "\r\n\x00") {
			return invalid("invalid or duplicate model override")
		}
		seen[v.Model] = true
		if len(v.Lengths) > 32 || len(v.Prompt) > 4096 || (v.ResponseContains != nil && len(*v.ResponseContains) > 1024) {
			return invalid("model override too large")
		}
		for _, n := range v.Lengths {
			if n < 1 || n > 8192 {
				return invalid("model state length out of range")
			}
		}
	}
	if c.Enabled && cfg.Codex.ResolvedTurnStatePolicy() == CodexTurnStatePolicyStrip {
		return invalid("cannot enable while turn-state-policy is strip")
	}
	if !slices.Contains([]string{"override", "missing"}, c.Mode) || !slices.Contains([]string{"continue", "error"}, c.MissingPolicy) || !slices.Contains([]string{"active", "all", "manual"}, c.Acquisition) || !slices.Contains([]string{"inherit", "direct", "custom"}, c.ProxyMode) {
		return invalid("invalid mode or policy")
	}
	if c.TTLMinutes < 1 || c.TTLMinutes > 1440 || c.RefreshBeforeMinutes < 1 || c.RefreshBeforeMinutes >= c.TTLMinutes || c.ActiveMinutes < 1 || c.ActiveMinutes > 10080 || c.Concurrency < 1 || c.Concurrency > 16 || c.RetrySeconds < 1 || c.RetrySeconds > 3600 || c.MaxAttempts < 1 || c.MaxAttempts > 10 {
		return invalid("invalid lifetime, refresh, activity or acquisition limits")
	}
	if len(c.Prompt) > 4096 || len(c.ResponseContains) > 1024 || len(c.Models) > 256 || len(c.ExcludedCredentials) > 1024 || len(c.Priorities) > 128 || len(c.Lengths) > 32 {
		return invalid("too many matchers or probe text too long")
	}
	if len(c.ErrorType) > 128 || len(c.ErrorCode) > 128 || len(c.ErrorMessage) > 4096 {
		return invalid("error response too long")
	}
	for _, n := range c.Lengths {
		if n < 1 || n > 8192 {
			return invalid("state lengths must be between 1 and 8192")
		}
	}
	for _, n := range c.Priorities {
		if int64(n) < -APIKeyPriorityLimit || int64(n) > APIKeyPriorityLimit {
			return invalid("priority out of range")
		}
	}
	for _, list := range [][]string{c.Models, c.ExcludedCredentials} {
		for _, v := range list {
			if strings.TrimSpace(v) == "" || len(v) > 512 || strings.ContainsAny(v, "\r\n\x00") {
				return invalid("invalid model or credential selector")
			}
		}
	}
	if c.ProxyMode == "custom" {
		if len(c.ProxyURL) > 4096 || strings.ContainsAny(c.ProxyURL, "\r\n\x00") {
			return invalid("invalid proxy URL")
		}
		for _, match := range CodexStateProxyPlaceholder.FindAllStringSubmatch(c.ProxyURL, -1) {
			n, _ := strconv.Atoi(match[1])
			if n < 1 || n > 64 {
				return invalid("proxy placeholders support 1–64 digits")
			}
		}
		resolved := CodexStateProxyPlaceholder.ReplaceAllString(c.ProxyURL, "123")
		if strings.ContainsAny(resolved, "{}") {
			return invalid("unsupported proxy placeholder")
		}
		u, err := url.Parse(resolved)
		if err != nil || u.Hostname() == "" || !slices.Contains([]string{"http", "https", "socks5", "socks5h"}, u.Scheme) || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return invalid("invalid proxy URL")
		}
	}
	return nil
}
