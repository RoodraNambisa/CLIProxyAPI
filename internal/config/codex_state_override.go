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
	ResponseGuard              *CodexResponseGuardConfig `yaml:"-" json:"-"`
	AcceptedReturnedModels     []string                  `yaml:"-" json:"accepted_returned_models,omitempty"`
	ReturnedLengthMode         string                    `yaml:"-" json:"returned_length_mode,omitempty"`
	Strategy                   string                    `yaml:"strategy,omitempty" json:"strategy,omitempty"`
	CookieVerifyAfterAcquire   bool                      `yaml:"cookie-verify-after-acquire,omitempty" json:"cookie-verify-after-acquire,omitempty"`
	CookiePoolMode             string                    `yaml:"cookie-pool-mode,omitempty" json:"cookie-pool-mode,omitempty"`
	CookiePoolGroup            string                    `yaml:"cookie-pool-group,omitempty" json:"cookie-pool-group,omitempty"`
	CookieAcquisitionModel     string                    `yaml:"cookie-acquisition-model,omitempty" json:"cookie-acquisition-model,omitempty"`
	CookieBackupCount          int                       `yaml:"cookie-backup-count,omitempty" json:"cookie-backup-count,omitempty"`
	CookieMaxAgeSeconds        int                       `yaml:"cookie-max-age-seconds,omitempty" json:"cookie-max-age-seconds,omitempty"`
	CookieRefreshBeforeSeconds int                       `yaml:"cookie-refresh-before-seconds,omitempty" json:"cookie-refresh-before-seconds,omitempty"`
	TTLSeconds                 *int                      `yaml:"ttl-seconds,omitempty" json:"ttl-seconds,omitempty"`
	RefreshBeforeSeconds       *int                      `yaml:"refresh-before-seconds,omitempty" json:"refresh-before-seconds,omitempty"`
	MissingReturnedState       string                    `yaml:"missing-returned-state,omitempty" json:"missing-returned-state,omitempty"`

	Rules                           *[]CodexStateRule         `yaml:"rules,omitempty" json:"rules,omitempty"`
	Enabled                         bool                      `yaml:"enabled" json:"enabled"`
	Priorities                      APIKeyPriorityList        `yaml:"priorities" json:"priorities"`
	IncludedCredentials             []string                  `yaml:"included-credentials" json:"included-credentials"`
	ExcludedCredentials             []string                  `yaml:"excluded-credentials" json:"excluded-credentials"`
	Models                          []string                  `yaml:"models" json:"models"`
	Mode                            string                    `yaml:"mode" json:"mode"`
	MissingPolicy                   string                    `yaml:"missing-policy" json:"missing-policy"`
	Acquisition                     string                    `yaml:"acquisition" json:"acquisition"`
	ActiveMinutes                   int                       `yaml:"active-minutes" json:"active-minutes"`
	TTLMinutes                      int                       `yaml:"ttl-minutes" json:"ttl-minutes"`
	RefreshBeforeMinutes            int                       `yaml:"refresh-before-minutes" json:"refresh-before-minutes"`
	Concurrency                     int                       `yaml:"concurrency" json:"concurrency"`
	RetrySeconds                    int                       `yaml:"retry-seconds" json:"retry-seconds"`
	MaxAttempts                     int                       `yaml:"max-attempts" json:"max-attempts"`
	RetryRoundIntervalMinutes       int                       `yaml:"retry-round-interval-minutes" json:"retry-round-interval-minutes"`
	MaxRetryRounds                  int                       `yaml:"max-retry-rounds" json:"max-retry-rounds"`
	ProxyMode                       string                    `yaml:"proxy-mode" json:"proxy-mode"`
	ProxyURL                        string                    `yaml:"proxy-url" json:"proxy-url"`
	Lengths                         []int                     `yaml:"lengths" json:"lengths"`
	MatchModel                      *bool                     `yaml:"match-model,omitempty" json:"match-model,omitempty"`
	Prompt                          string                    `yaml:"prompt" json:"prompt"`
	ResponseContains                string                    `yaml:"response-contains" json:"response-contains"`
	ErrorType                       string                    `yaml:"error-type" json:"error-type"`
	ErrorCode                       string                    `yaml:"error-code" json:"error-code"`
	ErrorMessage                    string                    `yaml:"error-message" json:"error-message"`
	ModelOverrides                  []CodexStateModelOverride `yaml:"model-overrides" json:"model-overrides"`
	PlanLengths                     []CodexStatePlanLengths   `yaml:"plan-lengths" json:"plan-lengths"`
	InvalidateOnStateLengthMismatch bool                      `yaml:"invalidate-on-state-length-mismatch" json:"invalidate-on-state-length-mismatch"`
	InvalidateOnModelMismatch       bool                      `yaml:"invalidate-on-model-mismatch" json:"invalidate-on-model-mismatch"`
}

type CodexStatePlanLengths struct {
	PlanTypes []string `yaml:"plan-types" json:"plan-types"`
	Models    []string `yaml:"models,omitempty" json:"models,omitempty"`
	Lengths   []int    `yaml:"lengths" json:"lengths"`
}

// NormalizeCodexStatePlanType treats Business and Team as the same subscription family.
func NormalizeCodexStatePlanType(value string) string {
	value = NormalizeRoutingPlanType(value)
	if value == "business" {
		return "team"
	}
	if value == "" {
		return "unknown"
	}
	return value
}

// ForCredential prioritizes plan/model rules, then plan rules, over model/global lengths.
// Equally specific rules use the first match. Other probe settings retain their model policy.
func (c CodexStateOverrideConfig) ForCredential(plan, model string) CodexStateOverrideConfig {
	c = c.ForModel(model)
	plan = NormalizeCodexStatePlanType(plan)
	for _, specific := range []bool{true, false} {
		for _, rule := range c.PlanLengths {
			if (len(rule.Models) > 0) != specific || (specific && !slices.Contains(rule.Models, model)) {
				continue
			}
			for _, candidate := range rule.PlanTypes {
				if NormalizeCodexStatePlanType(candidate) == plan {
					c.Lengths = slices.Clone(rule.Lengths)
					return c
				}
			}
		}
	}
	return c
}

type CodexStateModelOverride struct {
	CodexStateStrategySettings `yaml:",inline"`
	Model                      string  `yaml:"model" json:"model"`
	Lengths                    []int   `yaml:"lengths" json:"lengths"`
	MatchModel                 *bool   `yaml:"match-model,omitempty" json:"match-model,omitempty"`
	Prompt                     string  `yaml:"prompt" json:"prompt"`
	ResponseContains           *string `yaml:"response-contains,omitempty" json:"response-contains,omitempty"`
}

// ForModel applies only validation/probe overrides; selection is still catalog-scoped.
func (c CodexStateOverrideConfig) ForModel(model string) CodexStateOverrideConfig {
	c = c.Resolved()
	for _, v := range c.ModelOverrides {
		if v.Model == model {
			applyStateStrategySettings(&c, v.CodexStateStrategySettings, nil, "model-override")
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

func (c CodexStateOverrideConfig) clone() CodexStateOverrideConfig {
	c.AcceptedReturnedModels = slices.Clone(c.AcceptedReturnedModels)
	if c.ResponseGuard != nil {
		value := c.ResponseGuard.Clone()
		c.ResponseGuard = &value
	}
	c.Rules = cloneCodexStateRules(c.Rules)
	c.TTLSeconds = cloneStateValue(c.TTLSeconds)
	c.RefreshBeforeSeconds = cloneStateValue(c.RefreshBeforeSeconds)
	c.PlanLengths = slices.Clone(c.PlanLengths)
	for i, v := range c.PlanLengths {
		c.PlanLengths[i].PlanTypes = slices.Clone(v.PlanTypes)
		c.PlanLengths[i].Models = slices.Clone(v.Models)
		c.PlanLengths[i].Lengths = slices.Clone(v.Lengths)
	}
	c.ModelOverrides = slices.Clone(c.ModelOverrides)
	for i, v := range c.ModelOverrides {
		c.ModelOverrides[i].Lengths = slices.Clone(v.Lengths)
		c.ModelOverrides[i].CodexStateStrategySettings = v.CodexStateStrategySettings.clone()
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
	c.IncludedCredentials = slices.Clone(c.IncludedCredentials)
	c.ExcludedCredentials = slices.Clone(c.ExcludedCredentials)
	c.Lengths = slices.Clone(c.Lengths)
	if c.MatchModel != nil {
		value := *c.MatchModel
		c.MatchModel = &value
	}
	return c
}

func (c CodexStateOverrideConfig) Resolved() CodexStateOverrideConfig {
	c = c.clone()
	if c.CookiePoolMode == "" {
		c.CookiePoolMode = "auto"
	}
	if c.Strategy == "" {
		c.Strategy = "state"
	}
	if c.MissingReturnedState == "" {
		c.MissingReturnedState = "ignore"
	}
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
	if c.RetryRoundIntervalMinutes == 0 {
		c.RetryRoundIntervalMinutes = 30
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
	if err := cfg.ValidateCodexAutoCookie(); err != nil {
		return err
	}
	if cfg == nil {
		return nil
	}
	c := cfg.Codex.StateOverride.Resolved()
	invalid := func(message string) error { return fmt.Errorf("codex.state-override: %s", message) }
	if len(c.PlanLengths) > 64 {
		return invalid("too many plan length rules")
	}
	for _, rule := range c.PlanLengths {
		if len(rule.PlanTypes) == 0 || len(rule.PlanTypes) > 32 || len(rule.Models) > 256 || rule.Lengths == nil || len(rule.Lengths) > 32 {
			return invalid("plan length rules require plan-types and a lengths array")
		}
		for _, values := range [][]string{rule.PlanTypes, rule.Models} {
			for _, value := range values {
				if strings.TrimSpace(value) == "" || value != strings.TrimSpace(value) || len(value) > 256 || strings.ContainsAny(value, "\r\n\x00") {
					return invalid("invalid plan type or model in plan length rule")
				}
			}
		}
		for _, length := range rule.Lengths {
			if length < 1 || length > 8192 {
				return invalid("plan state length out of range")
			}
		}
	}
	if len(c.ModelOverrides) > 256 {
		return invalid("too many model overrides")
	}
	seen := map[string]bool{}
	for _, v := range c.ModelOverrides {
		if err := v.CodexStateStrategySettings.validateExplicit(); err != nil {
			return invalid(err.Error())
		}
		candidate := c
		applyStateStrategySettings(&candidate, v.CodexStateStrategySettings, nil, "")
		if err := candidate.validateStateStrategy(); err != nil {
			return invalid(err.Error())
		}
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
	if c.Enabled && c.HasStateStrategy() && cfg.Codex.ResolvedTurnStatePolicy() == CodexTurnStatePolicyStrip {
		return invalid("cannot enable while turn-state-policy is strip")
	}
	if !slices.Contains([]string{"override", "missing"}, c.Mode) || !slices.Contains([]string{"continue", "error", "hide"}, c.MissingPolicy) || !slices.Contains([]string{"active", "all", "manual"}, c.Acquisition) || !slices.Contains([]string{"inherit", "direct", "custom"}, c.ProxyMode) {
		return invalid("invalid mode or policy")
	}
	if c.RetryRoundIntervalMinutes < 1 || c.RetryRoundIntervalMinutes > 1440 || c.MaxRetryRounds < 0 || c.MaxRetryRounds > 10 {
		return invalid("retry round interval must be 1–1440 minutes and extra rounds 0–10")
	}
	if err := c.validateStateStrategy(); err != nil {
		return invalid(err.Error())
	}
	if c.TTLMinutes < 1 || c.TTLMinutes > 1440 || c.RefreshBeforeMinutes < 1 || c.ActiveMinutes < 1 || c.ActiveMinutes > 10080 || c.Concurrency < 1 || c.Concurrency > 16 || c.RetrySeconds < 1 || c.RetrySeconds > 3600 || c.MaxAttempts < 1 || c.MaxAttempts > 10 {
		return invalid("invalid lifetime, refresh, activity or acquisition limits")
	}
	if len(c.Prompt) > 4096 || len(c.ResponseContains) > 1024 || len(c.Models) > 256 || len(c.IncludedCredentials) > 1024 || len(c.ExcludedCredentials) > 1024 || len(c.Priorities) > 128 || len(c.Lengths) > 32 {
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
	for _, list := range [][]string{c.Models, c.IncludedCredentials, c.ExcludedCredentials} {
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
	return cfg.validateCodexStateRules()
}
