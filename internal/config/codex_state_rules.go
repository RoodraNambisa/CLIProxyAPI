package config

import (
	"fmt"
	"slices"
	"strings"
)

// CodexStateRule is evaluated in list order for each credential/model pair.
// An empty selector means "any" within that selector type.
type CodexStateRule struct {
	ID                  string                 `yaml:"id" json:"id"`
	Name                string                 `yaml:"name" json:"name"`
	Enabled             *bool                  `yaml:"enabled,omitempty" json:"enabled,omitempty"`
	Action              string                 `yaml:"action" json:"action"`
	Priorities          APIKeyPriorityList     `yaml:"priorities" json:"priorities"`
	Credentials         []string               `yaml:"credentials" json:"credentials"`
	ExcludedCredentials []string               `yaml:"excluded-credentials" json:"excluded-credentials"`
	PlanTypes           []string               `yaml:"plan-types" json:"plan-types"`
	Models              []string               `yaml:"models" json:"models"`
	Settings            CodexStateRuleSettings `yaml:"settings" json:"settings"`
}

// Pointer fields distinguish inheritance from an explicitly configured zero or false value.
type CodexStateRuleSettings struct {
	Mode                            *string `yaml:"mode,omitempty" json:"mode,omitempty"`
	MissingPolicy                   *string `yaml:"missing-policy,omitempty" json:"missing-policy,omitempty"`
	Acquisition                     *string `yaml:"acquisition,omitempty" json:"acquisition,omitempty"`
	ActiveMinutes                   *int    `yaml:"active-minutes,omitempty" json:"active-minutes,omitempty"`
	TTLMinutes                      *int    `yaml:"ttl-minutes,omitempty" json:"ttl-minutes,omitempty"`
	RefreshBeforeMinutes            *int    `yaml:"refresh-before-minutes,omitempty" json:"refresh-before-minutes,omitempty"`
	RetrySeconds                    *int    `yaml:"retry-seconds,omitempty" json:"retry-seconds,omitempty"`
	MaxAttempts                     *int    `yaml:"max-attempts,omitempty" json:"max-attempts,omitempty"`
	ProxyMode                       *string `yaml:"proxy-mode,omitempty" json:"proxy-mode,omitempty"`
	ProxyURL                        *string `yaml:"proxy-url,omitempty" json:"proxy-url,omitempty"`
	Lengths                         *[]int  `yaml:"lengths,omitempty" json:"lengths,omitempty"`
	MatchModel                      *bool   `yaml:"match-model,omitempty" json:"match-model,omitempty"`
	Prompt                          *string `yaml:"prompt,omitempty" json:"prompt,omitempty"`
	ResponseContains                *string `yaml:"response-contains,omitempty" json:"response-contains,omitempty"`
	ErrorType                       *string `yaml:"error-type,omitempty" json:"error-type,omitempty"`
	ErrorCode                       *string `yaml:"error-code,omitempty" json:"error-code,omitempty"`
	ErrorMessage                    *string `yaml:"error-message,omitempty" json:"error-message,omitempty"`
	InvalidateOnStateLengthMismatch *bool   `yaml:"invalidate-on-state-length-mismatch,omitempty" json:"invalidate-on-state-length-mismatch,omitempty"`
	InvalidateOnModelMismatch       *bool   `yaml:"invalidate-on-model-mismatch,omitempty" json:"invalidate-on-model-mismatch,omitempty"`
}

// CodexStateScope contains non-secret routing metadata for one credential/model pair.
type CodexStateScope struct {
	ID, ShortID, Name, Plan, Model string
	Priority                       int
	Aliases                        []string
}

type CodexStateRuleMatch struct {
	RuleID    string            `json:"rule_id,omitempty"`
	RuleName  string            `json:"rule_name,omitempty"`
	RuleIndex int               `json:"rule_index"`
	Action    string            `json:"action"`
	Sources   map[string]string `json:"sources,omitempty"`
}

func (r CodexStateRule) matches(scope CodexStateScope, includeModel bool) bool {
	if r.Enabled != nil && !*r.Enabled {
		return false
	}
	if containsStateCredential(r.ExcludedCredentials, scope) {
		return false
	}
	if len(r.Priorities) > 0 && !slices.Contains(r.Priorities, scope.Priority) {
		return false
	}
	if len(r.Credentials) > 0 && !containsStateCredential(r.Credentials, scope) {
		return false
	}
	if len(r.PlanTypes) > 0 && !slices.ContainsFunc(r.PlanTypes, func(value string) bool {
		return NormalizeCodexStatePlanType(value) == NormalizeCodexStatePlanType(scope.Plan)
	}) {
		return false
	}
	return !includeModel || len(r.Models) == 0 || slices.Contains(r.Models, scope.Model) || slices.ContainsFunc(scope.Aliases, func(alias string) bool { return slices.Contains(r.Models, alias) })
}

func containsStateCredential(values []string, scope CodexStateScope) bool {
	return slices.Contains(values, scope.ID) || (scope.ShortID != "" && slices.Contains(values, scope.ShortID)) || (scope.Name != "" && slices.Contains(values, scope.Name))
}

// MatchesCredential keeps the legacy eligibility check and lets model-specific rules
// enter the model loop without falsely enabling a credential outside every rule.
func (c CodexStateOverrideConfig) MatchesCredential(scope CodexStateScope) bool {
	if !c.Enabled {
		return false
	}
	if c.Rules == nil {
		included := containsStateCredential(c.IncludedCredentials, scope)
		return !containsStateCredential(c.ExcludedCredentials, scope) && (len(c.Priorities) == 0 && len(c.IncludedCredentials) == 0 || included || slices.Contains(c.Priorities, scope.Priority))
	}
	for _, rule := range *c.Rules {
		if rule.matches(scope, false) {
			return true
		}
	}
	return false
}

// PolicyFor resolves one effective policy. It is the only rule resolver used by
// managed State runtime code and the management preview endpoint.
func (c CodexStateOverrideConfig) PolicyFor(scope CodexStateScope) (CodexStateOverrideConfig, CodexStateRuleMatch, bool) {
	match := CodexStateRuleMatch{RuleIndex: -1, Action: "unmatched", Sources: map[string]string{}}
	if !c.Enabled {
		match.Action = "disabled"
		return CodexStateOverrideConfig{}, match, false
	}
	if c.Rules == nil {
		if !c.MatchesCredential(scope) || len(c.Models) > 0 && !slices.Contains(c.Models, scope.Model) && !slices.ContainsFunc(scope.Aliases, func(alias string) bool { return slices.Contains(c.Models, alias) }) {
			return CodexStateOverrideConfig{}, match, false
		}
		match.RuleName, match.Action = "legacy", "manage"
		return c.ForCredential(scope.Plan, scope.Model), match, true
	}
	for index, rule := range *c.Rules {
		if !rule.matches(scope, true) {
			continue
		}
		match.RuleID, match.RuleName, match.RuleIndex = rule.ID, rule.Name, index
		match.Action = rule.Action
		if match.Action == "" {
			match.Action = "manage"
		}
		if match.Action == "skip" {
			return CodexStateOverrideConfig{}, match, false
		}
		defaults := c
		defaults.Rules = nil
		policy := defaults.ForCredential(scope.Plan, scope.Model)
		applyCodexStateRuleSettings(&policy, rule.Settings, match.Sources)
		policy.Enabled = true
		policy.Rules = nil
		policy.Priorities, policy.IncludedCredentials, policy.ExcludedCredentials, policy.Models = nil, nil, nil, nil
		return policy, match, true
	}
	return CodexStateOverrideConfig{}, match, false
}

func applyCodexStateRuleSettings(policy *CodexStateOverrideConfig, settings CodexStateRuleSettings, sources map[string]string) {
	mark := func(name string, ok bool) {
		if ok {
			sources[name] = "rule"
		} else {
			sources[name] = "default"
		}
	}
	if settings.Mode != nil {
		policy.Mode = *settings.Mode
	}
	mark("mode", settings.Mode != nil)
	if settings.MissingPolicy != nil {
		policy.MissingPolicy = *settings.MissingPolicy
	}
	mark("missing-policy", settings.MissingPolicy != nil)
	if settings.Acquisition != nil {
		policy.Acquisition = *settings.Acquisition
	}
	mark("acquisition", settings.Acquisition != nil)
	if settings.ActiveMinutes != nil {
		policy.ActiveMinutes = *settings.ActiveMinutes
	}
	mark("active-minutes", settings.ActiveMinutes != nil)
	if settings.TTLMinutes != nil {
		policy.TTLMinutes = *settings.TTLMinutes
	}
	mark("ttl-minutes", settings.TTLMinutes != nil)
	if settings.RefreshBeforeMinutes != nil {
		policy.RefreshBeforeMinutes = *settings.RefreshBeforeMinutes
	}
	mark("refresh-before-minutes", settings.RefreshBeforeMinutes != nil)
	if settings.RetrySeconds != nil {
		policy.RetrySeconds = *settings.RetrySeconds
	}
	mark("retry-seconds", settings.RetrySeconds != nil)
	if settings.MaxAttempts != nil {
		policy.MaxAttempts = *settings.MaxAttempts
	}
	mark("max-attempts", settings.MaxAttempts != nil)
	if settings.ProxyMode != nil {
		policy.ProxyMode = *settings.ProxyMode
	}
	mark("proxy-mode", settings.ProxyMode != nil)
	if settings.ProxyURL != nil {
		policy.ProxyURL = *settings.ProxyURL
	}
	mark("proxy-url", settings.ProxyURL != nil)
	if settings.Lengths != nil {
		policy.Lengths = slices.Clone(*settings.Lengths)
	}
	mark("lengths", settings.Lengths != nil)
	if settings.MatchModel != nil {
		value := *settings.MatchModel
		policy.MatchModel = &value
	}
	mark("match-model", settings.MatchModel != nil)
	if settings.Prompt != nil {
		policy.Prompt = *settings.Prompt
	}
	mark("prompt", settings.Prompt != nil)
	if settings.ResponseContains != nil {
		policy.ResponseContains = *settings.ResponseContains
	}
	mark("response-contains", settings.ResponseContains != nil)
	if settings.ErrorType != nil {
		policy.ErrorType = *settings.ErrorType
	}
	mark("error-type", settings.ErrorType != nil)
	if settings.ErrorCode != nil {
		policy.ErrorCode = *settings.ErrorCode
	}
	mark("error-code", settings.ErrorCode != nil)
	if settings.ErrorMessage != nil {
		policy.ErrorMessage = *settings.ErrorMessage
	}
	mark("error-message", settings.ErrorMessage != nil)
	if settings.InvalidateOnStateLengthMismatch != nil {
		policy.InvalidateOnStateLengthMismatch = *settings.InvalidateOnStateLengthMismatch
	}
	mark("invalidate-on-state-length-mismatch", settings.InvalidateOnStateLengthMismatch != nil)
	if settings.InvalidateOnModelMismatch != nil {
		policy.InvalidateOnModelMismatch = *settings.InvalidateOnModelMismatch
	}
	mark("invalidate-on-model-mismatch", settings.InvalidateOnModelMismatch != nil)
}

func cloneCodexStateRules(rules *[]CodexStateRule) *[]CodexStateRule {
	if rules == nil {
		return nil
	}
	cloned := slices.Clone(*rules)
	for i := range cloned {
		rule := &cloned[i]
		rule.Priorities = slices.Clone(rule.Priorities)
		rule.Credentials = slices.Clone(rule.Credentials)
		rule.ExcludedCredentials = slices.Clone(rule.ExcludedCredentials)
		rule.PlanTypes = slices.Clone(rule.PlanTypes)
		rule.Models = slices.Clone(rule.Models)
		if rule.Enabled != nil {
			value := *rule.Enabled
			rule.Enabled = &value
		}
		cloneCodexStateRuleSettings(&rule.Settings)
	}
	return &cloned
}

func cloneCodexStateRuleSettings(settings *CodexStateRuleSettings) {
	if settings.Lengths != nil {
		values := slices.Clone(*settings.Lengths)
		settings.Lengths = &values
	}
	if settings.Mode != nil {
		value := *settings.Mode
		settings.Mode = &value
	}
	if settings.MissingPolicy != nil {
		value := *settings.MissingPolicy
		settings.MissingPolicy = &value
	}
	if settings.Acquisition != nil {
		value := *settings.Acquisition
		settings.Acquisition = &value
	}
	if settings.ActiveMinutes != nil {
		value := *settings.ActiveMinutes
		settings.ActiveMinutes = &value
	}
	if settings.TTLMinutes != nil {
		value := *settings.TTLMinutes
		settings.TTLMinutes = &value
	}
	if settings.RefreshBeforeMinutes != nil {
		value := *settings.RefreshBeforeMinutes
		settings.RefreshBeforeMinutes = &value
	}
	if settings.RetrySeconds != nil {
		value := *settings.RetrySeconds
		settings.RetrySeconds = &value
	}
	if settings.MaxAttempts != nil {
		value := *settings.MaxAttempts
		settings.MaxAttempts = &value
	}
	if settings.ProxyMode != nil {
		value := *settings.ProxyMode
		settings.ProxyMode = &value
	}
	if settings.ProxyURL != nil {
		value := *settings.ProxyURL
		settings.ProxyURL = &value
	}
	if settings.MatchModel != nil {
		value := *settings.MatchModel
		settings.MatchModel = &value
	}
	if settings.Prompt != nil {
		value := *settings.Prompt
		settings.Prompt = &value
	}
	if settings.ResponseContains != nil {
		value := *settings.ResponseContains
		settings.ResponseContains = &value
	}
	if settings.ErrorType != nil {
		value := *settings.ErrorType
		settings.ErrorType = &value
	}
	if settings.ErrorCode != nil {
		value := *settings.ErrorCode
		settings.ErrorCode = &value
	}
	if settings.ErrorMessage != nil {
		value := *settings.ErrorMessage
		settings.ErrorMessage = &value
	}
	if settings.InvalidateOnStateLengthMismatch != nil {
		value := *settings.InvalidateOnStateLengthMismatch
		settings.InvalidateOnStateLengthMismatch = &value
	}
	if settings.InvalidateOnModelMismatch != nil {
		value := *settings.InvalidateOnModelMismatch
		settings.InvalidateOnModelMismatch = &value
	}
}

func (cfg *Config) validateCodexStateRules() error {
	if cfg.Codex.StateOverride.Rules == nil {
		return nil
	}
	if len(*cfg.Codex.StateOverride.Rules) > 128 {
		return fmt.Errorf("codex.state-override: too many rules")
	}
	seen := map[string]bool{}
	for index, rule := range *cfg.Codex.StateOverride.Rules {
		invalid := func(reason string) error { return fmt.Errorf("codex.state-override.rules[%d]: %s", index, reason) }
		if strings.TrimSpace(rule.ID) == "" || len(rule.ID) > 128 || strings.ContainsAny(rule.ID, "\r\n\x00") || seen[rule.ID] {
			return invalid("invalid or duplicate rule ID")
		}
		seen[rule.ID] = true
		if len(rule.Name) > 256 || strings.ContainsAny(rule.Name, "\r\n\x00") || rule.Action != "" && rule.Action != "manage" && rule.Action != "skip" {
			return invalid("invalid rule name or action")
		}
		if len(rule.Priorities) > 128 || len(rule.Credentials) > 1024 || len(rule.ExcludedCredentials) > 1024 || len(rule.PlanTypes) > 32 || len(rule.Models) > 256 {
			return invalid("too many rule selectors")
		}
		for _, value := range append(append(append(slices.Clone(rule.Credentials), rule.ExcludedCredentials...), rule.PlanTypes...), rule.Models...) {
			if strings.TrimSpace(value) == "" || len(value) > 512 || strings.ContainsAny(value, "\r\n\x00") {
				return invalid("invalid rule selector")
			}
		}
		for _, priority := range rule.Priorities {
			if int64(priority) < -APIKeyPriorityLimit || int64(priority) > APIKeyPriorityLimit {
				return invalid("priority out of range")
			}
		}
		for _, length := range dereferenceStateLengths(rule.Settings.Lengths) {
			if length < 1 || length > 8192 {
				return invalid("rule State length out of range")
			}
		}
		if err := validateCodexStateRuleSettings(cfg, rule.Settings); err != nil {
			return invalid(err.Error())
		}
	}
	return nil
}

func dereferenceStateLengths(values *[]int) []int {
	if values == nil {
		return nil
	}
	return *values
}

func validateCodexStateRuleSettings(cfg *Config, settings CodexStateRuleSettings) error {
	for _, value := range []*int{settings.ActiveMinutes, settings.TTLMinutes, settings.RefreshBeforeMinutes, settings.RetrySeconds, settings.MaxAttempts} {
		if value != nil && *value <= 0 {
			return fmt.Errorf("rule intervals and limits must be positive")
		}
	}
	for _, value := range []*string{settings.Mode, settings.MissingPolicy, settings.Acquisition, settings.ProxyMode, settings.Prompt, settings.ErrorType, settings.ErrorCode} {
		if value != nil && strings.TrimSpace(*value) == "" {
			return fmt.Errorf("rule policy cannot be empty")
		}
	}
	base := cfg.Codex.StateOverride
	base.Rules = nil
	applyCodexStateRuleSettings(&base, settings, map[string]string{})
	candidate := *cfg
	candidate.Codex.StateOverride = base
	if err := candidate.ValidateCodexStateOverride(); err != nil {
		return err
	}

	return nil
}
