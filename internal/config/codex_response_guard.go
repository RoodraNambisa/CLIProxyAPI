package config

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// CodexResponseGuardConfig is independent of managed State eligibility.
type CodexResponseGuardConfig struct {
	Enabled                    bool `yaml:"enabled" json:"enabled"`
	CodexResponseGuardSettings `yaml:",inline"`
	Rules                      *[]CodexResponseGuardRule `yaml:"rules,omitempty" json:"rules,omitempty"`
}

type CodexResponseGuardSettings struct {
	Mode                  *string   `yaml:"mode,omitempty" json:"mode,omitempty"`
	MatchModel            *bool     `yaml:"match-model,omitempty" json:"match-model,omitempty"`
	AllowedReturnedModels *[]string `yaml:"allowed-returned-models,omitempty" json:"allowed-returned-models,omitempty"`
	LengthMode            *string   `yaml:"length-mode,omitempty" json:"length-mode,omitempty"`
	Lengths               *[]int    `yaml:"lengths,omitempty" json:"lengths,omitempty"`
	MissingModel          *string   `yaml:"missing-model,omitempty" json:"missing-model,omitempty"`
	MissingState          *string   `yaml:"missing-state,omitempty" json:"missing-state,omitempty"`
	OnReject              *string   `yaml:"on-reject,omitempty" json:"on-reject,omitempty"`
	ClearAffinity         *string   `yaml:"clear-affinity,omitempty" json:"clear-affinity,omitempty"`
	LateMismatch          *string   `yaml:"late-mismatch,omitempty" json:"late-mismatch,omitempty"`
	ErrorType             *string   `yaml:"error-type,omitempty" json:"error-type,omitempty"`
	ErrorCode             *string   `yaml:"error-code,omitempty" json:"error-code,omitempty"`
	ErrorMessage          *string   `yaml:"error-message,omitempty" json:"error-message,omitempty"`
}

type CodexResponseGuardRule struct {
	ID                  string                            `yaml:"id" json:"id"`
	Name                string                            `yaml:"name" json:"name"`
	Enabled             *bool                             `yaml:"enabled,omitempty" json:"enabled,omitempty"`
	Priorities          APIKeyPriorityList                `yaml:"priorities" json:"priorities"`
	Credentials         []string                          `yaml:"credentials" json:"credentials"`
	ExcludedCredentials []string                          `yaml:"excluded-credentials" json:"excluded-credentials"`
	PlanTypes           []string                          `yaml:"plan-types" json:"plan-types"`
	Models              []string                          `yaml:"models" json:"models"`
	Settings            CodexResponseGuardSettings        `yaml:"settings" json:"settings"`
	ModelOverrides      []CodexResponseGuardModelOverride `yaml:"model-overrides,omitempty" json:"model-overrides,omitempty"`
}

type CodexResponseGuardModelOverride struct {
	ID       string                     `yaml:"id" json:"id"`
	Enabled  *bool                      `yaml:"enabled,omitempty" json:"enabled,omitempty"`
	Models   []string                   `yaml:"models" json:"models"`
	Settings CodexResponseGuardSettings `yaml:"settings" json:"settings"`
}

type CodexResponseGuardPolicy struct {
	Mode                  string            `json:"mode"`
	MatchModel            bool              `json:"match_model"`
	AllowedReturnedModels []string          `json:"allowed_returned_models"`
	LengthMode            string            `json:"length_mode"`
	Lengths               []int             `json:"lengths"`
	MissingModel          string            `json:"missing_model"`
	MissingState          string            `json:"missing_state"`
	OnReject              string            `json:"on_reject"`
	ClearAffinity         string            `json:"clear_affinity"`
	LateMismatch          string            `json:"late_mismatch"`
	ErrorType             string            `json:"error_type"`
	ErrorCode             string            `json:"error_code"`
	ErrorMessage          string            `json:"error_message"`
	Rule                  int               `json:"rule"`
	RuleID                string            `json:"rule_id,omitempty"`
	RuleName              string            `json:"rule_name,omitempty"`
	ModelOverrideID       string            `json:"model_override_id,omitempty"`
	Sources               map[string]string `json:"sources"`
}

type CodexResponseEvidence struct {
	Model        string `json:"model"`
	StatePresent bool   `json:"state_present"`
	StateLength  int    `json:"state_length"`
}

type CodexResponseVerdict struct {
	Model   string   `json:"model"`
	State   string   `json:"state"`
	Reasons []string `json:"reasons"`
}

func (v CodexResponseVerdict) Accepted() bool { return len(v.Reasons) == 0 }

func CodexReturnedModelAccepted(requested, returned string, allowed []string) bool {
	return returned == requested || slices.Contains(allowed, returned)
}

func CodexReturnedLengthAccepted(length int, mode string, lengths []int) bool {
	if mode == "off" || len(lengths) == 0 {
		return true
	}
	contains := slices.Contains(lengths, length)
	if mode == "deny" {
		return !contains
	}
	return contains
}

// Evaluate reports missing information separately. final permits the configured
// missing-field policy only at an admission/completion boundary.
func (p CodexResponseGuardPolicy) Evaluate(requested string, e CodexResponseEvidence, final bool) CodexResponseVerdict {
	v := CodexResponseVerdict{Model: "disabled", State: "disabled", Reasons: []string{}}
	if p.Mode == "off" {
		return v
	}
	if p.MatchModel || p.MissingModel == "reject" {
		v.Model = "accepted"
		if e.Model == "" {
			v.Model = "missing"
			if final && p.MissingModel == "reject" {
				v.Reasons = append(v.Reasons, "missing_model")
			}
		} else if p.MatchModel && !CodexReturnedModelAccepted(requested, e.Model, p.AllowedReturnedModels) {
			v.Model = "rejected"
			v.Reasons = append(v.Reasons, "model_mismatch")
		}
	}
	if p.LengthMode != "off" && len(p.Lengths) > 0 || p.MissingState == "reject" {
		v.State = "accepted"
		if !e.StatePresent {
			v.State = "missing"
			if final && p.MissingState == "reject" {
				v.Reasons = append(v.Reasons, "missing_state")
			}
		} else if !CodexReturnedLengthAccepted(e.StateLength, p.LengthMode, p.Lengths) {
			v.State = "rejected"
			v.Reasons = append(v.Reasons, "state_length_mismatch")
		}
	}
	return v
}

func (c CodexResponseGuardConfig) Clone() CodexResponseGuardConfig {
	data, _ := json.Marshal(c)
	var result CodexResponseGuardConfig
	_ = json.Unmarshal(data, &result)
	return result
}

func (r CodexResponseGuardRule) matches(scope CodexStateScope, model bool) bool {
	return (CodexStateRule{Enabled: r.Enabled, Priorities: r.Priorities, Credentials: r.Credentials, ExcludedCredentials: r.ExcludedCredentials, PlanTypes: r.PlanTypes, Models: r.Models}).matches(scope, model)
}

func (c CodexResponseGuardConfig) PolicyFor(scope CodexStateScope) CodexResponseGuardPolicy {
	p := CodexResponseGuardPolicy{Mode: "off", MatchModel: true, LengthMode: "off", MissingModel: "allow", MissingState: "allow", OnReject: "error", ClearAffinity: "session", LateMismatch: "observe", ErrorType: "rate_limit_exceeded", ErrorCode: "rate_limit_exceeded", ErrorMessage: "Rate limit exceeded. Please try again later.", Sources: map[string]string{}}
	if !c.Enabled {
		return p
	}
	p.apply(c.CodexResponseGuardSettings, "default")
	if c.Rules == nil {
		return p
	}
	for i, r := range *c.Rules {
		if !r.matches(scope, true) {
			continue
		}
		p.Rule, p.RuleID, p.RuleName = i+1, r.ID, r.Name
		p.apply(r.Settings, "rule")
		for _, o := range r.ModelOverrides {
			if o.Enabled != nil && !*o.Enabled {
				continue
			}
			if !(CodexStateRule{Models: o.Models}).matches(scope, true) {
				continue
			}
			p.ModelOverrideID = o.ID
			p.apply(o.Settings, "model-override")
			break
		}
		return p
	}
	p.Mode = "off"
	return p
}

func (p *CodexResponseGuardPolicy) apply(s CodexResponseGuardSettings, source string) {
	fields := []struct {
		key  string
		from *string
		to   *string
	}{
		{"mode", s.Mode, &p.Mode}, {"length-mode", s.LengthMode, &p.LengthMode}, {"missing-model", s.MissingModel, &p.MissingModel}, {"missing-state", s.MissingState, &p.MissingState}, {"on-reject", s.OnReject, &p.OnReject}, {"clear-affinity", s.ClearAffinity, &p.ClearAffinity}, {"late-mismatch", s.LateMismatch, &p.LateMismatch}, {"error-type", s.ErrorType, &p.ErrorType}, {"error-code", s.ErrorCode, &p.ErrorCode}, {"error-message", s.ErrorMessage, &p.ErrorMessage},
	}
	for _, f := range fields {
		if f.from != nil {
			*f.to = *f.from
			p.Sources[f.key] = source
		}
	}
	if s.MatchModel != nil {
		p.MatchModel = *s.MatchModel
		p.Sources["match-model"] = source
	}
	if s.AllowedReturnedModels != nil {
		p.AllowedReturnedModels = slices.Clone(*s.AllowedReturnedModels)
		p.Sources["allowed-returned-models"] = source
	}
	if s.Lengths != nil {
		p.Lengths = slices.Clone(*s.Lengths)
		p.Sources["lengths"] = source
	}
}

// ApplyStateAcceptance changes only explicit acceptance values, never resource
// activation, acquisition requirements or invalidation switches.
func (p CodexResponseGuardPolicy) ApplyStateAcceptance(state CodexStateOverrideConfig) CodexStateOverrideConfig {
	if p.Mode == "off" {
		return state
	}
	if p.Sources["allowed-returned-models"] != "" {
		state.AcceptedReturnedModels = slices.Clone(p.AllowedReturnedModels)
	}
	if p.Sources["length-mode"] != "" {
		state.ReturnedLengthMode = p.LengthMode
	}
	if p.Sources["lengths"] != "" {
		state.Lengths = append([]int{}, p.Lengths...)
	}
	return state
}

func (c CodexConfig) ManagedStateConfig() CodexStateOverrideConfig {
	state := c.StateOverride
	guard := c.ResponseGuard
	state.ResponseGuard = &guard
	return state
}

func (c CodexStateOverrideConfig) ApplyResponseAcceptance(scope CodexStateScope, p CodexStateOverrideConfig) CodexStateOverrideConfig {
	if c.ResponseGuard != nil {
		p = c.ResponseGuard.PolicyFor(scope).ApplyStateAcceptance(p)
	}
	return p
}

func (c CodexStateOverrideConfig) CheckReturnedLength() bool {
	return c.ReturnedLengthMode != "off" && len(c.Lengths) > 0
}

func (cfg *Config) ValidateCodexResponseGuard() error {
	if cfg == nil {
		return nil
	}
	c := cfg.Codex.ResponseGuard
	if err := validateResponseGuardSettings(c.CodexResponseGuardSettings); err != nil {
		return fmt.Errorf("codex.response-guard: %w", err)
	}
	if c.Rules == nil {
		return nil
	}
	if len(*c.Rules) > 128 {
		return fmt.Errorf("codex.response-guard supports at most 128 rules")
	}
	ids := map[string]bool{}
	for i, r := range *c.Rules {
		if r.ID == "" || ids[r.ID] {
			return fmt.Errorf("codex.response-guard rule %d needs a unique id", i+1)
		}
		ids[r.ID] = true
		if err := validateResponseGuardSettings(r.Settings); err != nil {
			return fmt.Errorf("codex.response-guard rule %d: %w", i+1, err)
		}
		for _, list := range [][]string{r.Credentials, r.ExcludedCredentials, r.PlanTypes, r.Models} {
			if err := validateGuardNames(list); err != nil {
				return err
			}
		}
		for _, n := range r.Priorities {
			if int64(n) < -APIKeyPriorityLimit || int64(n) > APIKeyPriorityLimit {
				return fmt.Errorf("invalid response guard priority")
			}
		}
		models := map[string]bool{}
		overrideIDs := map[string]bool{}
		if len(r.ModelOverrides) > 128 {
			return fmt.Errorf("too many response guard model overrides")
		}
		for _, o := range r.ModelOverrides {
			if err := validateResponseGuardSettings(o.Settings); err != nil {
				return err
			}
			if err := validateGuardNames(o.Models); err != nil {
				return err
			}
			if o.ID == "" || overrideIDs[o.ID] || len(o.Models) == 0 {
				return fmt.Errorf("response guard model overrides require id and models")
			}
			overrideIDs[o.ID] = true
			if o.Enabled != nil && !*o.Enabled {
				continue
			}
			for _, model := range o.Models {
				if models[model] {
					return fmt.Errorf("overlapping response guard model override: %s", model)
				}
				models[model] = true
			}
		}
	}
	return nil
}

func validateGuardNames(values []string) error {
	if len(values) > 128 {
		return fmt.Errorf("response guard matcher list is too large")
	}
	for _, v := range values {
		if strings.TrimSpace(v) == "" || len(v) > 256 || strings.ContainsAny(v, "\r\n\x00") {
			return fmt.Errorf("invalid response guard matcher")
		}
	}
	return nil
}

func validateResponseGuardSettings(s CodexResponseGuardSettings) error {
	for _, f := range []struct {
		key     string
		value   *string
		allowed []string
	}{
		{"mode", s.Mode, []string{"off", "observe", "enforce"}}, {"length-mode", s.LengthMode, []string{"off", "allow", "deny"}}, {"missing-model", s.MissingModel, []string{"allow", "reject"}}, {"missing-state", s.MissingState, []string{"allow", "reject"}}, {"on-reject", s.OnReject, []string{"error", "retry"}}, {"clear-affinity", s.ClearAffinity, []string{"none", "session", "credential"}}, {"late-mismatch", s.LateMismatch, []string{"observe", "abort"}},
	} {
		if f.value != nil && !slices.Contains(f.allowed, *f.value) {
			return fmt.Errorf("invalid %s", f.key)
		}
	}
	if s.AllowedReturnedModels != nil {
		if err := validateGuardNames(*s.AllowedReturnedModels); err != nil {
			return err
		}
	}
	if s.Lengths != nil {
		if len(*s.Lengths) > 128 {
			return fmt.Errorf("too many response guard lengths")
		}
		for _, n := range *s.Lengths {
			if n < 1 || n > 8192 {
				return fmt.Errorf("response guard lengths must be within 1–8192")
			}
		}
	}
	for _, v := range []*string{s.ErrorType, s.ErrorCode, s.ErrorMessage} {
		if v != nil && (strings.TrimSpace(*v) == "" || len(*v) > 2048 || strings.ContainsAny(*v, "\r\n\x00")) {
			return fmt.Errorf("invalid response guard error text")
		}
	}
	return nil
}
