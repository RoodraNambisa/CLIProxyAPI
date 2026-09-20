package config

import (
	"fmt"
	"slices"
	"strings"
)

func (c CodexStateOverrideConfig) stateDefaultsFor(scope CodexStateScope, sources map[string]string) CodexStateOverrideConfig {
	policy := c.ForCredential(scope.Plan, scope.Model)
	for _, override := range c.ModelOverrides {
		if override.Model != scope.Model {
			continue
		}
		if override.Lengths != nil {
			sources["lengths"] = "global-model"
		}
		if override.MatchModel != nil {
			sources["match-model"] = "global-model"
		}
		if override.Prompt != "" {
			sources["prompt"] = "global-model"
		}
		if override.ResponseContains != nil {
			sources["response-contains"] = "global-model"
		}
		break
	}
	for _, specific := range []bool{true, false} {
		for _, rule := range c.PlanLengths {
			if (len(rule.Models) > 0) != specific || specific && !slices.Contains(rule.Models, scope.Model) {
				continue
			}
			if slices.ContainsFunc(rule.PlanTypes, func(plan string) bool {
				return NormalizeCodexStatePlanType(plan) == NormalizeCodexStatePlanType(scope.Plan)
			}) {
				sources["lengths"] = "plan"
				return policy
			}
		}
	}
	return policy
}

func validateCodexStateModelOverrides(cfg *Config, rule CodexStateRule) error {
	if len(rule.ModelOverrides) > 256 {
		return fmt.Errorf("too many model overrides")
	}
	ids, models := map[string]bool{}, map[string]bool{}
	base := *cfg
	base.Codex.StateOverride.Rules = nil
	applyCodexStateRuleSettings(&base.Codex.StateOverride, rule.Settings, map[string]string{})
	for index, item := range rule.ModelOverrides {
		invalid := func(reason string) error { return fmt.Errorf("model-overrides[%d]: %s", index, reason) }
		if item.ID == "" || item.ID != strings.TrimSpace(item.ID) || len(item.ID) > 128 || strings.ContainsAny(item.ID, "\r\n\x00") || ids[item.ID] {
			return invalid("invalid or duplicate ID")
		}
		ids[item.ID] = true
		if len(item.Models) == 0 || len(item.Models) > 256 {
			return invalid("requires 1–256 exact models")
		}
		seen := map[string]bool{}
		for _, model := range item.Models {
			if model == "" || model != strings.TrimSpace(model) || len(model) > 256 || strings.ContainsAny(model, "*?\r\n\x00") || seen[model] {
				return invalid("invalid or duplicate exact model")
			}
			seen[model] = true
			if item.Enabled == nil || *item.Enabled {
				if models[model] {
					return invalid("model occurs in multiple enabled overrides")
				}
				models[model] = true
			}
		}
		if err := validateCodexStateRuleSettings(&base, item.Settings); err != nil {
			return invalid(err.Error())
		}
		for _, length := range dereferenceStateLengths(item.Settings.Lengths) {
			if length < 1 || length > 8192 {
				return invalid("State length out of range")
			}
		}
	}
	return nil
}
