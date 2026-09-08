package config

import (
	"fmt"
	"regexp"
	"strings"
)

// RequestScopedErrorRule matches one upstream status and any literal or regex.
// An entirely empty rule is inactive. Nonempty rules must be valid.
type RequestScopedErrorRule struct {
	Status      int      `yaml:"status,omitempty" json:"status,omitempty"`
	Match       []string `yaml:"match,omitempty" json:"match,omitempty"`
	MatchRegexr []string `yaml:"match-regexr,omitempty" json:"match-regexr,omitempty"`
	Action      string   `yaml:"action,omitempty" json:"action,omitempty"`
}

type RequestScopedErrorAction string

const (
	RequestScopedActionStop                RequestScopedErrorAction = "stop"
	RequestScopedActionStopAndCooldown     RequestScopedErrorAction = "stop-and-cooldown"
	RequestScopedActionContinue            RequestScopedErrorAction = "continue"
	RequestScopedActionContinueAndCooldown RequestScopedErrorAction = "continue-and-cooldown"
)

type compiledRequestScopedErrorRule struct {
	status   int
	literals []string
	regexps  []*regexp.Regexp
	action   RequestScopedErrorAction
}

// CompiledRequestScopedErrors is immutable and safe to share with in-flight
// requests. Compile at configuration/credential publication, never per error.
type CompiledRequestScopedErrors struct {
	rules []compiledRequestScopedErrorRule
}

func CompileRequestScopedErrors(rules []RequestScopedErrorRule) (*CompiledRequestScopedErrors, error) {
	var compiled []compiledRequestScopedErrorRule
	for index, rule := range rules {
		action := RequestScopedErrorAction(strings.ToLower(strings.TrimSpace(rule.Action)))
		literals := make([]string, 0, len(rule.Match))
		patterns := make([]string, 0, len(rule.MatchRegexr))
		for _, value := range rule.Match {
			if value != "" {
				literals = append(literals, strings.Clone(value))
			}
		}
		for _, value := range rule.MatchRegexr {
			if value != "" {
				patterns = append(patterns, value)
			}
		}
		if rule.Status == 0 && action == "" && len(literals) == 0 && len(patterns) == 0 {
			continue
		}
		if rule.Status < 100 || rule.Status > 599 {
			return nil, fmt.Errorf("request-scoped-errors[%d].status must be an integer from 100 to 599", index)
		}
		switch action {
		case RequestScopedActionStop, RequestScopedActionStopAndCooldown, RequestScopedActionContinue, RequestScopedActionContinueAndCooldown:
		default:
			return nil, fmt.Errorf("request-scoped-errors[%d].action is invalid", index)
		}
		if len(literals) == 0 && len(patterns) == 0 {
			return nil, fmt.Errorf("request-scoped-errors[%d] requires a nonempty match or match-regexr", index)
		}
		next := compiledRequestScopedErrorRule{status: rule.Status, action: action, literals: literals}
		for patternIndex, pattern := range rule.MatchRegexr {
			if pattern == "" {
				continue
			}
			re, err := regexp.Compile(strings.Clone(pattern))
			if err != nil {
				// Patterns can contain private data. Report their position only.
				return nil, fmt.Errorf("request-scoped-errors[%d].match-regexr[%d] is invalid", index, patternIndex)
			}
			next.regexps = append(next.regexps, re)
		}
		compiled = append(compiled, next)
	}
	if len(compiled) == 0 {
		return nil, nil
	}
	return &CompiledRequestScopedErrors{rules: compiled}, nil
}

// Match preserves list order, exact status and case-sensitive literal matching.
// Literal and regex conditions within one rule are alternatives.
func (rules *CompiledRequestScopedErrors) Match(status int, body string) (RequestScopedErrorAction, bool) {
	if rules == nil {
		return "", false
	}
	for _, rule := range rules.rules {
		if rule.status != status {
			continue
		}
		for _, literal := range rule.literals {
			if strings.Contains(body, literal) {
				return rule.action, true
			}
		}
		for _, re := range rule.regexps {
			if re.MatchString(body) {
				return rule.action, true
			}
		}
	}
	return "", false
}
