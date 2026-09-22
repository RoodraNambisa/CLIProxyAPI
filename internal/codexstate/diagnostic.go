package codexstate

import "github.com/router-for-me/CLIProxyAPI/v6/internal/config"

// Diagnostic stores explicit one-shot tests separately from normal request State.
var Diagnostic = NewDiagnostic()

func NewDiagnostic() *Manager {
	m := New()
	m.diagnostic = true
	return m
}

// DiagnosticPolicy retains matching rule settings, falling back to common model/plan
// defaults for skipped or unmatched pairs. It does not require automatic acquisition.
func DiagnosticPolicy(cfg config.CodexStateOverrideConfig, c Credential) (config.CodexStateOverrideConfig, config.CodexStateRuleMatch) {
	cfg = cfg.Resolved()
	cfg.Enabled = true
	policy, match, ok := cfg.PolicyFor(c.Scope())
	if !ok {
		policy = cfg.ForCredential(c.Plan, c.Model)
		match = config.CodexStateRuleMatch{RuleIndex: -1, Action: "manual"}
	}
	policy = cfg.ApplyResponseAcceptance(c.Scope(), policy)
	policy.Rules = nil
	policy.Priorities, policy.IncludedCredentials, policy.ExcludedCredentials, policy.Models = nil, nil, nil, nil
	policy.ModelOverrides, policy.PlanLengths = nil, nil
	policy.Concurrency = 0
	return policy, match
}
