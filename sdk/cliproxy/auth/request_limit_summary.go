package auth

// RequestLimitSummary exposes independent request-window and concurrency policies
// used by the scheduler, rather than the provider's own quota.
type RequestLimitSummary struct {
	Limit             int    `json:"limit"`
	WindowMinutes     int    `json:"window_minutes"`
	Source            string `json:"source"`
	Priority          int    `json:"priority"`
	Rule              int    `json:"rule,omitempty"`
	ConcurrencyLimit  int    `json:"concurrency_limit"`
	ConcurrencySource string `json:"concurrency_source"`
	ConcurrencyRule   int    `json:"concurrency_rule,omitempty"`
}

func (m *Manager) AuthRequestLimitSummary(auth *Auth) RequestLimitSummary {
	result := RequestLimitSummary{WindowMinutes: 1, Source: "global", ConcurrencySource: "global", Priority: authPriority(auth)}
	if m == nil || m.scheduler == nil {
		return result
	}
	s := m.scheduler
	s.mu.RLock()
	defer s.mu.RUnlock()
	policy := s.requestLimitPolicyForAuthLocked(auth)
	result.Limit, result.WindowMinutes = policy.limit, policy.windowMinutes
	result.ConcurrencyLimit = policy.concurrency
	if _, concurrencyOverride := s.priorityConcurrency[result.Priority]; concurrencyOverride {
		result.ConcurrencySource = "priority"
	}
	_, limitOverride := s.priorityRequestLimits[result.Priority]
	_, windowOverride := s.priorityRequestWindows[result.Priority]
	if limitOverride || windowOverride {
		result.Source = "priority"
	}
	for layer, index := range routingSubscriptionMatchIndexes(s.prioritySubscriptionRules[result.Priority], auth) {
		if index >= 0 {
			rule := s.prioritySubscriptionRules[result.Priority][index]
			source := "subscription"
			if layer == 1 {
				source = "credential"
			}
			if rule.PerAuthRequestLimit != nil || rule.PerAuthRequestWindowMinutes != nil {
				result.Source, result.Rule = source, index+1
			}
			if rule.PerAuthConcurrency != nil {
				result.ConcurrencySource, result.ConcurrencyRule = source, index+1
			}
		}
	}

	if result.Limit == 0 && s.strategyForPriorityLocked(result.Priority) == schedulerStrategyFillFirst {
		if rpm := s.fillFirstPerAuthRPMForPriorityLocked(result.Priority); rpm > 0 {
			result.Limit, result.WindowMinutes, result.Source, result.Rule = rpm, 1, "fill_first", 0
		}
	}
	return result
}
