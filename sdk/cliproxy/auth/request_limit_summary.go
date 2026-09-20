package auth

// RequestLimitSummary exposes the policy used by the scheduler, not a
// simultaneous-request concurrency limit or the provider's own quota.
type RequestLimitSummary struct {
	Limit         int    `json:"limit"`
	WindowMinutes int    `json:"window_minutes"`
	Source        string `json:"source"`
	Priority      int    `json:"priority"`
	Rule          int    `json:"rule,omitempty"`
}

func (m *Manager) AuthRequestLimitSummary(auth *Auth) RequestLimitSummary {
	result := RequestLimitSummary{WindowMinutes: 1, Source: "global", Priority: authPriority(auth)}
	if m == nil || m.scheduler == nil {
		return result
	}
	s := m.scheduler
	s.mu.RLock()
	defer s.mu.RUnlock()
	policy := s.requestLimitPolicyForAuthLocked(auth)
	result.Limit, result.WindowMinutes = policy.limit, policy.windowMinutes
	_, limitOverride := s.priorityRequestLimits[result.Priority]
	_, windowOverride := s.priorityRequestWindows[result.Priority]
	if limitOverride || windowOverride {
		result.Source = "priority"
	}
	for layer, index := range routingSubscriptionMatchIndexes(s.prioritySubscriptionRules[result.Priority], auth) {
		if index >= 0 {
			result.Source, result.Rule = "subscription", index+1
			if layer == 1 {
				result.Source = "credential"
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
