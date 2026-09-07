package auth

import "slices"

// candidateFailuresLocked uses the same provider/model shard and selection
// scope as the failure being reported. A nil priorities slice means all tiers.
func (m *modelScheduler) candidateFailuresLocked(preferWebsocket bool, priorities []int, predicate func(*scheduledAuth) bool) candidateFailureChoice {
	var failures candidateFailureChoice
	if m == nil {
		return failures
	}
	for _, entry := range m.entries {
		if entry == nil || entry.auth == nil || entry.meta == nil {
			continue
		}
		if entry.state != scheduledStateCooldown && entry.state != scheduledStateBlocked {
			continue
		}
		if preferWebsocket && !entry.meta.websocketEnabled {
			continue
		}
		if priorities != nil && !slices.Contains(priorities, entry.meta.priority) {
			continue
		}
		if predicate != nil && !predicate(entry) {
			continue
		}
		failures.observe(entry.auth, m.modelKey)
	}
	return failures
}

func mixedCandidateFailuresLocked(shards []*modelScheduler, priorities []int, predicate func(*scheduledAuth) bool) candidateFailureChoice {
	var failures candidateFailureChoice
	for _, shard := range shards {
		failures.merge(shard.candidateFailuresLocked(false, priorities, predicate))
	}
	return failures
}
