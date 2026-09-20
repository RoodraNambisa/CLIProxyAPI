package codexstate

import "time"

func (e *entry) automaticRounds() bool {
	return !e.ManualOnly && e.policy.Acquisition != "manual" && e.policy.MaxRetryRounds > 0
}

func (e *entry) resetRetryCycle() {
	e.failures, e.ConsecutiveFailures, e.RetryRoundsUsed = 0, 0, 0
	e.Exhausted, e.RoundWaiting = false, false
	e.lastFailure, e.NextAttempt = time.Time{}, time.Time{}
}

// Schedule from the last failure, not the sync time, so hot reload cannot keep
// postponing a round. Only starting a round consumes the additional-round budget.
func (e *entry) scheduleRetry() {
	e.Exhausted, e.RoundWaiting = false, false
	e.NextAttempt = time.Time{}
	if e.RetryRoundsUsed > e.policy.MaxRetryRounds {
		e.Exhausted, e.manual = true, false
		return
	}
	if e.lastFailure.IsZero() {
		return
	}
	if e.failures < e.policy.MaxAttempts {
		e.NextAttempt = e.lastFailure.Add(time.Duration(e.policy.RetrySeconds) * time.Second)
		return
	}
	if e.automaticRounds() && e.RetryRoundsUsed < e.policy.MaxRetryRounds {
		e.RoundWaiting = true
		e.NextAttempt = e.lastFailure.Add(time.Duration(e.policy.RetryRoundIntervalMinutes) * time.Minute)
		return
	}
	e.Exhausted = true
}
