package auth

import "time"

// modelStateBlock preserves the local zero-deadline no-cooldown contract.
func modelStateBlock(state *ModelState, now time.Time) (bool, blockReason, time.Time) {
	if state == nil {
		return false, blockReasonNone, time.Time{}
	}
	if state.Status == StatusDisabled {
		return true, blockReasonDisabled, time.Time{}
	}
	if !state.Unavailable || !state.NextRetryAfter.After(now) {
		return false, blockReasonNone, time.Time{}
	}
	next := state.NextRetryAfter
	if state.Quota.NextRecoverAt.After(now) {
		next = state.Quota.NextRecoverAt
	}
	if state.Quota.Exceeded {
		return true, blockReasonCooldown, next
	}
	return true, blockReasonOther, next
}

func modelStatesBlock(states map[string]*ModelState, model string, now time.Time) (bool, blockReason, time.Time) {
	key := canonicalModelKey(model)
	blocked, reason, next := false, blockReasonNone, time.Time{}
	for storedModel, state := range states {
		if canonicalModelKey(storedModel) != key {
			continue
		}
		stateBlocked, stateReason, stateNext := modelStateBlock(state, now)
		if stateReason == blockReasonDisabled {
			return true, stateReason, stateNext
		}
		if stateBlocked && (!blocked || stateNext.After(next) || stateNext.Equal(next) && stateReason == blockReasonCooldown) {
			blocked, reason, next = true, stateReason, stateNext
		}
	}
	return blocked, reason, next
}
