package auth

import (
	"sort"
	"time"
)

// normalizeModelStates only changes an owned auth snapshot or a locked runtime auth.
func normalizeModelStates(auth *Auth) {
	if auth == nil || len(auth.ModelStates) == 0 {
		return
	}
	changed := false
	for model := range auth.ModelStates {
		if canonicalModelKey(model) != model {
			changed = true
			break
		}
	}
	if !changed {
		return
	}
	models := make([]string, 0, len(auth.ModelStates))
	for model := range auth.ModelStates {
		models = append(models, model)
	}
	sort.Strings(models)
	now := time.Now()
	normalized := make(map[string]*ModelState, len(auth.ModelStates))
	for _, model := range models {
		key := canonicalModelKey(model)
		normalized[key] = mergeModelStates(normalized[key], auth.ModelStates[model], now)
	}
	auth.ModelStates = normalized
}

func mergeModelStates(target, source *ModelState, now time.Time) *ModelState {
	if target == nil {
		return source.Clone()
	}
	if source == nil {
		return target
	}
	preferred, fallback := target, source
	if !source.UpdatedAt.Before(target.UpdatedAt) {
		preferred, fallback = source, target
	}
	merged := preferred.Clone()
	if merged.LastError == nil {
		merged.LastError = cloneError(fallback.LastError)
	}
	if merged.StatusMessage == "" {
		merged.StatusMessage = fallback.StatusMessage
	}
	// Keep availability fields together: an inactive deadline must not turn a
	// zero-deadline diagnostic state into a new cooldown.
	targetBlocked, targetReason, targetNext := modelStateBlock(target, now)
	sourceBlocked, sourceReason, sourceNext := modelStateBlock(source, now)
	availability := target
	if !targetBlocked || sourceBlocked && sourceNext.After(targetNext) {
		availability = source
	}
	if targetBlocked || sourceBlocked {
		merged.Unavailable = availability.Unavailable
		merged.NextRetryAfter = availability.NextRetryAfter
		merged.Quota = availability.Quota
		merged.Status = StatusError
		if targetBlocked && targetReason == blockReasonOther || sourceBlocked && sourceReason == blockReasonOther {
			// Credits may bypass quota only when no sibling has a non-quota block.
			merged.Quota.Exceeded = false
		}
	}
	merged.Quota.BackoffLevel = max(target.Quota.BackoffLevel, source.Quota.BackoffLevel)
	merged.Quota.StrikeCount = max(target.Quota.StrikeCount, source.Quota.StrikeCount)
	if target.Status == StatusDisabled || source.Status == StatusDisabled {
		merged.Status = StatusDisabled
	}
	return merged
}
