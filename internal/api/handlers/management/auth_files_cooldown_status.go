package management

import (
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type authCooldownStatus struct {
	Active     bool
	Scope      string
	Until      time.Time
	ModelCount int
}

func summarizeAuthCooldown(auth *coreauth.Auth, now time.Time) authCooldownStatus {
	if auth == nil || auth.Disabled || auth.Status == coreauth.StatusDisabled {
		return authCooldownStatus{}
	}
	if auth.Unavailable && strings.EqualFold(strings.TrimSpace(auth.CooldownScope), "auth") && auth.NextRetryAfter.After(now) {
		return authCooldownStatus{Active: true, Scope: "auth", Until: cooldownUntil(auth.NextRetryAfter, auth.Quota.NextRecoverAt)}
	}

	status := authCooldownStatus{Scope: "model"}
	for _, state := range auth.ModelStates {
		if !modelStateCooldownActive(state, now) {
			continue
		}
		status.Active = true
		status.ModelCount++
		if until := cooldownUntil(state.NextRetryAfter, state.Quota.NextRecoverAt); until.After(status.Until) {
			status.Until = until
		}
	}
	if !status.Active {
		return authCooldownStatus{}
	}
	return status
}

func modelCooldownForAuth(auth *coreauth.Auth, now time.Time, modelIDs ...string) authCooldownStatus {
	if auth == nil || auth.Disabled || auth.Status == coreauth.StatusDisabled {
		return authCooldownStatus{}
	}
	if auth.Unavailable && strings.EqualFold(strings.TrimSpace(auth.CooldownScope), "auth") && auth.NextRetryAfter.After(now) {
		return authCooldownStatus{Active: true, Scope: "auth", Until: cooldownUntil(auth.NextRetryAfter, auth.Quota.NextRecoverAt)}
	}
	for _, modelID := range modelIDs {
		modelID = strings.TrimSpace(modelID)
		if modelID == "" {
			continue
		}
		candidates := []string{modelID}
		if canonical := strings.TrimSpace(thinking.ParseSuffix(modelID).ModelName); canonical != "" && canonical != modelID {
			candidates = append(candidates, canonical)
		}
		for _, candidate := range candidates {
			state := auth.ModelStates[candidate]
			if modelStateCooldownActive(state, now) {
				return authCooldownStatus{Active: true, Scope: "model", Until: cooldownUntil(state.NextRetryAfter, state.Quota.NextRecoverAt), ModelCount: 1}
			}
		}
	}
	return authCooldownStatus{}
}

func modelStateCooldownActive(state *coreauth.ModelState, now time.Time) bool {
	return state != nil && state.Status != coreauth.StatusDisabled && state.Unavailable && state.NextRetryAfter.After(now)
}

func cooldownUntil(nextRetryAfter, nextRecoverAt time.Time) time.Time {
	if nextRecoverAt.After(nextRetryAfter) {
		return nextRecoverAt
	}
	return nextRetryAfter
}

func applyAuthCooldownStatus(entry map[string]any, status authCooldownStatus) {
	entry["cooldown_active"] = status.Active
	entry["cooldown_model_count"] = status.ModelCount
	if !status.Active {
		return
	}
	entry["cooldown_scope"] = status.Scope
	entry["cooldown_until"] = status.Until
}

func applyModelCooldownStatus(entry map[string]any, status authCooldownStatus) {
	entry["cooldown_active"] = status.Active
	if !status.Active {
		return
	}
	entry["scope"] = status.Scope
	entry["until"] = status.Until
}
