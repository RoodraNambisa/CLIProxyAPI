package auth

import "strings"

const (
	LifecycleStateLoginPending        = "login_pending"
	LifecycleStateActive              = "active"
	LifecycleStateRefreshing          = "refreshing"
	LifecycleStateReloginPending      = "relogin_pending"
	LifecycleStateReauthRequired      = "reauth_required"
	LifecycleStateInteractionRequired = "interaction_required"
	LifecycleStateDead                = "dead"
)

// RuntimeStatusForLifecycle maps a persistent lifecycle state to its runtime status.
func RuntimeStatusForLifecycle(state string) Status {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case LifecycleStateActive:
		return StatusActive
	case LifecycleStateRefreshing:
		return StatusRefreshing
	case LifecycleStateLoginPending, LifecycleStateReloginPending:
		return StatusPending
	case LifecycleStateReauthRequired, LifecycleStateInteractionRequired, LifecycleStateDead:
		return StatusError
	default:
		return StatusUnknown
	}
}

// LifecycleState returns an optional provider-defined persistent lifecycle state.
func (a *Auth) LifecycleState() string {
	if a == nil {
		return ""
	}
	if a.Attributes != nil && strings.TrimSpace(a.Attributes["compat_name"]) != "" {
		return ""
	}
	if a.Metadata != nil {
		state, _ := a.Metadata["lifecycle_state"].(string)
		if normalized := strings.ToLower(strings.TrimSpace(state)); normalized != "" {
			return normalized
		}
	}
	if !strings.EqualFold(strings.TrimSpace(a.Provider), "chatgpt-web") {
		return ""
	}
	if a.Metadata != nil {
		if token, _ := a.Metadata["access_token"].(string); strings.TrimSpace(token) != "" {
			return LifecycleStateActive
		}
	}
	return LifecycleStateLoginPending
}

// LifecycleSelectable reports whether a credential may participate in request routing.
// Credentials without lifecycle metadata retain the legacy behavior.
func (a *Auth) LifecycleSelectable() bool {
	state := a.LifecycleState()
	return state == "" || state == LifecycleStateActive
}

// LifecycleRefreshable reports whether the automatic token refresh loop may schedule the credential.
func (a *Auth) LifecycleRefreshable() bool {
	state := a.LifecycleState()
	return state == "" || state == LifecycleStateActive
}

func applyLifecycleRuntimeState(auth *Auth) {
	if auth == nil {
		return
	}
	state := auth.LifecycleState()
	if state == "" {
		return
	}
	status := RuntimeStatusForLifecycle(state)
	if status == StatusUnknown {
		return
	}
	auth.Status = status
	auth.StatusMessage = ""
	if auth.Metadata != nil {
		reason, _ := auth.Metadata["lifecycle_reason"].(string)
		auth.StatusMessage = strings.TrimSpace(reason)
	}
}
