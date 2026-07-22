package auth

import (
	"strings"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
)

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
		rawState, stateExists := a.Metadata["lifecycle_state"]
		state, stateIsString := rawState.(string)
		if stateExists && !stateIsString && strings.EqualFold(strings.TrimSpace(a.Provider), "chatgpt-web") {
			return LifecycleStateReauthRequired
		}
		if normalized := strings.ToLower(strings.TrimSpace(state)); normalized != "" {
			if strings.EqualFold(strings.TrimSpace(a.Provider), "chatgpt-web") {
				switch normalized {
				case LifecycleStateLoginPending,
					LifecycleStateActive,
					LifecycleStateRefreshing,
					LifecycleStateReloginPending,
					LifecycleStateReauthRequired,
					LifecycleStateInteractionRequired,
					LifecycleStateDead:
					return normalized
				default:
					return LifecycleStateReauthRequired
				}
			}
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
	if isCodexAgentIdentityAuth(a) {
		return false
	}
	state := a.LifecycleState()
	return state == "" || state == LifecycleStateActive
}

func isCodexAgentIdentityAuth(auth *Auth) bool {
	if auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") || auth.Metadata == nil {
		return false
	}
	authMode, _ := auth.Metadata["auth_mode"].(string)
	return strings.EqualFold(strings.TrimSpace(authMode), "agentIdentity")
}

func applyLifecycleRuntimeState(auth *Auth) {
	if auth == nil {
		return
	}
	state := auth.LifecycleState()
	if auth.Disabled {
		auth.Status = StatusDisabled
		auth.StatusMessage = ""
		if state != "" && state != LifecycleStateActive && auth.Metadata != nil {
			if reason, _ := auth.Metadata["lifecycle_reason"].(string); strings.TrimSpace(reason) != "" {
				auth.StatusMessage = lifecycleRuntimeReason(auth, reason)
			}
		}
		return
	}
	if state == "" {
		return
	}
	if state == LifecycleStateActive {
		now := time.Now()
		updateAggregatedAvailability(auth, now)
		if activeLifecycleCooldown(auth, now) {
			return
		}
	}
	status := RuntimeStatusForLifecycle(state)
	if status == StatusUnknown {
		return
	}
	auth.Status = status
	auth.StatusMessage = ""
	if auth.Metadata != nil {
		reason, _ := auth.Metadata["lifecycle_reason"].(string)
		auth.StatusMessage = lifecycleRuntimeReason(auth, reason)
	}
}

// ApplyLifecycleRuntimeState restores the runtime status represented by
// persistent provider lifecycle metadata.
func ApplyLifecycleRuntimeState(auth *Auth) {
	applyLifecycleRuntimeState(auth)
}

func activeLifecycleCooldown(auth *Auth, now time.Time) bool {
	if auth == nil {
		return false
	}
	if auth.Unavailable && auth.CooldownScope == cooldownScopeAuth && auth.NextRetryAfter.After(now) {
		return true
	}
	for _, state := range auth.ModelStates {
		if state != nil && state.Status != StatusDisabled && state.Unavailable && state.NextRetryAfter.After(now) {
			return true
		}
	}
	return false
}

func lifecycleRuntimeReason(auth *Auth, reason string) string {
	if auth != nil && strings.EqualFold(strings.TrimSpace(auth.Provider), chatgptwebauth.Provider) {
		return chatgptwebauth.SafeLifecycleReason(reason)
	}
	return strings.TrimSpace(reason)
}
