package auth

import (
	"context"
	"errors"
)

// effectiveCredentialRequestRetry preserves the local negative-as-zero contract.
func effectiveCredentialRequestRetry(auth *Auth, defaultRetry int) int {
	if override, ok := auth.RequestRetryOverride(); ok {
		return max(0, override)
	}
	return max(0, defaultRetry)
}

// requestRoundPickAllowed filters before scheduling or reserving capacity. The
// global default belongs to the logical request; each candidate is the current
// credential snapshot, including edits and runtime-instance retirement.
func (m *Manager) requestRoundPickAllowed(ctx context.Context, state *requestRoundState, maxCredentials, round, defaultRetry int) func(*Auth) bool {
	allowed := m.roundPickAllowed(state, maxCredentials, ctx)
	return func(auth *Auth) bool {
		return allowed(auth) && (round <= 0 || round <= effectiveCredentialRequestRetry(auth, defaultRetry))
	}
}

// An aggregate budget can outlive the candidates eligible for this request.
// Do not replace the previous failure or spin through empty remaining rounds.
func emptyCredentialRetryRound(state *requestRoundState, err error) bool {
	if state == nil || len(state.tried) != 0 || len(state.attempted) != 0 {
		return false
	}
	var authErr *Error
	return errors.As(err, &authErr) && authErr != nil && (authErr.Code == "auth_not_found" || authErr.Code == "auth_unavailable")
}
