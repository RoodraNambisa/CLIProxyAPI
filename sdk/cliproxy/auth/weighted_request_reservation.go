package auth

import (
	"time"

	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

// weightedRequestReservation lets a selector commit credits after reservation.
// A bound session may bypass the selector, so the caller also checks the result.
type weightedRequestReservation struct {
	manager     *Manager
	options     core.Options
	now         time.Time
	blocked     *authRequestLimitBlock
	rejected    map[string]struct{}
	acquiredID  string
	stalePolicy bool
}

func (r *weightedRequestReservation) acquire(auth *Auth) bool {
	if auth == nil || r.stalePolicy {
		return false
	}
	if r.acquiredID != "" {
		return r.acquiredID == auth.ID
	}
	policy := r.manager.routingAuthRequestLimitPolicyForAuth(auth)
	policy.requestSlot = r.options.AuthRequestSlot
	acquired, block := r.manager.authRequestLimiter().tryAcquireAt(auth.ID, policy, r.now)
	if acquired {
		r.acquiredID = auth.ID
		return true
	}
	r.stalePolicy = block.stalePolicy
	*r.blocked = earlierAuthRequestLimitBlock(*r.blocked, block)
	r.rejected[auth.ID] = struct{}{}
	return false
}
