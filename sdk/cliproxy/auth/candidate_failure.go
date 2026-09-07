package auth

import (
	"strings"
	"time"
)

type candidateFailureRecord struct {
	failure   *Error
	updatedAt time.Time
	authID    string
}

type candidateFailureChoice struct {
	model candidateFailureRecord
	auth  candidateFailureRecord
}

func (r *candidateFailureRecord) consider(failure *Error, updatedAt time.Time, authID string) {
	if failure == nil {
		return
	}
	if r.failure == nil || updatedAt.After(r.updatedAt) || (updatedAt.Equal(r.updatedAt) && authID > r.authID) {
		*r = candidateFailureRecord{failure: failure, updatedAt: updatedAt, authID: authID}
	}
}

// observe reads only the candidate model selected by the caller. Callers must
// first enforce eligibility and hold a scheduler lock or use an auth snapshot.
func (c *candidateFailureChoice) observe(candidate *Auth, model string) {
	if candidate == nil {
		return
	}
	state := candidate.ModelStates[model]
	if state == nil {
		state = candidate.ModelStates[canonicalModelKey(model)]
	}
	if state != nil {
		failure := state.LastError
		if failure == nil && strings.TrimSpace(state.StatusMessage) != "" {
			failure = &Error{Message: state.StatusMessage}
		}
		if failure != nil {
			updatedAt := state.UpdatedAt
			if updatedAt.IsZero() {
				updatedAt = candidate.UpdatedAt
			}
			c.model.consider(failure, updatedAt, candidate.ID)
			return
		}
	}
	failure := candidate.LastError
	if failure == nil && strings.TrimSpace(candidate.StatusMessage) != "" {
		failure = &Error{Message: candidate.StatusMessage}
	}
	c.auth.consider(failure, candidate.UpdatedAt, candidate.ID)
}

func (c *candidateFailureChoice) merge(other candidateFailureChoice) {
	c.model.consider(other.model.failure, other.model.updatedAt, other.model.authID)
	c.auth.consider(other.auth.failure, other.auth.updatedAt, other.auth.authID)
}

func (c candidateFailureChoice) latest() *Error {
	if c.model.failure != nil {
		return c.model.failure
	}
	return c.auth.failure
}
