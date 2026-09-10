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

// observe reads the candidate's logical model, including stored effort variants. Callers must
// first enforce eligibility and hold a scheduler lock or use an auth snapshot.
func (c *candidateFailureChoice) observe(candidate *Auth, model string) {
	if candidate == nil {
		return
	}
	var selectedFailure *Error
	var selectedAt time.Time
	selectedModel := ""
	key := canonicalModelKey(model)
	for stateModel, state := range candidate.ModelStates {
		if state == nil || canonicalModelKey(stateModel) != key {
			continue
		}
		failure := state.LastError
		if failure == nil && strings.TrimSpace(state.StatusMessage) != "" {
			failure = &Error{Message: state.StatusMessage}
		}
		if failure != nil {
			updatedAt := state.UpdatedAt
			if updatedAt.IsZero() {
				updatedAt = candidate.UpdatedAt
			}
			if selectedFailure == nil || updatedAt.After(selectedAt) || updatedAt.Equal(selectedAt) && stateModel > selectedModel {
				selectedFailure, selectedAt, selectedModel = failure, updatedAt, stateModel
			}
		}
	}
	if selectedFailure != nil {
		c.model.consider(selectedFailure, selectedAt, candidate.ID)
		return
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
