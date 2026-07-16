package chatgptweb

import (
	"errors"
	"fmt"
)

type AuthError struct {
	Code           string         `json:"code"`
	State          LifecycleState `json:"state"`
	LifecycleState LifecycleState `json:"lifecycle_state"`
	Status         int            `json:"status"`
	StatusCode     int            `json:"status_code"`
	Retryable      bool           `json:"retryable"`
	Terminal       bool           `json:"terminal"`
	Message        string         `json:"message,omitempty"`
	Cause          error          `json:"-"`
}

func (authError *AuthError) Error() string {
	if authError == nil {
		return "authentication failed"
	}
	if authError.Message != "" {
		return fmt.Sprintf("%s: %s", authError.Code, authError.Message)
	}
	return authError.Code
}

func (authError *AuthError) Unwrap() error {
	if authError == nil {
		return nil
	}
	return authError.Cause
}

func newAuthError(code string, state LifecycleState, status int, retryable, terminal bool, message string, cause error) *AuthError {
	return &AuthError{
		Code:           code,
		State:          state,
		LifecycleState: state,
		Status:         status,
		StatusCode:     status,
		Retryable:      retryable,
		Terminal:       terminal,
		Message:        message,
		Cause:          cause,
	}
}

func AsAuthError(err error) (*AuthError, bool) {
	var authError *AuthError
	if !errors.As(err, &authError) {
		return nil, false
	}
	return authError, true
}

func IsRetryable(err error) bool {
	authError, ok := AsAuthError(err)
	return ok && authError.Retryable
}

func IsTerminal(err error) bool {
	authError, ok := AsAuthError(err)
	return ok && authError.Terminal
}

func IsLifecycleState(err error, state LifecycleState) bool {
	authError, ok := AsAuthError(err)
	return ok && authError.State == state
}

func IsInteractionRequired(err error) bool {
	return IsLifecycleState(err, LifecycleInteractionRequired)
}
