package executor

import (
	"errors"
	"strings"
)

// ErrorResponseSourceSnapshot identifies the provider and credential priority
// responsible for a final execution error without exposing credential identity.
type ErrorResponseSourceSnapshot struct {
	Provider        string
	AuthPriority    int
	HasAuthPriority bool
}

// CredentialErrorResponseSource creates a normalized credential source snapshot.
func CredentialErrorResponseSource(provider string, authPriority int) ErrorResponseSourceSnapshot {
	return ErrorResponseSourceSnapshot{
		Provider:        strings.ToLower(strings.TrimSpace(provider)),
		AuthPriority:    authPriority,
		HasAuthPriority: true,
	}
}

// LocalErrorResponseSource creates a source snapshot for errors without a selected credential.
func LocalErrorResponseSource() ErrorResponseSourceSnapshot {
	return ErrorResponseSourceSnapshot{Provider: "local"}
}

type errorResponseSourceCarrier interface {
	ErrorResponseSource() ErrorResponseSourceSnapshot
}

type sourcedExecutionError struct {
	cause  error
	source ErrorResponseSourceSnapshot
}

func (e *sourcedExecutionError) Error() string {
	if e == nil || e.cause == nil {
		return ""
	}
	return e.cause.Error()
}

func (e *sourcedExecutionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *sourcedExecutionError) ErrorResponseSource() ErrorResponseSourceSnapshot {
	if e == nil {
		return ErrorResponseSourceSnapshot{}
	}
	return e.source
}

// WithErrorResponseSource attaches an immutable source snapshot unless one is already present.
func WithErrorResponseSource(err error, source ErrorResponseSourceSnapshot) error {
	if err == nil {
		return nil
	}
	if _, ok := ErrorResponseSourceOf(err); ok {
		return err
	}
	source.Provider = strings.ToLower(strings.TrimSpace(source.Provider))
	if source.Provider == "" {
		return err
	}
	if source.Provider == "local" {
		source.AuthPriority = 0
		source.HasAuthPriority = false
	}
	return &sourcedExecutionError{cause: err, source: source}
}

// ErrorResponseSourceOf returns a source snapshot attached anywhere in an error chain.
func ErrorResponseSourceOf(err error) (ErrorResponseSourceSnapshot, bool) {
	if err == nil {
		return ErrorResponseSourceSnapshot{}, false
	}
	var carrier errorResponseSourceCarrier
	if !errors.As(err, &carrier) || carrier == nil {
		return ErrorResponseSourceSnapshot{}, false
	}
	source := carrier.ErrorResponseSource()
	source.Provider = strings.ToLower(strings.TrimSpace(source.Provider))
	if source.Provider == "" {
		return ErrorResponseSourceSnapshot{}, false
	}
	if source.Provider == "local" {
		source.AuthPriority = 0
		source.HasAuthPriority = false
	}
	return source, true
}

// WithoutErrorResponseSource removes one outer source marker and restores the original error object.
func WithoutErrorResponseSource(err error) error {
	if sourced, ok := err.(*sourcedExecutionError); ok && sourced != nil {
		return sourced.cause
	}
	return err
}
