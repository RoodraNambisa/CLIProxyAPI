package auth

import (
	"encoding/json"
	"errors"
	"net/http"
)

// storedAuthFailureError retains a previous credential failure as diagnostic
// evidence. Unwrap follows only the current selection error: a stored failure
// must not change retry classification or imply this request reached upstream.
type storedAuthFailureError struct {
	current error
	failure *Error
}

func (e *storedAuthFailureError) Error() string   { return e.current.Error() }
func (e *storedAuthFailureError) Unwrap() error   { return e.current }
func (e *storedAuthFailureError) StatusCode() int { return statusCodeFromError(e.current) }
func (e *storedAuthFailureError) Headers() http.Header {
	var source interface{ Headers() http.Header }
	if errors.As(e.current, &source) && source != nil {
		return source.Headers().Clone()
	}
	return nil
}
func (e *storedAuthFailureError) MarshalJSON() ([]byte, error) { return json.Marshal(e.current) }

// WithStoredAuthFailure attaches an independent diagnostic snapshot without
// changing the current error's public text, JSON, classification or identity.
func WithStoredAuthFailure(current error, failure *Error) error {
	if current == nil || failure == nil {
		return current
	}
	return &storedAuthFailureError{current: current, failure: cloneError(failure)}
}

// StoredAuthFailureOf returns a caller-owned copy of the stored failure.
// The failure may contain private provider diagnostics; do not expose it as JSON.
func StoredAuthFailureOf(err error) *Error {
	var stored *storedAuthFailureError
	if errors.As(err, &stored) && stored != nil {
		return cloneError(stored.failure)
	}
	return nil
}
