package auth

import "net/http"

// responseHeaderError keeps response hints separate from selection control errors.
type responseHeaderError struct {
	cause   error
	headers http.Header
}

func (e *responseHeaderError) Error() string {
	if e == nil || e.cause == nil {
		return ""
	}
	return e.cause.Error()
}

func (e *responseHeaderError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *responseHeaderError) StatusCode() int {
	if e == nil {
		return 0
	}
	return statusCodeFromError(e.cause)
}

func (e *responseHeaderError) Headers() http.Header {
	if e == nil {
		return nil
	}
	return e.headers.Clone()
}

// WithResponseHeaders captures response hints without changing the underlying
// error's classification, identity or source. Callers own all returned maps.
func WithResponseHeaders(err error, headers http.Header) error {
	if err == nil || len(headers) == 0 {
		return err
	}
	return &responseHeaderError{cause: err, headers: headers.Clone()}
}
