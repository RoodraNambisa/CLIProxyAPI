package executor

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"time"
)

type upstreamAttemptContextKey struct{}
type upstreamAttemptState struct{ attempted atomic.Bool }

// WithUpstreamAttempt isolates transport evidence for one executor attempt.
func WithUpstreamAttempt(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, upstreamAttemptContextKey{}, &upstreamAttemptState{})
}

func TracksUpstreamAttempt(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	_, ok := ctx.Value(upstreamAttemptContextKey{}).(*upstreamAttemptState)
	return ok
}

func MarkUpstreamAttempt(ctx context.Context) {
	if ctx == nil {
		return
	}
	if state, ok := ctx.Value(upstreamAttemptContextKey{}).(*upstreamAttemptState); ok {
		state.attempted.Store(true)
	}
}

type upstreamAttemptError struct{ cause error }

func (e *upstreamAttemptError) Error() string         { return e.cause.Error() }
func (e *upstreamAttemptError) Unwrap() error         { return e.cause }
func (*upstreamAttemptError) UpstreamAttempted() bool { return true }

func (e *upstreamAttemptError) Headers() http.Header {
	var source interface{ Headers() http.Header }
	if errors.As(e.cause, &source) {
		return source.Headers()
	}
	return nil
}

func (e *upstreamAttemptError) RetryAfter() *time.Duration {
	var source interface{ RetryAfter() *time.Duration }
	if errors.As(e.cause, &source) {
		return source.RetryAfter()
	}
	return nil
}

type upstreamStatusAttemptError struct {
	*upstreamAttemptError
	status StatusError
}

func (e *upstreamStatusAttemptError) StatusCode() int { return e.status.StatusCode() }

func ErrorFromUpstreamAttempt(ctx context.Context, err error) error {
	if err == nil || ctx == nil || IsUpstreamAttemptError(err) {
		return err
	}
	state, _ := ctx.Value(upstreamAttemptContextKey{}).(*upstreamAttemptState)
	if state != nil && state.attempted.Load() {
		wrapped := &upstreamAttemptError{cause: err}
		var status StatusError
		if errors.As(err, &status) {
			return &upstreamStatusAttemptError{upstreamAttemptError: wrapped, status: status}
		}
		return wrapped
	}
	return err
}

func IsUpstreamAttemptError(err error) bool {
	var marked interface{ UpstreamAttempted() bool }
	return errors.As(err, &marked) && marked.UpstreamAttempted()
}

// PreferUpstreamError preserves an observed upstream cause over later local
// preparation/selection failures. Success and cancellation remain authoritative.
func PreferUpstreamError(previous, current error) error {
	if current == nil || errors.Is(current, context.Canceled) || errors.Is(current, context.DeadlineExceeded) {
		return current
	}
	if IsUpstreamAttemptError(previous) && !IsUpstreamAttemptError(current) {
		return previous
	}
	return current
}
