package executor

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestUpstreamAttemptTrackerIsolationAndErrorPriority(t *testing.T) {
	ctx := WithUpstreamAttempt(t.Context())
	cause := errors.New("upstream")
	if got := ErrorFromUpstreamAttempt(ctx, cause); got != cause {
		t.Fatal("local error was marked as an upstream attempt")
	}
	var work sync.WaitGroup
	for range 8 {
		work.Go(func() { MarkUpstreamAttempt(ctx) })
	}
	work.Wait()
	marked := ErrorFromUpstreamAttempt(ctx, cause)
	if _, ok := marked.(StatusError); ok {
		t.Fatal("status-less transport error gained an invented HTTP status")
	}
	if !IsUpstreamAttemptError(marked) || !errors.Is(marked, cause) || marked.Error() != cause.Error() {
		t.Fatal("marker lost upstream cause or text")
	}
	next := WithUpstreamAttempt(ctx)
	if IsUpstreamAttemptError(ErrorFromUpstreamAttempt(next, cause)) {
		t.Fatal("next attempt inherited the earlier network marker")
	}
	if PreferUpstreamError(marked, errors.New("no auth")) != marked || PreferUpstreamError(marked, context.Canceled) != context.Canceled || PreferUpstreamError(marked, nil) != nil {
		t.Fatal("error preference changed success or cancellation semantics")
	}
}

type observedStatusError struct{}

func (observedStatusError) Error() string              { return "rate limited" }
func (observedStatusError) StatusCode() int            { return 429 }
func (observedStatusError) Headers() http.Header       { return http.Header{"Retry-After": []string{"12"}} }
func (observedStatusError) RetryAfter() *time.Duration { value := 12 * time.Second; return &value }

func TestObservedUpstreamErrorKeepsDirectStatusHeadersAndRetryInterfaces(t *testing.T) {
	ctx := WithUpstreamAttempt(t.Context())
	MarkUpstreamAttempt(ctx)
	err := ErrorFromUpstreamAttempt(ctx, observedStatusError{})
	status, ok := err.(StatusError)
	if !ok || status.StatusCode() != 429 {
		t.Fatal("direct status interface was lost")
	}
	if err.(interface{ Headers() http.Header }).Headers().Get("Retry-After") != "12" {
		t.Fatal("response headers were lost")
	}
	if value := err.(interface{ RetryAfter() *time.Duration }).RetryAfter(); value == nil || *value != 12*time.Second {
		t.Fatal("retry hint was lost")
	}
}
