package executor

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

const ImageBootstrapPolicyMetadataKey = "image_bootstrap_policy"

type imageBootstrapPolicyContextKey struct{}

// ImageBootstrapPolicy is pinned even when both settings are disabled.
type ImageBootstrapPolicy struct {
	Timeout time.Duration
	Retries int
}

func (policy ImageBootstrapPolicy) Enabled() bool { return policy.Timeout > 0 || policy.Retries > 0 }

func WithImageBootstrapPolicy(ctx context.Context, policy ImageBootstrapPolicy) context.Context {
	return context.WithValue(ctx, imageBootstrapPolicyContextKey{}, policy)
}

func ImageBootstrapPolicyFromContext(ctx context.Context) (ImageBootstrapPolicy, bool) {
	if ctx == nil {
		return ImageBootstrapPolicy{}, false
	}
	policy, ok := ctx.Value(imageBootstrapPolicyContextKey{}).(ImageBootstrapPolicy)
	return policy, ok
}

// ImageBootstrapError permits credential failover without treating a local
// preparation failure as a credential fault or a whole-request timeout.
type ImageBootstrapError struct {
	Cause   error
	Timeout bool
}

func (err *ImageBootstrapError) Error() string {
	return fmt.Sprintf("chatgpt web image homepage request failed: %v", err.Cause)
}
func (err *ImageBootstrapError) Unwrap() error { return err.Cause }
func (err *ImageBootstrapError) StatusCode() int {
	if err.Timeout {
		return http.StatusGatewayTimeout
	}
	return http.StatusBadGateway
}
func (*ImageBootstrapError) SkipAuthResult() bool { return true }
func (*ImageBootstrapError) RetryOtherAuth() bool { return true }
func (err *ImageBootstrapError) ExecutionResultErrorCode() string {
	if err.Timeout {
		return "image_bootstrap_timeout"
	}
	return "image_bootstrap_network"
}

type ImageBootstrapMetrics struct {
	Attempts       uint64 `json:"attempts"`
	Retries        uint64 `json:"retries"`
	Timeouts       uint64 `json:"timeouts"`
	RetrySuccesses uint64 `json:"retry_successes"`
}

var imageBootstrapCounters struct {
	attempts       atomic.Uint64
	retries        atomic.Uint64
	timeouts       atomic.Uint64
	retrySuccesses atomic.Uint64
}

func ObserveImageBootstrapAttempt(retry bool) {
	imageBootstrapCounters.attempts.Add(1)
	if retry {
		imageBootstrapCounters.retries.Add(1)
	}
}
func ObserveImageBootstrapTimeout()      { imageBootstrapCounters.timeouts.Add(1) }
func ObserveImageBootstrapRetrySuccess() { imageBootstrapCounters.retrySuccesses.Add(1) }
func ImageBootstrapSnapshot() ImageBootstrapMetrics {
	return ImageBootstrapMetrics{
		Attempts: imageBootstrapCounters.attempts.Load(), Retries: imageBootstrapCounters.retries.Load(),
		Timeouts: imageBootstrapCounters.timeouts.Load(), RetrySuccesses: imageBootstrapCounters.retrySuccesses.Load(),
	}
}
