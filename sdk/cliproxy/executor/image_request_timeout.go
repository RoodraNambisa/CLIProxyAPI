package executor

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

const ImageRequestBudgetMetadataKey = "image_request_budget"

type imageRequestBudgetContextKey struct{}

// ImageRequestTimeoutError is a local, terminal budget failure, not an account fault.
type ImageRequestTimeoutError struct {
	Limit time.Duration
}

func (err *ImageRequestTimeoutError) Error() string {
	payload, _ := json.Marshal(map[string]any{"error": map[string]any{
		"message": "The image generation request exceeded its configured time limit.",
		"type":    "server_error", "code": "image_request_timeout",
	}})
	return string(payload)
}
func (*ImageRequestTimeoutError) Unwrap() error                    { return context.DeadlineExceeded }
func (*ImageRequestTimeoutError) StatusCode() int                  { return http.StatusGatewayTimeout }
func (*ImageRequestTimeoutError) SkipAuthResult() bool             { return true }
func (*ImageRequestTimeoutError) RetryOtherAuth() bool             { return false }
func (*ImageRequestTimeoutError) ExecutionResultErrorCode() string { return "image_request_timeout" }

func IsImageRequestTimeout(err error) bool {
	var timeout *ImageRequestTimeoutError
	return errors.As(err, &timeout)
}

// ImageRequestBudget keeps one start time and policy across retries and n aggregation.
// Only the selected provider's limit applies. Before selection, a common deadline
// is safe only when every eligible provider has a finite budget.
type ImageRequestBudget struct {
	started    time.Time
	codex      time.Duration
	web        time.Duration
	ctx        context.Context
	cancel     context.CancelCauseFunc
	stopParent func() bool
	mu         sync.Mutex
	timer      *time.Timer
	generation uint64
	selected   bool
	closed     bool
	timeout    *ImageRequestTimeoutError
}

func NewImageRequestBudget(parent context.Context, started time.Time, codex, web time.Duration) *ImageRequestBudget {
	if codex <= 0 && web <= 0 {
		return nil
	}
	if parent == nil {
		parent = context.Background()
	}
	if started.IsZero() {
		started = time.Now()
	}
	ctx, cancel := context.WithCancelCause(parent)
	budget := &ImageRequestBudget{started: started, codex: codex, web: web, ctx: ctx, cancel: cancel}
	budget.mu.Lock()
	budget.stopParent = context.AfterFunc(parent, budget.Close)
	budget.mu.Unlock()
	return budget
}

func (budget *ImageRequestBudget) Limit(provider string) time.Duration {
	if budget == nil {
		return 0
	}
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "codex":
		return budget.codex
	case "chatgpt-web":
		return budget.web
	default:
		return 0
	}
}

func (budget *ImageRequestBudget) Candidates(providers []string) error {
	if budget == nil {
		return nil
	}
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if budget.timeout != nil {
		return budget.timeout
	}
	if budget.selected || budget.closed {
		return nil
	}
	var limit time.Duration
	for _, provider := range providers {
		current := budget.Limit(provider)
		if current <= 0 {
			limit = 0
			break
		}
		limit = max(limit, current)
	}
	return budget.armLocked(limit)
}

func (budget *ImageRequestBudget) Select(provider string) error {
	if budget == nil {
		return nil
	}
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if budget.timeout != nil {
		return budget.timeout
	}
	if budget.closed {
		return context.Canceled
	}
	if err := budget.ctx.Err(); err != nil {
		return err
	}
	budget.selected = true
	return budget.armLocked(budget.Limit(provider))
}

func (budget *ImageRequestBudget) armLocked(limit time.Duration) error {
	budget.generation++
	if budget.timer != nil {
		budget.timer.Stop()
		budget.timer = nil
	}
	if limit <= 0 {
		return nil
	}
	if remaining := time.Until(budget.started.Add(limit)); remaining > 0 {
		generation := budget.generation
		budget.timer = time.AfterFunc(remaining, func() {
			budget.mu.Lock()
			defer budget.mu.Unlock()
			if !budget.closed && budget.timeout == nil && budget.generation == generation && budget.ctx.Err() == nil {
				budget.timeout = &ImageRequestTimeoutError{Limit: limit}
				budget.cancel(budget.timeout)
			}
		})
		return nil
	}
	if err := budget.ctx.Err(); err != nil {
		return err
	}
	budget.timeout = &ImageRequestTimeoutError{Limit: limit}
	budget.cancel(budget.timeout)
	return budget.timeout
}

func (budget *ImageRequestBudget) Err() error {
	if budget == nil {
		return nil
	}
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if budget.timeout != nil {
		return budget.timeout
	}
	return nil
}

func (budget *ImageRequestBudget) Close() {
	if budget == nil {
		return
	}
	budget.mu.Lock()
	budget.closed = true
	budget.generation++
	if budget.timer != nil {
		budget.timer.Stop()
		budget.timer = nil
	}
	stopParent := budget.stopParent
	budget.mu.Unlock()
	if stopParent != nil {
		stopParent()
	}
	budget.cancel(nil)
}

// Bind preserves caller values and cancellation without cancelling the downstream
// HTTP request; that connection must remain writable for the terminal error.
func (budget *ImageRequestBudget) Bind(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	if budget == nil {
		return parent, func() {}
	}
	ctx, cancel := context.WithCancelCause(parent)
	stop := context.AfterFunc(budget.ctx, func() { cancel(context.Cause(budget.ctx)) })
	if budget.ctx.Err() != nil {
		cancel(context.Cause(budget.ctx))
	}
	return context.WithValue(ctx, imageRequestBudgetContextKey{}, budget), func() { stop(); cancel(nil) }
}

func (budget *ImageRequestBudget) PreflightContext(parent context.Context, provider string) (context.Context, context.CancelFunc) {
	if limit := budget.Limit(provider); limit > 0 {
		return context.WithDeadlineCause(parent, budget.started.Add(limit), &ImageRequestTimeoutError{Limit: limit})
	}
	return parent, func() {}
}

func ImageRequestBudgetFromContext(ctx context.Context) *ImageRequestBudget {
	if ctx == nil {
		return nil
	}
	budget, _ := ctx.Value(imageRequestBudgetContextKey{}).(*ImageRequestBudget)
	return budget
}

func ImageRequestBudgetFromOptions(opts Options) *ImageRequestBudget {
	budget, _ := opts.Metadata[ImageRequestBudgetMetadataKey].(*ImageRequestBudget)
	return budget
}

func ImageRequestContextError(ctx context.Context, original error) error {
	if IsImageRequestTimeout(original) {
		return original
	}
	if ctx != nil {
		if cause := context.Cause(ctx); IsImageRequestTimeout(cause) {
			return cause
		}
		if err := ImageRequestBudgetFromContext(ctx).Err(); err != nil {
			return err
		}
	}
	return original
}

// GuardImageResponseBody makes the configured request budget interrupt body reads,
// including custom HTTP clients that do not observe cancellation after headers.
func GuardImageResponseBody(ctx context.Context, body io.ReadCloser) io.ReadCloser {
	if body == nil || ImageRequestBudgetFromContext(ctx) == nil {
		return body
	}
	guard := &imageBudgetBody{body: body, ctx: ctx}
	guard.mu.Lock()
	guard.stop = context.AfterFunc(ctx, func() { _ = guard.Close() })
	guard.mu.Unlock()
	return guard
}

type imageBudgetBody struct {
	body io.ReadCloser
	ctx  context.Context
	stop func() bool
	mu   sync.Mutex
	once sync.Once
	err  error
}

func (body *imageBudgetBody) Read(p []byte) (int, error) {
	if err := body.ctx.Err(); err != nil {
		return 0, ImageRequestContextError(body.ctx, err)
	}
	n, err := body.body.Read(p)
	if body.ctx.Err() != nil {
		err = ImageRequestContextError(body.ctx, body.ctx.Err())
	}
	return n, err
}
func (body *imageBudgetBody) Close() error {
	body.once.Do(func() {
		body.mu.Lock()
		stop := body.stop
		body.mu.Unlock()
		if stop != nil {
			stop()
		}
		body.err = body.body.Close()
	})
	return body.err
}
