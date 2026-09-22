package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

// ResponseGuardRecord contains metadata only; it never retains State or Cookie values.
type ResponseGuardRecord struct {
	ClientModelSet bool                        `json:"-"`
	ID             string                      `json:"id"`
	At             time.Time                   `json:"at"`
	Attempt        int                         `json:"attempt"`
	RequestedModel string                      `json:"requested_model"`
	UpstreamModel  string                      `json:"upstream_model"`
	OriginalModel  string                      `json:"original_model"`
	ResponseModel  string                      `json:"response_model"`
	StatePresent   bool                        `json:"state_present"`
	StateLength    int                         `json:"state_length"`
	Verdict        config.CodexResponseVerdict `json:"verdict"`
	Outcome        string                      `json:"outcome"`
	Phase          string                      `json:"phase"`
	Rule           int                         `json:"rule"`
	RuleID         string                      `json:"rule_id,omitempty"`
	RuleName       string                      `json:"rule_name,omitempty"`
	ClearAffinity  string                      `json:"clear_affinity"`
	Stream         bool                        `json:"stream"`
	Transport      string                      `json:"transport"`
	Status         int                         `json:"status"`
	UpstreamStatus int                         `json:"upstream_status"`
	Completed      bool                        `json:"completed"`
	Error          string                      `json:"error,omitempty"`
	Usage          map[string]int64            `json:"usage,omitempty"`
	Rewritten      bool                        `json:"rewritten"`
	RewriteRule    int                         `json:"rewrite_rule,omitempty"`
}

// ResponseGuardAttempt is shared only with the current upstream attempt and its
// downstream model rewriter. Publishing is serialized to prevent stale updates.
type ResponseGuardAttempt struct {
	Config  config.CodexResponseGuardConfig
	mu      sync.Mutex
	record  ResponseGuardRecord
	started bool
	update  func(ResponseGuardRecord)
}

func NewResponseGuardAttempt(cfg config.CodexResponseGuardConfig, requested string, number int, update func(ResponseGuardRecord)) *ResponseGuardAttempt {
	return &ResponseGuardAttempt{Config: cfg, record: ResponseGuardRecord{ID: fmt.Sprint(responseGuardSequence.Add(1)), At: time.Now().UTC(), Attempt: number, RequestedModel: requested}, update: update}
}

func (a *ResponseGuardAttempt) Start(p config.CodexResponseGuardPolicy, model string, stream bool, transport string, status int) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.started = true
	a.record.UpstreamModel, a.record.Stream, a.record.Transport = model, stream, transport
	a.record.Rule, a.record.RuleID, a.record.RuleName = p.Rule, p.RuleID, p.RuleName
	a.record.ClearAffinity, a.record.UpstreamStatus = p.ClearAffinity, status
	a.record.Status = 200
	a.record.Outcome = "pending"
	a.record.Verdict = config.CodexResponseVerdict{Model: "unknown", State: "unknown", Reasons: []string{}}
}

func (a *ResponseGuardAttempt) Update(fn func(*ResponseGuardRecord)) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.started {
		return
	}
	fn(&a.record)
	if a.update != nil {
		a.update(cloneGuardRecord(a.record))
	}
}

func (a *ResponseGuardAttempt) SetClientModel(model string, rewritten bool, rule int) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.started || a.record.ClientModelSet && a.record.ResponseModel == model && (!rewritten || a.record.Rewritten && a.record.RewriteRule == rule) {
		return
	}
	a.record.ResponseModel, a.record.ClientModelSet = model, true
	if rewritten {
		a.record.Rewritten, a.record.RewriteRule = true, rule
	}
	if a.update != nil {
		a.update(cloneGuardRecord(a.record))
	}
}

func (a *ResponseGuardAttempt) Snapshot() (ResponseGuardRecord, bool) {
	if a == nil {
		return ResponseGuardRecord{}, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return cloneGuardRecord(a.record), a.started
}

func cloneGuardRecord(r ResponseGuardRecord) ResponseGuardRecord {
	r.Verdict.Reasons = slices.Clone(r.Verdict.Reasons)
	if r.Usage != nil {
		usage := make(map[string]int64, len(r.Usage))
		for k, v := range r.Usage {
			usage[k] = v
		}
		r.Usage = usage
	}
	return r
}

var responseGuardSequence atomic.Uint64

type responseGuardRequestKey struct{}
type responseGuardModeKey struct{}
type responseGuardAttemptKey struct{}

func WithResponseGuardAttempt(ctx context.Context, attempt *ResponseGuardAttempt) context.Context {
	if attempt == nil {
		return ctx
	}
	return context.WithValue(ctx, responseGuardAttemptKey{}, attempt)
}

func ResponseGuardConfigFromContext(ctx context.Context) *config.CodexResponseGuardConfig {
	if ctx == nil {
		return nil
	}
	a, _ := ctx.Value(responseGuardAttemptKey{}).(*ResponseGuardAttempt)
	if a == nil {
		return nil
	}
	return &a.Config
}

type responseGuardRequest struct {
	mu       sync.Mutex
	count    int
	excluded map[string]struct{}
}

func WithResponseGuardRequest(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Value(responseGuardRequestKey{}) != nil {
		return ctx
	}
	return context.WithValue(ctx, responseGuardRequestKey{}, &responseGuardRequest{excluded: map[string]struct{}{}})
}

func NextResponseGuardAttempt(ctx context.Context) int {
	r, _ := ctx.Value(responseGuardRequestKey{}).(*responseGuardRequest)
	if r == nil {
		return 1
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.count++
	return r.count
}

func ExcludeResponseGuardAuth(ctx context.Context, id string) {
	r, _ := ctx.Value(responseGuardRequestKey{}).(*responseGuardRequest)
	if r != nil {
		r.mu.Lock()
		r.excluded[id] = struct{}{}
		r.mu.Unlock()
	}
}

func AddResponseGuardExclusions(ctx context.Context, tried map[string]struct{}) {
	r, _ := ctx.Value(responseGuardRequestKey{}).(*responseGuardRequest)
	if r != nil {
		r.mu.Lock()
		defer r.mu.Unlock()
		for id := range r.excluded {
			tried[id] = struct{}{}
		}
	}
}

// WithResponseGuardMode is a trusted management diagnostic option, never parsed
// from inference request headers or bodies.
func WithResponseGuardMode(ctx context.Context, mode string) context.Context {
	return context.WithValue(ctx, responseGuardModeKey{}, mode)
}
func ResponseGuardMode(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	s, _ := ctx.Value(responseGuardModeKey{}).(string)
	return s
}

type ResponseGuardError struct {
	Policy    config.CodexResponseGuardPolicy
	Committed bool
}

func (e *ResponseGuardError) Error() string {
	data, _ := json.Marshal(map[string]any{"error": map[string]string{"type": e.Policy.ErrorType, "code": e.Policy.ErrorCode, "message": e.Policy.ErrorMessage}})
	return string(data)
}
func (*ResponseGuardError) StatusCode() int             { return 429 }
func (*ResponseGuardError) SkipAuthResult() bool        { return true }
func (*ResponseGuardError) PreserveErrorResponse() bool { return true }
func (e *ResponseGuardError) RetryOtherAuth() bool {
	return e.Policy.OnReject == "retry" && !e.Committed
}
func (e *ResponseGuardError) RequestCommitted() bool { return e.Committed }
func IsResponseGuardError(err error) bool {
	var target *ResponseGuardError
	return errors.As(err, &target)
}
