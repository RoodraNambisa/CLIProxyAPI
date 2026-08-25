package executor

import (
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"
)

const chatGPTWebImagePollStallRetryAfter = 30 * time.Second

// ImagePollStallError reports process-wide poll transport stagnation. It is a
// local admission failure and must not affect credential state.
type ImagePollStallError struct{}

func (*ImagePollStallError) Error() string {
	payload, _ := json.Marshal(map[string]any{"error": map[string]any{
		"message":       "chatgpt web image polling is temporarily stalled",
		"type":          "server_error",
		"code":          "chatgpt_web_image_poll_stalled",
		"failure_stage": "admission",
	}})
	return string(payload)
}

func (*ImagePollStallError) StatusCode() int { return http.StatusServiceUnavailable }

func (*ImagePollStallError) SkipAuthResult() bool { return true }

func (*ImagePollStallError) RetryOtherAuth() bool { return false }

func (*ImagePollStallError) ExecutionResultErrorCode() string {
	return "chatgpt_web_image_poll_stalled"
}

func (*ImagePollStallError) ChatGPTWebFailureStage() string { return "admission" }

func (*ImagePollStallError) RetryAfter() *time.Duration {
	delay := chatGPTWebImagePollStallRetryAfter
	return &delay
}

func (*ImagePollStallError) Headers() http.Header {
	return http.Header{"Retry-After": []string{"30"}}
}

// IsImagePollStallError reports whether err is the process-wide poll breaker.
func IsImagePollStallError(err error) bool {
	var stallErr *ImagePollStallError
	return errors.As(err, &stallErr)
}

// ImagePollStallBreakerSnapshot is a low-cardinality process-wide view of the
// ChatGPT Web image poll breaker.
type ImagePollStallBreakerSnapshot struct {
	Enabled              bool          `json:"enabled"`
	Open                 bool          `json:"open"`
	StallSeconds         int           `json:"stall_seconds"`
	OpenedAt             *time.Time    `json:"opened_at,omitempty"`
	FullSince            *time.Time    `json:"full_since,omitempty"`
	LastCompletionAt     *time.Time    `json:"last_completion_at,omitempty"`
	NoCompletionAge      time.Duration `json:"no_completion_age_nanos"`
	Rejected             uint64        `json:"rejected"`
	TransportCompletions uint64        `json:"transport_completions"`
	CanceledCompletions  uint64        `json:"canceled_completions"`
}

type imagePollStallBreaker struct {
	mu sync.Mutex

	enabled          bool
	stallDuration    time.Duration
	open             bool
	openedAt         time.Time
	fullSince        time.Time
	lastCompletionAt time.Time
	rejected         uint64
	completed        uint64
	canceled         uint64
	now              func() time.Time
}

func newImagePollStallBreaker(now func() time.Time) *imagePollStallBreaker {
	if now == nil {
		now = time.Now
	}
	return &imagePollStallBreaker{now: now}
}

func (breaker *imagePollStallBreaker) configure(enabled bool, stallDuration time.Duration) {
	if breaker == nil {
		return
	}
	if stallDuration <= 0 {
		stallDuration = 120 * time.Second
	}
	breaker.mu.Lock()
	wasEnabled := breaker.enabled
	breaker.enabled = enabled
	breaker.stallDuration = stallDuration
	if !enabled {
		breaker.open = false
		breaker.openedAt = time.Time{}
		breaker.fullSince = time.Time{}
	} else if !wasEnabled {
		breaker.fullSince = time.Time{}
	}
	breaker.mu.Unlock()
}

func (breaker *imagePollStallBreaker) observeAdmission(snapshot ImageExecutionAdmissionSnapshot) {
	if breaker == nil {
		return
	}
	now := breaker.now()
	breaker.mu.Lock()
	breaker.observeAdmissionLocked(snapshot, now)
	breaker.evaluateLocked(snapshot, now)
	breaker.mu.Unlock()
}

func (breaker *imagePollStallBreaker) observeCompletion(canceled bool, snapshot ImageExecutionAdmissionSnapshot) {
	if breaker == nil {
		return
	}
	now := breaker.now()
	breaker.mu.Lock()
	if canceled {
		breaker.canceled++
	} else {
		breaker.completed++
		breaker.lastCompletionAt = now
		breaker.closeLocked()
	}
	breaker.observeAdmissionLocked(snapshot, now)
	if breaker.open && snapshot.Active == 0 {
		breaker.closeLocked()
	}
	breaker.mu.Unlock()
}

func (breaker *imagePollStallBreaker) reject(snapshot ImageExecutionAdmissionSnapshot) bool {
	if breaker == nil {
		return false
	}
	now := breaker.now()
	breaker.mu.Lock()
	breaker.observeAdmissionLocked(snapshot, now)
	breaker.evaluateLocked(snapshot, now)
	reject := breaker.enabled && breaker.open
	if reject {
		breaker.rejected++
	}
	breaker.mu.Unlock()
	return reject
}

func (breaker *imagePollStallBreaker) snapshot(admission ImageExecutionAdmissionSnapshot) ImagePollStallBreakerSnapshot {
	if breaker == nil {
		return ImagePollStallBreakerSnapshot{}
	}
	now := breaker.now()
	breaker.mu.Lock()
	breaker.observeAdmissionLocked(admission, now)
	breaker.evaluateLocked(admission, now)
	snapshot := ImagePollStallBreakerSnapshot{
		Enabled:              breaker.enabled,
		Open:                 breaker.open,
		StallSeconds:         int(breaker.stallDuration / time.Second),
		OpenedAt:             imagePollBreakerTimePointer(breaker.openedAt),
		FullSince:            imagePollBreakerTimePointer(breaker.fullSince),
		LastCompletionAt:     imagePollBreakerTimePointer(breaker.lastCompletionAt),
		Rejected:             breaker.rejected,
		TransportCompletions: breaker.completed,
		CanceledCompletions:  breaker.canceled,
	}
	baseline := breaker.fullSince
	if breaker.lastCompletionAt.After(baseline) {
		baseline = breaker.lastCompletionAt
	}
	if !baseline.IsZero() && now.After(baseline) {
		snapshot.NoCompletionAge = now.Sub(baseline)
	}
	breaker.mu.Unlock()
	return snapshot
}

func (breaker *imagePollStallBreaker) observeAdmissionLocked(snapshot ImageExecutionAdmissionSnapshot, now time.Time) {
	if !breaker.enabled {
		return
	}
	if snapshot.Limit > 0 && snapshot.Active >= snapshot.Limit {
		if breaker.fullSince.IsZero() {
			breaker.fullSince = now
		}
		return
	}
	breaker.fullSince = time.Time{}
	if breaker.open && snapshot.Active == 0 {
		breaker.closeLocked()
	}
}

func (breaker *imagePollStallBreaker) evaluateLocked(snapshot ImageExecutionAdmissionSnapshot, now time.Time) {
	if !breaker.enabled || breaker.open || snapshot.Limit <= 0 || snapshot.Active < snapshot.Limit || breaker.fullSince.IsZero() {
		return
	}
	baseline := breaker.fullSince
	if breaker.lastCompletionAt.After(baseline) {
		baseline = breaker.lastCompletionAt
	}
	if now.Sub(baseline) < breaker.stallDuration {
		return
	}
	breaker.open = true
	breaker.openedAt = now
}

func (breaker *imagePollStallBreaker) closeLocked() {
	breaker.open = false
	breaker.openedAt = time.Time{}
}

func imagePollBreakerTimePointer(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	copyValue := value.UTC()
	return &copyValue
}
