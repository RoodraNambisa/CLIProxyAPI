package executor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// RequestPhaseObserverMetadataKey carries a low-cardinality phase observer.
	RequestPhaseObserverMetadataKey = "request_phase_observer"

	ImagePhaseRequestTotal           = "route_request_total"
	ImagePhaseInputAdmission         = "route_input_admission"
	ImagePhaseInputParse             = "route_input_parse"
	ImagePhaseExecutionAdmission     = "web_execution_admission"
	ImagePhaseCredentialSelection    = "route_credential_selection"
	ImagePhaseRequestSlot            = "route_request_slot"
	ImagePhaseInputUpload            = "web_input_upload"
	ImagePhaseRequirements           = "web_requirements"
	ImagePhaseConversationPrepare    = "web_conversation_prepare"
	ImagePhaseUpstreamInitial        = "web_upstream_initial"
	ImagePhaseStreamSettle           = "web_stream_settle"
	ImagePhasePollSlotWait           = "web_poll_slot_wait"
	ImagePhasePollRequest            = "web_poll_request"
	ImagePhaseFinalizerWait          = "web_finalizer_wait"
	ImagePhaseDownload               = "web_download"
	ImagePhaseWebResponseEncode      = "web_response_encode"
	ImagePhaseResponseEncode         = "route_response_encode"
	ImagePhaseResponseWriteOperation = "route_response_write_operation"
)

var (
	defaultChatGPTWebImageExecutionAdmission = newImageExecutionAdmission()
	defaultChatGPTWebImageFinalizerAdmission = newImageExecutionAdmission()
	defaultChatGPTWebImagePollAdmission      = newConfiguredImageExecutionAdmission(64, 128)
	defaultChatGPTWebImageMemoryFinalizer    = newConfiguredImageExecutionAdmission(1, 64)
	globalImageRequestPhaseObserver          = newImageRequestPhaseObserver()
)

// ImageExecutionCapacityError reports bounded local overload before the first
// ChatGPT Web image upstream request.
type ImageExecutionCapacityError struct {
	reason string
}

func (err *ImageExecutionCapacityError) Error() string {
	payload, _ := json.Marshal(map[string]any{"error": map[string]any{
		"message":       "image generation capacity is temporarily exhausted",
		"type":          "server_error",
		"code":          "image_generation_capacity",
		"failure_stage": "admission",
	}})
	return string(payload)
}

func (*ImageExecutionCapacityError) StatusCode() int { return http.StatusServiceUnavailable }

func (*ImageExecutionCapacityError) SkipAuthResult() bool { return true }

func (*ImageExecutionCapacityError) RetryOtherAuth() bool { return true }

func (*ImageExecutionCapacityError) ExecutionResultErrorCode() string {
	return "image_generation_capacity"
}

func (*ImageExecutionCapacityError) ChatGPTWebFailureStage() string { return "admission" }

func (*ImageExecutionCapacityError) RetryAfter() *time.Duration {
	delay := time.Second
	return &delay
}

func (*ImageExecutionCapacityError) Headers() http.Header {
	return http.Header{"Retry-After": []string{"1"}}
}

// IsImageExecutionCapacityError reports whether err is a local image lifecycle
// admission failure rather than an upstream response.
func IsImageExecutionCapacityError(err error) bool {
	var capacityErr *ImageExecutionCapacityError
	return errors.As(err, &capacityErr)
}

// NewImageExecutionCapacityError creates a safe local overload error.
func NewImageExecutionCapacityError(reason string) error {
	return &ImageExecutionCapacityError{reason: strings.TrimSpace(reason)}
}

type imageExecutionWaiter struct {
	ready    chan struct{}
	granted  bool
	queued   time.Time
	activeID uint64
}

// ImageExecutionAdmissionSnapshot is a low-overhead view of one bounded gate.
type ImageExecutionAdmissionSnapshot struct {
	Limit            int           `json:"limit"`
	QueueLimit       int           `json:"queue_limit"`
	Active           int           `json:"active"`
	Queued           int           `json:"queued"`
	PeakActive       int           `json:"peak_active"`
	PeakQueued       int           `json:"peak_queued"`
	Admitted         uint64        `json:"admitted"`
	ImmediateRejects uint64        `json:"immediate_rejects"`
	QueueRejects     uint64        `json:"queue_rejects"`
	TimedOut         uint64        `json:"timed_out"`
	Canceled         uint64        `json:"canceled"`
	TotalWait        time.Duration `json:"total_wait_nanos"`
	MaxWait          time.Duration `json:"max_wait_nanos"`
	OldestActiveAge  time.Duration `json:"oldest_active_age_nanos"`
	ActiveOver5Min   int           `json:"active_over_5_minutes"`
	ActiveOver15Min  int           `json:"active_over_15_minutes"`
	ActiveOver25Min  int           `json:"active_over_25_minutes"`
	Shrinking        bool          `json:"shrinking"`
}

type imageExecutionAdmission struct {
	mu sync.Mutex

	limit        int
	queueLimit   int
	active       int
	waiters      []*imageExecutionWaiter
	nextActiveID uint64
	activeSince  map[uint64]time.Time
	peakActive   int
	peakQueued   int

	admitted        uint64
	immediateReject uint64
	queueReject     uint64
	timedOut        uint64
	canceled        uint64
	totalWait       time.Duration
	maxWait         time.Duration
}

func newImageExecutionAdmission() *imageExecutionAdmission {
	return &imageExecutionAdmission{activeSince: make(map[uint64]time.Time)}
}

func newConfiguredImageExecutionAdmission(limit, queueLimit int) *imageExecutionAdmission {
	admission := newImageExecutionAdmission()
	admission.configure(limit, queueLimit)
	return admission
}

// ConfigureChatGPTWebImageAdmissions applies the current process-wide policy.
// Existing holders remain valid when a limit is lowered.
func ConfigureChatGPTWebImageAdmissions(maxInFlight, queueLimit, maxFinalizers int) {
	defaultChatGPTWebImageExecutionAdmission.configure(maxInFlight, queueLimit)
	activeExecution := defaultChatGPTWebImageExecutionAdmission.snapshot().Active
	finalizerQueueLimit := max(maxInFlight, activeExecution)
	defaultChatGPTWebImageFinalizerAdmission.configure(maxFinalizers, finalizerQueueLimit)
}

// ConfigureChatGPTWebImageRuntimeAdmissions applies hot-reloadable limits to
// poll HTTP requests and memory-heavy finalization. Existing holders survive a
// shrink; new grants resume after active work falls below the new limit.
func ConfigureChatGPTWebImageRuntimeAdmissions(maxInFlight, pollConcurrency, memoryFinalizerConcurrency int) {
	activeExecution := defaultChatGPTWebImageExecutionAdmission.snapshot().Active
	queueLimit := max(maxInFlight, activeExecution)
	pollQueueLimit := queueLimit
	if queueLimit <= int(^uint(0)>>1)/2 {
		pollQueueLimit = queueLimit * 2
	}
	defaultChatGPTWebImagePollAdmission.configure(pollConcurrency, pollQueueLimit)
	defaultChatGPTWebImageMemoryFinalizer.configure(memoryFinalizerConcurrency, queueLimit)
}

// AcquireChatGPTWebImageExecution bounds the complete selected Web image
// attempt, including sleeping task polls.
func AcquireChatGPTWebImageExecution(ctx context.Context, wait time.Duration) (func(), error) {
	return defaultChatGPTWebImageExecutionAdmission.acquire(ctx, wait)
}

// AcquireChatGPTWebImageFinalizer bounds settled tasks admitted to finalizer
// staging. It waits only for request cancellation and never adds an upstream timeout.
func AcquireChatGPTWebImageFinalizer(ctx context.Context) (func(), error) {
	return defaultChatGPTWebImageFinalizerAdmission.acquire(ctx, -1)
}

// AcquireChatGPTWebImagePoll bounds one actual poll HTTP exchange. Sleeping
// intervals between polls do not retain this admission.
func AcquireChatGPTWebImagePoll(ctx context.Context) (func(), error) {
	return defaultChatGPTWebImagePollAdmission.acquire(ctx, -1)
}

// AcquireChatGPTWebImageMemoryFinalizer bounds disk-spooled download, decode,
// encode, and response materialization work.
func AcquireChatGPTWebImageMemoryFinalizer(ctx context.Context) (func(), error) {
	return defaultChatGPTWebImageMemoryFinalizer.acquire(ctx, -1)
}

// ChatGPTWebImageExecutionAdmissionSnapshot returns process-wide lifecycle metrics.
func ChatGPTWebImageExecutionAdmissionSnapshot() ImageExecutionAdmissionSnapshot {
	return defaultChatGPTWebImageExecutionAdmission.snapshot()
}

// ChatGPTWebImageFinalizerAdmissionSnapshot returns process-wide finalizer metrics.
func ChatGPTWebImageFinalizerAdmissionSnapshot() ImageExecutionAdmissionSnapshot {
	return defaultChatGPTWebImageFinalizerAdmission.snapshot()
}

// ChatGPTWebImagePollAdmissionSnapshot returns actual poll HTTP concurrency metrics.
func ChatGPTWebImagePollAdmissionSnapshot() ImageExecutionAdmissionSnapshot {
	return defaultChatGPTWebImagePollAdmission.snapshot()
}

// ChatGPTWebImageMemoryFinalizerAdmissionSnapshot returns memory-heavy finalizer metrics.
func ChatGPTWebImageMemoryFinalizerAdmissionSnapshot() ImageExecutionAdmissionSnapshot {
	return defaultChatGPTWebImageMemoryFinalizer.snapshot()
}

func (admission *imageExecutionAdmission) configure(limit, queueLimit int) {
	if admission == nil {
		return
	}
	if limit < 1 {
		limit = 1
	}
	if queueLimit < 0 {
		queueLimit = 0
	}
	admission.mu.Lock()
	admission.limit = limit
	admission.queueLimit = queueLimit
	admission.grantLocked()
	admission.mu.Unlock()
}

func (admission *imageExecutionAdmission) acquire(ctx context.Context, wait time.Duration) (func(), error) {
	if admission == nil {
		return func() {}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	admission.mu.Lock()
	if admission.limit < 1 {
		admission.limit = 1
	}
	if admission.active < admission.limit && len(admission.waiters) == 0 {
		activeID := admission.activateLocked(time.Now())
		admission.admitted++
		admission.updatePeaksLocked()
		admission.mu.Unlock()
		return admission.releaseFunc(activeID), nil
	}
	if wait == 0 {
		admission.immediateReject++
		admission.mu.Unlock()
		return nil, NewImageExecutionCapacityError("inflight_full")
	}
	if admission.queueLimit == 0 || len(admission.waiters) >= admission.queueLimit {
		admission.queueReject++
		admission.mu.Unlock()
		return nil, NewImageExecutionCapacityError("queue_full")
	}
	waiter := &imageExecutionWaiter{ready: make(chan struct{}), queued: time.Now()}
	admission.waiters = append(admission.waiters, waiter)
	admission.updatePeaksLocked()
	admission.mu.Unlock()

	var timer *time.Timer
	var timerChannel <-chan time.Time
	if wait > 0 {
		timer = time.NewTimer(wait)
		timerChannel = timer.C
		defer timer.Stop()
	}
	select {
	case <-waiter.ready:
		return admission.releaseFunc(waiter.activeID), nil
	case <-ctx.Done():
		admission.cancelWaiter(waiter, false)
		return nil, ctx.Err()
	case <-timerChannel:
		admission.cancelWaiter(waiter, true)
		return nil, NewImageExecutionCapacityError("admission_timeout")
	}
}

func (admission *imageExecutionAdmission) cancelWaiter(waiter *imageExecutionWaiter, timedOut bool) {
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if waiter.granted {
		admission.deactivateLocked(waiter.activeID)
		admission.grantLocked()
	} else {
		for index, candidate := range admission.waiters {
			if candidate == waiter {
				admission.waiters = append(admission.waiters[:index], admission.waiters[index+1:]...)
				break
			}
		}
	}
	if timedOut {
		admission.timedOut++
	} else {
		admission.canceled++
	}
}

func (admission *imageExecutionAdmission) releaseFunc(activeID uint64) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			admission.mu.Lock()
			admission.deactivateLocked(activeID)
			admission.grantLocked()
			admission.mu.Unlock()
		})
	}
}

func (admission *imageExecutionAdmission) grantLocked() {
	for admission.active < admission.limit && len(admission.waiters) > 0 {
		waiter := admission.waiters[0]
		admission.waiters = admission.waiters[1:]
		waiter.granted = true
		waiter.activeID = admission.activateLocked(time.Now())
		admission.admitted++
		waitDuration := time.Since(waiter.queued)
		admission.totalWait += waitDuration
		if waitDuration > admission.maxWait {
			admission.maxWait = waitDuration
		}
		admission.updatePeaksLocked()
		close(waiter.ready)
	}
}

func (admission *imageExecutionAdmission) activateLocked(now time.Time) uint64 {
	admission.nextActiveID++
	if admission.nextActiveID == 0 {
		admission.nextActiveID++
	}
	activeID := admission.nextActiveID
	admission.active++
	admission.activeSince[activeID] = now
	return activeID
}

func (admission *imageExecutionAdmission) deactivateLocked(activeID uint64) {
	if activeID == 0 {
		return
	}
	if _, exists := admission.activeSince[activeID]; !exists {
		return
	}
	delete(admission.activeSince, activeID)
	if admission.active > 0 {
		admission.active--
	}
}

func (admission *imageExecutionAdmission) updatePeaksLocked() {
	if admission.active > admission.peakActive {
		admission.peakActive = admission.active
	}
	if len(admission.waiters) > admission.peakQueued {
		admission.peakQueued = len(admission.waiters)
	}
}

func (admission *imageExecutionAdmission) snapshot() ImageExecutionAdmissionSnapshot {
	if admission == nil {
		return ImageExecutionAdmissionSnapshot{}
	}
	admission.mu.Lock()
	defer admission.mu.Unlock()
	now := time.Now()
	oldest := time.Duration(0)
	over5Minutes := 0
	over15Minutes := 0
	over25Minutes := 0
	for _, started := range admission.activeSince {
		age := now.Sub(started)
		if age > oldest {
			oldest = age
		}
		if age >= 5*time.Minute {
			over5Minutes++
		}
		if age >= 15*time.Minute {
			over15Minutes++
		}
		if age >= 25*time.Minute {
			over25Minutes++
		}
	}
	return ImageExecutionAdmissionSnapshot{
		Limit:            admission.limit,
		QueueLimit:       admission.queueLimit,
		Active:           admission.active,
		Queued:           len(admission.waiters),
		PeakActive:       admission.peakActive,
		PeakQueued:       admission.peakQueued,
		Admitted:         admission.admitted,
		ImmediateRejects: admission.immediateReject,
		QueueRejects:     admission.queueReject,
		TimedOut:         admission.timedOut,
		Canceled:         admission.canceled,
		TotalWait:        admission.totalWait,
		MaxWait:          admission.maxWait,
		OldestActiveAge:  oldest,
		ActiveOver5Min:   over5Minutes,
		ActiveOver15Min:  over15Minutes,
		ActiveOver25Min:  over25Minutes,
		Shrinking:        admission.active > admission.limit,
	}
}

// RequestPhaseObserver receives only fixed phase names and aggregate durations.
type RequestPhaseObserver interface {
	ObserveRequestPhase(name string, duration time.Duration)
}

type requestPhaseObserverContextKey struct{}

// GlobalImageRequestPhaseObserver returns the process-wide safe phase observer.
func GlobalImageRequestPhaseObserver() RequestPhaseObserver {
	return globalImageRequestPhaseObserver
}

// WithRequestPhaseObserver attaches an observer without exposing request data.
func WithRequestPhaseObserver(ctx context.Context, observer RequestPhaseObserver) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if observer == nil {
		return ctx
	}
	return context.WithValue(ctx, requestPhaseObserverContextKey{}, observer)
}

// RequestPhaseObserverFromContext returns the attached safe phase observer.
func RequestPhaseObserverFromContext(ctx context.Context) RequestPhaseObserver {
	if ctx == nil {
		return nil
	}
	observer, _ := ctx.Value(requestPhaseObserverContextKey{}).(RequestPhaseObserver)
	return observer
}

// RequestPhaseObserverFromMetadata returns the observer carried by execution metadata.
func RequestPhaseObserverFromMetadata(metadata map[string]any) RequestPhaseObserver {
	if len(metadata) == 0 {
		return nil
	}
	observer, _ := metadata[RequestPhaseObserverMetadataKey].(RequestPhaseObserver)
	return observer
}

// ObserveRequestPhase records one duration when metadata carries an observer.
func ObserveRequestPhase(metadata map[string]any, name string, started time.Time) {
	ObserveRequestPhaseDuration(metadata, name, time.Since(started))
}

// ObserveRequestPhaseDuration records one already measured fixed-name phase.
func ObserveRequestPhaseDuration(metadata map[string]any, name string, duration time.Duration) {
	observer := RequestPhaseObserverFromMetadata(metadata)
	if observer != nil {
		observer.ObserveRequestPhase(name, duration)
	}
}

// ObserveRequestPhaseContext records one fixed-name duration from context.
func ObserveRequestPhaseContext(ctx context.Context, name string, started time.Time) {
	observer := RequestPhaseObserverFromContext(ctx)
	if observer != nil {
		observer.ObserveRequestPhase(name, time.Since(started))
	}
}

// ImageRequestPhaseMetricSnapshot contains fixed-bucket aggregate timing only.
type ImageRequestPhaseMetricSnapshot struct {
	Count                   uint64 `json:"count"`
	TotalNanos              uint64 `json:"total_nanos"`
	MaxNanos                uint64 `json:"max_nanos"`
	UpTo1Millisecond        uint64 `json:"up_to_1_millisecond"`
	Over1To10Milliseconds   uint64 `json:"over_1_to_10_milliseconds"`
	Over10To100Milliseconds uint64 `json:"over_10_to_100_milliseconds"`
	Over100MillisecondsTo1S uint64 `json:"over_100_milliseconds_to_1_second"`
	Over1To10Seconds        uint64 `json:"over_1_to_10_seconds"`
	Over10Seconds           uint64 `json:"over_10_seconds"`
}

type imageRequestPhaseMetric struct {
	count      atomic.Uint64
	totalNanos atomic.Uint64
	maxNanos   atomic.Uint64
	buckets    [6]atomic.Uint64
}

type imageRequestPhaseObserver struct {
	metrics map[string]*imageRequestPhaseMetric
}

func newImageRequestPhaseObserver() *imageRequestPhaseObserver {
	metrics := make(map[string]*imageRequestPhaseMetric)
	for _, name := range []string{
		ImagePhaseRequestTotal, ImagePhaseInputAdmission, ImagePhaseInputParse,
		ImagePhaseExecutionAdmission, ImagePhaseCredentialSelection, ImagePhaseRequestSlot,
		ImagePhaseInputUpload, ImagePhaseRequirements, ImagePhaseConversationPrepare,
		ImagePhaseUpstreamInitial, ImagePhaseStreamSettle, ImagePhasePollSlotWait,
		ImagePhasePollRequest, ImagePhaseFinalizerWait, ImagePhaseDownload,
		ImagePhaseWebResponseEncode, ImagePhaseResponseEncode, ImagePhaseResponseWriteOperation,
	} {
		metrics[name] = &imageRequestPhaseMetric{}
	}
	return &imageRequestPhaseObserver{metrics: metrics}
}

func (observer *imageRequestPhaseObserver) ObserveRequestPhase(name string, duration time.Duration) {
	if observer == nil {
		return
	}
	metric := observer.metrics[strings.TrimSpace(name)]
	if metric == nil {
		return
	}
	nanos := uint64(max(duration.Nanoseconds(), 0))
	metric.count.Add(1)
	metric.totalNanos.Add(nanos)
	for {
		current := metric.maxNanos.Load()
		if nanos <= current || metric.maxNanos.CompareAndSwap(current, nanos) {
			break
		}
	}
	bucket := 5
	switch {
	case duration <= time.Millisecond:
		bucket = 0
	case duration <= 10*time.Millisecond:
		bucket = 1
	case duration <= 100*time.Millisecond:
		bucket = 2
	case duration <= time.Second:
		bucket = 3
	case duration <= 10*time.Second:
		bucket = 4
	}
	metric.buckets[bucket].Add(1)
}

// ImageRequestPhaseSnapshot returns fixed-name aggregate metrics for the
// all-provider handler phases and ChatGPT Web executor phases.
func ImageRequestPhaseSnapshot() map[string]ImageRequestPhaseMetricSnapshot {
	snapshot := make(map[string]ImageRequestPhaseMetricSnapshot, len(globalImageRequestPhaseObserver.metrics))
	for name, metric := range globalImageRequestPhaseObserver.metrics {
		value := ImageRequestPhaseMetricSnapshot{
			Count:                   metric.count.Load(),
			TotalNanos:              metric.totalNanos.Load(),
			MaxNanos:                metric.maxNanos.Load(),
			UpTo1Millisecond:        metric.buckets[0].Load(),
			Over1To10Milliseconds:   metric.buckets[1].Load(),
			Over10To100Milliseconds: metric.buckets[2].Load(),
			Over100MillisecondsTo1S: metric.buckets[3].Load(),
			Over1To10Seconds:        metric.buckets[4].Load(),
			Over10Seconds:           metric.buckets[5].Load(),
		}
		snapshot[name] = value
	}
	return snapshot
}
