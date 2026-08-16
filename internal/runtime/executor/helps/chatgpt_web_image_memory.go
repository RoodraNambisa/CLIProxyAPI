package helps

import (
	"context"
	"errors"
	"math"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/semaphore"
)

const (
	defaultChatGPTWebImageMemoryBytes = 256 << 20
	minChatGPTWebImageMemoryBytes     = 1 << 20
	maxChatGPTWebImageMemoryBytes     = 512 << 20
)

// ErrChatGPTWebImageMemoryQueueFull reports admission overload before work starts.
var ErrChatGPTWebImageMemoryQueueFull = errors.New("chatgpt web image memory wait queue is full")

// ErrChatGPTWebImageMemoryWorkingSetTooLarge reports a single request whose
// retained and transient buffers cannot fit in the configured process budget.
var ErrChatGPTWebImageMemoryWorkingSetTooLarge = errors.New("chatgpt web image request working set exceeds memory capacity")

// ChatGPTWebImageMemoryLeaseSetMetadataKey carries a request-scoped lease set
// from the HTTP image handler into the ChatGPT Web executor.
const ChatGPTWebImageMemoryLeaseSetMetadataKey = "chatgpt_web_image_memory_lease_set"

// ChatGPTWebImageMemoryRuntimeSnapshot describes image post-processing
// admission without exposing request or credential data.
type ChatGPTWebImageMemoryRuntimeSnapshot struct {
	CapacityBytes                 int64  `json:"capacity_bytes"`
	QueueLimit                    int64  `json:"queue_limit"`
	WaitingTasks                  int64  `json:"waiting_tasks"`
	WaitingBytes                  int64  `json:"waiting_bytes"`
	ProcessingTasks               int64  `json:"processing_tasks"`
	ProcessingBytes               int64  `json:"processing_bytes"`
	PeakProcessingBytes           int64  `json:"peak_processing_bytes"`
	Acquisitions                  uint64 `json:"acquisitions"`
	CanceledWaits                 uint64 `json:"canceled_waits"`
	QueueRejected                 uint64 `json:"queue_rejected"`
	ImmediateRejected             uint64 `json:"immediate_rejected"`
	CompletionReservations        int64  `json:"completion_reservations"`
	RevokedCompletionReservations uint64 `json:"revoked_completion_reservations"`
	FinalizationActive            int64  `json:"finalization_active"`
	FinalizationWaiting           int64  `json:"finalization_waiting"`
}

// ChatGPTWebImageMemoryAdmission bounds concurrent decoded image working sets.
type ChatGPTWebImageMemoryAdmission struct {
	capacity   int64
	queueLimit int64
	weighted   *semaphore.Weighted

	waitingTasks        atomic.Int64
	waitingBytes        atomic.Int64
	processingTasks     atomic.Int64
	processingBytes     atomic.Int64
	peakProcessingBytes atomic.Int64
	acquisitions        atomic.Uint64
	canceledWaits       atomic.Uint64
	queueRejected       atomic.Uint64
	immediateRejected   atomic.Uint64
}

// ChatGPTWebImageMemoryLeaseSet owns all long-lived image buffers for one request.
type ChatGPTWebImageMemoryLeaseSet struct {
	mu                  sync.Mutex
	inputRelease        func()
	completionRelease   func()
	completionBytes     int64
	completionAvailable int64
	completionRevoked   bool
	finalizationOwned   bool
	retainedBytes       int64
	pendingRetained     int64
	transientBytes      int64
	releases            []func()
	released            bool
}

type chatGPTWebImageMemoryLeaseSetContextKey struct{}

type chatGPTWebImageCompletionCoordinator struct {
	mu           sync.Mutex
	turn         chan struct{}
	finalizing   bool
	reservations map[*ChatGPTWebImageMemoryLeaseSet]struct{}

	reservationCount atomic.Int64
	revokedCount     atomic.Uint64
	active           atomic.Int64
	waiting          atomic.Int64
}

var defaultChatGPTWebImageMemoryAdmission = NewChatGPTWebImageMemoryAdmission(
	resolvedChatGPTWebImageMemoryCapacity(),
)

var defaultChatGPTWebImageCompletionCoordinator = &chatGPTWebImageCompletionCoordinator{
	turn:         make(chan struct{}, 1),
	reservations: make(map[*ChatGPTWebImageMemoryLeaseSet]struct{}),
}

// NewChatGPTWebImageMemoryAdmission creates an admission controller with a
// fixed byte budget. Values below one are normalized to one byte for tests.
func NewChatGPTWebImageMemoryAdmission(capacityBytes int64) *ChatGPTWebImageMemoryAdmission {
	if capacityBytes < 1 {
		capacityBytes = 1
	}
	return &ChatGPTWebImageMemoryAdmission{
		capacity:   capacityBytes,
		queueLimit: int64(chatGPTWebImageMemoryQueueLimit()),
		weighted:   semaphore.NewWeighted(capacityBytes),
	}
}

// NewChatGPTWebImageMemoryLeaseSet creates an empty request-scoped lease owner.
func NewChatGPTWebImageMemoryLeaseSet() *ChatGPTWebImageMemoryLeaseSet {
	return &ChatGPTWebImageMemoryLeaseSet{}
}

// AcquireChatGPTWebImageMemory reserves estimated decoded image memory. A
// request larger than the controller capacity runs exclusively instead of
// waiting forever for an impossible weight.
func AcquireChatGPTWebImageMemory(ctx context.Context, estimatedBytes int64) (func(), error) {
	return defaultChatGPTWebImageMemoryAdmission.Acquire(ctx, estimatedBytes)
}

// TryAcquireChatGPTWebImageMemory reserves memory without joining the wait queue.
func TryAcquireChatGPTWebImageMemory(estimatedBytes int64) (func(), bool) {
	return defaultChatGPTWebImageMemoryAdmission.TryAcquire(estimatedBytes)
}

// ChatGPTWebImageMemorySnapshot returns process-wide image admission metrics.
func ChatGPTWebImageMemorySnapshot() ChatGPTWebImageMemoryRuntimeSnapshot {
	return defaultChatGPTWebImageMemoryAdmission.Snapshot()
}

// Acquire reserves estimated decoded image memory until the returned release
// function is called.
func (admission *ChatGPTWebImageMemoryAdmission) Acquire(ctx context.Context, estimatedBytes int64) (func(), error) {
	return admission.acquire(ctx, estimatedBytes, false)
}

func (admission *ChatGPTWebImageMemoryAdmission) acquireCritical(ctx context.Context, estimatedBytes int64) (func(), error) {
	return admission.acquire(ctx, estimatedBytes, true)
}

func (admission *ChatGPTWebImageMemoryAdmission) acquire(ctx context.Context, estimatedBytes int64, critical bool) (func(), error) {
	if admission == nil {
		return func() {}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	weight := estimatedBytes
	if weight < 1 {
		weight = 1
	}
	if weight > admission.capacity {
		weight = admission.capacity
	}

	waiting := admission.waitingTasks.Add(1)
	if !critical && waiting > admission.queueLimit {
		admission.waitingTasks.Add(-1)
		admission.queueRejected.Add(1)
		return nil, ErrChatGPTWebImageMemoryQueueFull
	}
	admission.waitingBytes.Add(weight)
	errAcquire := admission.weighted.Acquire(ctx, weight)
	admission.waitingTasks.Add(-1)
	admission.waitingBytes.Add(-weight)
	if errAcquire != nil {
		admission.canceledWaits.Add(1)
		return nil, errAcquire
	}

	admission.acquisitions.Add(1)
	admission.processingTasks.Add(1)
	processingBytes := admission.processingBytes.Add(weight)
	updateChatGPTWebImageMemoryPeak(&admission.peakProcessingBytes, processingBytes)

	var releaseOnce sync.Once
	return func() {
		releaseOnce.Do(func() {
			admission.processingBytes.Add(-weight)
			admission.processingTasks.Add(-1)
			admission.weighted.Release(weight)
		})
	}, nil
}

// TryAcquire reserves memory immediately or reports current capacity pressure.
func (admission *ChatGPTWebImageMemoryAdmission) TryAcquire(estimatedBytes int64) (func(), bool) {
	if admission == nil {
		return func() {}, true
	}
	weight := admission.normalizedWeight(estimatedBytes)
	if !admission.weighted.TryAcquire(weight) {
		admission.immediateRejected.Add(1)
		return nil, false
	}
	admission.acquisitions.Add(1)
	admission.processingTasks.Add(1)
	processingBytes := admission.processingBytes.Add(weight)
	updateChatGPTWebImageMemoryPeak(&admission.peakProcessingBytes, processingBytes)
	return admission.releaseFunc(weight), true
}

func (admission *ChatGPTWebImageMemoryAdmission) normalizedWeight(estimatedBytes int64) int64 {
	weight := estimatedBytes
	if weight < 1 {
		weight = 1
	}
	if weight > admission.capacity {
		weight = admission.capacity
	}
	return weight
}

func (admission *ChatGPTWebImageMemoryAdmission) releaseFunc(weight int64) func() {
	var releaseOnce sync.Once
	return func() {
		releaseOnce.Do(func() {
			admission.processingBytes.Add(-weight)
			admission.processingTasks.Add(-1)
			admission.weighted.Release(weight)
		})
	}
}

// Acquire waits for memory and retains the lease until Release is called.
func (leases *ChatGPTWebImageMemoryLeaseSet) Acquire(ctx context.Context, estimatedBytes int64) error {
	if leases == nil {
		release, err := AcquireChatGPTWebImageMemory(ctx, estimatedBytes)
		if err != nil {
			return err
		}
		release()
		return nil
	}
	weight, err := leases.reserveRetainedWeight(estimatedBytes)
	if err != nil {
		return err
	}
	borrowed, release, err := leases.acquireLeaseMemory(ctx, weight)
	if err != nil {
		leases.cancelRetainedWeight(weight)
		return err
	}
	if err = leases.retain(release, weight); err != nil {
		leases.restoreCompletion(borrowed)
		return err
	}
	return nil
}

// TryAcquireInput reserves request input memory without joining the weighted
// FIFO wait queue. This prevents large uploads from blocking small requests.
func (leases *ChatGPTWebImageMemoryLeaseSet) TryAcquireInput(estimatedBytes int64) bool {
	release, acquired := TryAcquireChatGPTWebImageMemory(estimatedBytes)
	if !acquired {
		return false
	}
	if leases == nil {
		release()
		return true
	}
	leases.mu.Lock()
	if leases.released {
		leases.mu.Unlock()
		release()
		return false
	}
	previous := leases.inputRelease
	leases.inputRelease = release
	leases.mu.Unlock()
	if previous != nil {
		previous()
	}
	return true
}

// TryReserveCompletion holds a small allowance before upstream image work so
// polling and completion do not discover total local exhaustion after generation.
func (leases *ChatGPTWebImageMemoryLeaseSet) TryReserveCompletion(estimatedBytes int64) bool {
	if leases == nil || estimatedBytes <= 0 {
		return true
	}
	weight := estimatedBytes
	if capacity := ChatGPTWebImageMemorySnapshot().CapacityBytes; capacity > 0 && weight > capacity {
		weight = capacity
	}
	if weight < 1 {
		weight = 1
	}
	coordinator := defaultChatGPTWebImageCompletionCoordinator
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.finalizing {
		return false
	}
	leases.mu.Lock()
	if leases.released {
		leases.mu.Unlock()
		return false
	}
	if leases.completionRelease != nil {
		reserved := !leases.completionRevoked && leases.completionBytes >= weight
		leases.mu.Unlock()
		return reserved
	}
	leases.mu.Unlock()
	release, acquired := TryAcquireChatGPTWebImageMemory(weight)
	if !acquired {
		return false
	}
	leases.mu.Lock()
	if leases.released {
		leases.mu.Unlock()
		release()
		return false
	}
	if leases.completionRelease != nil {
		reserved := leases.completionBytes >= weight
		leases.mu.Unlock()
		release()
		return reserved
	}
	leases.completionRelease = release
	leases.completionBytes = weight
	leases.completionAvailable = weight
	leases.completionRevoked = false
	leases.mu.Unlock()
	if _, exists := coordinator.reservations[leases]; !exists {
		coordinator.reservations[leases] = struct{}{}
		coordinator.reservationCount.Add(1)
	}
	return true
}

// BeginFinalization serializes ownership of retained completion buffers. It
// revokes idle reservations from other in-flight requests before this request
// can hold a download while waiting for decode or encode memory.
func (leases *ChatGPTWebImageMemoryLeaseSet) BeginFinalization(ctx context.Context) (func(), error) {
	if leases == nil {
		return func() {}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	leases.mu.Lock()
	if leases.released {
		leases.mu.Unlock()
		return nil, context.Canceled
	}
	leases.mu.Unlock()

	coordinator := defaultChatGPTWebImageCompletionCoordinator
	coordinator.waiting.Add(1)
	select {
	case coordinator.turn <- struct{}{}:
		coordinator.waiting.Add(-1)
	case <-ctx.Done():
		coordinator.waiting.Add(-1)
		return nil, ctx.Err()
	}
	coordinator.active.Add(1)
	coordinator.mu.Lock()
	coordinator.finalizing = true
	others := make([]*ChatGPTWebImageMemoryLeaseSet, 0, len(coordinator.reservations))
	for candidate := range coordinator.reservations {
		if candidate != leases {
			others = append(others, candidate)
		}
	}
	coordinator.mu.Unlock()
	for _, candidate := range others {
		candidate.revokeCompletionReservation()
	}

	var releaseOnce sync.Once
	releaseTurn := func() {
		releaseOnce.Do(func() {
			leases.mu.Lock()
			leases.finalizationOwned = false
			leases.mu.Unlock()
			coordinator.mu.Lock()
			coordinator.finalizing = false
			coordinator.mu.Unlock()
			coordinator.active.Add(-1)
			<-coordinator.turn
		})
	}
	leases.mu.Lock()
	released := leases.released
	if !released {
		leases.finalizationOwned = true
	}
	leases.mu.Unlock()
	if released {
		releaseTurn()
		return nil, context.Canceled
	}
	return releaseTurn, nil
}

func (leases *ChatGPTWebImageMemoryLeaseSet) revokeCompletionReservation() {
	if leases == nil {
		return
	}
	var release func()
	leases.mu.Lock()
	if leases.completionRelease != nil {
		leases.completionRevoked = true
		if leases.completionAvailable >= leases.completionBytes {
			release = leases.detachCompletionReservationLocked()
		}
	}
	leases.mu.Unlock()
	if release != nil {
		unregisterChatGPTWebImageCompletionReservation(leases, true)
		release()
	}
}

func (leases *ChatGPTWebImageMemoryLeaseSet) detachCompletionReservationLocked() func() {
	release := leases.completionRelease
	leases.completionRelease = nil
	leases.completionBytes = 0
	leases.completionAvailable = 0
	leases.completionRevoked = false
	return release
}

func unregisterChatGPTWebImageCompletionReservation(leases *ChatGPTWebImageMemoryLeaseSet, revoked bool) {
	coordinator := defaultChatGPTWebImageCompletionCoordinator
	coordinator.mu.Lock()
	if _, exists := coordinator.reservations[leases]; exists {
		delete(coordinator.reservations, leases)
		coordinator.reservationCount.Add(-1)
		if revoked {
			coordinator.revokedCount.Add(1)
		}
	}
	coordinator.mu.Unlock()
}

// ReleaseInput drops multipart, data URL, and normalized request allowances.
func (leases *ChatGPTWebImageMemoryLeaseSet) ReleaseInput() {
	if leases == nil {
		return
	}
	leases.mu.Lock()
	release := leases.inputRelease
	leases.inputRelease = nil
	leases.mu.Unlock()
	if release != nil {
		release()
	}
}

// AcquireTransient borrows completion allowance during finalization. Before
// finalization it either uses a fully funded allowance or acquires immediately,
// so ordinary polling cannot hold a partial allowance while blocking finalizers.
func (leases *ChatGPTWebImageMemoryLeaseSet) AcquireTransient(ctx context.Context, estimatedBytes int64) (func(), error) {
	if leases == nil {
		return AcquireChatGPTWebImageMemory(ctx, estimatedBytes)
	}
	weight, err := leases.reserveTransientWeight(estimatedBytes)
	if err != nil {
		return nil, err
	}
	borrowed, releaseMemory, err := leases.acquireLeaseMemory(ctx, weight)
	if err != nil {
		leases.releaseTransientWeight(weight)
		return nil, err
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			if releaseMemory != nil {
				releaseMemory()
			}
			leases.restoreCompletion(borrowed)
			leases.releaseTransientWeight(weight)
		})
	}, nil
}

func (leases *ChatGPTWebImageMemoryLeaseSet) reserveRetainedWeight(estimatedBytes int64) (int64, error) {
	weight, capacity := normalizedChatGPTWebImageLeaseWeight(estimatedBytes)
	leases.mu.Lock()
	defer leases.mu.Unlock()
	if leases.released {
		return 0, context.Canceled
	}
	used := leases.retainedBytes + leases.pendingRetained + leases.transientBytes
	if used > capacity || weight > capacity-used {
		return 0, ErrChatGPTWebImageMemoryWorkingSetTooLarge
	}
	leases.pendingRetained += weight
	return weight, nil
}

func (leases *ChatGPTWebImageMemoryLeaseSet) cancelRetainedWeight(weight int64) {
	if leases == nil || weight <= 0 {
		return
	}
	leases.mu.Lock()
	leases.pendingRetained = max(leases.pendingRetained-weight, int64(0))
	leases.mu.Unlock()
}

func (leases *ChatGPTWebImageMemoryLeaseSet) reserveTransientWeight(estimatedBytes int64) (int64, error) {
	weight, capacity := normalizedChatGPTWebImageLeaseWeight(estimatedBytes)
	leases.mu.Lock()
	defer leases.mu.Unlock()
	if leases.released {
		return 0, context.Canceled
	}
	used := leases.retainedBytes + leases.pendingRetained + leases.transientBytes
	if used > capacity || weight > capacity-used {
		return 0, ErrChatGPTWebImageMemoryWorkingSetTooLarge
	}
	leases.transientBytes += weight
	return weight, nil
}

func (leases *ChatGPTWebImageMemoryLeaseSet) releaseTransientWeight(weight int64) {
	if leases == nil || weight <= 0 {
		return
	}
	leases.mu.Lock()
	leases.transientBytes = max(leases.transientBytes-weight, int64(0))
	leases.mu.Unlock()
}

func normalizedChatGPTWebImageLeaseWeight(estimatedBytes int64) (int64, int64) {
	capacity := ChatGPTWebImageMemorySnapshot().CapacityBytes
	if capacity < 1 {
		capacity = 1
	}
	weight := max(estimatedBytes, int64(1))
	if weight > capacity {
		weight = capacity
	}
	return weight, capacity
}

func (leases *ChatGPTWebImageMemoryLeaseSet) acquireLeaseMemory(ctx context.Context, weight int64) (int64, func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	leases.mu.Lock()
	if leases.released {
		leases.mu.Unlock()
		return 0, nil, context.Canceled
	}
	critical := leases.finalizationOwned
	if !critical && !leases.completionRevoked && leases.completionAvailable >= weight {
		leases.completionAvailable -= weight
		leases.mu.Unlock()
		return weight, nil, nil
	}
	leases.mu.Unlock()
	if !critical {
		if err := ctx.Err(); err != nil {
			return 0, nil, err
		}
		release, acquired := TryAcquireChatGPTWebImageMemory(weight)
		if !acquired {
			return 0, nil, ErrChatGPTWebImageMemoryQueueFull
		}
		return 0, release, nil
	}
	borrowed, remaining, err := leases.borrowCompletion(weight)
	if err != nil {
		return 0, nil, err
	}
	if remaining == 0 {
		return borrowed, nil, nil
	}
	release, err := defaultChatGPTWebImageMemoryAdmission.acquireCritical(ctx, remaining)
	if err != nil {
		leases.restoreCompletion(borrowed)
		return 0, nil, err
	}
	return borrowed, release, nil
}

func (leases *ChatGPTWebImageMemoryLeaseSet) borrowCompletion(estimatedBytes int64) (int64, int64, error) {
	if leases == nil {
		return 0, max(estimatedBytes, int64(1)), nil
	}
	capacity := ChatGPTWebImageMemorySnapshot().CapacityBytes
	weight := max(estimatedBytes, int64(1))
	if capacity > 0 && weight > capacity {
		weight = capacity
	}
	leases.mu.Lock()
	defer leases.mu.Unlock()
	if leases.released {
		return 0, 0, context.Canceled
	}
	if leases.completionRevoked {
		return 0, weight, nil
	}
	borrowed := min(weight, leases.completionAvailable)
	leases.completionAvailable -= borrowed
	return borrowed, weight - borrowed, nil
}

func (leases *ChatGPTWebImageMemoryLeaseSet) restoreCompletion(bytes int64) {
	if leases == nil || bytes <= 0 {
		return
	}
	var release func()
	leases.mu.Lock()
	if !leases.released {
		leases.completionAvailable += bytes
		if leases.completionAvailable > leases.completionBytes {
			leases.completionAvailable = leases.completionBytes
		}
		if leases.completionRevoked && leases.completionAvailable >= leases.completionBytes {
			release = leases.detachCompletionReservationLocked()
		}
	}
	leases.mu.Unlock()
	if release != nil {
		unregisterChatGPTWebImageCompletionReservation(leases, true)
		release()
	}
}

func (leases *ChatGPTWebImageMemoryLeaseSet) retain(release func(), weight int64) error {
	if leases == nil {
		if release != nil {
			release()
		}
		return nil
	}
	leases.mu.Lock()
	leases.pendingRetained = max(leases.pendingRetained-weight, int64(0))
	if leases.released {
		leases.mu.Unlock()
		if release != nil {
			release()
		}
		return context.Canceled
	}
	leases.retainedBytes += weight
	if release != nil {
		leases.releases = append(leases.releases, release)
	}
	leases.mu.Unlock()
	return nil
}

// Release drops every retained request buffer lease exactly once.
func (leases *ChatGPTWebImageMemoryLeaseSet) Release() {
	if leases == nil {
		return
	}
	leases.mu.Lock()
	if leases.released {
		leases.mu.Unlock()
		return
	}
	leases.released = true
	inputRelease := leases.inputRelease
	leases.inputRelease = nil
	completionWasRevoked := leases.completionRevoked
	completionRelease := leases.detachCompletionReservationLocked()
	releases := leases.releases
	leases.releases = nil
	leases.retainedBytes = 0
	leases.pendingRetained = 0
	leases.mu.Unlock()
	unregisterChatGPTWebImageCompletionReservation(leases, completionWasRevoked)
	for index := len(releases) - 1; index >= 0; index-- {
		release := releases[index]
		if release != nil {
			release()
		}
	}
	if completionRelease != nil {
		completionRelease()
	}
	if inputRelease != nil {
		inputRelease()
	}
}

// ChatGPTWebImageMemoryLeaseSetFromMetadata returns the request lease owner.
func ChatGPTWebImageMemoryLeaseSetFromMetadata(metadata map[string]any) *ChatGPTWebImageMemoryLeaseSet {
	if len(metadata) == 0 {
		return nil
	}
	leases, _ := metadata[ChatGPTWebImageMemoryLeaseSetMetadataKey].(*ChatGPTWebImageMemoryLeaseSet)
	return leases
}

// WithChatGPTWebImageMemoryLeaseSet attaches request-owned memory leases.
func WithChatGPTWebImageMemoryLeaseSet(ctx context.Context, leases *ChatGPTWebImageMemoryLeaseSet) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if leases == nil {
		return ctx
	}
	return context.WithValue(ctx, chatGPTWebImageMemoryLeaseSetContextKey{}, leases)
}

// ChatGPTWebImageMemoryLeaseSetFromContext returns request-owned memory leases.
func ChatGPTWebImageMemoryLeaseSetFromContext(ctx context.Context) *ChatGPTWebImageMemoryLeaseSet {
	if ctx == nil {
		return nil
	}
	leases, _ := ctx.Value(chatGPTWebImageMemoryLeaseSetContextKey{}).(*ChatGPTWebImageMemoryLeaseSet)
	return leases
}

// Snapshot returns a lock-free point-in-time view of the controller.
func (admission *ChatGPTWebImageMemoryAdmission) Snapshot() ChatGPTWebImageMemoryRuntimeSnapshot {
	if admission == nil {
		return ChatGPTWebImageMemoryRuntimeSnapshot{}
	}
	coordinator := defaultChatGPTWebImageCompletionCoordinator
	return ChatGPTWebImageMemoryRuntimeSnapshot{
		CapacityBytes:                 admission.capacity,
		QueueLimit:                    admission.queueLimit,
		WaitingTasks:                  admission.waitingTasks.Load(),
		WaitingBytes:                  admission.waitingBytes.Load(),
		ProcessingTasks:               admission.processingTasks.Load(),
		ProcessingBytes:               admission.processingBytes.Load(),
		PeakProcessingBytes:           admission.peakProcessingBytes.Load(),
		Acquisitions:                  admission.acquisitions.Load(),
		CanceledWaits:                 admission.canceledWaits.Load(),
		QueueRejected:                 admission.queueRejected.Load(),
		ImmediateRejected:             admission.immediateRejected.Load(),
		CompletionReservations:        coordinator.reservationCount.Load(),
		RevokedCompletionReservations: coordinator.revokedCount.Load(),
		FinalizationActive:            coordinator.active.Load(),
		FinalizationWaiting:           coordinator.waiting.Load(),
	}
}

func chatGPTWebImageMemoryQueueLimit() int {
	limit := 8 * runtime.GOMAXPROCS(0)
	if limit < 64 {
		return 64
	}
	if limit > 512 {
		return 512
	}
	return limit
}

func resolvedChatGPTWebImageMemoryCapacity() int64 {
	memoryLimit := debug.SetMemoryLimit(-1)
	if memoryLimit <= 0 || memoryLimit == math.MaxInt64 {
		return defaultChatGPTWebImageMemoryBytes
	}
	capacity := memoryLimit / 4
	if capacity < minChatGPTWebImageMemoryBytes {
		return minChatGPTWebImageMemoryBytes
	}
	if capacity > maxChatGPTWebImageMemoryBytes {
		return maxChatGPTWebImageMemoryBytes
	}
	return capacity
}

func updateChatGPTWebImageMemoryPeak(peak *atomic.Int64, value int64) {
	for {
		current := peak.Load()
		if value <= current || peak.CompareAndSwap(current, value) {
			return
		}
	}
}
