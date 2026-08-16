package helps

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
)

const (
	defaultChatGPTWebImageMemoryBytes = 512 << 20
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
	// BypassedCompletionReservations counts unique request lease sets admitted
	// without an idle reservation while another request owned finalization.
	BypassedCompletionReservations uint64 `json:"bypassed_completion_reservations"`
	FinalizationActive             int64  `json:"finalization_active"`
	FinalizationWaiting            int64  `json:"finalization_waiting"`
}

// ChatGPTWebImageMemoryAdmission bounds concurrent decoded image working sets.
type ChatGPTWebImageMemoryAdmission struct {
	mu         sync.Mutex
	capacity   int64
	used       int64
	queueLimit int64
	waiters    []*chatGPTWebImageMemoryWaiter

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

type chatGPTWebImageMemoryWaiter struct {
	ready           chan struct{}
	requestedWeight int64
	creditWeight    int64
	grantWeight     int64
	granted         bool
}

// ChatGPTWebImageMemoryLeaseSet owns all long-lived image buffers for one request.
type ChatGPTWebImageMemoryLeaseSet struct {
	mu                  sync.Mutex
	inputRelease        func()
	completionRelease   func()
	completionBytes     int64
	completionAvailable int64
	completionRevoked   bool
	completionBypassed  bool
	finalizationOwned   bool
	retainedBytes       int64
	retainedGrantBytes  int64
	pendingRetained     int64
	transientBytes      int64
	transientGrantBytes int64
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
	bypassedCount    atomic.Uint64
	active           atomic.Int64
	waiting          atomic.Int64
}

var defaultChatGPTWebImageMemoryAdmission = NewChatGPTWebImageMemoryAdmission(defaultChatGPTWebImageMemoryBytes)

var defaultChatGPTWebImageCompletionCoordinator = &chatGPTWebImageCompletionCoordinator{
	turn:         make(chan struct{}, 1),
	reservations: make(map[*ChatGPTWebImageMemoryLeaseSet]struct{}),
}

// NewChatGPTWebImageMemoryAdmission creates a resizable admission controller.
// Values below one are normalized to one byte for tests.
func NewChatGPTWebImageMemoryAdmission(capacityBytes int64) *ChatGPTWebImageMemoryAdmission {
	if capacityBytes < 1 {
		capacityBytes = 1
	}
	return &ChatGPTWebImageMemoryAdmission{
		capacity:   capacityBytes,
		queueLimit: int64(chatGPTWebImageMemoryQueueLimit()),
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

// ConfigureChatGPTWebImageMemoryCapacity applies a process-wide byte budget
// without replacing the admission controller or revoking active leases.
func ConfigureChatGPTWebImageMemoryCapacity(capacityBytes int64) {
	defaultChatGPTWebImageMemoryAdmission.Resize(capacityBytes)
}

// Resize updates the admission target in place. Existing leases remain valid;
// queued work resumes in FIFO order when the new target has room.
func (admission *ChatGPTWebImageMemoryAdmission) Resize(capacityBytes int64) {
	if admission == nil {
		return
	}
	if capacityBytes < 1 {
		capacityBytes = 1
	}
	admission.mu.Lock()
	admission.capacity = capacityBytes
	waitingBytes := int64(0)
	for _, waiter := range admission.waiters {
		waiter.grantWeight = admission.grantWeightLocked(waiter.requestedWeight, waiter.creditWeight)
		waitingBytes += waiter.grantWeight
	}
	admission.waitingBytes.Store(waitingBytes)
	admission.grantWaitersLocked()
	admission.mu.Unlock()
}

// Acquire reserves estimated decoded image memory until the returned release
// function is called.
func (admission *ChatGPTWebImageMemoryAdmission) Acquire(ctx context.Context, estimatedBytes int64) (func(), error) {
	release, _, err := admission.acquire(ctx, estimatedBytes, 0, false)
	return release, err
}

func (admission *ChatGPTWebImageMemoryAdmission) acquireCritical(ctx context.Context, requestedBytes, selfHeldBytes int64) (func(), int64, error) {
	return admission.acquire(ctx, requestedBytes, selfHeldBytes, true)
}

func (admission *ChatGPTWebImageMemoryAdmission) acquire(ctx context.Context, estimatedBytes, selfHeldBytes int64, critical bool) (func(), int64, error) {
	if admission == nil {
		weight := normalizedChatGPTWebImageMemoryRequest(estimatedBytes)
		return func() {}, weight, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		admission.canceledWaits.Add(1)
		return nil, 0, err
	}

	admission.mu.Lock()
	requestedWeight := normalizedChatGPTWebImageMemoryRequest(estimatedBytes)
	creditWeight := max(selfHeldBytes, int64(0))
	weight := admission.grantWeightLocked(requestedWeight, creditWeight)
	if len(admission.waiters) == 0 && admission.used <= admission.capacity && admission.used+weight <= admission.capacity {
		if weight > 0 {
			admission.activateLocked(weight)
		}
		admission.mu.Unlock()
		return admission.releaseFunc(weight), weight, nil
	}
	if !critical && int64(len(admission.waiters)) >= admission.queueLimit {
		admission.queueRejected.Add(1)
		admission.mu.Unlock()
		return nil, 0, ErrChatGPTWebImageMemoryQueueFull
	}
	waiter := &chatGPTWebImageMemoryWaiter{
		ready:           make(chan struct{}),
		requestedWeight: requestedWeight,
		creditWeight:    creditWeight,
		grantWeight:     weight,
	}
	admission.waiters = append(admission.waiters, waiter)
	admission.waitingTasks.Add(1)
	admission.waitingBytes.Add(weight)
	admission.mu.Unlock()

	select {
	case <-waiter.ready:
		if err := ctx.Err(); err != nil {
			admission.cancelWaiter(waiter)
			return nil, 0, err
		}
		return admission.releaseFunc(waiter.grantWeight), waiter.grantWeight, nil
	case <-ctx.Done():
		admission.cancelWaiter(waiter)
		return nil, 0, ctx.Err()
	}
}

// TryAcquire reserves memory immediately or reports current capacity pressure.
func (admission *ChatGPTWebImageMemoryAdmission) TryAcquire(estimatedBytes int64) (func(), bool) {
	release, _, acquired := admission.tryAcquire(estimatedBytes)
	return release, acquired
}

func (admission *ChatGPTWebImageMemoryAdmission) tryAcquire(estimatedBytes int64) (func(), int64, bool) {
	if admission == nil {
		return func() {}, normalizedChatGPTWebImageMemoryRequest(estimatedBytes), true
	}
	admission.mu.Lock()
	weight := admission.grantWeightLocked(normalizedChatGPTWebImageMemoryRequest(estimatedBytes), 0)
	if len(admission.waiters) > 0 || admission.used+weight > admission.capacity {
		admission.immediateRejected.Add(1)
		admission.mu.Unlock()
		return nil, 0, false
	}
	admission.activateLocked(weight)
	admission.mu.Unlock()
	return admission.releaseFunc(weight), weight, true
}

func normalizedChatGPTWebImageMemoryRequest(estimatedBytes int64) int64 {
	if estimatedBytes < 1 {
		return 1
	}
	return estimatedBytes
}

func (admission *ChatGPTWebImageMemoryAdmission) grantWeightLocked(requestedWeight, creditWeight int64) int64 {
	availableWeight := max(admission.capacity-max(creditWeight, int64(0)), int64(0))
	return min(normalizedChatGPTWebImageMemoryRequest(requestedWeight), availableWeight)
}

func (admission *ChatGPTWebImageMemoryAdmission) currentGrantWeight(requestedWeight, creditWeight int64) int64 {
	if admission == nil {
		return normalizedChatGPTWebImageMemoryRequest(requestedWeight)
	}
	admission.mu.Lock()
	weight := admission.grantWeightLocked(requestedWeight, creditWeight)
	admission.mu.Unlock()
	return weight
}

func (admission *ChatGPTWebImageMemoryAdmission) releaseFunc(weight int64) func() {
	if admission == nil || weight <= 0 {
		return func() {}
	}
	var releaseOnce sync.Once
	return func() {
		releaseOnce.Do(func() {
			admission.mu.Lock()
			admission.deactivateLocked(weight)
			admission.grantWaitersLocked()
			admission.mu.Unlock()
		})
	}
}

func (admission *ChatGPTWebImageMemoryAdmission) cancelWaiter(waiter *chatGPTWebImageMemoryWaiter) {
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if waiter.granted {
		waiter.granted = false
		if waiter.grantWeight > 0 {
			admission.deactivateLocked(waiter.grantWeight)
		}
	} else {
		for index, candidate := range admission.waiters {
			if candidate != waiter {
				continue
			}
			admission.waiters = append(admission.waiters[:index], admission.waiters[index+1:]...)
			admission.waitingTasks.Add(-1)
			admission.waitingBytes.Add(-waiter.grantWeight)
			break
		}
	}
	admission.canceledWaits.Add(1)
	admission.grantWaitersLocked()
}

func (admission *ChatGPTWebImageMemoryAdmission) activateLocked(weight int64) {
	admission.used += weight
	admission.acquisitions.Add(1)
	admission.processingTasks.Add(1)
	processingBytes := admission.processingBytes.Add(weight)
	updateChatGPTWebImageMemoryPeak(&admission.peakProcessingBytes, processingBytes)
}

func (admission *ChatGPTWebImageMemoryAdmission) deactivateLocked(weight int64) {
	admission.used -= weight
	admission.processingBytes.Add(-weight)
	admission.processingTasks.Add(-1)
}

func (admission *ChatGPTWebImageMemoryAdmission) grantWaitersLocked() {
	for len(admission.waiters) > 0 {
		if admission.used > admission.capacity {
			return
		}
		waiter := admission.waiters[0]
		if waiter.grantWeight > 0 && admission.used+waiter.grantWeight > admission.capacity {
			return
		}
		admission.waiters = admission.waiters[1:]
		admission.waitingTasks.Add(-1)
		admission.waitingBytes.Add(-waiter.grantWeight)
		waiter.granted = true
		if waiter.grantWeight > 0 {
			admission.activateLocked(waiter.grantWeight)
		}
		close(waiter.ready)
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
	requestedWeight, pendingWeight, err := leases.reserveRetainedWeight(estimatedBytes)
	if err != nil {
		return err
	}
	borrowed, release, actualWeight, err := leases.acquireLeaseMemory(ctx, requestedWeight)
	if err != nil {
		leases.cancelRetainedWeight(pendingWeight)
		return err
	}
	if err = leases.retain(release, pendingWeight, actualWeight, actualWeight-borrowed); err != nil {
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
	requestedWeight := normalizedChatGPTWebImageMemoryRequest(estimatedBytes)
	targetWeight := defaultChatGPTWebImageMemoryAdmission.currentGrantWeight(requestedWeight, 0)
	coordinator := defaultChatGPTWebImageCompletionCoordinator
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	leases.mu.Lock()
	if leases.released {
		leases.mu.Unlock()
		return false
	}
	if leases.completionRelease != nil {
		reserved := !leases.completionRevoked && leases.completionBytes >= targetWeight
		leases.mu.Unlock()
		return reserved
	}
	// The active finalizer already owns forward progress and revokes idle
	// reservations. Admit new bounded executions without another reservation
	// instead of turning its entire critical section into a zero-capacity window.
	// Their pre-finalization transient allocations remain non-blocking, so they
	// cannot reintroduce a reservation hold-and-wait cycle.
	if coordinator.finalizing {
		firstBypass := !leases.completionBypassed
		leases.completionBypassed = true
		leases.mu.Unlock()
		if firstBypass {
			coordinator.bypassedCount.Add(1)
		}
		return true
	}
	leases.mu.Unlock()
	release, actualWeight, acquired := defaultChatGPTWebImageMemoryAdmission.tryAcquire(requestedWeight)
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
		reserved := leases.completionBytes >= actualWeight
		leases.mu.Unlock()
		release()
		return reserved
	}
	leases.completionRelease = release
	leases.completionBytes = actualWeight
	leases.completionAvailable = actualWeight
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
	requestedWeight, pendingWeight, err := leases.reserveTransientWeight(estimatedBytes)
	if err != nil {
		return nil, err
	}
	borrowed, releaseMemory, actualWeight, err := leases.acquireLeaseMemory(ctx, requestedWeight)
	if err != nil {
		leases.releaseTransientWeight(pendingWeight, 0)
		return nil, err
	}
	grantedWeight := actualWeight - borrowed
	if err = leases.commitTransientWeight(pendingWeight, actualWeight, grantedWeight); err != nil {
		if releaseMemory != nil {
			releaseMemory()
		}
		leases.restoreCompletion(borrowed)
		return nil, err
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			if releaseMemory != nil {
				releaseMemory()
			}
			leases.restoreCompletion(borrowed)
			leases.releaseTransientWeight(actualWeight, grantedWeight)
		})
	}, nil
}

func (leases *ChatGPTWebImageMemoryLeaseSet) reserveRetainedWeight(estimatedBytes int64) (int64, int64, error) {
	requestedWeight, weight, capacity := normalizedChatGPTWebImageLeaseWeight(estimatedBytes)
	leases.mu.Lock()
	defer leases.mu.Unlock()
	if leases.released {
		return 0, 0, context.Canceled
	}
	used := leases.retainedBytes + leases.pendingRetained + leases.transientBytes
	if used > capacity || weight > capacity-used {
		return 0, 0, ErrChatGPTWebImageMemoryWorkingSetTooLarge
	}
	leases.pendingRetained += weight
	return requestedWeight, weight, nil
}

func (leases *ChatGPTWebImageMemoryLeaseSet) cancelRetainedWeight(weight int64) {
	if leases == nil || weight <= 0 {
		return
	}
	leases.mu.Lock()
	if !leases.released {
		leases.pendingRetained -= weight
	}
	leases.mu.Unlock()
}

func (leases *ChatGPTWebImageMemoryLeaseSet) reserveTransientWeight(estimatedBytes int64) (int64, int64, error) {
	requestedWeight, weight, capacity := normalizedChatGPTWebImageLeaseWeight(estimatedBytes)
	leases.mu.Lock()
	defer leases.mu.Unlock()
	if leases.released {
		return 0, 0, context.Canceled
	}
	used := leases.retainedBytes + leases.pendingRetained + leases.transientBytes
	if used > capacity || weight > capacity-used {
		return 0, 0, ErrChatGPTWebImageMemoryWorkingSetTooLarge
	}
	leases.transientBytes += weight
	return requestedWeight, weight, nil
}

func (leases *ChatGPTWebImageMemoryLeaseSet) commitTransientWeight(pendingWeight, actualWeight, grantedWeight int64) error {
	leases.mu.Lock()
	defer leases.mu.Unlock()
	if leases.released {
		leases.transientBytes -= pendingWeight
		return context.Canceled
	}
	leases.transientBytes += actualWeight - pendingWeight
	leases.transientGrantBytes += grantedWeight
	return nil
}

func (leases *ChatGPTWebImageMemoryLeaseSet) releaseTransientWeight(weight, grantedWeight int64) {
	if leases == nil || weight <= 0 {
		return
	}
	leases.mu.Lock()
	leases.transientBytes -= weight
	leases.transientGrantBytes -= grantedWeight
	leases.mu.Unlock()
}

func normalizedChatGPTWebImageLeaseWeight(estimatedBytes int64) (int64, int64, int64) {
	capacity := ChatGPTWebImageMemorySnapshot().CapacityBytes
	if capacity < 1 {
		capacity = 1
	}
	requestedWeight := normalizedChatGPTWebImageMemoryRequest(estimatedBytes)
	weight := requestedWeight
	if weight > capacity {
		weight = capacity
	}
	return requestedWeight, weight, capacity
}

func (leases *ChatGPTWebImageMemoryLeaseSet) acquireLeaseMemory(ctx context.Context, requestedWeight int64) (int64, func(), int64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	targetWeight := defaultChatGPTWebImageMemoryAdmission.currentGrantWeight(requestedWeight, 0)
	leases.mu.Lock()
	if leases.released {
		leases.mu.Unlock()
		return 0, nil, 0, context.Canceled
	}
	critical := leases.finalizationOwned
	if !critical && !leases.completionRevoked && leases.completionAvailable >= targetWeight {
		leases.completionAvailable -= targetWeight
		leases.mu.Unlock()
		return targetWeight, nil, targetWeight, nil
	}
	leases.mu.Unlock()
	if !critical {
		if err := ctx.Err(); err != nil {
			return 0, nil, 0, err
		}
		release, actualWeight, acquired := defaultChatGPTWebImageMemoryAdmission.tryAcquire(requestedWeight)
		if !acquired {
			return 0, nil, 0, ErrChatGPTWebImageMemoryQueueFull
		}
		return 0, release, actualWeight, nil
	}
	borrowed, selfHeldWeight, err := leases.borrowCompletion(requestedWeight)
	if err != nil {
		return 0, nil, 0, err
	}
	remainingWeight := max(requestedWeight-borrowed, int64(0))
	if remainingWeight == 0 {
		return borrowed, nil, borrowed, nil
	}
	release, grantedWeight, err := defaultChatGPTWebImageMemoryAdmission.acquireCritical(ctx, remainingWeight, selfHeldWeight)
	if err != nil {
		leases.restoreCompletion(borrowed)
		return 0, nil, 0, err
	}
	return borrowed, release, borrowed + grantedWeight, nil
}

func (leases *ChatGPTWebImageMemoryLeaseSet) borrowCompletion(estimatedBytes int64) (int64, int64, error) {
	if leases == nil {
		return 0, 0, nil
	}
	weight := defaultChatGPTWebImageMemoryAdmission.currentGrantWeight(estimatedBytes, 0)
	leases.mu.Lock()
	defer leases.mu.Unlock()
	if leases.released {
		return 0, 0, context.Canceled
	}
	selfHeldWeight := leases.completionBytes + leases.retainedGrantBytes + leases.transientGrantBytes
	if leases.completionRevoked {
		return 0, selfHeldWeight, nil
	}
	borrowed := min(weight, leases.completionAvailable)
	leases.completionAvailable -= borrowed
	return borrowed, selfHeldWeight, nil
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

func (leases *ChatGPTWebImageMemoryLeaseSet) retain(release func(), pendingWeight, actualWeight, grantedWeight int64) error {
	if leases == nil {
		if release != nil {
			release()
		}
		return nil
	}
	leases.mu.Lock()
	if leases.released {
		leases.mu.Unlock()
		if release != nil {
			release()
		}
		return context.Canceled
	}
	leases.pendingRetained -= pendingWeight
	leases.retainedBytes += actualWeight
	leases.retainedGrantBytes += grantedWeight
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
	leases.retainedGrantBytes = 0
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

// Snapshot returns a point-in-time view of the controller.
func (admission *ChatGPTWebImageMemoryAdmission) Snapshot() ChatGPTWebImageMemoryRuntimeSnapshot {
	if admission == nil {
		return ChatGPTWebImageMemoryRuntimeSnapshot{}
	}
	admission.mu.Lock()
	capacity := admission.capacity
	queueLimit := admission.queueLimit
	admission.mu.Unlock()
	coordinator := defaultChatGPTWebImageCompletionCoordinator
	return ChatGPTWebImageMemoryRuntimeSnapshot{
		CapacityBytes:                  capacity,
		QueueLimit:                     queueLimit,
		WaitingTasks:                   admission.waitingTasks.Load(),
		WaitingBytes:                   admission.waitingBytes.Load(),
		ProcessingTasks:                admission.processingTasks.Load(),
		ProcessingBytes:                admission.processingBytes.Load(),
		PeakProcessingBytes:            admission.peakProcessingBytes.Load(),
		Acquisitions:                   admission.acquisitions.Load(),
		CanceledWaits:                  admission.canceledWaits.Load(),
		QueueRejected:                  admission.queueRejected.Load(),
		ImmediateRejected:              admission.immediateRejected.Load(),
		CompletionReservations:         coordinator.reservationCount.Load(),
		RevokedCompletionReservations:  coordinator.revokedCount.Load(),
		BypassedCompletionReservations: coordinator.bypassedCount.Load(),
		FinalizationActive:             coordinator.active.Load(),
		FinalizationWaiting:            coordinator.waiting.Load(),
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

func updateChatGPTWebImageMemoryPeak(peak *atomic.Int64, value int64) {
	for {
		current := peak.Load()
		if value <= current || peak.CompareAndSwap(current, value) {
			return
		}
	}
}
