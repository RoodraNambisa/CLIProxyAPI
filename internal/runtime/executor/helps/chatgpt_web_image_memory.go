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

// ChatGPTWebImageMemoryLeaseSetMetadataKey carries a request-scoped lease set
// from the HTTP image handler into the ChatGPT Web executor.
const ChatGPTWebImageMemoryLeaseSetMetadataKey = "chatgpt_web_image_memory_lease_set"

// ChatGPTWebImageMemoryRuntimeSnapshot describes image post-processing
// admission without exposing request or credential data.
type ChatGPTWebImageMemoryRuntimeSnapshot struct {
	CapacityBytes       int64  `json:"capacity_bytes"`
	QueueLimit          int64  `json:"queue_limit"`
	WaitingTasks        int64  `json:"waiting_tasks"`
	WaitingBytes        int64  `json:"waiting_bytes"`
	ProcessingTasks     int64  `json:"processing_tasks"`
	ProcessingBytes     int64  `json:"processing_bytes"`
	PeakProcessingBytes int64  `json:"peak_processing_bytes"`
	Acquisitions        uint64 `json:"acquisitions"`
	CanceledWaits       uint64 `json:"canceled_waits"`
	QueueRejected       uint64 `json:"queue_rejected"`
	ImmediateRejected   uint64 `json:"immediate_rejected"`
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
	mu           sync.Mutex
	inputRelease func()
	releases     []func()
	released     bool
}

type chatGPTWebImageMemoryLeaseSetContextKey struct{}

var defaultChatGPTWebImageMemoryAdmission = NewChatGPTWebImageMemoryAdmission(
	resolvedChatGPTWebImageMemoryCapacity(),
)

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
	if waiting > admission.queueLimit {
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
	release, err := AcquireChatGPTWebImageMemory(ctx, estimatedBytes)
	if err != nil {
		return err
	}
	return leases.retain(release)
}

// AcquireInput waits for the request input lease. The executor releases this
// lease as soon as the final upstream request body has been sent.
func (leases *ChatGPTWebImageMemoryLeaseSet) AcquireInput(ctx context.Context, estimatedBytes int64) error {
	release, err := AcquireChatGPTWebImageMemory(ctx, estimatedBytes)
	if err != nil {
		return err
	}
	if leases == nil {
		release()
		return nil
	}
	leases.mu.Lock()
	if leases.released {
		leases.mu.Unlock()
		release()
		return context.Canceled
	}
	previous := leases.inputRelease
	leases.inputRelease = release
	leases.mu.Unlock()
	if previous != nil {
		previous()
	}
	return nil
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

// TryAcquire retains an immediate lease and fails fast under memory pressure.
func (leases *ChatGPTWebImageMemoryLeaseSet) TryAcquire(estimatedBytes int64) bool {
	release, acquired := TryAcquireChatGPTWebImageMemory(estimatedBytes)
	if !acquired {
		return false
	}
	return leases.retain(release) == nil
}

func (leases *ChatGPTWebImageMemoryLeaseSet) retain(release func()) error {
	if release == nil {
		return nil
	}
	if leases == nil {
		release()
		return nil
	}
	leases.mu.Lock()
	if leases.released {
		leases.mu.Unlock()
		release()
		return context.Canceled
	}
	leases.releases = append(leases.releases, release)
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
	releases := leases.releases
	leases.releases = nil
	leases.mu.Unlock()
	if inputRelease != nil {
		inputRelease()
	}
	for _, release := range releases {
		if release != nil {
			release()
		}
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
	return ChatGPTWebImageMemoryRuntimeSnapshot{
		CapacityBytes:       admission.capacity,
		QueueLimit:          admission.queueLimit,
		WaitingTasks:        admission.waitingTasks.Load(),
		WaitingBytes:        admission.waitingBytes.Load(),
		ProcessingTasks:     admission.processingTasks.Load(),
		ProcessingBytes:     admission.processingBytes.Load(),
		PeakProcessingBytes: admission.peakProcessingBytes.Load(),
		Acquisitions:        admission.acquisitions.Load(),
		CanceledWaits:       admission.canceledWaits.Load(),
		QueueRejected:       admission.queueRejected.Load(),
		ImmediateRejected:   admission.immediateRejected.Load(),
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
