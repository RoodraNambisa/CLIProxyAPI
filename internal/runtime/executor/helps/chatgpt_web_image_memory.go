package helps

import (
	"context"
	"math"
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

// ChatGPTWebImageMemoryRuntimeSnapshot describes image post-processing
// admission without exposing request or credential data.
type ChatGPTWebImageMemoryRuntimeSnapshot struct {
	CapacityBytes       int64  `json:"capacity_bytes"`
	WaitingTasks        int64  `json:"waiting_tasks"`
	WaitingBytes        int64  `json:"waiting_bytes"`
	ProcessingTasks     int64  `json:"processing_tasks"`
	ProcessingBytes     int64  `json:"processing_bytes"`
	PeakProcessingBytes int64  `json:"peak_processing_bytes"`
	Acquisitions        uint64 `json:"acquisitions"`
	CanceledWaits       uint64 `json:"canceled_waits"`
}

// ChatGPTWebImageMemoryAdmission bounds concurrent decoded image working sets.
type ChatGPTWebImageMemoryAdmission struct {
	capacity int64
	weighted *semaphore.Weighted

	waitingTasks        atomic.Int64
	waitingBytes        atomic.Int64
	processingTasks     atomic.Int64
	processingBytes     atomic.Int64
	peakProcessingBytes atomic.Int64
	acquisitions        atomic.Uint64
	canceledWaits       atomic.Uint64
}

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
		capacity: capacityBytes,
		weighted: semaphore.NewWeighted(capacityBytes),
	}
}

// AcquireChatGPTWebImageMemory reserves estimated decoded image memory. A
// request larger than the controller capacity runs exclusively instead of
// waiting forever for an impossible weight.
func AcquireChatGPTWebImageMemory(ctx context.Context, estimatedBytes int64) (func(), error) {
	return defaultChatGPTWebImageMemoryAdmission.Acquire(ctx, estimatedBytes)
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

	admission.waitingTasks.Add(1)
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

// Snapshot returns a lock-free point-in-time view of the controller.
func (admission *ChatGPTWebImageMemoryAdmission) Snapshot() ChatGPTWebImageMemoryRuntimeSnapshot {
	if admission == nil {
		return ChatGPTWebImageMemoryRuntimeSnapshot{}
	}
	return ChatGPTWebImageMemoryRuntimeSnapshot{
		CapacityBytes:       admission.capacity,
		WaitingTasks:        admission.waitingTasks.Load(),
		WaitingBytes:        admission.waitingBytes.Load(),
		ProcessingTasks:     admission.processingTasks.Load(),
		ProcessingBytes:     admission.processingBytes.Load(),
		PeakProcessingBytes: admission.peakProcessingBytes.Load(),
		Acquisitions:        admission.acquisitions.Load(),
		CanceledWaits:       admission.canceledWaits.Load(),
	}
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
