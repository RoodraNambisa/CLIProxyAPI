package helps

import (
	"errors"
	"io/fs"
	"sync"
	"sync/atomic"
)

// ChatGPTWebImageSpoolRuntimeSnapshot contains aggregate temporary-file
// lifecycle metrics. It intentionally contains no paths or request data.
type ChatGPTWebImageSpoolRuntimeSnapshot struct {
	CurrentFiles    int64  `json:"current_files"`
	CurrentBytes    int64  `json:"current_bytes"`
	PeakBytes       int64  `json:"peak_bytes"`
	CreatedFiles    uint64 `json:"created_files"`
	CleanedFiles    uint64 `json:"cleaned_files"`
	CleanupFailures uint64 `json:"cleanup_failures"`
}

type chatGPTWebImageSpoolTracker struct {
	currentFiles    atomic.Int64
	currentBytes    atomic.Int64
	peakBytes       atomic.Int64
	createdFiles    atomic.Uint64
	cleanedFiles    atomic.Uint64
	cleanupFailures atomic.Uint64
}

// ChatGPTWebImageSpoolFile tracks one temporary file without retaining its path.
type ChatGPTWebImageSpoolFile struct {
	mu       sync.Mutex
	tracker  *chatGPTWebImageSpoolTracker
	bytes    int64
	finished bool
}

var defaultChatGPTWebImageSpoolTracker chatGPTWebImageSpoolTracker

// BeginChatGPTWebImageSpool records one successfully created temporary file.
func BeginChatGPTWebImageSpool() *ChatGPTWebImageSpoolFile {
	return defaultChatGPTWebImageSpoolTracker.begin()
}

// ChatGPTWebImageSpoolSnapshot returns process-wide aggregate spool metrics.
func ChatGPTWebImageSpoolSnapshot() ChatGPTWebImageSpoolRuntimeSnapshot {
	return defaultChatGPTWebImageSpoolTracker.snapshot()
}

func (tracker *chatGPTWebImageSpoolTracker) begin() *ChatGPTWebImageSpoolFile {
	if tracker == nil {
		return nil
	}
	tracker.currentFiles.Add(1)
	tracker.createdFiles.Add(1)
	return &ChatGPTWebImageSpoolFile{tracker: tracker}
}

func (tracker *chatGPTWebImageSpoolTracker) snapshot() ChatGPTWebImageSpoolRuntimeSnapshot {
	if tracker == nil {
		return ChatGPTWebImageSpoolRuntimeSnapshot{}
	}
	currentBytes := tracker.currentBytes.Load()
	peakBytes := tracker.peakBytes.Load()
	if peakBytes < currentBytes {
		peakBytes = currentBytes
	}
	return ChatGPTWebImageSpoolRuntimeSnapshot{
		CurrentFiles:    tracker.currentFiles.Load(),
		CurrentBytes:    currentBytes,
		PeakBytes:       peakBytes,
		CreatedFiles:    tracker.createdFiles.Load(),
		CleanedFiles:    tracker.cleanedFiles.Load(),
		CleanupFailures: tracker.cleanupFailures.Load(),
	}
}

// SetBytes updates the retained byte count for this file. Calls after cleanup
// are ignored so late error paths cannot recreate a released gauge.
func (file *ChatGPTWebImageSpoolFile) SetBytes(bytes int64) {
	if file == nil || file.tracker == nil {
		return
	}
	if bytes < 0 {
		bytes = 0
	}
	file.mu.Lock()
	if file.finished {
		file.mu.Unlock()
		return
	}
	delta := bytes - file.bytes
	file.bytes = bytes
	current := file.tracker.currentBytes.Add(delta)
	file.mu.Unlock()
	for {
		peak := file.tracker.peakBytes.Load()
		if current <= peak || file.tracker.peakBytes.CompareAndSwap(peak, current) {
			return
		}
	}
}

// FinishCleanup records the single terminal cleanup outcome. A missing file is
// treated as already cleaned; a failed removal remains in the current gauges.
func (file *ChatGPTWebImageSpoolFile) FinishCleanup(err error) {
	if file == nil || file.tracker == nil {
		return
	}
	file.mu.Lock()
	if file.finished {
		file.mu.Unlock()
		return
	}
	file.finished = true
	bytes := file.bytes
	file.mu.Unlock()
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		file.tracker.cleanupFailures.Add(1)
		return
	}
	file.tracker.currentBytes.Add(-bytes)
	file.tracker.currentFiles.Add(-1)
	file.tracker.cleanedFiles.Add(1)
}
