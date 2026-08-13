package helps

import (
	"context"
	"testing"
	"time"
)

func TestChatGPTWebImageMemoryAdmissionSerializesWeightedWork(t *testing.T) {
	admission := NewChatGPTWebImageMemoryAdmission(10)
	releaseFirst, errAcquire := admission.Acquire(t.Context(), 8)
	if errAcquire != nil {
		t.Fatalf("Acquire(first) error = %v", errAcquire)
	}

	acquiredSecond := make(chan func(), 1)
	go func() {
		release, err := admission.Acquire(t.Context(), 8)
		if err == nil {
			acquiredSecond <- release
		}
	}()

	deadline := time.Now().Add(time.Second)
	for admission.Snapshot().WaitingTasks != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if snapshot := admission.Snapshot(); snapshot.WaitingTasks != 1 ||
		snapshot.ProcessingTasks != 1 || snapshot.ProcessingBytes != 8 {
		t.Fatalf("snapshot while blocked = %#v", snapshot)
	}

	releaseFirst()
	select {
	case releaseSecond := <-acquiredSecond:
		releaseSecond()
	case <-time.After(time.Second):
		t.Fatal("second weighted acquisition did not resume")
	}
	if snapshot := admission.Snapshot(); snapshot.ProcessingTasks != 0 ||
		snapshot.Acquisitions != 2 || snapshot.PeakProcessingBytes != 8 {
		t.Fatalf("final snapshot = %#v", snapshot)
	}
}

func TestChatGPTWebImageMemoryAdmissionCancellationAndOversizedWork(t *testing.T) {
	admission := NewChatGPTWebImageMemoryAdmission(10)
	release, errAcquire := admission.Acquire(t.Context(), 100)
	if errAcquire != nil {
		t.Fatalf("Acquire(oversized) error = %v", errAcquire)
	}
	if snapshot := admission.Snapshot(); snapshot.ProcessingBytes != 10 {
		t.Fatalf("oversized snapshot = %#v", snapshot)
	}

	canceledContext, cancel := context.WithCancel(t.Context())
	cancel()
	if _, errCanceled := admission.Acquire(canceledContext, 1); errCanceled == nil {
		t.Fatal("Acquire(canceled) error = nil")
	}
	release()
	if snapshot := admission.Snapshot(); snapshot.CanceledWaits != 1 || snapshot.ProcessingTasks != 0 {
		t.Fatalf("canceled snapshot = %#v", snapshot)
	}
}
