package helps

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
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

func TestChatGPTWebImageMemoryAdmissionRejectsBeyondBoundedQueue(t *testing.T) {
	admission := NewChatGPTWebImageMemoryAdmission(10)
	admission.queueLimit = 1
	releaseActive, errAcquire := admission.Acquire(t.Context(), 10)
	if errAcquire != nil {
		t.Fatalf("Acquire(active) error = %v", errAcquire)
	}
	waitContext, cancelWait := context.WithCancel(t.Context())
	waitResult := make(chan error, 1)
	go func() {
		_, err := admission.Acquire(waitContext, 1)
		waitResult <- err
	}()
	deadline := time.Now().Add(time.Second)
	for admission.Snapshot().WaitingTasks != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if _, errOverflow := admission.Acquire(t.Context(), 1); !errors.Is(errOverflow, ErrChatGPTWebImageMemoryQueueFull) {
		t.Fatalf("overflow error = %v, want queue full", errOverflow)
	}
	if snapshot := admission.Snapshot(); snapshot.QueueLimit != 1 || snapshot.QueueRejected != 1 {
		t.Fatalf("queue snapshot = %#v", snapshot)
	}
	cancelWait()
	if errWait := <-waitResult; !errors.Is(errWait, context.Canceled) {
		t.Fatalf("wait error = %v, want canceled", errWait)
	}
	releaseActive()
}

func TestChatGPTWebImageMemoryLeaseSetReleasesInputAndOutputsOnce(t *testing.T) {
	leases := NewChatGPTWebImageMemoryLeaseSet()
	var inputReleased atomic.Int32
	var outputReleased atomic.Int32
	leases.mu.Lock()
	leases.inputRelease = func() { inputReleased.Add(1) }
	leases.releases = append(leases.releases, func() { outputReleased.Add(1) })
	leases.mu.Unlock()

	leases.ReleaseInput()
	leases.ReleaseInput()
	if inputReleased.Load() != 1 || outputReleased.Load() != 0 {
		t.Fatalf("release input counts = input:%d output:%d", inputReleased.Load(), outputReleased.Load())
	}
	leases.Release()
	leases.Release()
	if inputReleased.Load() != 1 || outputReleased.Load() != 1 {
		t.Fatalf("final release counts = input:%d output:%d", inputReleased.Load(), outputReleased.Load())
	}
}

func TestChatGPTWebImageMemoryAdmissionBoundsFifteenHundredConcurrentRequests(t *testing.T) {
	const requests = 1500
	admission := NewChatGPTWebImageMemoryAdmission(64)
	admission.queueLimit = 512
	start := make(chan struct{})
	var wait sync.WaitGroup
	var succeeded atomic.Int64
	var rejected atomic.Int64
	var unexpected atomic.Int64
	wait.Add(requests)
	for range requests {
		go func() {
			defer wait.Done()
			<-start
			release, errAcquire := admission.Acquire(t.Context(), 8)
			switch {
			case errAcquire == nil:
				succeeded.Add(1)
				time.Sleep(time.Millisecond)
				release()
			case errors.Is(errAcquire, ErrChatGPTWebImageMemoryQueueFull):
				rejected.Add(1)
			default:
				unexpected.Add(1)
			}
		}()
	}
	close(start)
	wait.Wait()

	snapshot := admission.Snapshot()
	if succeeded.Load()+rejected.Load()+unexpected.Load() != requests || succeeded.Load() == 0 || rejected.Load() == 0 || unexpected.Load() != 0 {
		t.Fatalf("request outcomes = success:%d rejected:%d unexpected:%d", succeeded.Load(), rejected.Load(), unexpected.Load())
	}
	if snapshot.ProcessingTasks != 0 || snapshot.ProcessingBytes != 0 || snapshot.WaitingTasks != 0 || snapshot.PeakProcessingBytes > snapshot.CapacityBytes {
		t.Fatalf("final admission snapshot = %#v", snapshot)
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
