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

func TestChatGPTWebImageMemoryAdmissionResizeReweightsQueuedRequest(t *testing.T) {
	t.Run("expansion uses original request", func(t *testing.T) {
		admission := NewChatGPTWebImageMemoryAdmission(4)
		releaseActive, err := admission.Acquire(t.Context(), 4)
		if err != nil {
			t.Fatalf("Acquire(active) error = %v", err)
		}
		acquired := make(chan func(), 1)
		go func() {
			release, errAcquire := admission.Acquire(t.Context(), 8)
			if errAcquire == nil {
				acquired <- release
			}
		}()
		waitForImageMemorySnapshot(t, admission, func(snapshot ChatGPTWebImageMemoryRuntimeSnapshot) bool {
			return snapshot.WaitingTasks == 1 && snapshot.WaitingBytes == 4
		})

		admission.Resize(12)
		var releaseQueued func()
		select {
		case releaseQueued = <-acquired:
		case <-time.After(time.Second):
			t.Fatal("expanded admission did not grant queued request")
		}
		if snapshot := admission.Snapshot(); snapshot.CapacityBytes != 12 || snapshot.ProcessingBytes != 12 || snapshot.WaitingTasks != 0 {
			t.Fatalf("expanded snapshot = %#v", snapshot)
		}
		releaseQueued()
		releaseActive()
		assertImageMemoryAdmissionEmpty(t, admission)
	})

	t.Run("shrink preserves oversized exclusive contract", func(t *testing.T) {
		admission := NewChatGPTWebImageMemoryAdmission(8)
		releaseActive, err := admission.Acquire(t.Context(), 8)
		if err != nil {
			t.Fatalf("Acquire(active) error = %v", err)
		}
		acquired := make(chan func(), 1)
		go func() {
			release, errAcquire := admission.Acquire(t.Context(), 16)
			if errAcquire == nil {
				acquired <- release
			}
		}()
		waitForImageMemorySnapshot(t, admission, func(snapshot ChatGPTWebImageMemoryRuntimeSnapshot) bool {
			return snapshot.WaitingTasks == 1 && snapshot.WaitingBytes == 8
		})

		admission.Resize(4)
		if snapshot := admission.Snapshot(); snapshot.CapacityBytes != 4 || snapshot.ProcessingBytes != 8 || snapshot.WaitingBytes != 4 {
			t.Fatalf("shrunk blocked snapshot = %#v", snapshot)
		}
		releaseActive()
		var releaseQueued func()
		select {
		case releaseQueued = <-acquired:
		case <-time.After(time.Second):
			t.Fatal("shrunk admission did not grant exclusive request")
		}
		if snapshot := admission.Snapshot(); snapshot.ProcessingBytes != 4 || snapshot.ProcessingTasks != 1 {
			t.Fatalf("shrunk granted snapshot = %#v", snapshot)
		}
		releaseQueued()
		assertImageMemoryAdmissionEmpty(t, admission)
	})

	t.Run("zero grant waits for shrink convergence", func(t *testing.T) {
		admission := NewChatGPTWebImageMemoryAdmission(8)
		releaseSelfHeld, err := admission.Acquire(t.Context(), 4)
		if err != nil {
			t.Fatalf("Acquire(self-held) error = %v", err)
		}
		releaseOther, err := admission.Acquire(t.Context(), 4)
		if err != nil {
			t.Fatalf("Acquire(other) error = %v", err)
		}
		admission.Resize(4)
		acquired := make(chan func(), 1)
		go func() {
			release, _, errAcquire := admission.acquireCritical(t.Context(), 1, 4)
			if errAcquire == nil {
				acquired <- release
			}
		}()
		waitForImageMemorySnapshot(t, admission, func(snapshot ChatGPTWebImageMemoryRuntimeSnapshot) bool {
			return snapshot.ProcessingBytes == 8 && snapshot.WaitingTasks == 1 && snapshot.WaitingBytes == 0
		})
		select {
		case releaseQueued := <-acquired:
			releaseQueued()
			t.Fatal("zero-grant request bypassed the shrunken capacity")
		default:
		}

		releaseOther()
		var releaseQueued func()
		select {
		case releaseQueued = <-acquired:
		case <-time.After(time.Second):
			t.Fatal("zero-grant request did not resume after usage converged")
		}
		releaseQueued()
		releaseSelfHeld()
		assertImageMemoryAdmissionEmpty(t, admission)
		admission.mu.Lock()
		used := admission.used
		admission.mu.Unlock()
		if used != 0 {
			t.Fatalf("internal used after zero-grant release = %d", used)
		}
	})
}

func TestChatGPTWebImageMemoryAdmissionCancelGrantRaceDoesNotDoubleRelease(t *testing.T) {
	for iteration := range 100 {
		admission := NewChatGPTWebImageMemoryAdmission(1)
		releaseActive, err := admission.Acquire(t.Context(), 1)
		if err != nil {
			t.Fatalf("iteration %d: Acquire(active) error = %v", iteration, err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan struct {
			release func()
			err     error
		}, 1)
		go func() {
			release, errAcquire := admission.Acquire(ctx, 1)
			result <- struct {
				release func()
				err     error
			}{release: release, err: errAcquire}
		}()
		waitForImageMemorySnapshot(t, admission, func(snapshot ChatGPTWebImageMemoryRuntimeSnapshot) bool {
			return snapshot.WaitingTasks == 1
		})

		var race sync.WaitGroup
		race.Add(2)
		go func() {
			defer race.Done()
			cancel()
		}()
		go func() {
			defer race.Done()
			releaseActive()
		}()
		race.Wait()
		outcome := <-result
		if outcome.err == nil && outcome.release != nil {
			outcome.release()
		} else if !errors.Is(outcome.err, context.Canceled) {
			t.Fatalf("iteration %d: Acquire(waiter) error = %v", iteration, outcome.err)
		}
		assertImageMemoryAdmissionEmpty(t, admission)
		admission.mu.Lock()
		used := admission.used
		admission.mu.Unlock()
		if used != 0 {
			t.Fatalf("iteration %d: internal used = %d", iteration, used)
		}
	}
}

func TestChatGPTWebImageMemoryLeaseSetResizeUsesActualGrantWeight(t *testing.T) {
	t.Run("retained expansion", func(t *testing.T) {
		admission, leases, releaseTurn := installResizableImageMemoryTestState(t, 4)
		releaseActive, err := admission.Acquire(t.Context(), 4)
		if err != nil {
			t.Fatalf("Acquire(active) error = %v", err)
		}
		acquired := make(chan error, 1)
		go func() { acquired <- leases.Acquire(t.Context(), 8) }()
		waitForImageMemorySnapshot(t, admission, func(snapshot ChatGPTWebImageMemoryRuntimeSnapshot) bool {
			return snapshot.WaitingTasks == 1 && snapshot.WaitingBytes == 4
		})

		admission.Resize(12)
		if err = <-acquired; err != nil {
			t.Fatalf("LeaseSet.Acquire() error = %v", err)
		}
		leases.mu.Lock()
		retained := leases.retainedBytes
		pending := leases.pendingRetained
		leases.mu.Unlock()
		if retained != 8 || pending != 0 {
			t.Fatalf("lease accounting = retained:%d pending:%d, want 8/0", retained, pending)
		}
		if snapshot := admission.Snapshot(); snapshot.ProcessingBytes != 12 || snapshot.WaitingBytes != 0 {
			t.Fatalf("expanded lease snapshot = %#v", snapshot)
		}
		releaseTurn()
		leases.Release()
		releaseActive()
		assertImageMemoryAdmissionEmpty(t, admission)
	})

	t.Run("transient shrink", func(t *testing.T) {
		admission, leases, releaseTurn := installResizableImageMemoryTestState(t, 8)
		releaseActive, err := admission.Acquire(t.Context(), 8)
		if err != nil {
			t.Fatalf("Acquire(active) error = %v", err)
		}
		type transientResult struct {
			release func()
			err     error
		}
		acquired := make(chan transientResult, 1)
		go func() {
			release, errAcquire := leases.AcquireTransient(t.Context(), 16)
			acquired <- transientResult{release: release, err: errAcquire}
		}()
		waitForImageMemorySnapshot(t, admission, func(snapshot ChatGPTWebImageMemoryRuntimeSnapshot) bool {
			return snapshot.WaitingTasks == 1 && snapshot.WaitingBytes == 8
		})

		admission.Resize(4)
		if snapshot := admission.Snapshot(); snapshot.WaitingBytes != 4 || snapshot.ProcessingBytes != 8 {
			t.Fatalf("shrunk lease snapshot = %#v", snapshot)
		}
		releaseActive()
		outcome := <-acquired
		if outcome.err != nil {
			t.Fatalf("LeaseSet.AcquireTransient() error = %v", outcome.err)
		}
		leases.mu.Lock()
		transient := leases.transientBytes
		leases.mu.Unlock()
		if transient != 4 {
			t.Fatalf("transient accounting = %d, want 4", transient)
		}
		if snapshot := admission.Snapshot(); snapshot.ProcessingBytes != 4 || snapshot.WaitingTasks != 0 {
			t.Fatalf("shrunk transient snapshot = %#v", snapshot)
		}
		outcome.release()
		leases.mu.Lock()
		transient = leases.transientBytes
		leases.mu.Unlock()
		if transient != 0 {
			t.Fatalf("transient accounting after release = %d", transient)
		}
		releaseTurn()
		leases.Release()
		assertImageMemoryAdmissionEmpty(t, admission)
	})
}

func TestChatGPTWebImageMemoryLeaseSetShrinkCreditsOwnedFinalizerMemory(t *testing.T) {
	previousAdmission := defaultChatGPTWebImageMemoryAdmission
	previousCoordinator := defaultChatGPTWebImageCompletionCoordinator
	admission := NewChatGPTWebImageMemoryAdmission(8)
	defaultChatGPTWebImageMemoryAdmission = admission
	defaultChatGPTWebImageCompletionCoordinator = &chatGPTWebImageCompletionCoordinator{
		turn:         make(chan struct{}, 1),
		reservations: make(map[*ChatGPTWebImageMemoryLeaseSet]struct{}),
	}
	t.Cleanup(func() {
		defaultChatGPTWebImageMemoryAdmission = previousAdmission
		defaultChatGPTWebImageCompletionCoordinator = previousCoordinator
	})

	leases := NewChatGPTWebImageMemoryLeaseSet()
	if !leases.TryReserveCompletion(1) {
		t.Fatal("TryReserveCompletion() = false")
	}
	releaseTurn, err := leases.BeginFinalization(t.Context())
	if err != nil {
		t.Fatalf("BeginFinalization() error = %v", err)
	}
	if err = leases.Acquire(t.Context(), 2); err != nil {
		t.Fatalf("Acquire(retained) error = %v", err)
	}
	releaseBlocker, err := admission.Acquire(t.Context(), 6)
	if err != nil {
		t.Fatalf("Acquire(blocker) error = %v", err)
	}

	type transientResult struct {
		release func()
		err     error
	}
	acquired := make(chan transientResult, 1)
	go func() {
		release, errAcquire := leases.AcquireTransient(t.Context(), 6)
		acquired <- transientResult{release: release, err: errAcquire}
	}()
	waitForImageMemorySnapshot(t, admission, func(snapshot ChatGPTWebImageMemoryRuntimeSnapshot) bool {
		return snapshot.WaitingTasks == 1 && snapshot.WaitingBytes == 6 && snapshot.ProcessingBytes == 8
	})

	admission.Resize(4)
	if snapshot := admission.Snapshot(); snapshot.CapacityBytes != 4 || snapshot.WaitingBytes != 2 || snapshot.ProcessingBytes != 8 {
		t.Fatalf("shrunk credited snapshot = %#v", snapshot)
	}
	releaseBlocker()
	var outcome transientResult
	select {
	case outcome = <-acquired:
	case <-time.After(time.Second):
		t.Fatal("finalizer remained self-blocked after shrink")
	}
	if outcome.err != nil {
		t.Fatalf("AcquireTransient() error = %v", outcome.err)
	}
	leases.mu.Lock()
	retained := leases.retainedBytes
	retainedGrant := leases.retainedGrantBytes
	transient := leases.transientBytes
	transientGrant := leases.transientGrantBytes
	leases.mu.Unlock()
	if retained != 2 || retainedGrant != 1 || transient != 2 || transientGrant != 2 {
		t.Fatalf("lease accounting = retained:%d/%d transient:%d/%d", retained, retainedGrant, transient, transientGrant)
	}
	if snapshot := admission.Snapshot(); snapshot.ProcessingBytes != 4 || snapshot.WaitingTasks != 0 || snapshot.WaitingBytes != 0 {
		t.Fatalf("credited grant snapshot = %#v", snapshot)
	}

	outcome.release()
	releaseTurn()
	leases.Release()
	assertImageMemoryAdmissionEmpty(t, admission)
	admission.mu.Lock()
	used := admission.used
	admission.mu.Unlock()
	if used != 0 {
		t.Fatalf("internal used after release = %d", used)
	}
}

func installResizableImageMemoryTestState(t *testing.T, capacity int64) (*ChatGPTWebImageMemoryAdmission, *ChatGPTWebImageMemoryLeaseSet, func()) {
	t.Helper()
	previousAdmission := defaultChatGPTWebImageMemoryAdmission
	previousCoordinator := defaultChatGPTWebImageCompletionCoordinator
	admission := NewChatGPTWebImageMemoryAdmission(capacity)
	defaultChatGPTWebImageMemoryAdmission = admission
	defaultChatGPTWebImageCompletionCoordinator = &chatGPTWebImageCompletionCoordinator{
		turn:         make(chan struct{}, 1),
		reservations: make(map[*ChatGPTWebImageMemoryLeaseSet]struct{}),
	}
	t.Cleanup(func() {
		defaultChatGPTWebImageMemoryAdmission = previousAdmission
		defaultChatGPTWebImageCompletionCoordinator = previousCoordinator
	})
	leases := NewChatGPTWebImageMemoryLeaseSet()
	releaseTurn, err := leases.BeginFinalization(t.Context())
	if err != nil {
		t.Fatalf("BeginFinalization() error = %v", err)
	}
	return admission, leases, releaseTurn
}

func waitForImageMemorySnapshot(t *testing.T, admission *ChatGPTWebImageMemoryAdmission, ready func(ChatGPTWebImageMemoryRuntimeSnapshot) bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if ready(admission.Snapshot()) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("admission did not reach expected state: %#v", admission.Snapshot())
}

func assertImageMemoryAdmissionEmpty(t *testing.T, admission *ChatGPTWebImageMemoryAdmission) {
	t.Helper()
	snapshot := admission.Snapshot()
	if snapshot.ProcessingTasks != 0 || snapshot.ProcessingBytes != 0 || snapshot.WaitingTasks != 0 || snapshot.WaitingBytes != 0 {
		t.Fatalf("admission leaked: %#v", snapshot)
	}
}

func TestChatGPTWebImageCompletionReserveClampsAndFundsCompletion(t *testing.T) {
	previous := defaultChatGPTWebImageMemoryAdmission
	defaultChatGPTWebImageMemoryAdmission = NewChatGPTWebImageMemoryAdmission(4)
	t.Cleanup(func() { defaultChatGPTWebImageMemoryAdmission = previous })

	leases := NewChatGPTWebImageMemoryLeaseSet()
	if !leases.TryReserveCompletion(64) {
		t.Fatal("TryReserveCompletion() = false")
	}
	leases.mu.Lock()
	reserved := leases.completionBytes
	available := leases.completionAvailable
	leases.mu.Unlock()
	if reserved != 4 || available != 4 {
		t.Fatalf("completion accounting = reserved:%d available:%d, want 4/4", reserved, available)
	}
	if err := leases.Acquire(t.Context(), 4); err != nil {
		t.Fatalf("Acquire() could not borrow reserved completion capacity: %v", err)
	}
	if snapshot := ChatGPTWebImageMemorySnapshot(); snapshot.ProcessingBytes != 4 {
		t.Fatalf("snapshot while completion retained = %#v", snapshot)
	}
	leases.Release()
	leases.Release()
	if snapshot := ChatGPTWebImageMemorySnapshot(); snapshot.ProcessingBytes != 0 || snapshot.ProcessingTasks != 0 {
		t.Fatalf("snapshot after release = %#v", snapshot)
	}
}

func TestChatGPTWebImageCompletionReserveRestoresTransientCredit(t *testing.T) {
	previous := defaultChatGPTWebImageMemoryAdmission
	defaultChatGPTWebImageMemoryAdmission = NewChatGPTWebImageMemoryAdmission(8)
	t.Cleanup(func() { defaultChatGPTWebImageMemoryAdmission = previous })

	leases := NewChatGPTWebImageMemoryLeaseSet()
	if !leases.TryReserveCompletion(4) {
		t.Fatal("TryReserveCompletion() = false")
	}
	releaseTransient, err := leases.AcquireTransient(t.Context(), 4)
	if err != nil {
		t.Fatalf("AcquireTransient() error = %v", err)
	}
	releaseTransient()
	releaseTransient()
	if err = leases.Acquire(t.Context(), 4); err != nil {
		t.Fatalf("transient release did not restore completion credit: %v", err)
	}
	leases.Release()
	if snapshot := ChatGPTWebImageMemorySnapshot(); snapshot.ProcessingBytes != 0 {
		t.Fatalf("snapshot after release = %#v", snapshot)
	}
}

func TestChatGPTWebImageOrdinaryTransientCannotBlockFinalizationWithBorrowedReserve(t *testing.T) {
	previous := defaultChatGPTWebImageMemoryAdmission
	defaultChatGPTWebImageMemoryAdmission = NewChatGPTWebImageMemoryAdmission(4)
	t.Cleanup(func() { defaultChatGPTWebImageMemoryAdmission = previous })

	ordinary := NewChatGPTWebImageMemoryLeaseSet()
	finalizing := NewChatGPTWebImageMemoryLeaseSet()
	t.Cleanup(ordinary.Release)
	t.Cleanup(finalizing.Release)
	if !ordinary.TryReserveCompletion(1) || !finalizing.TryReserveCompletion(1) {
		t.Fatal("failed to reserve completion memory")
	}
	ordinaryContext, cancelOrdinary := context.WithTimeout(t.Context(), time.Second)
	defer cancelOrdinary()
	if _, err := ordinary.AcquireTransient(ordinaryContext, 4); !errors.Is(err, ErrChatGPTWebImageMemoryQueueFull) {
		t.Fatalf("ordinary AcquireTransient() error = %v, want queue full", err)
	}
	if snapshot := ChatGPTWebImageMemorySnapshot(); snapshot.WaitingTasks != 0 || snapshot.ImmediateRejected != 1 || snapshot.CompletionReservations != 2 {
		t.Fatalf("ordinary pressure snapshot = %#v", snapshot)
	}

	releaseTurn, err := finalizing.BeginFinalization(t.Context())
	if err != nil {
		t.Fatalf("BeginFinalization() error = %v", err)
	}
	defer releaseTurn()
	releaseTransient, err := finalizing.AcquireTransient(t.Context(), 4)
	if err != nil {
		t.Fatalf("finalizing AcquireTransient() error = %v", err)
	}
	releaseTransient()
	releaseTurn()
	ordinary.Release()
	finalizing.Release()
	if snapshot := ChatGPTWebImageMemorySnapshot(); snapshot.WaitingTasks != 0 || snapshot.ProcessingTasks != 0 || snapshot.ProcessingBytes != 0 ||
		snapshot.CompletionReservations != 0 || snapshot.FinalizationActive != 0 {
		t.Fatalf("final snapshot = %#v", snapshot)
	}
}

func TestChatGPTWebImageInputAdmissionFailsFast(t *testing.T) {
	previous := defaultChatGPTWebImageMemoryAdmission
	defaultChatGPTWebImageMemoryAdmission = NewChatGPTWebImageMemoryAdmission(4)
	t.Cleanup(func() { defaultChatGPTWebImageMemoryAdmission = previous })

	release, acquired := TryAcquireChatGPTWebImageMemory(4)
	if !acquired {
		t.Fatal("failed to fill test admission")
	}
	leases := NewChatGPTWebImageMemoryLeaseSet()
	started := time.Now()
	if leases.TryAcquireInput(1) {
		t.Fatal("TryAcquireInput() = true at capacity")
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("TryAcquireInput() waited %v", elapsed)
	}
	release()
	if snapshot := ChatGPTWebImageMemorySnapshot(); snapshot.ImmediateRejected != 1 || snapshot.ProcessingTasks != 0 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}

func TestChatGPTWebImageFinalizationAvoidsRetainedMemoryHoldAndWait(t *testing.T) {
	previous := defaultChatGPTWebImageMemoryAdmission
	defaultChatGPTWebImageMemoryAdmission = NewChatGPTWebImageMemoryAdmission(6)
	t.Cleanup(func() { defaultChatGPTWebImageMemoryAdmission = previous })

	first := NewChatGPTWebImageMemoryLeaseSet()
	second := NewChatGPTWebImageMemoryLeaseSet()
	if !first.TryReserveCompletion(1) || !second.TryReserveCompletion(1) {
		t.Fatal("failed to reserve completion memory")
	}
	firstTurn, err := first.BeginFinalization(t.Context())
	if err != nil {
		t.Fatalf("first BeginFinalization() error = %v", err)
	}
	defer firstTurn()
	type finalizationResult struct {
		release func()
		err     error
	}
	secondStarted := make(chan finalizationResult, 1)
	go func() {
		release, errBegin := second.BeginFinalization(t.Context())
		secondStarted <- finalizationResult{release: release, err: errBegin}
	}()
	deadline := time.Now().Add(time.Second)
	for ChatGPTWebImageMemorySnapshot().FinalizationWaiting == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if snapshot := ChatGPTWebImageMemorySnapshot(); snapshot.FinalizationActive != 1 || snapshot.FinalizationWaiting != 1 || snapshot.CompletionReservations != 1 {
		t.Fatalf("serialized finalization snapshot = %#v", snapshot)
	}
	bypassedBefore := ChatGPTWebImageMemorySnapshot().BypassedCompletionReservations
	third := NewChatGPTWebImageMemoryLeaseSet()
	if !third.TryReserveCompletion(1) {
		third.Release()
		t.Fatal("bounded execution was rejected during finalization")
	}
	if !third.TryReserveCompletion(1) {
		third.Release()
		t.Fatal("repeated bounded execution reservation was rejected")
	}
	if snapshot := ChatGPTWebImageMemorySnapshot(); snapshot.CompletionReservations != 1 || snapshot.BypassedCompletionReservations != bypassedBefore+1 {
		third.Release()
		t.Fatalf("completion bypass snapshot = %#v", snapshot)
	}
	if _, errThird := third.AcquireTransient(t.Context(), 6); !errors.Is(errThird, ErrChatGPTWebImageMemoryQueueFull) {
		third.Release()
		t.Fatalf("bypassed transient error = %v, want queue full", errThird)
	}
	if snapshot := ChatGPTWebImageMemorySnapshot(); snapshot.WaitingTasks != 0 {
		third.Release()
		t.Fatalf("bypassed execution joined ordinary wait queue: %#v", snapshot)
	}
	third.Release()
	canceledContext, cancel := context.WithCancel(t.Context())
	canceled := make(chan error, 1)
	go func() {
		_, errBegin := NewChatGPTWebImageMemoryLeaseSet().BeginFinalization(canceledContext)
		canceled <- errBegin
	}()
	deadline = time.Now().Add(time.Second)
	for ChatGPTWebImageMemorySnapshot().FinalizationWaiting < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if errCanceled := <-canceled; !errors.Is(errCanceled, context.Canceled) {
		t.Fatalf("canceled finalization error = %v", errCanceled)
	}
	if err := first.Acquire(t.Context(), 2); err != nil {
		t.Fatalf("first retained acquisition error = %v", err)
	}
	releaseTransient, err := first.AcquireTransient(t.Context(), 2)
	if err != nil {
		t.Fatalf("first transient acquisition error = %v", err)
	}
	releaseTransient()
	firstTurn()
	var secondTurn func()
	select {
	case result := <-secondStarted:
		if result.err != nil {
			t.Fatalf("second BeginFinalization() error = %v", result.err)
		}
		secondTurn = result.release
		defer secondTurn()
	case <-time.After(time.Second):
		t.Fatal("second finalizer did not advance after first left its critical phase")
	}
	// The first response still owns its completed payload while a slow client
	// writes it. Remaining capacity must still let the second finalizer finish.
	if err = second.Acquire(t.Context(), 2); err != nil {
		t.Fatalf("second retained acquisition error = %v", err)
	}
	releaseTransient, err = second.AcquireTransient(t.Context(), 2)
	if err != nil {
		t.Fatalf("second transient acquisition error = %v", err)
	}
	releaseTransient()
	secondTurn()
	second.Release()
	if snapshot := ChatGPTWebImageMemorySnapshot(); snapshot.FinalizationActive != 0 || snapshot.FinalizationWaiting != 0 || snapshot.ProcessingBytes != 2 {
		t.Fatalf("completed slow response blocked finalization: %#v", snapshot)
	}
	first.Release()
	if snapshot := ChatGPTWebImageMemorySnapshot(); snapshot.ProcessingTasks != 0 || snapshot.ProcessingBytes != 0 ||
		snapshot.CompletionReservations != 0 || snapshot.FinalizationActive != 0 || snapshot.FinalizationWaiting != 0 {
		t.Fatalf("finalization leaked: %#v", snapshot)
	}
}

func TestChatGPTWebImageBypassedCompletionFinishesNextTurnUnderMemoryPressure(t *testing.T) {
	previousAdmission := defaultChatGPTWebImageMemoryAdmission
	previousCoordinator := defaultChatGPTWebImageCompletionCoordinator
	defaultChatGPTWebImageMemoryAdmission = NewChatGPTWebImageMemoryAdmission(6)
	defaultChatGPTWebImageCompletionCoordinator = &chatGPTWebImageCompletionCoordinator{
		turn:         make(chan struct{}, 1),
		reservations: make(map[*ChatGPTWebImageMemoryLeaseSet]struct{}),
	}
	t.Cleanup(func() {
		defaultChatGPTWebImageMemoryAdmission = previousAdmission
		defaultChatGPTWebImageCompletionCoordinator = previousCoordinator
	})

	first := NewChatGPTWebImageMemoryLeaseSet()
	bypassed := NewChatGPTWebImageMemoryLeaseSet()
	t.Cleanup(first.Release)
	t.Cleanup(bypassed.Release)
	if !first.TryReserveCompletion(1) {
		t.Fatal("failed to reserve first completion memory")
	}
	firstTurn, err := first.BeginFinalization(t.Context())
	if err != nil {
		t.Fatalf("first BeginFinalization() error = %v", err)
	}
	defer firstTurn()
	if !bypassed.TryReserveCompletion(1) || !bypassed.TryReserveCompletion(1) {
		t.Fatal("execution was rejected while first finalizer was active")
	}
	if snapshot := ChatGPTWebImageMemorySnapshot(); snapshot.BypassedCompletionReservations != 1 || snapshot.CompletionReservations != 1 {
		t.Fatalf("bypassed reservation snapshot = %#v", snapshot)
	}
	if err = first.Acquire(t.Context(), 2); err != nil {
		t.Fatalf("first retained acquisition error = %v", err)
	}

	type turnResult struct {
		release func()
		err     error
	}
	nextTurn := make(chan turnResult, 1)
	go func() {
		release, errBegin := bypassed.BeginFinalization(t.Context())
		nextTurn <- turnResult{release: release, err: errBegin}
	}()
	deadline := time.Now().Add(time.Second)
	for ChatGPTWebImageMemorySnapshot().FinalizationWaiting == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	firstTurn()
	var bypassedTurn func()
	select {
	case result := <-nextTurn:
		if result.err != nil {
			t.Fatalf("bypassed BeginFinalization() error = %v", result.err)
		}
		bypassedTurn = result.release
		defer bypassedTurn()
	case <-time.After(time.Second):
		t.Fatal("bypassed execution did not enter the next finalization turn")
	}
	if err = bypassed.Acquire(t.Context(), 3); err != nil {
		t.Fatalf("bypassed retained acquisition error = %v", err)
	}
	type transientResult struct {
		release func()
		err     error
	}
	transient := make(chan transientResult, 1)
	go func() {
		release, errAcquire := bypassed.AcquireTransient(t.Context(), 3)
		transient <- transientResult{release: release, err: errAcquire}
	}()
	deadline = time.Now().Add(time.Second)
	for ChatGPTWebImageMemorySnapshot().WaitingTasks == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if snapshot := ChatGPTWebImageMemorySnapshot(); snapshot.WaitingTasks != 1 || snapshot.FinalizationActive != 1 {
		t.Fatalf("bypassed finalizer pressure snapshot = %#v", snapshot)
	}
	newcomer := NewChatGPTWebImageMemoryLeaseSet()
	if !newcomer.TryReserveCompletion(1) {
		newcomer.Release()
		t.Fatal("new bounded execution was rejected behind critical waiter")
	}
	if _, errNewcomer := newcomer.AcquireTransient(t.Context(), 1); !errors.Is(errNewcomer, ErrChatGPTWebImageMemoryQueueFull) {
		newcomer.Release()
		t.Fatalf("new transient error = %v, want queue full behind critical waiter", errNewcomer)
	}
	newcomer.Release()
	if snapshot := ChatGPTWebImageMemorySnapshot(); snapshot.WaitingTasks != 1 || snapshot.BypassedCompletionReservations != 2 {
		t.Fatalf("new execution preempted critical waiter: %#v", snapshot)
	}
	first.Release()
	select {
	case result := <-transient:
		if result.err != nil {
			t.Fatalf("bypassed transient acquisition error = %v", result.err)
		}
		result.release()
	case <-time.After(time.Second):
		t.Fatal("bypassed finalizer did not advance after memory release")
	}
	bypassedTurn()
	bypassed.Release()
	if snapshot := ChatGPTWebImageMemorySnapshot(); snapshot.ProcessingTasks != 0 || snapshot.ProcessingBytes != 0 ||
		snapshot.WaitingTasks != 0 || snapshot.CompletionReservations != 0 || snapshot.FinalizationActive != 0 || snapshot.FinalizationWaiting != 0 {
		t.Fatalf("bypassed finalization leaked: %#v", snapshot)
	}
}

func TestChatGPTWebImageFinalizationBypassesOnlyOrdinaryWaitQueueLimit(t *testing.T) {
	for _, test := range []struct {
		name    string
		acquire func(context.Context, *ChatGPTWebImageMemoryLeaseSet) (func(), error)
	}{
		{
			name: "retained download",
			acquire: func(ctx context.Context, leases *ChatGPTWebImageMemoryLeaseSet) (func(), error) {
				return func() {}, leases.Acquire(ctx, 1)
			},
		},
		{
			name: "transient working set",
			acquire: func(ctx context.Context, leases *ChatGPTWebImageMemoryLeaseSet) (func(), error) {
				return leases.AcquireTransient(ctx, 1)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			previous := defaultChatGPTWebImageMemoryAdmission
			admission := NewChatGPTWebImageMemoryAdmission(1)
			admission.queueLimit = 1
			defaultChatGPTWebImageMemoryAdmission = admission
			t.Cleanup(func() { defaultChatGPTWebImageMemoryAdmission = previous })

			leases := NewChatGPTWebImageMemoryLeaseSet()
			t.Cleanup(leases.Release)
			releaseTurn, err := leases.BeginFinalization(t.Context())
			if err != nil {
				t.Fatalf("BeginFinalization() error = %v", err)
			}
			t.Cleanup(releaseTurn)
			releaseActive, err := admission.Acquire(t.Context(), 1)
			if err != nil {
				t.Fatalf("active Acquire() error = %v", err)
			}
			t.Cleanup(releaseActive)

			type acquireResult struct {
				release func()
				err     error
			}
			ordinary := make(chan acquireResult, 1)
			go func() {
				release, errAcquire := admission.Acquire(t.Context(), 1)
				ordinary <- acquireResult{release: release, err: errAcquire}
			}()
			deadline := time.Now().Add(time.Second)
			for admission.Snapshot().WaitingTasks != 1 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}

			critical := make(chan acquireResult, 1)
			go func() {
				release, errAcquire := test.acquire(t.Context(), leases)
				critical <- acquireResult{release: release, err: errAcquire}
			}()
			deadline = time.Now().Add(time.Second)
			for admission.Snapshot().WaitingTasks != 2 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if snapshot := admission.Snapshot(); snapshot.WaitingTasks != 2 || snapshot.QueueLimit != 1 || snapshot.QueueRejected != 0 {
				t.Fatalf("critical waiter did not bypass ordinary queue rejection: %#v", snapshot)
			}
			if _, errOverflow := admission.Acquire(t.Context(), 1); !errors.Is(errOverflow, ErrChatGPTWebImageMemoryQueueFull) {
				t.Fatalf("ordinary overflow error = %v, want queue full", errOverflow)
			}
			if snapshot := admission.Snapshot(); snapshot.WaitingTasks != 2 || snapshot.QueueRejected != 1 {
				t.Fatalf("critical exception widened the ordinary queue: %#v", snapshot)
			}

			releaseActive()
			ordinaryResult := <-ordinary
			if ordinaryResult.err != nil {
				t.Fatalf("ordinary waiter error = %v", ordinaryResult.err)
			}
			t.Cleanup(ordinaryResult.release)
			select {
			case result := <-critical:
				if result.release != nil {
					result.release()
				}
				t.Fatalf("critical waiter bypassed weighted FIFO early: %v", result.err)
			default:
			}
			ordinaryResult.release()
			var criticalResult acquireResult
			select {
			case criticalResult = <-critical:
			case <-time.After(time.Second):
				t.Fatal("critical finalization did not resume")
			}
			if criticalResult.err != nil {
				t.Fatalf("critical finalization error = %v", criticalResult.err)
			}
			if criticalResult.release != nil {
				criticalResult.release()
			}
			releaseTurn()
			leases.Release()
			if snapshot := admission.Snapshot(); snapshot.WaitingTasks != 0 || snapshot.ProcessingTasks != 0 || snapshot.ProcessingBytes != 0 {
				t.Fatalf("critical finalization leaked: %#v", snapshot)
			}
		})
	}
}

func TestChatGPTWebImageFinalizationRejectsImpossibleOwnWorkingSetWithoutWaiting(t *testing.T) {
	previous := defaultChatGPTWebImageMemoryAdmission
	defaultChatGPTWebImageMemoryAdmission = NewChatGPTWebImageMemoryAdmission(4)
	t.Cleanup(func() { defaultChatGPTWebImageMemoryAdmission = previous })

	leases := NewChatGPTWebImageMemoryLeaseSet()
	t.Cleanup(leases.Release)
	if !leases.TryReserveCompletion(1) {
		t.Fatal("TryReserveCompletion() = false")
	}
	releaseTurn, err := leases.BeginFinalization(t.Context())
	if err != nil {
		t.Fatalf("BeginFinalization() error = %v", err)
	}
	defer releaseTurn()
	if err = leases.Acquire(t.Context(), 4); err != nil {
		t.Fatalf("Acquire(retained capacity) error = %v", err)
	}
	started := time.Now()
	if _, err = leases.AcquireTransient(t.Context(), 1); !errors.Is(err, ErrChatGPTWebImageMemoryWorkingSetTooLarge) {
		t.Fatalf("AcquireTransient() error = %v, want working set too large", err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("impossible transient acquisition waited %v", elapsed)
	}
	if snapshot := ChatGPTWebImageMemorySnapshot(); snapshot.WaitingTasks != 0 || snapshot.ProcessingBytes != 4 {
		t.Fatalf("impossible working set changed admission state: %#v", snapshot)
	}
}
