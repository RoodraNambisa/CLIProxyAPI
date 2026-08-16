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
	if third := NewChatGPTWebImageMemoryLeaseSet(); third.TryReserveCompletion(1) {
		third.Release()
		t.Fatal("new completion reservation was admitted during finalization")
	}
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
