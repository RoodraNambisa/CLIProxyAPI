package proxypool

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCheckAdmissionBoundsConcurrencyAndResizes(t *testing.T) {
	admission := newCheckAdmission(2)
	releases := make(chan func(), 4)
	var acquired atomic.Int64
	var wait sync.WaitGroup
	for range 4 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			release, errAcquire := admission.acquire(t.Context())
			if errAcquire != nil {
				t.Errorf("acquire() error = %v", errAcquire)
				return
			}
			acquired.Add(1)
			releases <- release
		}()
	}
	waitForCondition(t, time.Second, func() bool {
		snapshot := admission.snapshot()
		return snapshot.Active == 2 && snapshot.Queued == 2
	})
	admission.resize(3)
	waitForCondition(t, time.Second, func() bool { return admission.snapshot().Active == 3 })
	if snapshot := admission.snapshot(); snapshot.PeakActive != 3 || snapshot.Limit != 3 {
		t.Fatalf("expanded snapshot = %+v", snapshot)
	}
	admission.resize(1)
	for range 3 {
		(<-releases)()
	}
	waitForCondition(t, time.Second, func() bool {
		snapshot := admission.snapshot()
		return snapshot.Active == 1 && snapshot.Queued == 0
	})
	(<-releases)()
	wait.Wait()
	if snapshot := admission.snapshot(); snapshot.Active != 0 || snapshot.Queued != 0 || acquired.Load() != 4 {
		t.Fatalf("final snapshot/acquired = %+v/%d", snapshot, acquired.Load())
	}
}

func TestCheckAdmissionCancellationDoesNotLeakSlot(t *testing.T) {
	admission := newCheckAdmission(1)
	release, errAcquire := admission.acquire(t.Context())
	if errAcquire != nil {
		t.Fatalf("first acquire() error = %v", errAcquire)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, errWait := admission.acquire(ctx)
		done <- errWait
	}()
	waitForCondition(t, time.Second, func() bool { return admission.snapshot().Queued == 1 })
	cancel()
	if errWait := <-done; errWait == nil {
		t.Fatal("canceled acquire() error = nil")
	}
	release()
	release()
	if snapshot := admission.snapshot(); snapshot.Active != 0 || snapshot.Queued != 0 || snapshot.Canceled != 1 {
		t.Fatalf("snapshot after cancellation = %+v", snapshot)
	}
}

func TestCheckAdmissionRecordsCumulativeOutcomes(t *testing.T) {
	admission := newCheckAdmission(1)
	admission.recordResult(true)
	admission.recordResult(false)
	snapshot := admission.snapshot()
	if snapshot.Completed != 2 || snapshot.Succeeded != 1 || snapshot.Failed != 1 {
		t.Fatalf("outcome snapshot = %+v", snapshot)
	}
}

func TestRequestPathProxySelectionDoesNotWaitForHealthAdmission(t *testing.T) {
	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), proxyPoolTestConfig("3334"))
	manager.check = successfulTrace
	release, errAcquire := manager.admission.acquire(t.Context())
	if errAcquire != nil {
		t.Fatalf("acquire() error = %v", errAcquire)
	}
	defer release()

	done := make(chan error, 1)
	go func() {
		_, errResolve := manager.Resolve(t.Context(), proxyPoolTestAuth("request-path"))
		done <- errResolve
	}()
	select {
	case errResolve := <-done:
		if errResolve != nil {
			t.Fatalf("Resolve() error = %v", errResolve)
		}
	case <-time.After(time.Second):
		t.Fatal("request-path proxy selection waited for the scheduled-check admission")
	}
}

func waitForCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not reached before timeout")
}
