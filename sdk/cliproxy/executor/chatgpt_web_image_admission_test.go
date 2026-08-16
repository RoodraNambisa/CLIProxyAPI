package executor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"
)

func waitForImageAdmissionSnapshot(t *testing.T, admission *imageExecutionAdmission, predicate func(ImageExecutionAdmissionSnapshot) bool) ImageExecutionAdmissionSnapshot {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		snapshot := admission.snapshot()
		if predicate(snapshot) {
			return snapshot
		}
		time.Sleep(time.Millisecond)
	}
	snapshot := admission.snapshot()
	t.Fatalf("admission condition not reached: %#v", snapshot)
	return snapshot
}

func TestImageExecutionCapacityErrorUsesStableSafeContract(t *testing.T) {
	err := NewImageExecutionCapacityError("internal_queue_detail")
	var payload map[string]any
	if errDecode := json.Unmarshal([]byte(err.Error()), &payload); errDecode != nil {
		t.Fatalf("json.Unmarshal() error = %v", errDecode)
	}
	errorPayload, _ := payload["error"].(map[string]any)
	if errorPayload["code"] != "image_generation_capacity" || errorPayload["failure_stage"] != "admission" {
		t.Fatalf("error payload = %#v", errorPayload)
	}
	if _, exposed := errorPayload["reason"]; exposed {
		t.Fatalf("error payload exposed internal reason: %#v", errorPayload)
	}
	statusErr, ok := err.(interface{ StatusCode() int })
	if !ok || statusErr.StatusCode() != http.StatusServiceUnavailable {
		t.Fatalf("status contract = (%T, %v)", err, ok)
	}
	headerErr, ok := err.(interface{ Headers() http.Header })
	if !ok || headerErr.Headers().Get("Retry-After") != "1" {
		t.Fatalf("Retry-After contract = (%T, %v)", err, ok)
	}
	if !IsImageExecutionCapacityError(err) {
		t.Fatalf("IsImageExecutionCapacityError(%T) = false", err)
	}
}

func TestImageExecutionAdmissionFIFOExpansionAndDoubleRelease(t *testing.T) {
	admission := newImageExecutionAdmission()
	admission.configure(1, 2)
	releaseFirst, err := admission.acquire(t.Context(), time.Second)
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}

	type result struct {
		index   int
		release func()
		err     error
	}
	results := make(chan result, 2)
	for index := 1; index <= 2; index++ {
		index := index
		go func() {
			release, errAcquire := admission.acquire(t.Context(), time.Second)
			results <- result{index: index, release: release, err: errAcquire}
		}()
		waitForImageAdmissionSnapshot(t, admission, func(snapshot ImageExecutionAdmissionSnapshot) bool {
			return snapshot.Queued == index
		})
	}

	admission.configure(2, 2)
	firstGranted := <-results
	if firstGranted.err != nil || firstGranted.index != 1 {
		t.Fatalf("first grant = %#v, want waiter 1", firstGranted)
	}
	if snapshot := admission.snapshot(); snapshot.Active != 2 || snapshot.Queued != 1 {
		t.Fatalf("snapshot after first expansion = %#v", snapshot)
	}
	admission.configure(3, 2)
	secondGranted := <-results
	if secondGranted.err != nil || secondGranted.index != 2 {
		t.Fatalf("second grant = %#v, want waiter 2", secondGranted)
	}
	firstGranted.release()
	firstGranted.release()
	secondGranted.release()
	releaseFirst()
	releaseFirst()
	if snapshot := admission.snapshot(); snapshot.Active != 0 || snapshot.Queued != 0 || snapshot.PeakActive != 3 {
		t.Fatalf("final snapshot = %#v", snapshot)
	}
}

func TestImageExecutionAdmissionTimeoutCancellationAndHotLowering(t *testing.T) {
	admission := newImageExecutionAdmission()
	admission.configure(2, 2)
	releaseA, _ := admission.acquire(t.Context(), 0)
	releaseB, _ := admission.acquire(t.Context(), 0)
	admission.configure(1, 2)
	if _, err := admission.acquire(t.Context(), 0); !IsImageExecutionCapacityError(err) {
		t.Fatalf("acquire after lowering error = %T %v", err, err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	canceled := make(chan error, 1)
	go func() {
		_, err := admission.acquire(ctx, time.Second)
		canceled <- err
	}()
	waitForImageAdmissionSnapshot(t, admission, func(snapshot ImageExecutionAdmissionSnapshot) bool { return snapshot.Queued == 1 })
	cancel()
	if err := <-canceled; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled error = %v", err)
	}
	if _, err := admission.acquire(t.Context(), time.Millisecond); !IsImageExecutionCapacityError(err) {
		t.Fatalf("timeout error = %T %v", err, err)
	}
	releaseA()
	if snapshot := admission.snapshot(); snapshot.Active != 1 {
		t.Fatalf("active after first release = %#v", snapshot)
	}
	releaseB()
	release, err := admission.acquire(t.Context(), 0)
	if err != nil {
		t.Fatalf("acquire after drain: %v", err)
	}
	release()
	snapshot := admission.snapshot()
	if snapshot.Active != 0 || snapshot.Canceled != 1 || snapshot.TimedOut != 1 || snapshot.ImmediateRejects != 1 {
		t.Fatalf("final snapshot = %#v", snapshot)
	}
}

func TestImageExecutionAdmissionBoundsBurstBehindLongTasks(t *testing.T) {
	admission := newImageExecutionAdmission()
	admission.configure(2, 4)
	releaseA, errA := admission.acquire(t.Context(), 0)
	releaseB, errB := admission.acquire(t.Context(), 0)
	if errA != nil || errB != nil {
		t.Fatalf("fill admission = %v / %v", errA, errB)
	}

	waitResults := make(chan error, 4)
	for queued := 1; queued <= 4; queued++ {
		go func() {
			_, errAcquire := admission.acquire(t.Context(), 50*time.Millisecond)
			waitResults <- errAcquire
		}()
		waitForImageAdmissionSnapshot(t, admission, func(snapshot ImageExecutionAdmissionSnapshot) bool {
			return snapshot.Queued == queued
		})
	}
	for attempt := 0; attempt < 10; attempt++ {
		started := time.Now()
		if _, err := admission.acquire(t.Context(), time.Second); !IsImageExecutionCapacityError(err) {
			t.Fatalf("overflow attempt %d error = %T %v", attempt, err, err)
		}
		if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
			t.Fatalf("overflow attempt %d waited %v", attempt, elapsed)
		}
	}
	for attempt := 0; attempt < 4; attempt++ {
		if err := <-waitResults; !IsImageExecutionCapacityError(err) {
			t.Fatalf("queued attempt %d error = %T %v", attempt, err, err)
		}
	}
	if snapshot := admission.snapshot(); snapshot.Active != 2 || snapshot.Queued != 0 || snapshot.Admitted != 2 ||
		snapshot.QueueRejects != 10 || snapshot.TimedOut != 4 {
		t.Fatalf("saturated admission snapshot = %#v", snapshot)
	}
	releaseA()
	releaseB()
	if snapshot := admission.snapshot(); snapshot.Active != 0 || snapshot.Queued != 0 {
		t.Fatalf("released admission snapshot = %#v", snapshot)
	}
}

func TestImageExecutionAdmissionGrantCancellationRaceDoesNotLeak(t *testing.T) {
	for iteration := 0; iteration < 100; iteration++ {
		admission := newImageExecutionAdmission()
		admission.configure(1, 1)
		releaseActive, err := admission.acquire(t.Context(), 0)
		if err != nil {
			t.Fatalf("iteration %d active acquire: %v", iteration, err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan struct {
			release func()
			err     error
		}, 1)
		go func() {
			release, errAcquire := admission.acquire(ctx, time.Second)
			result <- struct {
				release func()
				err     error
			}{release: release, err: errAcquire}
		}()
		waitForImageAdmissionSnapshot(t, admission, func(snapshot ImageExecutionAdmissionSnapshot) bool {
			return snapshot.Queued == 1
		})
		start := make(chan struct{})
		var race sync.WaitGroup
		race.Add(2)
		go func() {
			defer race.Done()
			<-start
			releaseActive()
		}()
		go func() {
			defer race.Done()
			<-start
			cancel()
		}()
		close(start)
		race.Wait()
		outcome := <-result
		if outcome.err == nil && outcome.release != nil {
			outcome.release()
		} else if !errors.Is(outcome.err, context.Canceled) {
			t.Fatalf("iteration %d outcome error = %v", iteration, outcome.err)
		}
		if snapshot := admission.snapshot(); snapshot.Active != 0 || snapshot.Queued != 0 {
			t.Fatalf("iteration %d leaked admission: %#v", iteration, snapshot)
		}
	}
}

func TestChatGPTWebFinalizerWaitsWhenExecutionQueueDisabled(t *testing.T) {
	ConfigureChatGPTWebImageAdmissions(2, 0, 1)
	t.Cleanup(func() { ConfigureChatGPTWebImageAdmissions(64, 64, 8) })
	first, err := AcquireChatGPTWebImageFinalizer(t.Context())
	if err != nil {
		t.Fatalf("first finalizer: %v", err)
	}
	secondResult := make(chan struct {
		release func()
		err     error
	}, 1)
	go func() {
		release, errAcquire := AcquireChatGPTWebImageFinalizer(t.Context())
		secondResult <- struct {
			release func()
			err     error
		}{release: release, err: errAcquire}
	}()
	waitForImageAdmissionSnapshot(t, defaultChatGPTWebImageFinalizerAdmission, func(snapshot ImageExecutionAdmissionSnapshot) bool {
		return snapshot.Active == 1 && snapshot.Queued == 1 && snapshot.QueueLimit == 2
	})
	first()
	second := <-secondResult
	if second.err != nil {
		t.Fatalf("second finalizer: %v", second.err)
	}
	second.release()
	if snapshot := ChatGPTWebImageFinalizerAdmissionSnapshot(); snapshot.Active != 0 || snapshot.Queued != 0 {
		t.Fatalf("finalizer snapshot = %#v", snapshot)
	}
}

func TestImageExecutionAdmissionBoundsFifteenHundredRequests(t *testing.T) {
	const requests = 1500
	admission := newImageExecutionAdmission()
	admission.configure(32, 64)
	start := make(chan struct{})
	releaseWorkers := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(requests)
	for range requests {
		go func() {
			defer wait.Done()
			<-start
			release, err := admission.acquire(t.Context(), 100*time.Millisecond)
			if err != nil {
				return
			}
			<-releaseWorkers
			release()
		}()
	}
	close(start)
	waitForImageAdmissionSnapshot(t, admission, func(snapshot ImageExecutionAdmissionSnapshot) bool {
		return snapshot.Active == 32 && snapshot.Queued == 64
	})
	close(releaseWorkers)
	wait.Wait()
	snapshot := admission.snapshot()
	if snapshot.Active != 0 || snapshot.Queued != 0 || snapshot.PeakActive > 32 || snapshot.PeakQueued > 64 || snapshot.QueueRejects == 0 {
		t.Fatalf("bounded snapshot = %#v", snapshot)
	}
}

func TestImageRequestPhaseSnapshotUsesNamedBuckets(t *testing.T) {
	observer := newImageRequestPhaseObserver()
	observer.ObserveRequestPhase(ImagePhaseInputAdmission, 5*time.Millisecond)
	metric := observer.metrics[ImagePhaseInputAdmission]
	if metric == nil {
		t.Fatal("input admission metric missing")
	}
	value := ImageRequestPhaseMetricSnapshot{
		Count:                 metric.count.Load(),
		Over1To10Milliseconds: metric.buckets[1].Load(),
	}
	if value.Count != 1 || value.Over1To10Milliseconds != 1 {
		t.Fatalf("metric = %#v", value)
	}
}
