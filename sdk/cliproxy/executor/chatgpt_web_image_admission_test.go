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

func TestChatGPTWebRuntimeAdmissionsStartWithCompatibleDefaults(t *testing.T) {
	ConfigureChatGPTWebImageAdmissions(64, 64, 8)
	ConfigureChatGPTWebImageRuntimeAdmissions(64, 64, 1)
	t.Cleanup(func() {
		ConfigureChatGPTWebImageAdmissions(64, 64, 8)
		ConfigureChatGPTWebImageRuntimeAdmissions(64, 64, 1)
	})

	poll := ChatGPTWebImagePollAdmissionSnapshot()
	memoryFinalizer := ChatGPTWebImageMemoryFinalizerAdmissionSnapshot()
	if poll.Limit != 64 || poll.QueueLimit != 128 {
		t.Fatalf("poll startup admission = %#v", poll)
	}
	if memoryFinalizer.Limit != 1 || memoryFinalizer.QueueLimit != 64 {
		t.Fatalf("memory finalizer startup admission = %#v", memoryFinalizer)
	}
}

func TestChatGPTWebRuntimeAdmissionsHotResizeWithoutRevokingHolders(t *testing.T) {
	ConfigureChatGPTWebImageAdmissions(4, 4, 2)
	ConfigureChatGPTWebImageRuntimeAdmissions(4, 2, 2)
	t.Cleanup(func() {
		ConfigureChatGPTWebImageAdmissions(64, 64, 8)
		ConfigureChatGPTWebImageRuntimeAdmissions(64, 64, 1)
	})

	releasePollA, err := AcquireChatGPTWebImagePoll(t.Context())
	if err != nil {
		t.Fatalf("AcquireChatGPTWebImagePoll(A) error = %v", err)
	}
	releasePollB, err := AcquireChatGPTWebImagePoll(t.Context())
	if err != nil {
		releasePollA()
		t.Fatalf("AcquireChatGPTWebImagePoll(B) error = %v", err)
	}
	ConfigureChatGPTWebImageRuntimeAdmissions(4, 1, 3)
	if snapshot := ChatGPTWebImagePollAdmissionSnapshot(); snapshot.Limit != 1 || snapshot.Active != 2 || !snapshot.Shrinking {
		t.Fatalf("poll admission after shrink = %#v", snapshot)
	}

	pollResult := make(chan struct {
		release func()
		err     error
	}, 1)
	go func() {
		release, errAcquire := AcquireChatGPTWebImagePoll(t.Context())
		pollResult <- struct {
			release func()
			err     error
		}{release: release, err: errAcquire}
	}()
	waitForImageAdmissionSnapshot(t, defaultChatGPTWebImagePollAdmission, func(snapshot ImageExecutionAdmissionSnapshot) bool {
		return snapshot.Queued == 1
	})
	releasePollA()
	select {
	case result := <-pollResult:
		if result.release != nil {
			result.release()
		}
		t.Fatalf("poll waiter granted before active reached the shrunken limit: %v", result.err)
	default:
	}
	releasePollB()
	result := <-pollResult
	if result.err != nil {
		t.Fatalf("poll waiter after shrink error = %v", result.err)
	}
	result.release()

	if snapshot := ChatGPTWebImageMemoryFinalizerAdmissionSnapshot(); snapshot.Limit != 3 || snapshot.Active != 0 || snapshot.QueueLimit != 4 {
		t.Fatalf("memory finalizer expansion = %#v", snapshot)
	}
	releases := make([]func(), 0, 3)
	for range 3 {
		release, errAcquire := AcquireChatGPTWebImageMemoryFinalizer(t.Context())
		if errAcquire != nil {
			t.Fatalf("AcquireChatGPTWebImageMemoryFinalizer() error = %v", errAcquire)
		}
		releases = append(releases, release)
	}
	for _, release := range releases {
		release()
	}
	if snapshot := ChatGPTWebImageMemoryFinalizerAdmissionSnapshot(); snapshot.Active != 0 || snapshot.Queued != 0 || snapshot.PeakActive < 3 {
		t.Fatalf("memory finalizer final snapshot = %#v", snapshot)
	}
}

func TestChatGPTWebRuntimeAdmissionsUseLatestExecutorConfiguration(t *testing.T) {
	ConfigureChatGPTWebImageAdmissions(2, 2, 1)
	ConfigureChatGPTWebImageRuntimeAdmissions(2, 3, 2)
	ConfigureChatGPTWebImageAdmissions(7, 5, 4)
	ConfigureChatGPTWebImageRuntimeAdmissions(7, 11, 6)
	t.Cleanup(func() {
		ConfigureChatGPTWebImageAdmissions(64, 64, 8)
		ConfigureChatGPTWebImageRuntimeAdmissions(64, 64, 1)
	})
	if snapshot := ChatGPTWebImagePollAdmissionSnapshot(); snapshot.Limit != 11 || snapshot.QueueLimit != 14 {
		t.Fatalf("latest poll configuration = %#v", snapshot)
	}
	if snapshot := ChatGPTWebImageMemoryFinalizerAdmissionSnapshot(); snapshot.Limit != 6 || snapshot.QueueLimit != 7 {
		t.Fatalf("latest memory finalizer configuration = %#v", snapshot)
	}
}

func TestImageExecutionAdmissionAcceptsTwoThousandInFlightAndRemainsBounded(t *testing.T) {
	admission := newImageExecutionAdmission()
	admission.configure(2000, 2000)
	releases := make([]func(), 0, 2000)
	for index := 0; index < 2000; index++ {
		release, err := admission.acquire(t.Context(), 0)
		if err != nil {
			t.Fatalf("acquire %d of 2000: %v", index+1, err)
		}
		releases = append(releases, release)
	}
	if snapshot := admission.snapshot(); snapshot.Limit != 2000 || snapshot.QueueLimit != 2000 || snapshot.Active != 2000 || snapshot.PeakActive != 2000 {
		t.Fatalf("2000-task admission snapshot = %#v", snapshot)
	}
	if release, err := admission.acquire(t.Context(), 0); err == nil {
		release()
		t.Fatal("2001st immediate acquisition exceeded configured lifecycle bound")
	}
	for _, release := range releases {
		release()
	}
	if snapshot := admission.snapshot(); snapshot.Active != 0 || snapshot.Queued != 0 || snapshot.ImmediateRejects != 1 {
		t.Fatalf("released 2000-task admission snapshot = %#v", snapshot)
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

func TestImageRequestPhaseRollingSamplerReturnsWindowDeltas(t *testing.T) {
	sampler := newImageRequestPhaseRollingSampler(time.Minute, 4)
	started := time.Unix(100, 0)
	first := map[string]ImageRequestPhaseMetricSnapshot{
		ImagePhasePollRequest: {Count: 10, TotalNanos: 100, UpTo1Millisecond: 4, Over1To10Milliseconds: 6},
	}
	if snapshot := sampler.observe(started, first); snapshot.Available || snapshot.HistorySamples != 1 {
		t.Fatalf("first snapshot = %#v", snapshot)
	}
	second := map[string]ImageRequestPhaseMetricSnapshot{
		ImagePhasePollRequest: {Count: 14, TotalNanos: 300, UpTo1Millisecond: 5, Over1To10Milliseconds: 9},
	}
	snapshot := sampler.observe(started.Add(5*time.Second), second)
	metric := snapshot.Metrics[ImagePhasePollRequest]
	if !snapshot.Available || snapshot.SampleSeconds != 5 || snapshot.HistorySamples != 2 ||
		metric.Count != 4 || metric.TotalNanos != 200 || metric.AverageNanos != 50 ||
		metric.UpTo1Millisecond != 1 || metric.Over1To10Milliseconds != 3 {
		t.Fatalf("snapshot = %#v, metric = %#v", snapshot, metric)
	}
}

func TestImageRequestPhaseRollingSamplerKeepsBoundedWindow(t *testing.T) {
	sampler := newImageRequestPhaseRollingSampler(10*time.Second, 3)
	started := time.Unix(100, 0)
	for index := range 5 {
		sampler.observe(started.Add(time.Duration(index)*5*time.Second), map[string]ImageRequestPhaseMetricSnapshot{
			ImagePhasePollRequest: {Count: uint64(index)},
		})
	}
	snapshot := sampler.observe(started.Add(25*time.Second), map[string]ImageRequestPhaseMetricSnapshot{
		ImagePhasePollRequest: {Count: 5},
	})
	if !snapshot.Available || snapshot.HistorySamples > 3 || snapshot.SampleSeconds > 10 ||
		snapshot.Metrics[ImagePhasePollRequest].Count != 2 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}

func TestImageRequestPhaseRollingSamplerRejectsStalePair(t *testing.T) {
	sampler := newImageRequestPhaseRollingSampler(time.Minute, 4)
	started := time.Unix(100, 0)
	_ = sampler.observe(started, map[string]ImageRequestPhaseMetricSnapshot{
		ImagePhasePollRequest: {Count: 1},
	})
	snapshot := sampler.observe(started.Add(2*time.Minute), map[string]ImageRequestPhaseMetricSnapshot{
		ImagePhasePollRequest: {Count: 2},
	})
	if snapshot.Available || snapshot.HistorySamples != 1 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}

func TestImageRequestPhaseRollingSamplerResetsOnCounterRegression(t *testing.T) {
	sampler := newImageRequestPhaseRollingSampler(time.Minute, 4)
	started := time.Unix(100, 0)
	_ = sampler.observe(started, map[string]ImageRequestPhaseMetricSnapshot{
		ImagePhasePollRequest: {Count: 2, TotalNanos: 10},
	})
	snapshot := sampler.observe(started.Add(time.Second), map[string]ImageRequestPhaseMetricSnapshot{
		ImagePhasePollRequest: {Count: 1, TotalNanos: 5},
	})
	if snapshot.Available || snapshot.HistorySamples != 1 {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}
