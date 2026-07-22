package chatgptweb

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fastschema/qjs"
	wazerosys "github.com/tetratelabs/wazero/sys"
)

const sentinelRuntimeTestSDK = `var SentinelSDK=function(t){
const P={};
async function _n(){return "sdk-turnstile-token"}
async function Nt(){return "sdk-snapshot-token"}
function Et(){globalThis.__collector_started=(globalThis.__collector_started||0)+1;return Promise.resolve()}
function D(t,n){globalThis.__bound_challenge=t;globalThis.__bound_requirements=n}
async function we(){}
async function ye(){}
return t.init=we,t.sessionObserverToken=async function(t){return null},t.token=ye,t
}({});`

const sentinelRuntimeStatefulTestSDK = `var SentinelSDK=function(t){
const P={};
async function _n(){const previous=JSON.stringify.__sentinel_secret||"clean";JSON.stringify.__sentinel_secret="leaked";globalThis.__pending_sentinel_promise=new Promise(()=>{});return previous}
async function Nt(){return "sdk-snapshot-token"}
function Et(){return Promise.resolve()}
function D(){}
async function we(){}
async function ye(){}
return t.init=we,t.sessionObserverToken=async function(t){return null},t.token=ye,t
}({});`

const sentinelRuntimeFailingTurnstileSDK = `var SentinelSDK=function(t){
const P={};
async function _n(){throw new Error("turnstile failed")}
async function Nt(){return "sdk-snapshot-token"}
function Et(){return Promise.resolve()}
function D(){}
async function we(){}
async function ye(){}
return t.init=we,t.sessionObserverToken=async function(t){return null},t.token=ye,t
}({});`

const sentinelRuntimeBlockingTurnstileSDK = `var SentinelSDK=function(t){
const P={};
async function _n(){for(;;){}}
async function Nt(){return "sdk-snapshot-token"}
function Et(){return Promise.resolve()}
function D(){}
async function we(){}
async function ye(){}
return t.init=we,t.sessionObserverToken=async function(t){return null},t.token=ye,t
}({});`

type sentinelRuntimeRetryAfterError struct {
	delay time.Duration
}

func (err sentinelRuntimeRetryAfterError) Error() string { return "temporarily unavailable" }

func (err sentinelRuntimeRetryAfterError) RetryAfter() *time.Duration { return &err.delay }

func TestSentinelRuntimeManagerIsLazy(t *testing.T) {
	manager := newSentinelRuntimeTestManagerWithConfig(SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 1, CacheVersions: 3})
	defer manager.Close()
	snapshot := manager.Snapshot()
	if snapshot.Initialized || snapshot.Busy != 0 || snapshot.Queued != 0 || snapshot.SourceCacheEntries != 0 || snapshot.BytecodeCacheEntries != 0 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	if manager.activeQJS.Load() != 0 {
		t.Fatalf("active QJS runtimes = %d", manager.activeQJS.Load())
	}
}

func TestSentinelRuntimeManagerClampsDirectRuntimeLimits(t *testing.T) {
	manager := newSentinelRuntimeTestManagerWithConfig(SentinelRuntimeConfig{
		Enabled:       true,
		Workers:       sentinelSDKMaxWorkers + 100,
		QueueSize:     sentinelSDKMaxQueueSize + 100,
		CacheVersions: sentinelSDKMaxCacheVersions + 100,
	})
	defer manager.Close()
	snapshot := manager.Snapshot()
	if snapshot.WorkerLimit != sentinelSDKMaxWorkers || snapshot.SDKWorkers != sentinelSDKMaxWorkers {
		t.Fatalf("worker limits = %+v", snapshot)
	}
	if snapshot.SDKQueueSize != sentinelSDKMaxQueueSize || snapshot.SDKCacheVersions != sentinelSDKMaxCacheVersions {
		t.Fatalf("runtime limits = %+v", snapshot)
	}

	manager.UpdateConfig(SentinelRuntimeConfig{Enabled: true, Workers: -1, QueueSize: -1, CacheVersions: -1})
	snapshot = manager.Snapshot()
	if snapshot.SDKWorkers != 0 || snapshot.SDKQueueSize != 0 || snapshot.SDKCacheVersions != 3 {
		t.Fatalf("normalized runtime limits = %+v", snapshot)
	}
}

func TestSentinelRuntimeManagerClosesIdleWorker(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	lease, err := manager.acquire(t.Context(), sentinelPriorityFallback)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = lease.runtimeWorker(t.Context()); err != nil {
		t.Fatal(err)
	}
	lease.release()
	manager.Close()
	if manager.activeQJS.Load() != 0 {
		t.Fatalf("active QJS runtimes = %d, last error = %q", manager.activeQJS.Load(), manager.Snapshot().LastError)
	}
}

func TestNormalizeSentinelHashPreservesBase64Case(t *testing.T) {
	digest := sha256.Sum256([]byte("Sentinel SDK"))
	encoded := "sha256-" + base64.StdEncoding.EncodeToString(digest[:])
	if got := normalizeSentinelHash(encoded); got != fmt.Sprintf("%x", digest) {
		t.Fatalf("normalizeSentinelHash() = %q", got)
	}
}

func TestSentinelRuntimeManagerQueueIsHardBounded(t *testing.T) {
	manager := newSentinelRuntimeTestManagerWithConfig(SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 1, CacheVersions: 3})
	defer manager.Close()
	first, err := manager.acquire(t.Context(), sentinelPriorityFallback)
	if err != nil {
		t.Fatal(err)
	}
	secondResult := make(chan *sentinelRuntimeLease, 1)
	secondError := make(chan error, 1)
	go func() {
		lease, acquireErr := manager.acquire(context.Background(), sentinelPriorityFallback)
		if acquireErr != nil {
			secondError <- acquireErr
			return
		}
		secondResult <- lease
	}()
	waitForSentinelRuntimeState(t, manager, func(snapshot SentinelRuntimeSnapshot) bool { return snapshot.Queued == 1 })
	_, err = manager.acquire(t.Context(), sentinelPriorityFallback)
	var runtimeErr *SentinelRuntimeError
	if !errors.As(err, &runtimeErr) || runtimeErr.Code != "sentinel_sdk_busy" || runtimeErr.RetryAfter != time.Second {
		t.Fatalf("third acquire error = %#v", err)
	}
	first.release()
	select {
	case lease := <-secondResult:
		lease.release()
	case err = <-secondError:
		t.Fatalf("second acquire error = %v", err)
	case <-time.After(time.Second):
		t.Fatal("queued acquire was not granted")
	}
}

func TestSentinelRuntimeObserverAdmissionIsGloballyBounded(t *testing.T) {
	manager := newSentinelRuntimeTestManagerWithConfig(SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 1, CacheVersions: 3})
	defer manager.Close()
	_, request := sentinelRuntimeTestRequests(t, true)
	started := make(chan struct{})
	release := make(chan struct{})
	var fetches atomic.Int64
	request.Fetcher = func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
		if fetches.Add(1) == 1 {
			close(started)
		}
		<-release
		return []byte(sentinelRuntimeTestSDK), "application/javascript", target, nil
	}
	first, err := manager.BeginObserver(t.Context(), request)
	if err != nil || first == nil {
		t.Fatalf("first BeginObserver() = %#v, %v", first, err)
	}
	<-started
	second, err := manager.BeginObserver(t.Context(), request)
	if err != nil || second == nil {
		first.Close()
		t.Fatalf("second BeginObserver() = %#v, %v", second, err)
	}
	waitForSentinelRuntimeSourcePending(t, manager, 1)
	waitForSentinelRuntimeSourceWaiters(t, manager, 1)
	waitForSentinelRuntimeState(t, manager, func(snapshot SentinelRuntimeSnapshot) bool { return snapshot.Queued == 1 })
	third, err := manager.BeginObserver(t.Context(), request)
	var runtimeErr *SentinelRuntimeError
	if third != nil || !errors.As(err, &runtimeErr) || runtimeErr.Code != "sentinel_sdk_busy" {
		first.Close()
		second.Close()
		t.Fatalf("third BeginObserver() = %#v, %#v", third, err)
	}
	otherRequest := request
	otherRequest.TransportKey = "other-proxy"
	other, err := manager.BeginObserver(t.Context(), otherRequest)
	if other != nil || !errors.As(err, &runtimeErr) || runtimeErr.Code != "sentinel_sdk_busy" {
		first.Close()
		second.Close()
		t.Fatalf("other BeginObserver() = %#v, %#v", other, err)
	}
	for index := 0; index < 1000; index++ {
		overflow, overflowErr := manager.BeginObserver(t.Context(), request)
		if overflow != nil || !errors.As(overflowErr, &runtimeErr) || runtimeErr.Code != "sentinel_sdk_busy" {
			first.Close()
			second.Close()
			t.Fatalf("overflow BeginObserver(%d) = %#v, %#v", index, overflow, overflowErr)
		}
	}
	manager.mu.Lock()
	observerCount := len(manager.observers)
	manager.mu.Unlock()
	snapshot := manager.Snapshot()
	if observerCount != 2 || snapshot.Busy != 1 || snapshot.Queued != 1 {
		first.Close()
		second.Close()
		t.Fatalf("Observer admission state = observers:%d snapshot:%+v", observerCount, snapshot)
	}
	if fetches.Load() != 1 {
		t.Fatalf("unique SDK fetches before admission = %d", fetches.Load())
	}
	close(release)
	first.Close()
	second.Close()
	waitForSentinelRuntimeState(t, manager, func(snapshot SentinelRuntimeSnapshot) bool {
		return snapshot.Busy == 0 && snapshot.Queued == 0 && snapshot.ObserverSessions == 0
	})
}

func TestSentinelRuntimeCanceledSourceWaitersDoNotAccumulate(t *testing.T) {
	manager := newSentinelRuntimeTestManagerWithConfig(SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 1, CacheVersions: 3})
	defer manager.Close()
	_, request := sentinelRuntimeTestRequests(t, true)
	started := make(chan struct{})
	release := make(chan struct{})
	request.Fetcher = func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
		close(started)
		<-release
		return []byte(sentinelRuntimeTestSDK), "application/javascript", target, nil
	}
	leaderResult := make(chan error, 1)
	go func() {
		_, sourceErr := manager.source(context.Background(), request)
		leaderResult <- sourceErr
	}()
	<-started
	waitForSentinelRuntimeSourceWaiters(t, manager, 1)
	for index := 0; index < 20; index++ {
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, sourceErr := manager.source(ctx, request)
			result <- sourceErr
		}()
		waitForSentinelRuntimeSourceWaiters(t, manager, 2)
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled source waiter %d error = %v", index, err)
		}
		waitForSentinelRuntimeSourceWaiters(t, manager, 1)
	}
	close(release)
	if err := <-leaderResult; err != nil {
		t.Fatalf("leader source() error = %v", err)
	}
	waitForSentinelRuntimeSourcePending(t, manager, 0)
	waitForSentinelRuntimeSourceWaiters(t, manager, 0)
}

func TestSentinelRuntimeSourceFetchesHaveGlobalHardLimit(t *testing.T) {
	manager := newSentinelRuntimeTestManagerWithConfig(SentinelRuntimeConfig{Enabled: true, Workers: 4, QueueSize: 4, CacheVersions: 3})
	defer manager.Close()
	_, baseRequest := sentinelRuntimeTestRequests(t, true)
	started := make(chan struct{}, sentinelSDKSourceFetchMax)
	release := make(chan struct{})
	baseRequest.Fetcher = func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
		started <- struct{}{}
		<-release
		return []byte(sentinelRuntimeTestSDK), "application/javascript", target, nil
	}
	results := make(chan error, sentinelSDKSourceFetchMax)
	for index := 0; index < sentinelSDKSourceFetchMax; index++ {
		request := baseRequest
		request.TransportKey = fmt.Sprintf("proxy-%d", index)
		go func() {
			_, sourceErr := manager.source(context.Background(), request)
			results <- sourceErr
		}()
	}
	for index := 0; index < sentinelSDKSourceFetchMax; index++ {
		<-started
	}
	waitForSentinelRuntimeSourcePending(t, manager, sentinelSDKSourceFetchMax)
	if snapshot := manager.Snapshot(); snapshot.SourcePending != sentinelSDKSourceFetchMax || snapshot.SourceWaiters != sentinelSDKSourceFetchMax {
		t.Fatalf("source admission state = %+v", snapshot)
	}
	overflow := baseRequest
	overflow.TransportKey = "overflow"
	if _, err := manager.source(t.Context(), overflow); err == nil {
		t.Fatal("overflow source() error = nil")
	} else {
		var runtimeErr *SentinelRuntimeError
		if !errors.As(err, &runtimeErr) || runtimeErr.Code != "sentinel_sdk_busy" {
			t.Fatalf("overflow source() error = %#v", err)
		}
	}
	close(release)
	for index := 0; index < sentinelSDKSourceFetchMax; index++ {
		if err := <-results; err != nil {
			t.Fatalf("source flight %d error = %v", index, err)
		}
	}
	waitForSentinelRuntimeSourcePending(t, manager, 0)
}

func TestSentinelRuntimeQueuedObserverCancellationReleasesReservation(t *testing.T) {
	manager := newSentinelRuntimeTestManagerWithConfig(SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 1, CacheVersions: 3})
	defer manager.Close()
	active, err := manager.acquire(t.Context(), sentinelPriorityFallback)
	if err != nil {
		t.Fatal(err)
	}
	_, request := sentinelRuntimeTestRequests(t, true)
	var fetches atomic.Int64
	request.Fetcher = sentinelRuntimeTestFetcher(&fetches)
	ctx, cancel := context.WithCancel(context.Background())
	observer, err := manager.BeginObserver(ctx, request)
	if err != nil || observer == nil {
		active.release()
		t.Fatalf("BeginObserver() = %#v, %v", observer, err)
	}
	waitForSentinelRuntimeState(t, manager, func(snapshot SentinelRuntimeSnapshot) bool { return snapshot.Queued == 1 })
	cancel()
	observer.Close()
	if fetches.Load() != 0 {
		active.release()
		t.Fatalf("SDK fetches before queued Observer admission = %d", fetches.Load())
	}
	if snapshot := manager.Snapshot(); snapshot.Queued != 0 || snapshot.Busy != 1 || snapshot.ObserverSessions != 0 {
		active.release()
		t.Fatalf("snapshot after Observer cancellation = %+v", snapshot)
	}
	active.release()
}

func TestSentinelObserverCloseDoesNotWaitForSharedSourceFetch(t *testing.T) {
	manager := NewSentinelRuntimeManager(SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 2, CacheVersions: 3})
	manager.random = zeroReader{}
	defer manager.Close()
	_, request := sentinelRuntimeTestRequests(t, true)
	started := make(chan struct{})
	release := make(chan struct{})
	request.Fetcher = func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
		close(started)
		<-release
		return []byte(sentinelRuntimeTestSDK), "application/javascript", target, nil
	}
	observer, err := manager.BeginObserver(t.Context(), request)
	if err != nil || observer == nil {
		t.Fatalf("BeginObserver() = %#v, %v", observer, err)
	}
	<-started
	sharedResult := make(chan error, 1)
	go func() {
		_, sourceErr := manager.source(context.Background(), request)
		sharedResult <- sourceErr
	}()
	waitForSentinelRuntimeSourceWaiters(t, manager, 2)
	closed := make(chan struct{})
	go func() {
		observer.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Observer.Close() waited for the shared SDK fetch")
	}
	if snapshot := manager.Snapshot(); snapshot.Busy != 0 || snapshot.Queued != 0 || snapshot.ObserverSessions != 0 {
		t.Fatalf("snapshot after Observer close = %+v", snapshot)
	}
	close(release)
	if err = <-sharedResult; err != nil {
		t.Fatalf("shared source() error = %v", err)
	}
}

func TestSentinelRuntimeLastSourceWaiterCancelsFetch(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	_, request := sentinelRuntimeTestRequests(t, false)
	started := make(chan struct{})
	stopped := make(chan struct{})
	request.Fetcher = func(ctx context.Context, _ string, _ int64) ([]byte, string, string, error) {
		close(started)
		<-ctx.Done()
		close(stopped)
		return nil, "", "", ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, sourceErr := manager.source(ctx, request)
		result <- sourceErr
	}()
	<-started
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("source() error = %v", err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("source fetch was not canceled after its last waiter left")
	}
	waitForSentinelRuntimeSourcePending(t, manager, 0)
	waitForSentinelRuntimeSourceWaiters(t, manager, 0)
	manager.mu.Lock()
	flights := len(manager.sourceFlights)
	manager.mu.Unlock()
	if flights != 0 {
		t.Fatalf("source flights = %d", flights)
	}
}

func TestSentinelRuntimeNewWaiterDoesNotJoinAbandonedSourceFlight(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	_, request := sentinelRuntimeTestRequests(t, false)
	firstStarted := make(chan struct{})
	firstCanceled := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondStarted := make(chan struct{})
	var fetches atomic.Int64
	request.Fetcher = func(ctx context.Context, target string, _ int64) ([]byte, string, string, error) {
		if fetches.Add(1) == 1 {
			close(firstStarted)
			<-ctx.Done()
			close(firstCanceled)
			<-releaseFirst
			return nil, "", "", ctx.Err()
		}
		close(secondStarted)
		return []byte(sentinelRuntimeTestSDK), "application/javascript", target, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	firstResult := make(chan error, 1)
	go func() {
		_, sourceErr := manager.source(ctx, request)
		firstResult <- sourceErr
	}()
	<-firstStarted
	cancel()
	if err := <-firstResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("first source() error = %v", err)
	}
	<-firstCanceled
	secondResult := make(chan error, 1)
	go func() {
		_, sourceErr := manager.source(context.Background(), request)
		secondResult <- sourceErr
	}()
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		close(releaseFirst)
		t.Fatal("new source waiter joined the abandoned flight")
	}
	if err := <-secondResult; err != nil {
		close(releaseFirst)
		t.Fatalf("second source() error = %v", err)
	}
	close(releaseFirst)
	waitForSentinelRuntimeSourcePending(t, manager, 0)
}

func TestSentinelRuntimeManagerQueueUsesPriorityOrder(t *testing.T) {
	manager := newSentinelRuntimeTestManagerWithConfig(SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 2, CacheVersions: 3})
	defer manager.Close()
	active, err := manager.acquire(t.Context(), sentinelPriorityFallback)
	if err != nil {
		t.Fatal(err)
	}
	order := make(chan sentinelTaskPriority, 2)
	acquire := func(priority sentinelTaskPriority) {
		lease, acquireErr := manager.acquire(context.Background(), priority)
		if acquireErr != nil {
			return
		}
		order <- priority
		lease.release()
	}
	go acquire(sentinelPriorityPrecompile)
	waitForSentinelRuntimeState(t, manager, func(snapshot SentinelRuntimeSnapshot) bool { return snapshot.Queued == 1 })
	go acquire(sentinelPriorityObserverSnapshot)
	waitForSentinelRuntimeState(t, manager, func(snapshot SentinelRuntimeSnapshot) bool { return snapshot.Queued == 2 })
	active.release()
	select {
	case first := <-order:
		if first != sentinelPriorityObserverSnapshot {
			t.Fatalf("first granted priority = %d", first)
		}
	case <-time.After(time.Second):
		t.Fatal("queued work was not granted")
	}
	select {
	case second := <-order:
		if second != sentinelPriorityPrecompile {
			t.Fatalf("second granted priority = %d", second)
		}
	case <-time.After(time.Second):
		t.Fatal("second queued work was not granted")
	}
}

func TestSentinelRuntimeCollectorNeverOvertakesFallback(t *testing.T) {
	manager := newSentinelRuntimeTestManagerWithConfig(SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 4, CacheVersions: 3})
	defer manager.Close()
	active, err := manager.acquire(t.Context(), sentinelPriorityFallback)
	if err != nil {
		t.Fatal(err)
	}
	order := make(chan sentinelTaskPriority, 2)
	acquire := func(priority sentinelTaskPriority) {
		lease, acquireErr := manager.acquire(context.Background(), priority)
		if acquireErr != nil {
			return
		}
		order <- priority
		lease.release()
	}
	go acquire(sentinelPriorityObserverCollector)
	waitForSentinelRuntimeState(t, manager, func(snapshot SentinelRuntimeSnapshot) bool { return snapshot.Queued == 1 })
	go acquire(sentinelPriorityFallback)
	waitForSentinelRuntimeState(t, manager, func(snapshot SentinelRuntimeSnapshot) bool { return snapshot.Queued == 2 })
	active.release()
	select {
	case first := <-order:
		if first != sentinelPriorityFallback {
			t.Fatalf("first priority = %d, want fallback", first)
		}
	case <-time.After(time.Second):
		t.Fatal("fallback was not granted")
	}
	select {
	case second := <-order:
		if second != sentinelPriorityObserverCollector {
			t.Fatalf("second priority = %d, want collector", second)
		}
	case <-time.After(time.Second):
		t.Fatal("collector was not granted after fallback")
	}
}

func TestSentinelRuntimeManagerReservesWorkerForFallback(t *testing.T) {
	manager := newSentinelRuntimeTestManagerWithConfig(SentinelRuntimeConfig{Enabled: true, Workers: 2, QueueSize: 2, CacheVersions: 3})
	defer manager.Close()
	observerLease, err := manager.acquire(t.Context(), sentinelPriorityObserverCollector)
	if err != nil {
		t.Fatal(err)
	}
	queuedObserver := make(chan *sentinelRuntimeLease, 1)
	go func() {
		lease, _ := manager.acquire(context.Background(), sentinelPriorityObserverCollector)
		queuedObserver <- lease
	}()
	waitForSentinelRuntimeState(t, manager, func(snapshot SentinelRuntimeSnapshot) bool { return snapshot.Queued == 1 })
	fallbackLease, err := manager.acquire(t.Context(), sentinelPriorityFallback)
	if err != nil {
		t.Fatalf("fallback acquire error = %v", err)
	}
	fallbackLease.release()
	select {
	case lease := <-queuedObserver:
		if lease != nil {
			lease.release()
		}
		observerLease.release()
	case <-time.After(50 * time.Millisecond):
		observerLease.release()
		lease := <-queuedObserver
		lease.release()
	}
}

func TestSentinelRuntimeHotShrinkAllowsGrantedLeasesToFinish(t *testing.T) {
	manager := newSentinelRuntimeTestManagerWithConfig(SentinelRuntimeConfig{Enabled: true, Workers: 2, QueueSize: 1, CacheVersions: 3})
	defer manager.Close()
	first, err := manager.acquire(t.Context(), sentinelPriorityFallback)
	if err != nil {
		t.Fatal(err)
	}
	second, err := manager.acquire(t.Context(), sentinelPriorityFallback)
	if err != nil {
		first.release()
		t.Fatal(err)
	}
	manager.UpdateConfig(SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 1, CacheVersions: 3})
	firstWorker, err := first.runtimeWorker(t.Context())
	if err != nil {
		t.Fatalf("first runtimeWorker() error = %v", err)
	}
	secondWorker, err := second.runtimeWorker(t.Context())
	if err != nil {
		t.Fatalf("second runtimeWorker() error = %v", err)
	}
	firstRuntime, err := firstWorker.newRuntime(t.Context(), manager.cacheGeneration)
	if err != nil {
		t.Fatalf("first newRuntime() error = %v", err)
	}
	secondRuntime, err := secondWorker.newRuntime(t.Context(), manager.cacheGeneration)
	if err != nil {
		_ = firstRuntime.close()
		t.Fatalf("second newRuntime() error = %v", err)
	}
	if active := manager.activeQJS.Load(); active != 2 {
		t.Fatalf("active QJS runtimes = %d", active)
	}
	if !secondRuntime.close() || !firstRuntime.close() {
		t.Fatal("granted runtime cleanup failed")
	}
	first.release()
	second.release()
	manager.mu.Lock()
	workerCount := manager.workerCount
	manager.mu.Unlock()
	if workerCount > 1 || manager.Snapshot().Busy != 0 {
		t.Fatalf("worker count = %d, snapshot = %+v", workerCount, manager.Snapshot())
	}
}

func TestSentinelRuntimeManagerSolvesCompatibilityFallback(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	request, sdkRequest := sentinelRuntimeTestRequests(t, false)
	var fetches atomic.Int64
	sdkRequest.Fetcher = sentinelRuntimeTestFetcher(&fetches)
	token, err := manager.SolveTurnstile(t.Context(), request, sdkRequest, nil)
	if err != nil {
		t.Fatalf("SolveTurnstile() error = %v", err)
	}
	if token != "sdk-turnstile-token" {
		t.Fatalf("token = %q", token)
	}
	if fetches.Load() != 1 {
		t.Fatalf("SDK fetches = %d", fetches.Load())
	}
	snapshot := manager.Snapshot()
	if !snapshot.Initialized || snapshot.FallbackCount != 1 || snapshot.SourceCacheEntries != 1 || snapshot.BytecodeCacheEntries != 1 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	if manager.maxActiveQJS.Load() > int64(snapshot.WorkerLimit) {
		t.Fatalf("max active QJS = %d, worker limit = %d", manager.maxActiveQJS.Load(), snapshot.WorkerLimit)
	}
}

func TestSentinelSDKEnvironmentCurrentScriptUsesExactSDKNode(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	request, sdkRequest := sentinelRuntimeTestRequests(t, false)
	sdkURL := sdkRequest.ScriptSources[0]
	scriptSources := []string{sdkURL, "https://chatgpt.com/c/other.js"}
	sdkRequest.ScriptSources = scriptSources
	sdkRequest.Environment.ScriptSources = scriptSources
	sdkSource := strings.Replace(
		sentinelRuntimeTestSDK,
		`async function _n(){return "sdk-turnstile-token"}`,
		`async function _n(){return document.currentScript.src}`,
		1,
	)
	sdkRequest.Fetcher = func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
		return []byte(sdkSource), "application/javascript", target, nil
	}
	token, err := manager.SolveTurnstile(t.Context(), request, sdkRequest, nil)
	if err != nil {
		t.Fatalf("SolveTurnstile() error = %v", err)
	}
	if token != sdkURL {
		t.Fatalf("document.currentScript.src = %q, want %q", token, sdkURL)
	}
}

func TestSentinelSDKEnvironmentURLAndBase64Compatibility(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	request, sdkRequest := sentinelRuntimeTestRequests(t, false)
	sdkSource := strings.Replace(
		sentinelRuntimeTestSDK,
		`async function _n(){return "sdk-turnstile-token"}`,
		`async function _n(){
let invalidName="";
try{atob("YQ$==")}catch(error){invalidName=error.name}
let invalidBtoaName="";
try{btoa("\u0100")}catch(error){invalidBtoaName=error.name}
return JSON.stringify({
root:new URL("/x","https://chatgpt.com/a/b?old=1#old").href,
parent:new URL("../x","https://chatgpt.com/a/b").href,
encodedParent:new URL("%2e%2e/x","https://chatgpt.com/a/b").href,
backslashParent:new URL("..\\x","https://chatgpt.com/a/b").href,
space:new URL("a b","https://chatgpt.com/a/").href,
encodedSpace:new URL("a%20b","https://chatgpt.com/a/").href,
duplicateSlash:new URL("a//b","https://chatgpt.com/a/").href,
query:new URL("?q=1","https://chatgpt.com/a/b?old=1#old").href,
fragment:new URL("#new","https://chatgpt.com/a/b?old=1#old").href,
base64:atob("Y Q=="),
invalid:invalidName,
btoa:btoa("a"),
invalidBtoa:invalidBtoaName
})}`,
		1,
	)
	sdkRequest.Fetcher = func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
		return []byte(sdkSource), "application/javascript", target, nil
	}
	token, err := manager.SolveTurnstile(t.Context(), request, sdkRequest, nil)
	if err != nil {
		t.Fatalf("SolveTurnstile() error = %v", err)
	}
	var result map[string]string
	if err = json.Unmarshal([]byte(token), &result); err != nil {
		t.Fatalf("decode SDK compatibility result: %v", err)
	}
	want := map[string]string{
		"root":            "https://chatgpt.com/x",
		"parent":          "https://chatgpt.com/x",
		"encodedParent":   "https://chatgpt.com/x",
		"backslashParent": "https://chatgpt.com/x",
		"space":           "https://chatgpt.com/a/a%20b",
		"encodedSpace":    "https://chatgpt.com/a/a%20b",
		"duplicateSlash":  "https://chatgpt.com/a/a//b",
		"query":           "https://chatgpt.com/a/b?q=1",
		"fragment":        "https://chatgpt.com/a/b?old=1#new",
		"base64":          "a",
		"invalid":         "InvalidCharacterError",
		"btoa":            "YQ==",
		"invalidBtoa":     "InvalidCharacterError",
	}
	for key, expected := range want {
		if result[key] != expected {
			t.Errorf("%s = %q, want %q", key, result[key], expected)
		}
	}
}

func TestSentinelRuntimeSDKAdapterContractFixture(t *testing.T) {
	patched, err := patchSentinelSDK([]byte(sentinelRuntimeTestSDK), sentinelSDKExportsV2Adapter)
	if err != nil {
		t.Fatalf("patchSentinelSDK() error = %v", err)
	}
	if !strings.Contains(string(patched), sentinelSDKExportInjection) {
		t.Fatal("patched SDK does not publish the required adapter exports")
	}

	manager := NewSentinelRuntimeManager(SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 2, CacheVersions: 3})
	manager.random = zeroReader{}
	defer manager.Close()
	request, sdkRequest := sentinelRuntimeTestRequests(t, false)
	sdkRequest.Fetcher = sentinelRuntimeTestFetcher(nil)
	if token, solveErr := manager.SolveTurnstile(t.Context(), request, sdkRequest, nil); solveErr != nil || token != "sdk-turnstile-token" {
		t.Fatalf("SolveTurnstile() = %q, %v", token, solveErr)
	}

	_, observerRequest := sentinelRuntimeTestRequests(t, true)
	observerRequest.Fetcher = sentinelRuntimeTestFetcher(nil)
	observer, err := manager.BeginObserver(t.Context(), observerRequest)
	if err != nil || observer == nil {
		t.Fatalf("BeginObserver() = %#v, %v", observer, err)
	}
	defer observer.Close()
	snapshot, err := observer.Snapshot(t.Context())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	var payload map[string]any
	if err = json.Unmarshal([]byte(snapshot), &payload); err != nil {
		t.Fatalf("decode Observer snapshot: %v", err)
	}
	if payload["so"] != "sdk-snapshot-token" {
		t.Fatalf("Observer snapshot = %#v", payload)
	}
}

func TestSentinelRuntimeKnownSDKHashUsesVerifiedAdapter(t *testing.T) {
	adapter, known := resolveKnownSentinelSDKAdapter(sentinelSDKVerifiedExportsV2SHA256)
	if !known {
		t.Fatal("current Sentinel SDK hash has no verified adapter")
	}
	if adapter != sentinelSDKExportsV2Adapter {
		t.Fatalf("current Sentinel SDK adapter = %#v", adapter)
	}
	if _, ok := resolveSentinelSDKAdapter(sentinelSDKVerifiedExportsV2SHA256, []byte("var SentinelSDK=function(){};")); ok {
		t.Fatal("known SDK hash bypassed the source-shape adapter contract")
	}
	if resolved, ok := resolveSentinelSDKAdapter("new-sdk-hash", []byte(sentinelRuntimeTestSDK)); !ok || resolved != sentinelSDKExportsV2Adapter {
		t.Fatalf("compatible unknown SDK adapter = %#v, %v", resolved, ok)
	}
	for _, source := range []string{
		"var SentinelSDK=function(){};",
		sentinelRuntimeTestSDK + sentinelSDKExportMarker,
		strings.Replace(sentinelRuntimeTestSDK, "function _n(", "function renamed(", 1),
	} {
		if _, ok := resolveSentinelSDKAdapter("new-sdk-hash", []byte(source)); ok {
			t.Fatalf("incompatible unknown SDK source accepted: %q", source)
		}
	}
}

func TestSentinelRuntimeRejectsIncompatibleSDKSourceBeforeCompilation(t *testing.T) {
	manager := NewSentinelRuntimeManager(SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 1, CacheVersions: 3})
	defer manager.Close()
	_, request := sentinelRuntimeTestRequests(t, false)
	request.Fetcher = func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
		return []byte("var SentinelSDK=function(){};"), "application/javascript", target, nil
	}
	_, err := manager.source(t.Context(), request)
	var runtimeErr *SentinelRuntimeError
	if !errors.As(err, &runtimeErr) || runtimeErr.Code != "sentinel_sdk_unavailable" {
		t.Fatalf("source() error = %#v", err)
	}
	if !strings.Contains(err.Error(), "no compatible export adapter") {
		t.Fatalf("source() error = %v", err)
	}
	if snapshot := manager.Snapshot(); snapshot.SourceCacheEntries != 0 || snapshot.BytecodeCacheEntries != 0 || snapshot.Initialized {
		t.Fatalf("snapshot after incompatible SDK = %+v", snapshot)
	}
}

func TestSentinelRuntimeObserverWaitsForRecoveredCollectorPromise(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	_, sdkRequest := sentinelRuntimeTestRequests(t, true)
	recoveringSDK := strings.Replace(
		sentinelRuntimeTestSDK,
		`function Et(){globalThis.__collector_started=(globalThis.__collector_started||0)+1;return Promise.resolve()}`,
		`function Et(){return Promise.reject(new Error("collector transient failure")).catch(()=>"recovered")}`,
		1,
	)
	sdkRequest.Fetcher = func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
		return []byte(recoveringSDK), "application/javascript", target, nil
	}
	observer, err := manager.BeginObserver(t.Context(), sdkRequest)
	if err != nil || observer == nil {
		t.Fatalf("BeginObserver() = %#v, %v", observer, err)
	}
	defer observer.Close()
	if snapshot, snapshotErr := observer.Snapshot(t.Context()); snapshotErr != nil || !strings.Contains(snapshot, "sdk-snapshot-token") {
		t.Fatalf("Snapshot() = %q, %v", snapshot, snapshotErr)
	}
}

func TestSentinelRuntimeSourceAdmissionPrecedesRuntimeLease(t *testing.T) {
	manager := newSentinelRuntimeTestManagerWithConfig(SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 0, CacheVersions: 3})
	defer manager.Close()
	request, firstSDKRequest := sentinelRuntimeTestRequests(t, false)
	started := make(chan struct{})
	release := make(chan struct{})
	firstSDKRequest.Fetcher = func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
		close(started)
		<-release
		return []byte(sentinelRuntimeTestSDK), "application/javascript", target, nil
	}
	firstResult := make(chan error, 1)
	go func() {
		_, solveErr := manager.SolveTurnstile(context.Background(), request, firstSDKRequest, nil)
		firstResult <- solveErr
	}()
	<-started
	if snapshot := manager.Snapshot(); snapshot.Busy != 0 || snapshot.Queued != 0 {
		close(release)
		t.Fatalf("runtime pool was occupied by source download: %+v", snapshot)
	}
	secondSDKRequest := firstSDKRequest
	secondSDKRequest.SDKURL = "https://sentinel.openai.com/sentinel/20260722/sdk.js"
	var secondFetches atomic.Int64
	secondSDKRequest.Fetcher = sentinelRuntimeTestFetcher(&secondFetches)
	_, err := manager.SolveTurnstile(t.Context(), request, secondSDKRequest, nil)
	var runtimeErr *SentinelRuntimeError
	if !errors.As(err, &runtimeErr) || runtimeErr.Code != "sentinel_sdk_busy" {
		close(release)
		t.Fatalf("second SolveTurnstile() error = %#v", err)
	}
	if secondFetches.Load() != 0 {
		close(release)
		t.Fatalf("second SDK fetches = %d", secondFetches.Load())
	}
	close(release)
	if err = <-firstResult; err != nil {
		t.Fatalf("first SolveTurnstile() error = %v", err)
	}
}

func TestSentinelRuntimeCachedSourceExecutesWhileAnotherDownloadWaits(t *testing.T) {
	manager := newSentinelRuntimeTestManagerWithConfig(SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 1, CacheVersions: 3})
	defer manager.Close()
	request, cachedRequest := sentinelRuntimeTestRequests(t, false)
	cachedRequest.TransportKey = "proxy-a"
	cachedRequest.Fetcher = sentinelRuntimeTestFetcher(nil)
	if _, err := manager.source(t.Context(), cachedRequest); err != nil {
		t.Fatalf("prime source cache: %v", err)
	}

	blockedRequest := cachedRequest
	blockedRequest.SDKURL = "https://sentinel.openai.com/sentinel/20260722/sdk.js"
	blockedRequest.TransportKey = "proxy-b"
	started := make(chan struct{})
	release := make(chan struct{})
	blockedRequest.Fetcher = func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
		close(started)
		<-release
		return []byte(sentinelRuntimeTestSDK), "application/javascript", target, nil
	}
	blockedResult := make(chan error, 1)
	go func() {
		_, solveErr := manager.SolveTurnstile(context.Background(), request, blockedRequest, nil)
		blockedResult <- solveErr
	}()
	<-started
	if _, err := manager.SolveTurnstile(t.Context(), request, cachedRequest, nil); err != nil {
		close(release)
		t.Fatalf("cached SolveTurnstile() error = %v", err)
	}
	close(release)
	if err := <-blockedResult; err != nil {
		t.Fatalf("blocked SolveTurnstile() error = %v", err)
	}
}

func TestSentinelRuntimeManagerPrefersTypedCompatibilityFallback(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	request, sdkRequest := sentinelRuntimeTestRequests(t, false)
	request.DX = encodeConversationTurnstileProgram(t, request.RequirementsToken, []any{
		[]any{2, 40, "navigator"},
		[]any{6, 41, 10, 40},
		[]any{2, 42, "futureCapability"},
		[]any{6, 43, 41, 42},
		[]any{7, 43},
	})
	sdkRequest.Challenge["turnstile"] = map[string]any{"required": true, "dx": request.DX}
	sdkRequest.Fetcher = sentinelRuntimeTestFetcher(nil)
	if _, err := manager.SolveTurnstile(t.Context(), request, sdkRequest, nil); err != nil {
		t.Fatalf("SolveTurnstile() error = %v", err)
	}
	manager.mu.Lock()
	preferredCount := len(manager.preferred)
	preferredHintCount := len(manager.preferredHints)
	manager.mu.Unlock()
	if preferredCount != 1 || preferredHintCount != 1 {
		t.Fatalf("preferred caches = %d/%d, want 1/1", preferredCount, preferredHintCount)
	}
	if token, err := manager.SolveTurnstile(t.Context(), request, sdkRequest, nil); err != nil || token != "sdk-turnstile-token" {
		t.Fatalf("preferred SolveTurnstile() = %q, %v", token, err)
	}
}

func TestSentinelRuntimeManagerReusesCompatibilityClassAcrossDynamicLiterals(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	request, sdkRequest := sentinelRuntimeTestRequests(t, false)
	program := func(literal string) []any {
		return []any{[]any{36, literal}}
	}
	request.DX = encodeConversationTurnstileProgram(t, request.RequirementsToken, program("first-dynamic-value"))
	sdkRequest.Challenge["turnstile"] = map[string]any{"required": true, "dx": request.DX}
	sdkRequest.Fetcher = sentinelRuntimeTestFetcher(nil)
	if token, err := manager.SolveTurnstile(t.Context(), request, sdkRequest, nil); err != nil || token != "sdk-turnstile-token" {
		t.Fatalf("first SolveTurnstile() = %q, %v", token, err)
	}

	request.DX = encodeConversationTurnstileProgram(t, request.RequirementsToken, program("second-dynamic-value"))
	sdkRequest.Challenge["turnstile"] = map[string]any{"required": true, "dx": request.DX}
	token, err := manager.SolveTurnstile(t.Context(), request, sdkRequest, nil)
	if err != nil {
		t.Fatalf("second SolveTurnstile() error = %v", err)
	}
	if token != "sdk-turnstile-token" {
		t.Fatalf("second token = %q, want cached SDK compatibility class", token)
	}
}

func TestSentinelRuntimeRealSDKFixture(t *testing.T) {
	fixturePath := os.Getenv("CHATGPT_WEB_SENTINEL_SDK_FIXTURE")
	if fixturePath == "" {
		t.Skip("CHATGPT_WEB_SENTINEL_SDK_FIXTURE is not set")
	}
	source, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("read Sentinel SDK fixture: %v", err)
	}
	digest := sha256.Sum256(source)
	if _, known := resolveKnownSentinelSDKAdapter(hexDigest(digest[:])); !known {
		t.Fatalf("real SDK fixture hash %s has no verified adapter", hexDigest(digest[:]))
	}
	if _, compatible := resolveSentinelSDKAdapter("future-sdk-hash", source); !compatible {
		t.Fatal("real SDK fixture does not satisfy the source-shape adapter contract")
	}
	manager := NewSentinelRuntimeManager(SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 2, CacheVersions: 3})
	defer manager.Close()
	request, sdkRequest := sentinelRuntimeTestRequests(t, false)
	sdkRequest.Fetcher = func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
		return source, "application/javascript", target, nil
	}
	token, err := manager.SolveTurnstile(t.Context(), request, sdkRequest, nil)
	if err != nil {
		t.Fatalf("SolveTurnstile() with real SDK fixture error = %v", err)
	}
	if strings.TrimSpace(token) == "" {
		t.Fatal("SolveTurnstile() with real SDK fixture returned an empty token")
	}
}

func TestSentinelRuntimeManagerReusesWorkerSlotWithoutSharingRuntimeState(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	request, sdkRequest := sentinelRuntimeTestRequests(t, false)
	sdkRequest.Fetcher = func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
		return []byte(sentinelRuntimeStatefulTestSDK), "application/javascript", target, nil
	}
	first, err := manager.SolveTurnstile(t.Context(), request, sdkRequest, nil)
	if err != nil || first != "clean" {
		t.Fatalf("first SolveTurnstile() = %q, %v", first, err)
	}
	if manager.activeQJS.Load() != 0 {
		t.Fatalf("active QJS runtimes after first solve = %d", manager.activeQJS.Load())
	}
	manager.mu.Lock()
	if manager.workerCount != 1 || len(manager.idleWorkers) != 1 {
		workerCount := manager.workerCount
		idleCount := len(manager.idleWorkers)
		manager.mu.Unlock()
		t.Fatalf("worker state after first solve: count=%d idle=%d snapshot=%+v", workerCount, idleCount, manager.Snapshot())
	}
	firstWorker := manager.idleWorkers[0]
	manager.mu.Unlock()
	second, err := manager.SolveTurnstile(t.Context(), request, sdkRequest, nil)
	if err != nil || second != "clean" {
		t.Fatalf("second SolveTurnstile() = %q, %v", second, err)
	}
	if manager.activeQJS.Load() != 0 {
		t.Fatalf("active QJS runtimes after second solve = %d", manager.activeQJS.Load())
	}
	manager.mu.Lock()
	sameWorker := manager.workerCount == 1 && len(manager.idleWorkers) == 1 && manager.idleWorkers[0] == firstWorker
	manager.mu.Unlock()
	if !sameWorker {
		t.Fatal("Sentinel SDK worker was not reused")
	}
}

func TestSentinelRuntimeManagerCanceledExecutionReplacesWorker(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	request, sdkRequest := sentinelRuntimeTestRequests(t, false)
	sdkRequest.Fetcher = func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
		return []byte(sentinelRuntimeBlockingTurnstileSDK), "application/javascript", target, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	startedAt := time.Now()
	if _, err := manager.SolveTurnstile(ctx, request, sdkRequest, nil); err == nil {
		t.Fatal("SolveTurnstile() error = nil")
	}
	if elapsed := time.Since(startedAt); elapsed > 2*time.Second {
		t.Fatalf("infinite QuickJS loop stopped after %s", elapsed)
	}
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("context error = %v", ctx.Err())
	}
	digest := sha256.Sum256([]byte(sentinelRuntimeBlockingTurnstileSDK))
	if manager.circuitOpen(hexDigest(digest[:])) {
		t.Fatal("caller cancellation opened the SDK circuit")
	}
	waitForSentinelRuntimeState(t, manager, func(snapshot SentinelRuntimeSnapshot) bool {
		return snapshot.Busy == 0 && manager.activeQJS.Load() == 0
	})

	_, retryRequest := sentinelRuntimeTestRequests(t, false)
	retryRequest.ScriptSources = []string{"https://sentinel.openai.com/sentinel/20260723/sdk.js"}
	retryRequest.Environment.ScriptSources = append([]string(nil), retryRequest.ScriptSources...)
	retryRequest.Fetcher = sentinelRuntimeTestFetcher(nil)
	token, err := manager.SolveTurnstile(t.Context(), request, retryRequest, nil)
	if err != nil || token != "sdk-turnstile-token" {
		t.Fatalf("retry SolveTurnstile() = %q, %v", token, err)
	}
}

func TestSentinelRuntimeCreationUsesStableLifetimeContext(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	originalCreateRuntime := manager.createRuntime
	started := make(chan context.Context, 1)
	release := make(chan struct{})
	manager.createRuntime = func(option qjs.Option) (*qjs.Runtime, error) {
		started <- option.Context
		<-release
		return originalCreateRuntime(option)
	}
	worker := &sentinelQJSWorker{manager: manager}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		runtimeQJS, runtimeErr := worker.newRuntime(ctx, manager.cacheGeneration)
		if runtimeQJS != nil {
			_ = runtimeQJS.close()
		}
		result <- runtimeErr
	}()
	lifetimeContext := <-started
	cancel()
	if err := lifetimeContext.Err(); err != nil {
		t.Fatalf("QJS creation context was canceled with its caller: %v", err)
	}
	close(release)
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("newRuntime() error = %v", err)
	}
	if active := manager.activeQJS.Load(); active != 0 {
		t.Fatalf("active QJS runtimes = %d", active)
	}
}

func TestSentinelRuntimeCreationFailureReleasesCapacityForRetry(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	originalCreateRuntime := manager.createRuntime
	var attempts atomic.Int64
	manager.createRuntime = func(option qjs.Option) (*qjs.Runtime, error) {
		if attempts.Add(1) == 1 {
			return nil, errors.New("temporary QJS creation failure")
		}
		return originalCreateRuntime(option)
	}
	firstWorker := &sentinelQJSWorker{manager: manager}
	if _, err := firstWorker.newRuntime(t.Context(), manager.cacheGeneration); err == nil {
		t.Fatal("first newRuntime() error = nil")
	}
	if firstWorker.reusable() {
		t.Fatal("worker remained reusable after QJS creation failure")
	}
	if active := manager.activeQJS.Load(); active != 0 {
		t.Fatalf("active QJS runtimes after creation failure = %d", active)
	}
	secondWorker := &sentinelQJSWorker{manager: manager}
	runtimeQJS, err := secondWorker.newRuntime(t.Context(), manager.cacheGeneration)
	if err != nil {
		t.Fatalf("retry newRuntime() error = %v", err)
	}
	if !runtimeQJS.close() {
		t.Fatal("retry runtime cleanup failed")
	}
	if active := manager.activeQJS.Load(); active != 0 {
		t.Fatalf("active QJS runtimes after retry = %d", active)
	}
}

func TestSentinelQJSRuntimeClosedByContextRequiresWazeroExitError(t *testing.T) {
	if sentinelQJSRuntimeClosedByContext(fmt.Errorf("ordinary error: %w", context.Canceled)) {
		t.Fatal("ordinary context cancellation was treated as a closed Wazero runtime")
	}
	exitErr := wazerosys.NewExitError(wazerosys.ExitCodeContextCanceled)
	if !sentinelQJSRuntimeClosedByContext(fmt.Errorf("QJS call failed: %w", exitErr)) {
		t.Fatal("Wazero context cancellation was not recognized")
	}
}

func TestSentinelRuntimeCancellationAfterLastQJSCallReleasesCapacity(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	lease, err := manager.acquire(t.Context(), sentinelPriorityFallback)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := lease.runtimeWorker(t.Context())
	if err != nil {
		lease.release()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	err = worker.run(ctx, manager.cacheGeneration, func(runtimeQJS *qjs.Runtime) error {
		value, errEval := runtimeQJS.Eval("cancel-after-call.js", qjs.Code("1"))
		if errEval != nil {
			return errEval
		}
		if !safeFreeSentinelQJSValue(value) {
			return errors.New("release cancellation test value")
		}
		cancel()
		<-runtimeQJS.Context().Done()
		return nil
	})
	lease.release()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("worker.run() error = %v", err)
	}
	if active := manager.activeQJS.Load(); active != 0 {
		t.Fatalf("active QJS runtimes after late cancellation = %d", active)
	}
}

func TestSentinelRuntimeManagerCleanupFailureRetiresWorkerAndRecoversCapacity(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	var closeAttempts atomic.Int64
	manager.closeRuntime = func(runtimeQJS *qjs.Runtime) bool {
		if closeAttempts.Add(1) == 1 {
			return false
		}
		_ = safeCloseSentinelQJS(runtimeQJS)
		return true
	}

	lease, err := manager.acquire(t.Context(), sentinelPriorityFallback)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := lease.runtimeWorker(t.Context())
	if err != nil {
		lease.release()
		t.Fatal(err)
	}
	if err = worker.run(t.Context(), manager.cacheGeneration, func(*qjs.Runtime) error { return nil }); err == nil {
		lease.release()
		t.Fatal("worker.run() cleanup error = nil")
	}
	lease.release()
	if worker.reusable() {
		t.Fatal("worker remained reusable after runtime cleanup failure")
	}
	waitForSentinelRuntimeState(t, manager, func(SentinelRuntimeSnapshot) bool { return manager.activeQJS.Load() == 0 })

	retryLease, err := manager.acquire(t.Context(), sentinelPriorityFallback)
	if err != nil {
		t.Fatal(err)
	}
	retryWorker, err := retryLease.runtimeWorker(t.Context())
	if err != nil {
		retryLease.release()
		t.Fatal(err)
	}
	err = retryWorker.run(t.Context(), manager.cacheGeneration, func(*qjs.Runtime) error { return nil })
	retryLease.release()
	if err != nil {
		t.Fatalf("retry worker error = %v", err)
	}
}

func TestSentinelRuntimeCloseFailureWithoutExecutionIsReclaimed(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	manager.activeQJS.Store(1)
	var closeAttempts atomic.Int64
	manager.closeRuntime = func(*qjs.Runtime) bool { return closeAttempts.Add(1) > 1 }
	runtimeQJS := &sentinelQJSRuntime{
		manager:    manager,
		generation: manager.cacheGeneration,
	}

	if runtimeQJS.close() {
		t.Fatal("runtime close failure was treated as released")
	}
	waitForSentinelRuntimeState(t, manager, func(SentinelRuntimeSnapshot) bool { return manager.activeQJS.Load() == 0 })
}

func TestSentinelRuntimeManagerCloseWaitsForActiveTaskAndFailedRuntimeReclaim(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	finishTask, err := manager.beginSDKTask()
	if err != nil {
		t.Fatal(err)
	}
	manager.activeQJS.Store(1)
	reclaimStarted := make(chan struct{})
	releaseReclaim := make(chan struct{})
	var closeAttempts atomic.Int64
	manager.closeRuntime = func(*qjs.Runtime) bool {
		if closeAttempts.Add(1) == 1 {
			return false
		}
		select {
		case <-reclaimStarted:
		default:
			close(reclaimStarted)
		}
		<-releaseReclaim
		return true
	}

	closed := make(chan struct{})
	go func() {
		manager.Close()
		close(closed)
	}()
	deadline := time.Now().Add(time.Second)
	for {
		manager.mu.Lock()
		closing := manager.closed
		manager.mu.Unlock()
		if closing {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("manager did not start closing")
		}
		time.Sleep(time.Millisecond)
	}

	runtimeQJS := &sentinelQJSRuntime{manager: manager, generation: manager.cacheGeneration}
	if runtimeQJS.close() {
		t.Fatal("runtime close failure was treated as released")
	}
	finishTask()
	select {
	case <-reclaimStarted:
	case <-time.After(time.Second):
		t.Fatal("failed runtime reclaim did not start")
	}
	select {
	case <-closed:
		t.Fatal("manager closed before the failed runtime was reclaimed")
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseReclaim)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("manager did not close after the failed runtime was reclaimed")
	}
	if active := manager.activeQJS.Load(); active != 0 {
		t.Fatalf("active QJS runtimes after close = %d", active)
	}
}

func TestSentinelRuntimePreservesBusyErrorFromQJSCapacity(t *testing.T) {
	newCachedRuntime := func(t *testing.T, observer bool) (*SentinelRuntimeManager, SentinelSDKRequest, *sentinelSourceCacheEntry) {
		t.Helper()
		manager := newSentinelRuntimeTestManager()
		_, sdkRequest := sentinelRuntimeTestRequests(t, observer)
		source := &sentinelSourceCacheEntry{
			key:        sdkRequest.ScriptSources[0] + "\x00hash",
			url:        sdkRequest.ScriptSources[0],
			version:    "20260721",
			hash:       "hash",
			source:     []byte(sentinelRuntimeTestSDK),
			fetchedAt:  time.Now(),
			generation: manager.cacheGeneration,
		}
		manager.mu.Lock()
		manager.putSourceLocked(source)
		manager.putBytecodeLocked(&sentinelBytecodeCacheEntry{
			key:      source.hash + ":" + sentinelSDKAdapterVersion,
			hash:     source.hash,
			bytecode: []byte("cached-bytecode"),
		})
		manager.mu.Unlock()
		return manager, sdkRequest, source
	}
	assertBusy := func(t *testing.T, err error) {
		t.Helper()
		var runtimeErr *SentinelRuntimeError
		if !errors.As(err, &runtimeErr) || runtimeErr.Code != "sentinel_sdk_busy" || runtimeErr.RetryAfter != time.Second {
			t.Fatalf("runtime error = %#v", err)
		}
	}

	t.Run("turnstile fallback", func(t *testing.T) {
		manager, sdkRequest, source := newCachedRuntime(t, false)
		manager.activeQJS.Store(1)
		_, err := manager.solveTurnstileWithSDK(t.Context(), sdkRequest, source, "signature", false, nil, "dx", "requirements")
		assertBusy(t, err)
		manager.activeQJS.Store(0)
		manager.Close()
	})

	t.Run("observer", func(t *testing.T) {
		manager, sdkRequest, _ := newCachedRuntime(t, true)
		manager.activeQJS.Store(1)
		observer, err := manager.BeginObserver(t.Context(), sdkRequest)
		if err != nil || observer == nil {
			manager.activeQJS.Store(0)
			manager.Close()
			t.Fatalf("BeginObserver() = %#v, %v", observer, err)
		}
		_, err = observer.Snapshot(t.Context())
		assertBusy(t, err)
		observer.Close()
		manager.activeQJS.Store(0)
		manager.Close()
	})
}

func TestSentinelRuntimeCloseWaitsForIndependentFallbackAfterObserverFailure(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	ready := make(chan struct{})
	close(ready)
	observerCtx, cancelObserver := context.WithCancel(context.Background())
	defer cancelObserver()
	observer := &SentinelObserver{
		manager: manager,
		ctx:     observerCtx,
		cancel:  cancelObserver,
		ready:   ready,
		err:     errors.New("collector initialization failed"),
	}
	_, sdkRequest := sentinelRuntimeTestRequests(t, false)
	started := make(chan struct{})
	release := make(chan struct{})
	sdkRequest.Fetcher = func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
		close(started)
		<-release
		return []byte(sentinelRuntimeTestSDK), "application/javascript", target, nil
	}
	solveDone := make(chan error, 1)
	go func() {
		_, solveErr := manager.solveTurnstileWithSDK(
			context.Background(), sdkRequest, nil, "signature", false, observer, "dx", "requirements",
		)
		solveDone <- solveErr
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("independent fallback did not start its source fetch")
	}
	manager.mu.Lock()
	activeTasks := manager.activeTasks
	manager.mu.Unlock()
	if activeTasks != 1 {
		close(release)
		t.Fatalf("active SDK tasks = %d, want 1", activeTasks)
	}
	closed := make(chan struct{})
	go func() {
		manager.Close()
		close(closed)
	}()
	select {
	case <-closed:
		close(release)
		t.Fatal("manager closed before the independent fallback completed")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-solveDone; err == nil {
		t.Fatal("independent fallback error = nil after manager close")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("manager did not close after the independent fallback completed")
	}
}

func TestSentinelRuntimeBusyErrorDoesNotOpenSDKCircuit(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	generation := manager.cacheGeneration
	recordSentinelCircuitForError(manager, t.Context(), generation, "sdk-hash", newSentinelRuntimeError("sentinel_sdk_busy", time.Second, errors.New("capacity full")))
	if manager.circuitOpen("sdk-hash") {
		t.Fatal("local runtime capacity error opened the SDK circuit")
	}
}

func TestSentinelRuntimeManagerDisabledUsesLegacyGoVM(t *testing.T) {
	manager := newSentinelRuntimeTestManagerWithConfig(SentinelRuntimeConfig{Enabled: false, Workers: 1, QueueSize: 1, CacheVersions: 3})
	defer manager.Close()
	request, sdkRequest := sentinelRuntimeTestRequests(t, false)
	var fetches atomic.Int64
	sdkRequest.Fetcher = sentinelRuntimeTestFetcher(&fetches)
	_, err := manager.SolveTurnstile(t.Context(), request, sdkRequest, nil)
	if err != nil {
		t.Fatalf("legacy SolveTurnstile() error = %v", err)
	}
	if fetches.Load() != 0 || manager.Snapshot().Initialized {
		t.Fatalf("fetches = %d, snapshot = %+v", fetches.Load(), manager.Snapshot())
	}
}

func TestSentinelRuntimeManagerMalformedChallengeDoesNotFetchSDK(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	request, sdkRequest := sentinelRuntimeTestRequests(t, false)
	request.DX = "not-base64"
	var fetches atomic.Int64
	sdkRequest.Fetcher = sentinelRuntimeTestFetcher(&fetches)
	if _, err := manager.SolveTurnstile(t.Context(), request, sdkRequest, nil); err == nil {
		t.Fatal("SolveTurnstile() error = nil")
	}
	if fetches.Load() != 0 || manager.Snapshot().Initialized {
		t.Fatalf("fetches = %d, snapshot = %+v", fetches.Load(), manager.Snapshot())
	}
}

func TestSentinelRuntimeManagerCanceledChallengeDoesNotFetchSDK(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	request, sdkRequest := sentinelRuntimeTestRequests(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var fetches atomic.Int64
	sdkRequest.Fetcher = sentinelRuntimeTestFetcher(&fetches)
	if _, err := manager.SolveTurnstile(ctx, request, sdkRequest, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("SolveTurnstile() error = %v", err)
	}
	if fetches.Load() != 0 || manager.Snapshot().Initialized {
		t.Fatalf("fetches = %d, snapshot = %+v", fetches.Load(), manager.Snapshot())
	}
}

func TestSentinelRuntimeObserverCollectorAndSnapshot(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	_, sdkRequest := sentinelRuntimeTestRequests(t, true)
	sdkRequest.Fetcher = sentinelRuntimeTestFetcher(nil)
	observer, err := manager.BeginObserver(t.Context(), sdkRequest)
	if err != nil || observer == nil {
		t.Fatalf("BeginObserver() = %#v, %v", observer, err)
	}
	token, err := observer.Snapshot(t.Context())
	if err != nil {
		observer.Close()
		t.Fatalf("Snapshot() error = %v", err)
	}
	var payload map[string]any
	if err = json.Unmarshal([]byte(token), &payload); err != nil {
		observer.Close()
		t.Fatalf("decode SO token: %v", err)
	}
	if payload["so"] != "sdk-snapshot-token" || payload["c"] != "challenge-token" || payload["id"] != "device-id" || payload["flow"] != "conversation" {
		observer.Close()
		t.Fatalf("SO token = %#v", payload)
	}
	if manager.Snapshot().ObserverSessions != 1 {
		observer.Close()
		t.Fatalf("snapshot = %+v", manager.Snapshot())
	}
	observer.Close()
	if manager.Snapshot().ObserverSessions != 0 {
		t.Fatalf("snapshot after close = %+v", manager.Snapshot())
	}
}

func TestSentinelRuntimeObserverCloseCancelsActiveSnapshot(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	_, request := sentinelRuntimeTestRequests(t, true)
	blockingSnapshotSDK := strings.Replace(
		sentinelRuntimeTestSDK,
		`async function Nt(){return "sdk-snapshot-token"}`,
		`async function Nt(){for(;;){}}`,
		1,
	)
	request.Fetcher = func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
		return []byte(blockingSnapshotSDK), "application/javascript", target, nil
	}
	observer, err := manager.BeginObserver(t.Context(), request)
	if err != nil || observer == nil {
		t.Fatalf("BeginObserver() = %#v, %v", observer, err)
	}
	if err = observer.wait(t.Context()); err != nil {
		observer.Close()
		t.Fatal(err)
	}
	observer.mu.Lock()
	instance := observer.instance
	observer.mu.Unlock()
	if instance == nil {
		observer.Close()
		t.Fatal("Observer instance is nil")
	}

	snapshotResult := make(chan error, 1)
	go func() {
		_, snapshotErr := observer.Snapshot(context.Background())
		snapshotResult <- snapshotErr
	}()
	deadline := time.Now().Add(time.Second)
	for {
		if !instance.mu.TryLock() {
			break
		}
		instance.mu.Unlock()
		if time.Now().After(deadline) {
			observer.Close()
			t.Fatal("Snapshot did not enter the SDK runtime")
		}
		time.Sleep(time.Millisecond)
	}

	closed := make(chan struct{})
	go func() {
		observer.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Observer.Close did not cancel the active snapshot")
	}
	select {
	case snapshotErr := <-snapshotResult:
		if snapshotErr == nil {
			t.Fatal("Snapshot() error = nil after Observer.Close")
		}
	case <-time.After(time.Second):
		t.Fatal("Snapshot did not return after Observer.Close")
	}
}

func TestSentinelRuntimeObserverWaitsForAsyncCollector(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	_, request := sentinelRuntimeTestRequests(t, true)
	rejectingSDK := strings.Replace(
		sentinelRuntimeTestSDK,
		`function Et(){globalThis.__collector_started=(globalThis.__collector_started||0)+1;return Promise.resolve()}`,
		`async function Et(){await Promise.resolve();throw new Error("collector rejected")}`,
		1,
	)
	request.Fetcher = func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
		return []byte(rejectingSDK), "application/javascript", target, nil
	}
	observer, err := manager.BeginObserver(t.Context(), request)
	if err != nil || observer == nil {
		t.Fatalf("BeginObserver() = %#v, %v", observer, err)
	}
	defer observer.Close()
	if err = observer.wait(t.Context()); err == nil {
		t.Fatal("observer collector rejection was not propagated")
	}
	digest := sha256.Sum256([]byte(rejectingSDK))
	if !manager.circuitOpen(hexDigest(digest[:])) {
		t.Fatal("Sentinel SDK collector failure did not open the SDK circuit")
	}
}

func TestSentinelRuntimeObserverCapturesHandledInternalCollectorPromise(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	_, request := sentinelRuntimeTestRequests(t, true)
	recoveringSDK := strings.Replace(
		sentinelRuntimeTestSDK,
		`function Et(){globalThis.__collector_started=(globalThis.__collector_started||0)+1;return Promise.resolve()}`,
		`function Et(){Promise.resolve().then(()=>{throw new Error("collector rejected")}).catch(()=>Promise.resolve().then(()=>{globalThis.__collector_recovered=true}))}`,
		1,
	)
	recoveringSDK = strings.Replace(
		recoveringSDK,
		`async function Nt(){return "sdk-snapshot-token"}`,
		`async function Nt(){if(!globalThis.__collector_recovered)throw new Error("collector recovery is pending");return "sdk-snapshot-token"}`,
		1,
	)
	request.Fetcher = func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
		return []byte(recoveringSDK), "application/javascript", target, nil
	}
	observer, err := manager.BeginObserver(t.Context(), request)
	if err != nil || observer == nil {
		t.Fatalf("BeginObserver() = %#v, %v", observer, err)
	}
	defer observer.Close()
	if err = observer.wait(t.Context()); err != nil {
		t.Fatalf("internal collector recovery error = %v", err)
	}
	if _, err = observer.Snapshot(t.Context()); err != nil {
		t.Fatalf("Snapshot() before handled collector completed: %v", err)
	}
}

func TestSentinelRuntimeObserverRejectsUnknownCollectorAdapter(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	_, request := sentinelRuntimeTestRequests(t, true)
	unsupportedSDK := strings.Replace(
		sentinelRuntimeTestSDK,
		`function Et(){globalThis.__collector_started=(globalThis.__collector_started||0)+1;return Promise.resolve()}`,
		`function Et(){globalThis.__collector_started=(globalThis.__collector_started||0)+1}`,
		1,
	)
	request.Fetcher = func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
		return []byte(unsupportedSDK), "application/javascript", target, nil
	}
	observer, err := manager.BeginObserver(t.Context(), request)
	if err != nil || observer == nil {
		t.Fatalf("BeginObserver() = %#v, %v", observer, err)
	}
	defer observer.Close()
	if err = observer.wait(t.Context()); err == nil {
		t.Fatal("observer accepted an SDK without an observable collector promise")
	}
}

func TestSentinelObserverUnavailablePreservesRetryAfter(t *testing.T) {
	ready := make(chan struct{})
	close(ready)
	observer := &SentinelObserver{
		ready: ready,
		err: newSentinelRuntimeError(
			"sentinel_sdk_unavailable",
			37*time.Second,
			errors.New("SDK source circuit is open"),
		),
	}
	_, err := observer.Snapshot(t.Context())
	var runtimeErr *SentinelRuntimeError
	if !errors.As(err, &runtimeErr) || runtimeErr.Code != "sentinel_session_observer_unavailable" || runtimeErr.RetryAfter != 37*time.Second {
		t.Fatalf("Snapshot() error = %#v", err)
	}
}

func TestSentinelRuntimeObserverHasNoFixedLifecycleDeadline(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	_, sdkRequest := sentinelRuntimeTestRequests(t, true)
	sdkRequest.Fetcher = sentinelRuntimeTestFetcher(nil)
	observer, err := manager.BeginObserver(t.Context(), sdkRequest)
	if err != nil || observer == nil {
		t.Fatalf("BeginObserver() = %#v, %v", observer, err)
	}
	defer observer.Close()
	if err = observer.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	observer.mu.Lock()
	instance := observer.instance
	observer.mu.Unlock()
	if instance == nil {
		t.Fatal("observer instance is nil")
	}
	if _, hasDeadline := instance.ctx.Deadline(); hasDeadline {
		t.Fatal("observer instance has a fixed lifecycle deadline")
	}
}

func TestSentinelRuntimeObserverTurnstileFailureOpensCircuitWithoutWaitingForOwnWorker(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	request, sdkRequest := sentinelRuntimeTestRequests(t, true)
	sdkRequest.Fetcher = func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
		return []byte(sentinelRuntimeFailingTurnstileSDK), "application/javascript", target, nil
	}
	observer, err := manager.BeginObserver(t.Context(), sdkRequest)
	if err != nil || observer == nil {
		t.Fatalf("BeginObserver() = %#v, %v", observer, err)
	}
	defer observer.Close()
	if err = observer.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	startedAt := time.Now()
	_, err = manager.SolveTurnstile(ctx, request, sdkRequest, observer)
	var runtimeErr *SentinelRuntimeError
	if !errors.As(err, &runtimeErr) || runtimeErr.Code != "sentinel_sdk_unavailable" {
		t.Fatalf("SolveTurnstile() error = %#v", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 500*time.Millisecond {
		t.Fatalf("SolveTurnstile() blocked for %s", elapsed)
	}
	digest := sha256.Sum256([]byte(sentinelRuntimeFailingTurnstileSDK))
	if !manager.circuitOpen(hexDigest(digest[:])) {
		t.Fatal("Sentinel SDK Turnstile failure did not open the SDK circuit")
	}
}

func TestSentinelRuntimeTurnstileFailureOpensCircuit(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	request, sdkRequest := sentinelRuntimeTestRequests(t, false)
	sdkRequest.Fetcher = func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
		return []byte(sentinelRuntimeFailingTurnstileSDK), "application/javascript", target, nil
	}
	_, err := manager.SolveTurnstile(t.Context(), request, sdkRequest, nil)
	var runtimeErr *SentinelRuntimeError
	if !errors.As(err, &runtimeErr) || runtimeErr.Code != "sentinel_sdk_unavailable" {
		t.Fatalf("SolveTurnstile() error = %#v", err)
	}
	digest := sha256.Sum256([]byte(sentinelRuntimeFailingTurnstileSDK))
	if !manager.circuitOpen(hexDigest(digest[:])) {
		t.Fatal("Sentinel SDK Turnstile failure did not open the SDK circuit")
	}
	_, err = manager.SolveTurnstile(t.Context(), request, sdkRequest, nil)
	if !errors.As(err, &runtimeErr) || runtimeErr.RetryAfter <= 0 {
		t.Fatalf("circuit breaker error = %#v", err)
	}
}

func TestSentinelRuntimeSnapshotFailureOpensCircuit(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	_, sdkRequest := sentinelRuntimeTestRequests(t, true)
	failingSDK := strings.Replace(sentinelRuntimeTestSDK, `async function Nt(){return "sdk-snapshot-token"}`, `async function Nt(){throw new Error("snapshot failed")}`, 1)
	sdkRequest.Fetcher = func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
		return []byte(failingSDK), "application/javascript", target, nil
	}
	observer, err := manager.BeginObserver(t.Context(), sdkRequest)
	if err != nil || observer == nil {
		t.Fatalf("BeginObserver() = %#v, %v", observer, err)
	}
	defer observer.Close()
	if err = observer.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err = observer.Snapshot(t.Context()); err == nil {
		t.Fatal("Snapshot() error = nil")
	}
	digest := sha256.Sum256([]byte(failingSDK))
	if !manager.circuitOpen(hexDigest(digest[:])) {
		t.Fatal("Sentinel SDK snapshot failure did not open the SDK circuit")
	}
}

func TestSentinelRuntimeManagerDisableDrainsAcceptedQueueBeforeClearingCache(t *testing.T) {
	manager := newSentinelRuntimeTestManagerWithConfig(SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 1, CacheVersions: 3})
	active, err := manager.acquire(t.Context(), sentinelPriorityFallback)
	if err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	manager.putSourceLocked(&sentinelSourceCacheEntry{key: "https://sentinel.openai.com/sentinel/test/sdk.js", hash: "hash", source: []byte("sdk"), fetchedAt: manager.now()})
	manager.mu.Unlock()
	queuedResult := make(chan *sentinelRuntimeLease, 1)
	queuedErr := make(chan error, 1)
	go func() {
		lease, acquireErr := manager.acquire(context.Background(), sentinelPriorityFallback)
		if acquireErr != nil {
			queuedErr <- acquireErr
			return
		}
		queuedResult <- lease
	}()
	waitForSentinelRuntimeState(t, manager, func(snapshot SentinelRuntimeSnapshot) bool { return snapshot.Queued == 1 })
	manager.UpdateConfig(SentinelRuntimeConfig{Enabled: false, Workers: 1, QueueSize: 1, CacheVersions: 3})
	if snapshot := manager.Snapshot(); snapshot.Queued != 1 || snapshot.SourceCacheEntries != 1 || snapshot.Available {
		t.Fatalf("snapshot while draining = %+v", snapshot)
	}
	active.release()
	select {
	case queued := <-queuedResult:
		if snapshot := manager.Snapshot(); snapshot.Busy != 1 || snapshot.SourceCacheEntries != 1 {
			t.Fatalf("snapshot with accepted queue running = %+v", snapshot)
		}
		queued.release()
	case err = <-queuedErr:
		t.Fatalf("queued acquire error = %v", err)
	case <-time.After(time.Second):
		t.Fatal("queued acquire was not granted")
	}
	waitForSentinelRuntimeState(t, manager, func(snapshot SentinelRuntimeSnapshot) bool {
		return snapshot.Busy == 0 && snapshot.Queued == 0 && snapshot.SourceCacheEntries == 0 && !snapshot.Available
	})
	manager.Close()
}

func TestSentinelRuntimeManagerDisableThenEnableStillClearsAfterActiveWork(t *testing.T) {
	manager := newSentinelRuntimeTestManagerWithConfig(SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 1, CacheVersions: 3})
	defer manager.Close()
	active, err := manager.acquire(t.Context(), sentinelPriorityFallback)
	if err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	manager.putSourceLocked(&sentinelSourceCacheEntry{key: "https://sentinel.openai.com/sentinel/test/sdk.js", hash: "hash", source: []byte("sdk"), fetchedAt: manager.now()})
	manager.mu.Unlock()
	manager.UpdateConfig(SentinelRuntimeConfig{Enabled: false, Workers: 1, QueueSize: 1, CacheVersions: 3})
	manager.UpdateConfig(SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 1, CacheVersions: 3})

	queued := make(chan *sentinelRuntimeLease, 1)
	queuedErr := make(chan error, 1)
	go func() {
		lease, acquireErr := manager.acquire(context.Background(), sentinelPriorityFallback)
		if acquireErr != nil {
			queuedErr <- acquireErr
			return
		}
		queued <- lease
	}()
	waitForSentinelRuntimeState(t, manager, func(snapshot SentinelRuntimeSnapshot) bool { return snapshot.Queued == 1 })
	active.release()
	select {
	case lease := <-queued:
		if snapshot := manager.Snapshot(); snapshot.SourceCacheEntries != 1 || snapshot.BytecodeCacheEntries != 0 || snapshot.Available {
			t.Fatalf("snapshot while old work drains = %+v", snapshot)
		}
		lease.release()
	case err = <-queuedErr:
		t.Fatalf("queued acquire error = %v", err)
	case <-time.After(time.Second):
		t.Fatal("queued acquire was not granted after cache cleanup")
	}
	waitForSentinelRuntimeState(t, manager, func(snapshot SentinelRuntimeSnapshot) bool {
		return snapshot.SourceCacheEntries == 0 && snapshot.BytecodeCacheEntries == 0 && snapshot.Available
	})
}

func TestSentinelRuntimeDisableLetsActiveObserverFinish(t *testing.T) {
	manager := newSentinelRuntimeTestManagerWithConfig(SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 1, CacheVersions: 3})
	defer manager.Close()
	_, request := sentinelRuntimeTestRequests(t, true)
	request.Fetcher = sentinelRuntimeTestFetcher(nil)
	observer, err := manager.BeginObserver(t.Context(), request)
	if err != nil || observer == nil {
		t.Fatalf("BeginObserver() = %#v, %v", observer, err)
	}
	if err = observer.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	manager.UpdateConfig(SentinelRuntimeConfig{Enabled: false, Workers: 1, QueueSize: 1, CacheVersions: 3})
	if snapshot := manager.Snapshot(); snapshot.Busy != 1 || snapshot.ObserverSessions != 1 || snapshot.Available {
		t.Fatalf("snapshot after disable = %+v", snapshot)
	}
	if token, snapshotErr := observer.Snapshot(t.Context()); snapshotErr != nil || !strings.Contains(token, "sdk-snapshot-token") {
		t.Fatalf("Snapshot() = %q, %v", token, snapshotErr)
	}
	observer.Close()
	if snapshot := manager.Snapshot(); snapshot.Busy != 0 || snapshot.ObserverSessions != 0 || snapshot.Available {
		t.Fatalf("snapshot after observer completion = %+v", snapshot)
	}
	manager.UpdateConfig(SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 1, CacheVersions: 3})
	lease, err := manager.acquire(t.Context(), sentinelPriorityFallback)
	if err != nil {
		t.Fatalf("acquire after re-enable error = %v", err)
	}
	lease.release()
}

func TestSentinelRuntimeDisableLetsAcceptedObserverSolveCompatibilityFallback(t *testing.T) {
	manager := newSentinelRuntimeTestManagerWithConfig(SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 1, CacheVersions: 3})
	defer manager.Close()
	goRequest, sdkRequest := sentinelRuntimeTestRequests(t, true)
	sdkRequest.Fetcher = sentinelRuntimeTestFetcher(nil)
	observer, err := manager.BeginObserver(t.Context(), sdkRequest)
	if err != nil || observer == nil {
		t.Fatalf("BeginObserver() = %#v, %v", observer, err)
	}
	defer observer.Close()
	if err = observer.wait(t.Context()); err != nil {
		t.Fatal(err)
	}

	manager.UpdateConfig(SentinelRuntimeConfig{Enabled: false, Workers: 1, QueueSize: 1, CacheVersions: 3})
	token, err := manager.SolveTurnstile(t.Context(), goRequest, sdkRequest, observer)
	if err != nil || token != "sdk-turnstile-token" {
		t.Fatalf("SolveTurnstile() after disable = %q, %v", token, err)
	}
}

func TestSentinelRuntimeManagerRequiresExactTrustedSDKSource(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
		sources []string
	}{
		{name: "missing", baseURL: "https://chatgpt.com"},
		{name: "http", baseURL: "https://chatgpt.com", sources: []string{"http://sentinel.openai.com/sentinel/test/sdk.js"}},
		{name: "untrusted host", baseURL: "https://chatgpt.com", sources: []string{"https://example.com/sentinel/test/sdk.js"}},
		{name: "untrusted path", baseURL: "https://chatgpt.com", sources: []string{"https://sentinel.openai.com/backend-api/sentinel/sdk.js"}},
		{name: "backend path on SDK host", baseURL: "https://chatgpt.com", sources: []string{"https://sentinel.openai.com/backend-api/sentinel/20260721/sdk.js"}},
		{name: "unversioned path", baseURL: "https://chatgpt.com", sources: []string{"https://sentinel.openai.com/sentinel/not-a-version/sdk.js"}},
		{name: "nested version path", baseURL: "https://chatgpt.com", sources: []string{"https://sentinel.openai.com/sentinel/20260721/nested/sdk.js"}},
		{name: "duplicate leading slash", baseURL: "https://chatgpt.com", sources: []string{"https://sentinel.openai.com//sentinel/20260721/sdk.js"}},
		{name: "trailing slash", baseURL: "https://chatgpt.com", sources: []string{"https://sentinel.openai.com/sentinel/20260721/sdk.js/"}},
		{name: "case variant", baseURL: "https://chatgpt.com", sources: []string{"https://sentinel.openai.com/Sentinel/20260721/sdk.js"}},
		{name: "query", baseURL: "https://chatgpt.com", sources: []string{"https://sentinel.openai.com/sentinel/20260721/sdk.js?cache=1"}},
		{name: "fragment", baseURL: "https://chatgpt.com", sources: []string{"https://sentinel.openai.com/sentinel/20260721/sdk.js#fragment"}},
		{name: "untrusted port", baseURL: "https://chatgpt.com", sources: []string{"https://sentinel.openai.com:8443/sentinel/test/sdk.js"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := resolveSentinelSDKSource(test.baseURL, test.sources); err == nil {
				t.Fatal("resolveSentinelSDKSource() error = nil")
			}
		})
	}
	urlValue, version, err := resolveSentinelSDKSource("https://chatgpt.com", []string{"https://sentinel.openai.com/sentinel/20260721/sdk.js"})
	if err != nil || urlValue != "https://sentinel.openai.com/sentinel/20260721/sdk.js" || version != "20260721" {
		t.Fatalf("resolveSentinelSDKSource() = %q, %q, %v", urlValue, version, err)
	}
	backendURL, backendVersion, err := resolveSentinelSDKSource("https://chatgpt.com", []string{"/backend-api/sentinel/20260721f9f6/sdk.js"})
	if err != nil || backendURL != "https://chatgpt.com/backend-api/sentinel/20260721f9f6/sdk.js" || backendVersion != "20260721f9f6" {
		t.Fatalf("resolve backend Sentinel source = %q, %q, %v", backendURL, backendVersion, err)
	}
	chatGPTURL, chatGPTVersion, err := resolveSentinelSDKSource("https://chatgpt.com", []string{"/sentinel/20260721f9f6/sdk.js"})
	if err != nil || chatGPTURL != "https://chatgpt.com/sentinel/20260721f9f6/sdk.js" || chatGPTVersion != "20260721f9f6" {
		t.Fatalf("resolve ChatGPT Sentinel source = %q, %q, %v", chatGPTURL, chatGPTVersion, err)
	}
}

func TestSentinelRuntimeSourceRejectsInvalidIntegrityAndResponse(t *testing.T) {
	tests := []struct {
		name              string
		expected          string
		integrityRequired bool
		contentType       string
		source            []byte
		finalURL          string
		wantFetches       int64
	}{
		{name: "invalid integrity", expected: "sha256-invalid", contentType: "application/javascript", source: []byte(sentinelRuntimeTestSDK), wantFetches: 0},
		{name: "unsupported integrity", integrityRequired: true, contentType: "application/javascript", source: []byte(sentinelRuntimeTestSDK), wantFetches: 0},
		{name: "invalid content type", contentType: "text/html", source: []byte(sentinelRuntimeTestSDK), wantFetches: 1},
		{name: "oversized", contentType: "application/javascript", source: make([]byte, sentinelSDKMaxSourceBytes+1), wantFetches: 1},
		{name: "untrusted final URL", contentType: "application/javascript", source: []byte(sentinelRuntimeTestSDK), finalURL: "https://sentinel.openai.com/assets/sdk.js", wantFetches: 1},
		{name: "untrusted final port", contentType: "application/javascript", source: []byte(sentinelRuntimeTestSDK), finalURL: "https://sentinel.openai.com:8443/sentinel/test/sdk.js", wantFetches: 1},
		{name: "redirect changes version", contentType: "application/javascript", source: []byte(sentinelRuntimeTestSDK), finalURL: "https://sentinel.openai.com/sentinel/20260722/sdk.js", wantFetches: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manager := newSentinelRuntimeTestManager()
			defer manager.Close()
			_, request := sentinelRuntimeTestRequests(t, false)
			request.ExpectedSHA256 = test.expected
			request.IntegrityRequired = test.integrityRequired
			var fetches atomic.Int64
			request.Fetcher = func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
				fetches.Add(1)
				finalURL := test.finalURL
				if finalURL == "" {
					finalURL = target
				}
				return test.source, test.contentType, finalURL, nil
			}
			if _, err := manager.source(t.Context(), request); err == nil {
				t.Fatal("source() error = nil")
			}
			if fetches.Load() != test.wantFetches || manager.Snapshot().SourceCacheEntries != 0 {
				t.Fatalf("fetches=%d snapshot=%+v", fetches.Load(), manager.Snapshot())
			}
		})
	}
}

func TestSentinelRuntimeSourceAcceptsAnyExpectedSHA256(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	_, request := sentinelRuntimeTestRequests(t, false)
	goodDigest := sha256.Sum256([]byte(sentinelRuntimeTestSDK))
	badDigest := sha256.Sum256([]byte("old Sentinel SDK"))
	request.ExpectedSHA256 = "sha256-" + base64.StdEncoding.EncodeToString(badDigest[:]) + " sha256-" + base64.StdEncoding.EncodeToString(goodDigest[:])
	request.Fetcher = sentinelRuntimeTestFetcher(nil)
	if _, err := manager.source(t.Context(), request); err != nil {
		t.Fatalf("source() error with matching secondary hash = %v", err)
	}
}

func TestSentinelRuntimeCallerCancellationDoesNotOpenCircuit(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	generation := manager.cacheGeneration
	recordSentinelCircuitForError(manager, ctx, generation, "hash", context.Canceled)
	if manager.circuitOpen("hash") {
		t.Fatal("caller cancellation opened circuit")
	}
	recordSentinelCircuitForError(manager, context.Background(), generation, "hash", errors.New("SDK execution failed"))
	if !manager.circuitOpen("hash") {
		t.Fatal("SDK execution failure did not open circuit")
	}
}

func TestSentinelRuntimeCircuitReturnsRemainingRetryAfter(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	now := time.Unix(1000, 0)
	manager.now = func() time.Time { return now }
	manager.recordCircuit(manager.cacheGeneration, "hash")
	now = now.Add(17 * time.Second)
	retryAfter, open := manager.circuitRetryAfter("hash")
	if !open || retryAfter != sentinelSDKCircuitBreakerTTL-17*time.Second {
		t.Fatalf("circuitRetryAfter() = %s, %v", retryAfter, open)
	}
}

func TestSentinelRuntimeSourceFailurePreservesUpstreamRetryAfter(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	now := time.Unix(1000, 0)
	manager.now = func() time.Time { return now }
	err := manager.recordSourceFailure(
		manager.cacheGeneration,
		"source-key",
		fmt.Errorf("fetch SDK: %w", sentinelRuntimeRetryAfterError{delay: 37 * time.Second}),
	)
	var runtimeErr *SentinelRuntimeError
	if !errors.As(err, &runtimeErr) || runtimeErr.RetryAfter != 37*time.Second {
		t.Fatalf("recordSourceFailure() error = %#v", err)
	}
	retryAfter, open := manager.sourceCircuitRetryAfter("source-key")
	if !open || retryAfter != 37*time.Second {
		t.Fatalf("sourceCircuitRetryAfter() = %s, %v", retryAfter, open)
	}
}

func TestSentinelRuntimeStaleGenerationCannotRestoreCircuitOrPreferredState(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	staleGeneration := manager.cacheGeneration
	manager.UpdateConfig(SentinelRuntimeConfig{Enabled: false, Workers: 1, QueueSize: 1, CacheVersions: 3})
	manager.UpdateConfig(SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 1, CacheVersions: 3})

	manager.recordCircuit(staleGeneration, "stale-hash")
	manager.markPreferredForChallenge(
		staleGeneration,
		"stale-hash",
		SentinelProgramTurnstile,
		"stale-signature",
		"stale-challenge",
		"stale-requirements",
	)
	manager.recordError(staleGeneration, "stale-error")

	manager.mu.Lock()
	circuits := len(manager.circuits)
	preferred := len(manager.preferred)
	hints := len(manager.preferredHints)
	lastError := manager.lastError
	manager.mu.Unlock()
	if circuits != 0 || preferred != 0 || hints != 0 || lastError != "" {
		t.Fatalf("stale state restored: circuits=%d preferred=%d hints=%d last_error=%q", circuits, preferred, hints, lastError)
	}
}

func TestSentinelRuntimeCompatibilityCachesAreBounded(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	for index := 0; index < sentinelSDKPreferredMax+100; index++ {
		manager.markPreferredForChallenge(
			manager.cacheGeneration,
			"hash",
			SentinelProgramTurnstile,
			fmt.Sprintf("signature-%d", index),
			fmt.Sprintf("challenge-%d", index),
			"requirements",
		)
	}
	for index := 0; index < sentinelSDKCircuitMax+10; index++ {
		manager.recordCircuit(manager.cacheGeneration, fmt.Sprintf("hash-%d", index))
	}
	for index := 0; index < sentinelSDKSourceCircuitMax+10; index++ {
		_ = manager.recordSourceFailure(manager.cacheGeneration, fmt.Sprintf("source-%d", index), errors.New("SDK download failed"))
	}
	manager.mu.Lock()
	preferredCount := len(manager.preferred)
	preferredHintCount := len(manager.preferredHints)
	circuitCount := len(manager.circuits)
	sourceCircuitCount := len(manager.sourceCircuits)
	manager.mu.Unlock()
	if preferredCount > sentinelSDKPreferredMax || preferredHintCount > sentinelSDKPreferredMax || circuitCount > sentinelSDKCircuitMax || sourceCircuitCount > sentinelSDKSourceCircuitMax {
		t.Fatalf("cache sizes: preferred=%d hints=%d circuits=%d source_circuits=%d", preferredCount, preferredHintCount, circuitCount, sourceCircuitCount)
	}
}

func TestSentinelRuntimeExpiredPreferredStateIsPrunedWithoutSource(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	now := time.Unix(2000, 0)
	manager.now = func() time.Time { return now }
	manager.markPreferredForChallenge(manager.cacheGeneration, "hash", SentinelProgramTurnstile, "signature", "challenge", "requirements")
	now = now.Add(sentinelSDKPreferredTTL + time.Second)
	if manager.hasActivePreferred() || manager.hasPreferred.Load() {
		t.Fatal("expired preferred state remained active")
	}
	manager.mu.Lock()
	preferredCount := len(manager.preferred)
	manager.mu.Unlock()
	if preferredCount != 0 {
		t.Fatalf("expired preferred entries = %d", preferredCount)
	}
}

func TestSentinelRuntimeSourceSingleflightSeparatesExpectedHashes(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	_, request := sentinelRuntimeTestRequests(t, false)
	goodDigest := sha256.Sum256([]byte(sentinelRuntimeTestSDK))
	badDigest := sha256.Sum256([]byte("different SDK"))
	var fetches atomic.Int64
	request.Fetcher = func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
		fetches.Add(1)
		time.Sleep(10 * time.Millisecond)
		return []byte(sentinelRuntimeTestSDK), "application/javascript", target, nil
	}
	good := request
	good.ExpectedSHA256 = hexDigest(goodDigest[:])
	bad := request
	bad.ExpectedSHA256 = hexDigest(badDigest[:])
	var wait sync.WaitGroup
	wait.Add(2)
	errorsOut := make(chan error, 2)
	go func() {
		defer wait.Done()
		_, err := manager.source(context.Background(), good)
		errorsOut <- err
	}()
	go func() {
		defer wait.Done()
		_, err := manager.source(context.Background(), bad)
		errorsOut <- err
	}()
	wait.Wait()
	close(errorsOut)
	successes := 0
	failures := 0
	for sourceErr := range errorsOut {
		if sourceErr == nil {
			successes++
		} else {
			failures++
		}
	}
	if successes != 1 || failures != 1 || fetches.Load() != 2 {
		t.Fatalf("successes=%d failures=%d fetches=%d", successes, failures, fetches.Load())
	}
}

func TestSentinelRuntimeSourceCancellationDoesNotPoisonSharedFetch(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	_, request := sentinelRuntimeTestRequests(t, false)
	started := make(chan struct{})
	release := make(chan struct{})
	var fetches atomic.Int64
	request.Fetcher = func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
		if fetches.Add(1) == 1 {
			close(started)
		}
		<-release
		return []byte(sentinelRuntimeTestSDK), "application/javascript", target, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, sourceErr := manager.source(ctx, request)
		result <- sourceErr
	}()
	<-started
	sharedResult := make(chan error, 1)
	go func() {
		_, sourceErr := manager.source(context.Background(), request)
		sharedResult <- sourceErr
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("source() error = %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("canceled source() waited for the shared fetch")
	}
	close(release)
	if err := <-sharedResult; err != nil {
		t.Fatalf("shared source() error = %v", err)
	}
	if _, err := manager.source(t.Context(), request); err != nil {
		t.Fatalf("cached source() error = %v", err)
	}
	if fetches.Load() != 1 {
		t.Fatalf("SDK fetches = %d", fetches.Load())
	}
}

func TestSentinelRuntimeSourceFetchHasNoTotalDeadline(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	_, request := sentinelRuntimeTestRequests(t, false)
	request.Fetcher = func(ctx context.Context, target string, _ int64) ([]byte, string, string, error) {
		if _, hasDeadline := ctx.Deadline(); hasDeadline {
			return nil, "", "", errors.New("SDK fetch unexpectedly has a total deadline")
		}
		return []byte(sentinelRuntimeTestSDK), "application/javascript", target, nil
	}
	if _, err := manager.source(t.Context(), request); err != nil {
		t.Fatalf("source() error = %v", err)
	}
}

func TestSentinelRuntimeSourceFailureOpensBoundedCircuit(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	now := time.Unix(3000, 0)
	manager.now = func() time.Time { return now }
	_, request := sentinelRuntimeTestRequests(t, false)
	var fetches atomic.Int64
	request.Fetcher = func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
		if fetches.Add(1) == 1 {
			return nil, "", "", errors.New("temporary SDK download failure")
		}
		return []byte(sentinelRuntimeTestSDK), "application/javascript", target, nil
	}
	_, err := manager.source(t.Context(), request)
	var runtimeErr *SentinelRuntimeError
	if !errors.As(err, &runtimeErr) || runtimeErr.Code != "sentinel_sdk_unavailable" || runtimeErr.RetryAfter != sentinelSDKCircuitBreakerTTL {
		t.Fatalf("first source() error = %#v", err)
	}
	_, err = manager.source(t.Context(), request)
	if !errors.As(err, &runtimeErr) || runtimeErr.RetryAfter != sentinelSDKCircuitBreakerTTL || fetches.Load() != 1 {
		t.Fatalf("circuit source() error = %#v, fetches = %d", err, fetches.Load())
	}
	now = now.Add(sentinelSDKCircuitBreakerTTL + time.Second)
	if _, err = manager.source(t.Context(), request); err != nil {
		t.Fatalf("source() after circuit expiry error = %v", err)
	}
	if fetches.Load() != 2 {
		t.Fatalf("SDK fetches after circuit expiry = %d", fetches.Load())
	}
}

func TestSentinelRuntimeSourceFailureIsIsolatedByTransport(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	_, request := sentinelRuntimeTestRequests(t, false)
	request.TransportKey = "proxy-a"
	var failedFetches atomic.Int64
	request.Fetcher = func(_ context.Context, _ string, _ int64) ([]byte, string, string, error) {
		failedFetches.Add(1)
		return nil, "", "", errors.New("proxy a failed")
	}
	if _, err := manager.source(t.Context(), request); err == nil {
		t.Fatal("proxy A source() error = nil")
	}

	healthy := request
	healthy.TransportKey = "proxy-b"
	var healthyFetches atomic.Int64
	healthy.Fetcher = sentinelRuntimeTestFetcher(&healthyFetches)
	if _, err := manager.source(t.Context(), healthy); err != nil {
		t.Fatalf("proxy B source() error = %v", err)
	}
	if failedFetches.Load() != 1 || healthyFetches.Load() != 1 {
		t.Fatalf("fetches: failed=%d healthy=%d", failedFetches.Load(), healthyFetches.Load())
	}

	cached := healthy
	cached.TransportKey = "proxy-c"
	cached.Fetcher = func(context.Context, string, int64) ([]byte, string, string, error) {
		t.Fatal("validated SDK source was not shared across transports")
		return nil, "", "", nil
	}
	if _, err := manager.source(t.Context(), cached); err != nil {
		t.Fatalf("shared cached source() error = %v", err)
	}
}

func TestSentinelRuntimeSourceDoesNotRepopulateCacheAfterDisable(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	_, request := sentinelRuntimeTestRequests(t, false)
	started := make(chan struct{})
	release := make(chan struct{})
	request.Fetcher = func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
		close(started)
		<-release
		return []byte(sentinelRuntimeTestSDK), "application/javascript", target, nil
	}
	result := make(chan error, 1)
	go func() {
		_, sourceErr := manager.source(context.Background(), request)
		result <- sourceErr
	}()
	<-started
	manager.UpdateConfig(SentinelRuntimeConfig{Enabled: false, Workers: 1, QueueSize: 2, CacheVersions: 3})
	close(release)
	if err := <-result; err != nil {
		t.Fatalf("accepted in-flight source() error = %v", err)
	}
	if snapshot := manager.Snapshot(); snapshot.SourceCacheEntries != 0 || snapshot.BytecodeCacheEntries != 0 {
		t.Fatalf("snapshot after disable = %+v", snapshot)
	}
	manager.UpdateConfig(SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 2, CacheVersions: 3})
	var refetches atomic.Int64
	request.Fetcher = sentinelRuntimeTestFetcher(&refetches)
	if _, err := manager.source(t.Context(), request); err != nil {
		t.Fatalf("source() after re-enable error = %v", err)
	}
	if refetches.Load() != 1 {
		t.Fatalf("SDK refetches after re-enable = %d", refetches.Load())
	}
}

func TestSentinelRuntimeDisableLetsAcceptedSourceFetchFinishWithoutCircuit(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	_, request := sentinelRuntimeTestRequests(t, false)
	started := make(chan struct{})
	release := make(chan struct{})
	request.Fetcher = func(ctx context.Context, target string, _ int64) ([]byte, string, string, error) {
		close(started)
		select {
		case <-release:
			return []byte(sentinelRuntimeTestSDK), "application/javascript", target, nil
		case <-ctx.Done():
			return nil, "", "", ctx.Err()
		}
	}
	result := make(chan error, 1)
	go func() {
		_, sourceErr := manager.source(context.Background(), request)
		result <- sourceErr
	}()
	<-started
	manager.UpdateConfig(SentinelRuntimeConfig{Enabled: false, Workers: 1, QueueSize: 2, CacheVersions: 3})
	select {
	case err := <-result:
		t.Fatalf("accepted source fetch finished before release: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatalf("accepted source() error after disable = %v", err)
	}
	manager.mu.Lock()
	sourceCircuitCount := len(manager.sourceCircuits)
	manager.mu.Unlock()
	if sourceCircuitCount != 0 {
		t.Fatalf("source circuits after disable = %d", sourceCircuitCount)
	}
}

func TestSentinelRuntimeDisableLetsInitializingAndQueuedObserverFinish(t *testing.T) {
	manager := newSentinelRuntimeTestManagerWithConfig(SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 1, CacheVersions: 3})
	defer manager.Close()
	active, err := manager.acquire(t.Context(), sentinelPriorityFallback)
	if err != nil {
		t.Fatal(err)
	}
	_, request := sentinelRuntimeTestRequests(t, true)
	started := make(chan struct{})
	releaseSource := make(chan struct{})
	request.Fetcher = func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
		close(started)
		<-releaseSource
		return []byte(sentinelRuntimeTestSDK), "application/javascript", target, nil
	}
	observer, err := manager.BeginObserver(t.Context(), request)
	if err != nil || observer == nil {
		active.release()
		t.Fatalf("BeginObserver() = %#v, %v", observer, err)
	}
	defer observer.Close()
	waitForSentinelRuntimeState(t, manager, func(snapshot SentinelRuntimeSnapshot) bool { return snapshot.Queued == 1 })
	manager.UpdateConfig(SentinelRuntimeConfig{Enabled: false, Workers: 1, QueueSize: 1, CacheVersions: 3})
	if snapshot := manager.Snapshot(); snapshot.Queued != 1 || snapshot.SourcePending != 0 || snapshot.SourceWaiters != 0 || snapshot.Available {
		t.Fatalf("queued Observer state = %+v", snapshot)
	}
	active.release()
	<-started
	if snapshot := manager.Snapshot(); snapshot.SourcePending != 1 || snapshot.SourceWaiters != 1 || snapshot.Available {
		t.Fatalf("initializing Observer state = %+v", snapshot)
	}
	close(releaseSource)
	if err = observer.wait(t.Context()); err != nil {
		t.Fatalf("accepted Observer initialization error = %v", err)
	}
	if token, snapshotErr := observer.Snapshot(t.Context()); snapshotErr != nil || !strings.Contains(token, "sdk-snapshot-token") {
		t.Fatalf("Snapshot() = %q, %v", token, snapshotErr)
	}
	observer.Close()
	waitForSentinelRuntimeState(t, manager, func(snapshot SentinelRuntimeSnapshot) bool {
		return snapshot.Busy == 0 && snapshot.SourceCacheEntries == 0 && snapshot.BytecodeCacheEntries == 0
	})
}

func TestSentinelRuntimeDisableAndReenableKeepsAcceptedObserverGeneration(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	_, request := sentinelRuntimeTestRequests(t, true)
	request.Fetcher = sentinelRuntimeTestFetcher(nil)
	if _, err := manager.source(t.Context(), request); err != nil {
		t.Fatalf("prime source cache: %v", err)
	}
	active, err := manager.acquire(t.Context(), sentinelPriorityFallback)
	if err != nil {
		t.Fatal(err)
	}

	manager.mu.Lock()
	initialGeneration := manager.cacheGeneration
	manager.mu.Unlock()
	observer, err := manager.BeginObserver(t.Context(), request)
	if err != nil || observer == nil {
		active.release()
		t.Fatalf("BeginObserver() = %#v, %v", observer, err)
	}
	defer observer.Close()
	waitForSentinelRuntimeState(t, manager, func(snapshot SentinelRuntimeSnapshot) bool { return snapshot.Queued == 1 })
	manager.UpdateConfig(SentinelRuntimeConfig{Enabled: false, Workers: 1, QueueSize: 1, CacheVersions: 3})
	manager.UpdateConfig(SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 1, CacheVersions: 3})
	manager.mu.Lock()
	generationWhileDraining := manager.cacheGeneration
	manager.mu.Unlock()
	if generationWhileDraining != initialGeneration {
		active.release()
		t.Fatalf("generation while accepted Observer drains = %d, want %d", generationWhileDraining, initialGeneration)
	}

	active.release()
	if err = observer.wait(t.Context()); err != nil {
		t.Fatalf("accepted Observer initialization error = %v", err)
	}
	observer.Close()
	waitForSentinelRuntimeState(t, manager, func(snapshot SentinelRuntimeSnapshot) bool { return snapshot.Available })
	manager.mu.Lock()
	finalGeneration := manager.cacheGeneration
	manager.mu.Unlock()
	if finalGeneration != initialGeneration+1 {
		t.Fatalf("generation after Observer completion = %d, want %d", finalGeneration, initialGeneration+1)
	}
}

func TestSentinelRuntimeSourceRejectsFlightFromDifferentGeneration(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	_, request := sentinelRuntimeTestRequests(t, false)
	request.Fetcher = sentinelRuntimeTestFetcher(nil)
	sourceURL, _, err := resolveSentinelSDKRequestSource(request)
	if err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	generation := manager.cacheGeneration
	flightKey := sourceURL + "\x00\x00default\x00" + strconv.FormatUint(generation, 10)
	flight := &sentinelSourceFlight{
		done: make(chan struct{}),
		entry: &sentinelSourceCacheEntry{
			url:        sourceURL,
			hash:       "different-generation",
			fetchedAt:  time.Now(),
			generation: generation + 1,
		},
	}
	close(flight.done)
	manager.sourceFlights[flightKey] = flight
	manager.mu.Unlock()
	defer func() {
		manager.mu.Lock()
		delete(manager.sourceFlights, flightKey)
		manager.mu.Unlock()
	}()

	_, err = manager.source(t.Context(), request)
	var runtimeErr *SentinelRuntimeError
	if !errors.As(err, &runtimeErr) || runtimeErr.Code != "sentinel_sdk_unavailable" {
		t.Fatalf("source() error = %#v", err)
	}
}

func TestSentinelRuntimeSourceAndBytecodeSingleflight(t *testing.T) {
	manager := newSentinelRuntimeTestManagerWithConfig(SentinelRuntimeConfig{Enabled: true, Workers: 4, QueueSize: 8, CacheVersions: 3})
	defer manager.Close()
	request, sdkRequest := sentinelRuntimeTestRequests(t, false)
	var fetches atomic.Int64
	sdkRequest.Fetcher = func(ctx context.Context, target string, limit int64) ([]byte, string, string, error) {
		fetches.Add(1)
		time.Sleep(20 * time.Millisecond)
		return []byte(sentinelRuntimeTestSDK), "text/javascript; charset=utf-8", target, nil
	}
	const workers = 4
	var wait sync.WaitGroup
	errorsOut := make(chan error, workers)
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, solveErr := manager.SolveTurnstile(context.Background(), request, sdkRequest, nil)
			errorsOut <- solveErr
		}()
	}
	wait.Wait()
	close(errorsOut)
	for solveErr := range errorsOut {
		if solveErr != nil {
			t.Fatalf("SolveTurnstile() error = %v", solveErr)
		}
	}
	if fetches.Load() != 1 {
		t.Fatalf("SDK fetches = %d", fetches.Load())
	}
	if manager.maxActiveQJS.Load() > workers {
		t.Fatalf("max active QJS = %d", manager.maxActiveQJS.Load())
	}
}

func TestSentinelRuntimeBytecodeFollowerCanCancelWithoutPoisoningCompile(t *testing.T) {
	manager := newSentinelRuntimeTestManager()
	defer manager.Close()
	active, err := manager.acquire(t.Context(), sentinelPriorityFallback)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(sentinelRuntimeTestSDK))
	source := &sentinelSourceCacheEntry{
		key:       "https://sentinel.openai.com/sentinel/test/sdk.js",
		url:       "https://sentinel.openai.com/sentinel/test/sdk.js",
		hash:      hexDigest(digest[:]),
		source:    []byte(sentinelRuntimeTestSDK),
		fetchedAt: time.Now(),
	}
	first := make(chan error, 1)
	go func() {
		_, compileErr := manager.bytecode(context.Background(), source, sentinelPriorityPrecompile)
		first <- compileErr
	}()
	waitForSentinelRuntimeState(t, manager, func(snapshot SentinelRuntimeSnapshot) bool { return snapshot.Queued == 1 })
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = manager.bytecode(canceledCtx, source, sentinelPriorityPrecompile); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled bytecode() error = %v", err)
	}
	active.release()
	if err = <-first; err != nil {
		t.Fatalf("shared bytecode compile error = %v", err)
	}
}

func TestSentinelRuntimeBytecodeFollowerHonorsZeroQueue(t *testing.T) {
	manager := newSentinelRuntimeTestManagerWithConfig(SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 0, CacheVersions: 3})
	defer manager.Close()
	digest := sha256.Sum256([]byte(sentinelRuntimeTestSDK))
	source := &sentinelSourceCacheEntry{
		url:        "https://sentinel.openai.com/sentinel/20260721/sdk.js",
		hash:       hexDigest(digest[:]),
		source:     []byte(sentinelRuntimeTestSDK),
		fetchedAt:  time.Now(),
		generation: manager.cacheGeneration,
	}
	key := source.hash + ":" + sentinelSDKAdapterVersion
	flightKey := key + "\x00" + strconv.FormatUint(manager.cacheGeneration, 10)
	flight := &sentinelBytecodeFlight{done: make(chan struct{})}
	manager.mu.Lock()
	manager.bytecodeFlights[flightKey] = flight
	manager.mu.Unlock()
	defer manager.completeBytecodeFlight(flightKey, flight, nil, errors.New("test flight stopped"), false)

	_, err := manager.bytecode(t.Context(), source, sentinelPriorityFallback)
	var runtimeErr *SentinelRuntimeError
	if !errors.As(err, &runtimeErr) || runtimeErr.Code != "sentinel_sdk_busy" {
		t.Fatalf("bytecode() error = %#v", err)
	}
	manager.mu.Lock()
	waiters := manager.bytecodeWaiters
	manager.mu.Unlock()
	if waiters != 0 {
		t.Fatalf("bytecode waiters = %d, want 0", waiters)
	}
}

func TestSentinelRuntimeBytecodeFollowersShareFlightAndCanCancel(t *testing.T) {
	manager := newSentinelRuntimeTestManagerWithConfig(SentinelRuntimeConfig{Enabled: true, Workers: 2, QueueSize: 2, CacheVersions: 3})
	defer manager.Close()
	lease, err := manager.acquire(t.Context(), sentinelPriorityObserverCollector)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.release()
	digest := sha256.Sum256([]byte(sentinelRuntimeTestSDK))
	source := &sentinelSourceCacheEntry{
		key:       "https://sentinel.openai.com/sentinel/20260721/sdk.js",
		url:       "https://sentinel.openai.com/sentinel/20260721/sdk.js",
		hash:      hexDigest(digest[:]),
		source:    []byte(sentinelRuntimeTestSDK),
		fetchedAt: time.Now(),
	}
	key := source.hash + ":" + sentinelSDKAdapterVersion
	manager.mu.Lock()
	flightKey := key + "\x00" + strconv.FormatUint(manager.cacheGeneration, 10)
	flight := &sentinelBytecodeFlight{done: make(chan struct{})}
	manager.bytecodeFlights[flightKey] = flight
	manager.mu.Unlock()
	defer manager.completeBytecodeFlight(flightKey, flight, nil, errors.New("test flight stopped"), false)

	for _, test := range []struct {
		name string
		call func(context.Context) ([]byte, error)
	}{
		{name: "reserved", call: func(ctx context.Context) ([]byte, error) {
			return manager.bytecodeWithLease(ctx, source, sentinelPriorityObserverCollector, lease)
		}},
		{name: "nonreserved", call: func(ctx context.Context) ([]byte, error) {
			return manager.bytecode(ctx, source, sentinelPriorityFallback)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			started := time.Now()
			if _, callErr := test.call(ctx); !errors.Is(callErr, context.DeadlineExceeded) {
				t.Fatalf("follower error = %v", callErr)
			}
			if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
				t.Fatalf("follower cancellation took %s", elapsed)
			}
			manager.mu.Lock()
			waiters := manager.bytecodeWaiters
			manager.mu.Unlock()
			if waiters != 0 {
				t.Fatalf("bytecode waiters after cancellation = %d", waiters)
			}
		})
	}
}

func TestSentinelRuntimeBytecodeCompileUsesCallerPriority(t *testing.T) {
	manager := newSentinelRuntimeTestManagerWithConfig(SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 2, CacheVersions: 3})
	defer manager.Close()
	active, err := manager.acquire(t.Context(), sentinelPriorityFallback)
	if err != nil {
		t.Fatal(err)
	}
	newSource := func(suffix string) *sentinelSourceCacheEntry {
		sourceBytes := []byte(sentinelRuntimeTestSDK + "\n// " + suffix)
		digest := sha256.Sum256(sourceBytes)
		return &sentinelSourceCacheEntry{
			key:       "https://sentinel.openai.com/sentinel/" + suffix + "/sdk.js",
			url:       "https://sentinel.openai.com/sentinel/" + suffix + "/sdk.js",
			hash:      hexDigest(digest[:]),
			source:    sourceBytes,
			fetchedAt: time.Now(),
		}
	}
	type result struct {
		priority sentinelTaskPriority
		err      error
	}
	results := make(chan result, 2)
	sharedSource := newSource("shared")
	go func() {
		_, compileErr := manager.bytecode(context.Background(), sharedSource, sentinelPriorityPrecompile)
		results <- result{priority: sentinelPriorityPrecompile, err: compileErr}
	}()
	waitForSentinelRuntimeState(t, manager, func(snapshot SentinelRuntimeSnapshot) bool { return snapshot.Queued == 1 })
	go func() {
		_, compileErr := manager.bytecode(context.Background(), sharedSource, sentinelPriorityFallback)
		results <- result{priority: sentinelPriorityFallback, err: compileErr}
	}()
	waitForSentinelRuntimeState(t, manager, func(snapshot SentinelRuntimeSnapshot) bool { return snapshot.Queued == 2 })
	active.release()
	first := <-results
	if first.err != nil {
		t.Fatalf("first bytecode compile error = %v", first.err)
	}
	if first.priority != sentinelPriorityFallback {
		t.Fatalf("first bytecode compile priority = %d, want fallback", first.priority)
	}
	if second := <-results; second.err != nil {
		t.Fatalf("second bytecode compile error = %v", second.err)
	}
}

func newSentinelRuntimeTestManagerWithConfig(config SentinelRuntimeConfig) *SentinelRuntimeManager {
	return newSentinelRuntimeManager(config, func(hash string, source []byte) (sentinelSDKAdapter, bool) {
		if adapter, known := resolveKnownSentinelSDKAdapter(hash); known {
			return adapter, true
		}
		return resolveSentinelSDKAdapter(hash, source)
	})
}

func newSentinelRuntimeTestManager() *SentinelRuntimeManager {
	manager := newSentinelRuntimeTestManagerWithConfig(SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 2, CacheVersions: 3})
	manager.random = zeroReader{}
	return manager
}

func sentinelRuntimeTestRequests(t *testing.T, observer bool) (ConversationTurnstileSolveRequest, SentinelSDKRequest) {
	t.Helper()
	const requirementsToken = "requirements"
	dx := encodeConversationTurnstileProgram(t, requirementsToken, []any{[]any{36, "argument"}})
	challenge := map[string]any{
		"token": "challenge-token",
		"turnstile": map[string]any{
			"required": true,
			"dx":       dx,
		},
	}
	if observer {
		challenge["so"] = map[string]any{
			"required":     true,
			"collector_dx": dx,
			"snapshot_dx":  dx,
		}
	}
	environment := ConversationTurnstileEnvironment{
		Persona:       DefaultPersona(),
		ScriptSources: []string{"https://sentinel.openai.com/sentinel/20260721/sdk.js"},
		Location:      "https://chatgpt.com/",
	}
	return ConversationTurnstileSolveRequest{
			DX:                dx,
			RequirementsToken: requirementsToken,
			Environment:       environment,
			Reader:            zeroReader{},
			Now:               time.Now,
		}, SentinelSDKRequest{
			BaseURL:           "https://chatgpt.com",
			ScriptSources:     environment.ScriptSources,
			Challenge:         challenge,
			RequirementsToken: requirementsToken,
			Environment:       environment,
			DeviceID:          "device-id",
			Flow:              "conversation",
		}
}

func sentinelRuntimeTestFetcher(counter *atomic.Int64) SentinelSDKFetcher {
	return func(_ context.Context, target string, _ int64) ([]byte, string, string, error) {
		if counter != nil {
			counter.Add(1)
		}
		return []byte(sentinelRuntimeTestSDK), "application/javascript", target, nil
	}
}

func waitForSentinelRuntimeState(t *testing.T, manager *SentinelRuntimeManager, predicate func(SentinelRuntimeSnapshot) bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if predicate(manager.Snapshot()) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("runtime state was not reached: %+v", manager.Snapshot())
}

func waitForSentinelRuntimeSourcePending(t *testing.T, manager *SentinelRuntimeManager, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		manager.mu.Lock()
		pending := manager.sourcePending
		manager.mu.Unlock()
		if pending == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	manager.mu.Lock()
	pending := manager.sourcePending
	manager.mu.Unlock()
	t.Fatalf("source pending = %d, want %d", pending, want)
}

func waitForSentinelRuntimeSourceWaiters(t *testing.T, manager *SentinelRuntimeManager, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		manager.mu.Lock()
		waiters := manager.sourceWaiters
		manager.mu.Unlock()
		if waiters == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	manager.mu.Lock()
	waiters := manager.sourceWaiters
	manager.mu.Unlock()
	t.Fatalf("source waiters = %d, want %d", waiters, want)
}

func hexDigest(value []byte) string {
	return fmt.Sprintf("%x", value)
}

func BenchmarkSentinelRuntimeManagerGoVMHotPath(b *testing.B) {
	const requirementsToken = "requirements-token"
	dx := encodeConversationTurnstileProgram(b, requirementsToken, []any{
		[]any{2, 40, "turn"},
		[]any{2, 41, "stile"},
		[]any{5, 40, 41},
		[]any{7, 3, 40},
	})
	request := ConversationTurnstileSolveRequest{
		DX:                dx,
		RequirementsToken: requirementsToken,
		Environment:       ConversationTurnstileEnvironment{Persona: DefaultPersona()},
		Reader:            zeroReader{},
		Now:               func() time.Time { return time.Unix(1_700_000_000, 0) },
	}
	manager := newSentinelRuntimeTestManagerWithConfig(SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 1, CacheVersions: 3})
	b.Cleanup(manager.Close)
	const sourceURL = "https://sentinel.openai.com/sentinel/20260724/sdk.js"
	digest := sha256.Sum256([]byte(sentinelRuntimeTestSDK))
	manager.mu.Lock()
	manager.putSourceLocked(&sentinelSourceCacheEntry{
		key:       sourceURL + "\x00" + hexDigest(digest[:]),
		url:       sourceURL,
		hash:      hexDigest(digest[:]),
		source:    []byte(sentinelRuntimeTestSDK),
		fetchedAt: time.Now(),
	})
	manager.mu.Unlock()
	primedSDKRequest := SentinelSDKRequest{BaseURL: "https://chatgpt.com", ScriptSources: []string{sourceURL}}
	b.Run("legacy", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := BuildConversationTurnstileTokenWithEnvironment(b.Context(), request.DX, request.RequirementsToken, request.Environment, request.Reader, request.Now); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("strict_go_vm", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := (GoConversationTurnstileSolver{}).Solve(b.Context(), request); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("sdk_enabled_go_vm", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := manager.SolveTurnstile(b.Context(), request, SentinelSDKRequest{}, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("sdk_enabled_primed_source", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := manager.SolveTurnstile(b.Context(), request, primedSDKRequest, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
	manager.markPreferredForChallenge(
		manager.cacheGeneration,
		hexDigest(digest[:]),
		SentinelProgramTurnstile,
		"different-program",
		"different-challenge",
		request.RequirementsToken,
	)
	b.Run("sdk_enabled_preferred_miss", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := manager.SolveTurnstile(b.Context(), request, primedSDKRequest, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkSentinelQJSIsolatedRuntimeLifecycle(b *testing.B) {
	manager := newSentinelRuntimeTestManagerWithConfig(SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 1, CacheVersions: 1})
	defer manager.Close()
	lease, err := manager.acquire(b.Context(), sentinelPriorityFallback)
	if err != nil {
		b.Fatal(err)
	}
	defer lease.release()
	worker, err := lease.runtimeWorker(b.Context())
	if err != nil {
		b.Fatal(err)
	}
	if err = worker.run(b.Context(), manager.cacheGeneration, func(runtimeQJS *qjs.Runtime) error {
		_, evalErr := runtimeQJS.Eval("warmup.js", qjs.Code("1 + 1"))
		return evalErr
	}); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for range b.N {
		if err = worker.run(b.Context(), manager.cacheGeneration, func(runtimeQJS *qjs.Runtime) error {
			_, evalErr := runtimeQJS.Eval("benchmark.js", qjs.Code("1 + 1"))
			return evalErr
		}); err != nil {
			b.Fatal(err)
		}
	}
}
