package auth

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

type refreshPersistenceAdmissionTestStore struct {
	concurrency int
	admission   int
}

type boundedRefreshPersistenceExecutor struct {
	*chatGPTWebUnauthorizedRefreshExecutor
	release chan struct{}
	calls   atomic.Int64
	active  atomic.Int64
	peak    atomic.Int64
}

func (executor *boundedRefreshPersistenceExecutor) Refresh(ctx context.Context, auth *Auth) (*Auth, error) {
	executor.calls.Add(1)
	active := executor.active.Add(1)
	defer executor.active.Add(-1)
	for {
		peak := executor.peak.Load()
		if active <= peak || executor.peak.CompareAndSwap(peak, active) {
			break
		}
	}
	select {
	case <-executor.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	updated := auth.Clone()
	updated.Metadata["access_token"] = "fresh"
	return updated, nil
}

func (*refreshPersistenceAdmissionTestStore) List(context.Context) ([]*Auth, error) { return nil, nil }
func (*refreshPersistenceAdmissionTestStore) Save(context.Context, *Auth) (string, error) {
	return "", nil
}
func (*refreshPersistenceAdmissionTestStore) Delete(context.Context, string) error { return nil }
func (store *refreshPersistenceAdmissionTestStore) RefreshPersistenceConcurrency() int {
	return store.concurrency
}
func (store *refreshPersistenceAdmissionTestStore) RefreshPersistenceAdmissionConcurrency() int {
	return store.admission
}

func TestRefreshPersistenceUsesSeparateLifecycleAdmission(t *testing.T) {
	coordinator := newRefreshPersistenceCoordinator(&refreshPersistenceAdmissionTestStore{
		concurrency: 1,
		admission:   4,
	})
	if snapshot := coordinator.snapshot(); snapshot.Concurrency != 4 || snapshot.TransactionConcurrency != 1 {
		t.Fatalf("refresh persistence metrics = %+v, want lifecycle admission 4 and transaction concurrency 1", snapshot)
	}
}

func TestRefreshPersistencePriorityHasBoundedFairness(t *testing.T) {
	coordinator := &refreshPersistenceCoordinator{concurrency: 1, queueLimit: 64}
	coordinator.mu.Lock()
	maintenance := &refreshPersistenceWaiter{authID: "maintenance", priority: RefreshPersistencePriorityMaintenance}
	importWaiter := &refreshPersistenceWaiter{authID: "import", priority: RefreshPersistencePriorityImport}
	coordinator.queues[int(RefreshPersistencePriorityMaintenance)] = append(
		coordinator.queues[int(RefreshPersistencePriorityMaintenance)],
		maintenance,
	)
	coordinator.queues[int(RefreshPersistencePriorityImport)] = append(
		coordinator.queues[int(RefreshPersistencePriorityImport)],
		importWaiter,
	)
	for index := 0; index < 32; index++ {
		coordinator.queues[int(RefreshPersistencePrioritySession)] = append(
			coordinator.queues[int(RefreshPersistencePrioritySession)],
			&refreshPersistenceWaiter{authID: "session", priority: RefreshPersistencePrioritySession},
		)
	}
	importPosition := -1
	maintenancePosition := -1
	for position := 0; position < 20; position++ {
		waiter := coordinator.popWaiterLocked()
		if waiter == importWaiter {
			importPosition = position
		}
		if waiter == maintenance {
			maintenancePosition = position
		}
	}
	coordinator.mu.Unlock()
	if importPosition < 0 || importPosition > refreshPersistenceSessionBurst {
		t.Fatalf("import waiter position = %d, want at most %d", importPosition, refreshPersistenceSessionBurst)
	}
	if maintenancePosition < 0 || maintenancePosition > refreshPersistenceMaintenanceBurst {
		t.Fatalf("maintenance waiter position = %d, want at most %d", maintenancePosition, refreshPersistenceMaintenanceBurst)
	}
}

func TestRefreshPersistenceQueueCancellationDoesNotLeakSlot(t *testing.T) {
	coordinator := &refreshPersistenceCoordinator{concurrency: 1, queueLimit: 1}
	active, errActive := coordinator.acquireContext(t.Context(), RefreshPersistencePrioritySession, "active")
	if errActive != nil {
		t.Fatal(errActive)
	}
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() {
		_, errAcquire := coordinator.acquireContext(ctx, RefreshPersistencePriorityMaintenance, "queued")
		result <- errAcquire
	}()
	deadline := time.Now().Add(time.Second)
	for coordinator.snapshot().Queued != 1 {
		if time.Now().After(deadline) {
			t.Fatal("refresh persistence waiter did not queue")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if errAcquire := <-result; !errors.Is(errAcquire, context.Canceled) {
		t.Fatalf("queued acquire error = %v, want context canceled", errAcquire)
	}
	if snapshot := coordinator.snapshot(); snapshot.Queued != 0 || snapshot.Active != 1 {
		t.Fatalf("refresh persistence snapshot after cancellation = %#v", snapshot)
	}
	active.release()
	if snapshot := coordinator.snapshot(); snapshot.Active != 0 {
		t.Fatalf("refresh persistence active after release = %d", snapshot.Active)
	}
}

func TestRefreshPersistenceGrantRecordsQueueWait(t *testing.T) {
	coordinator := &refreshPersistenceCoordinator{concurrency: 1, queueLimit: 1}
	active, errActive := coordinator.acquireContext(
		t.Context(),
		RefreshPersistencePrioritySession,
		"active",
	)
	if errActive != nil {
		t.Fatal(errActive)
	}
	result := make(chan *refreshPersistenceReservation, 1)
	go func() {
		reservation, _ := coordinator.acquireContext(
			context.Background(),
			RefreshPersistencePriorityMaintenance,
			"queued",
		)
		result <- reservation
	}()
	deadline := time.Now().Add(time.Second)
	for coordinator.snapshot().Queued != 1 {
		if time.Now().After(deadline) {
			t.Fatal("refresh persistence waiter did not queue")
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(5 * time.Millisecond)
	active.release()
	reservation := <-result
	if reservation == nil {
		t.Fatal("queued refresh did not receive a reservation")
	}
	reservation.release()
	if snapshot := coordinator.snapshot(); snapshot.QueueWaitNanos < uint64(time.Millisecond) {
		t.Fatalf("queue wait nanos = %d, want a recorded wait", snapshot.QueueWaitNanos)
	}
}

func TestRefreshPersistenceCanceledGrantIsReleased(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previousProcs)
	for iteration := 0; iteration < 64; iteration++ {
		coordinator := &refreshPersistenceCoordinator{concurrency: 1, queueLimit: 1}
		active, errActive := coordinator.acquireContext(
			t.Context(),
			RefreshPersistencePrioritySession,
			"active",
		)
		if errActive != nil {
			t.Fatal(errActive)
		}
		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan error, 1)
		go func() {
			reservation, errAcquire := coordinator.acquireContext(
				ctx,
				RefreshPersistencePriorityMaintenance,
				"queued",
			)
			if reservation != nil {
				reservation.release()
			}
			result <- errAcquire
		}()
		deadline := time.Now().Add(time.Second)
		for coordinator.snapshot().Queued != 1 {
			if time.Now().After(deadline) {
				t.Fatal("refresh persistence waiter did not queue")
			}
			runtime.Gosched()
		}
		cancel()
		active.release()
		if errAcquire := <-result; !errors.Is(errAcquire, context.Canceled) {
			t.Fatalf("iteration %d canceled grant error = %v", iteration, errAcquire)
		}
		if snapshot := coordinator.snapshot(); snapshot.Active != 0 || snapshot.Queued != 0 {
			t.Fatalf("iteration %d canceled grant leaked capacity: %#v", iteration, snapshot)
		}
	}
}

func TestRefreshPersistenceCanceledContextDoesNotAcquireAvailableSlot(t *testing.T) {
	coordinator := &refreshPersistenceCoordinator{concurrency: 1, queueLimit: 1}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	reservation, errAcquire := coordinator.acquireContext(
		ctx,
		RefreshPersistencePrioritySession,
		"canceled",
	)
	if !errors.Is(errAcquire, context.Canceled) {
		t.Fatalf("canceled acquire error = %v, want context canceled", errAcquire)
	}
	if reservation != nil {
		t.Fatal("canceled acquire unexpectedly returned a reservation")
	}
	if snapshot := coordinator.snapshot(); snapshot.Active != 0 || snapshot.Acquired != 0 {
		t.Fatalf("canceled acquire snapshot = %#v", snapshot)
	}
}

func TestRefreshPersistenceCloseRejectsQueuedWorkWithoutLeakingActiveSlot(t *testing.T) {
	coordinator := &refreshPersistenceCoordinator{concurrency: 1, queueLimit: 1}
	active, errActive := coordinator.acquireContext(
		t.Context(),
		RefreshPersistencePrioritySession,
		"active",
	)
	if errActive != nil {
		t.Fatal(errActive)
	}
	result := make(chan error, 1)
	go func() {
		_, errAcquire := coordinator.acquireContext(
			context.Background(),
			RefreshPersistencePriorityMaintenance,
			"queued",
		)
		result <- errAcquire
	}()
	deadline := time.Now().Add(time.Second)
	for coordinator.snapshot().Queued != 1 {
		if time.Now().After(deadline) {
			t.Fatal("refresh persistence waiter did not queue")
		}
		time.Sleep(time.Millisecond)
	}
	coordinator.close()
	var authErr *Error
	if errAcquire := <-result; !errors.As(errAcquire, &authErr) || authErr.Code != "refresh_persist_closed" {
		t.Fatalf("queued acquire after close error = %#v", errAcquire)
	}
	if snapshot := coordinator.snapshot(); snapshot.Queued != 0 || snapshot.Active != 1 {
		t.Fatalf("closed coordinator snapshot = %#v", snapshot)
	}
	active.release()
	if snapshot := coordinator.snapshot(); snapshot.Active != 0 {
		t.Fatalf("closed coordinator active = %d, want zero", snapshot.Active)
	}
}

func TestRefreshAuthAdmissionFailureReleasesRuntimeExecution(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		block func(*refreshPersistenceCoordinator) func()
	}{
		{
			name: "queue full",
			block: func(coordinator *refreshPersistenceCoordinator) func() {
				coordinator.queueLimit = 0
				reservation, errAcquire := coordinator.acquireContext(
					t.Context(),
					RefreshPersistencePrioritySession,
					"blocker",
				)
				if errAcquire != nil {
					t.Fatal(errAcquire)
				}
				return reservation.release
			},
		},
		{
			name: "coordinator closed",
			block: func(coordinator *refreshPersistenceCoordinator) func() {
				coordinator.close()
				return func() {}
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			manager := NewManager(&chatGPTWebRefreshPersistenceStore{}, nil, nil)
			manager.RegisterExecutor(&chatGPTWebUnauthorizedRefreshExecutor{})
			installed, errRegister := manager.Register(WithSkipPersist(t.Context()), &Auth{
				ID:       "admission-runtime-" + testCase.name,
				Provider: "chatgpt-web",
				Status:   StatusActive,
				Metadata: map[string]any{
					"type":             "chatgpt-web",
					"access_token":     "stale",
					"refresh_token":    "refresh",
					"refresh_strategy": "web_oauth_rt",
					"lifecycle_state":  LifecycleStateActive,
				},
			})
			if errRegister != nil {
				t.Fatal(errRegister)
			}
			coordinator := manager.refreshPersistence.Load()
			releaseBlocker := testCase.block(coordinator)
			manager.refreshAuthExpected(t.Context(), installed.ID, installed, time.Time{})

			manager.mu.RLock()
			trackedExecutions := len(manager.refreshExecutions)
			manager.mu.RUnlock()
			if trackedExecutions != 0 {
				t.Fatalf("tracked refresh executions = %d, want zero", trackedExecutions)
			}
			releaseBlocker()
			if errClose := manager.CloseExecutors(); errClose != nil {
				t.Fatal(errClose)
			}
		})
	}
}

func TestRequestRefreshBackpressurePrecedesTokenExchange(t *testing.T) {
	store := &chatGPTWebRefreshPersistenceStore{}
	manager := NewManager(store, nil, nil)
	executor := &chatGPTWebUnauthorizedRefreshExecutor{}
	manager.RegisterExecutor(executor)
	installed, errRegister := manager.Register(WithSkipPersist(t.Context()), &Auth{
		ID:       "refresh-admission-full",
		Provider: "chatgpt-web",
		Status:   StatusActive,
		Metadata: map[string]any{
			"type":             "chatgpt-web",
			"access_token":     "stale",
			"refresh_token":    "refresh",
			"refresh_strategy": "web_oauth_rt",
			"lifecycle_state":  LifecycleStateActive,
		},
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	coordinator := manager.refreshPersistence.Load()
	coordinator.queueLimit = 0
	active, errActive := coordinator.acquireContext(t.Context(), RefreshPersistencePrioritySession, "blocker")
	if errActive != nil {
		t.Fatal(errActive)
	}
	flight, errStart := manager.startChatGPTWebRequestRefreshFlight(
		t.Context(),
		installed.ID,
		"stale",
		installed,
	)
	if errStart != nil {
		t.Fatal(errStart)
	}
	<-flight.done
	result := flight.result
	var authErr *Error
	if !errors.As(result.Err, &authErr) || authErr.Code != "refresh_persist_backpressure" {
		t.Fatalf("refresh flight error = %#v", result.Err)
	}
	if executor.refreshCalls != 0 {
		t.Fatalf("refresh calls = %d, want zero before persistence admission", executor.refreshCalls)
	}
	active.release()
	if errClose := manager.CloseExecutors(); errClose != nil {
		t.Fatal(errClose)
	}
}

func TestDifferentCredentialRefreshStormIsLifecycleBounded(t *testing.T) {
	store := &refreshPersistenceAdmissionTestStore{concurrency: 1, admission: 4}
	manager := NewManager(store, nil, nil)
	manager.refreshPersistence.Load().queueLimit = 8
	executor := &boundedRefreshPersistenceExecutor{
		chatGPTWebUnauthorizedRefreshExecutor: &chatGPTWebUnauthorizedRefreshExecutor{},
		release:                               make(chan struct{}),
	}
	manager.RegisterExecutor(executor)
	const total = 20
	flights := make([]*chatGPTWebRequestRefreshFlight, 0, total)
	for index := 0; index < total; index++ {
		authID := fmt.Sprintf("refresh-storm-%02d", index)
		installed, errRegister := manager.Register(WithSkipPersist(t.Context()), &Auth{
			ID: authID, Provider: "chatgpt-web", Status: StatusActive,
			Metadata: map[string]any{
				"type":             "chatgpt-web",
				"access_token":     "stale",
				"refresh_token":    "refresh",
				"refresh_strategy": "web_oauth_rt",
				"lifecycle_state":  LifecycleStateActive,
			},
		})
		if errRegister != nil {
			t.Fatal(errRegister)
		}
		flight, errStart := manager.startChatGPTWebRequestRefreshFlight(
			t.Context(), authID, "stale", installed,
		)
		if errStart != nil {
			t.Fatal(errStart)
		}
		flights = append(flights, flight)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		snapshot := manager.RefreshPersistenceMetrics()
		if snapshot.Active == 4 && snapshot.Queued == 8 && snapshot.Rejected == 8 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("refresh storm metrics = %#v", snapshot)
		}
		time.Sleep(time.Millisecond)
	}
	if calls := executor.calls.Load(); calls != 4 {
		t.Fatalf("token exchanges before capacity release = %d, want 4", calls)
	}
	close(executor.release)
	rejected := 0
	for _, flight := range flights {
		<-flight.done
		result := flight.result
		if result.Err == nil {
			continue
		}
		var authErr *Error
		if errors.As(result.Err, &authErr) && authErr.Code == "refresh_persist_backpressure" {
			rejected++
			continue
		}
		t.Fatalf("refresh storm result error = %v", result.Err)
	}
	if rejected != 8 || executor.calls.Load() != 12 || executor.peak.Load() > 4 {
		t.Fatalf("refresh storm rejected=%d calls=%d peak=%d", rejected, executor.calls.Load(), executor.peak.Load())
	}
	if errClose := manager.CloseExecutors(); errClose != nil {
		t.Fatal(errClose)
	}
}
