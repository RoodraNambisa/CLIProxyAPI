package auth

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"
)

const refreshPersistenceQueueLimit = 512

const (
	refreshPersistenceSessionBurst     = 8
	refreshPersistenceMaintenanceBurst = 16
)

// RefreshPersistenceMetricsSnapshot reports process-local refresh write pressure.
type RefreshPersistenceMetricsSnapshot struct {
	Enabled                bool                             `json:"enabled"`
	Concurrency            int                              `json:"concurrency"`
	TransactionConcurrency int                              `json:"transaction_concurrency"`
	QueueLimit             int                              `json:"queue_limit"`
	Queued                 int64                            `json:"queued"`
	QueuedSession          int64                            `json:"queued_session"`
	QueuedImport           int64                            `json:"queued_import"`
	QueuedMaintenance      int64                            `json:"queued_maintenance"`
	Active                 int64                            `json:"active"`
	PeakActive             int64                            `json:"peak_active"`
	OldestQueuedNanos      int64                            `json:"oldest_queued_nanos"`
	QueueWaitNanos         uint64                           `json:"queue_wait_nanos"`
	Acquired               uint64                           `json:"acquired"`
	BackpressureEvents     uint64                           `json:"refresh_persist_backpressure"`
	Rejected               uint64                           `json:"rejected"`
	Batches                uint64                           `json:"batches"`
	BatchItems             uint64                           `json:"batch_items"`
	Coalesced              uint64                           `json:"coalesced"`
	Pushes                 uint64                           `json:"pushes"`
	PushDurationNanos      uint64                           `json:"push_duration_nanos"`
	OldestStoreWaitNanos   int64                            `json:"oldest_store_wait_nanos"`
	ResultPersistence      ResultPersistenceMetricsSnapshot `json:"result_persistence"`
}

type refreshPersistenceCoordinator struct {
	mu                     sync.Mutex
	concurrency            int
	transactionConcurrency int
	queueLimit             int
	queues                 [3][]*refreshPersistenceWaiter
	active                 int
	peakActive             int
	acquired               uint64
	queueWait              uint64
	backpressure           uint64
	rejected               uint64
	generation             uint64
	sinceImport            int
	sinceMaint             int
	closed                 bool
}

type refreshPersistenceWaiter struct {
	queuedAt time.Time
	grant    chan *refreshPersistenceReservation
	priority RefreshPersistencePriority
	authID   string
	queued   bool
}

type refreshPersistenceReservation struct {
	coordinator *refreshPersistenceCoordinator
	info        RefreshPersistenceBatchInfo
	once        sync.Once
}

type refreshPersistenceReservationContextKey struct{}

type refreshPersistenceCoordinatorExpectation struct {
	coordinator *refreshPersistenceCoordinator
}

type refreshPersistenceCoordinatorExpectationContextKey struct{}

var errRefreshPersistenceStoreChanged = errors.New("refresh persistence store changed")

func withRefreshPersistenceCoordinatorExpectation(ctx context.Context, coordinator *refreshPersistenceCoordinator) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(
		ctx,
		refreshPersistenceCoordinatorExpectationContextKey{},
		refreshPersistenceCoordinatorExpectation{coordinator: coordinator},
	)
}

func (m *Manager) refreshPersistenceCoordinatorMatchesContext(ctx context.Context) bool {
	if m == nil || ctx == nil {
		return true
	}
	expectation, ok := ctx.Value(refreshPersistenceCoordinatorExpectationContextKey{}).(refreshPersistenceCoordinatorExpectation)
	return !ok || m.refreshPersistence.Load() == expectation.coordinator
}

func newRefreshPersistenceCoordinator(store Store) *refreshPersistenceCoordinator {
	capable, ok := store.(RefreshPersistenceConcurrencyStore)
	if !ok {
		return nil
	}
	concurrency := capable.RefreshPersistenceConcurrency()
	transactionConcurrency := concurrency
	if transactionConcurrency <= 0 {
		return nil
	}
	if admissionStore, supported := store.(RefreshPersistenceAdmissionStore); supported {
		concurrency = admissionStore.RefreshPersistenceAdmissionConcurrency()
	}
	if concurrency <= 0 {
		return nil
	}
	return &refreshPersistenceCoordinator{
		concurrency:            concurrency,
		transactionConcurrency: transactionConcurrency,
		queueLimit:             refreshPersistenceQueueLimit,
	}
}

func (coordinator *refreshPersistenceCoordinator) acquireContext(ctx context.Context, priority RefreshPersistencePriority, authID string) (*refreshPersistenceReservation, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if errContext := ctx.Err(); errContext != nil {
		return nil, errContext
	}
	if coordinator == nil {
		return &refreshPersistenceReservation{}, nil
	}
	priority = normalizeRefreshPersistencePriority(priority)
	waiter := &refreshPersistenceWaiter{
		queuedAt: time.Now(),
		grant:    make(chan *refreshPersistenceReservation, 1),
		priority: priority,
		authID:   authID,
	}
	coordinator.mu.Lock()
	if errContext := ctx.Err(); errContext != nil {
		coordinator.mu.Unlock()
		return nil, errContext
	}
	if coordinator.closed {
		coordinator.mu.Unlock()
		return nil, refreshPersistenceClosedError()
	}
	if coordinator.active < coordinator.concurrency && coordinator.queuedLocked() == 0 {
		reservation := coordinator.grantLocked(waiter, time.Now())
		coordinator.mu.Unlock()
		return reservation, nil
	}
	if coordinator.queuedLocked() >= coordinator.queueLimit {
		coordinator.rejected++
		coordinator.mu.Unlock()
		return nil, refreshPersistenceBackpressureError()
	}
	waiter.queued = true
	coordinator.queues[int(priority)] = append(coordinator.queues[int(priority)], waiter)
	coordinator.backpressure++
	coordinator.mu.Unlock()

	select {
	case reservation := <-waiter.grant:
		if reservation == nil {
			return nil, refreshPersistenceClosedError()
		}
		if errContext := ctx.Err(); errContext != nil {
			reservation.release()
			return nil, errContext
		}
		return reservation, nil
	case <-ctx.Done():
		coordinator.mu.Lock()
		if waiter.queued {
			coordinator.removeWaiterLocked(waiter)
			coordinator.mu.Unlock()
			return nil, ctx.Err()
		}
		coordinator.mu.Unlock()
		select {
		case reservation := <-waiter.grant:
			reservation.release()
		default:
		}
		return nil, ctx.Err()
	}
}

func normalizeRefreshPersistencePriority(priority RefreshPersistencePriority) RefreshPersistencePriority {
	if priority > RefreshPersistencePrioritySession {
		return RefreshPersistencePriorityMaintenance
	}
	return priority
}

func refreshPersistenceBackpressureError() error {
	return &Error{
		Code:       "refresh_persist_backpressure",
		Message:    "refresh persistence queue is full",
		Retryable:  true,
		HTTPStatus: http.StatusServiceUnavailable,
	}
}

func refreshPersistenceClosedError() error {
	return &Error{
		Code:       "refresh_persist_closed",
		Message:    "refresh persistence is shutting down",
		Retryable:  true,
		HTTPStatus: http.StatusServiceUnavailable,
	}
}

func (coordinator *refreshPersistenceCoordinator) grantLocked(waiter *refreshPersistenceWaiter, now time.Time) *refreshPersistenceReservation {
	coordinator.active++
	coordinator.acquired++
	if coordinator.active > coordinator.peakActive {
		coordinator.peakActive = coordinator.active
	}
	coordinator.generation++
	if waiter != nil && waiter.queued {
		waiter.queued = false
		coordinator.queueWait += uint64(max(now.Sub(waiter.queuedAt), 0))
	}
	return &refreshPersistenceReservation{
		coordinator: coordinator,
		info: RefreshPersistenceBatchInfo{
			AuthID:     waiter.authID,
			Generation: coordinator.generation,
			Priority:   waiter.priority,
		},
	}
}

func (coordinator *refreshPersistenceCoordinator) removeWaiterLocked(target *refreshPersistenceWaiter) {
	if coordinator == nil || target == nil || !target.queued {
		return
	}
	priority := int(normalizeRefreshPersistencePriority(target.priority))
	queue := coordinator.queues[priority]
	for index, waiter := range queue {
		if waiter != target {
			continue
		}
		coordinator.queues[priority] = append(queue[:index], queue[index+1:]...)
		target.queued = false
		return
	}
}

func (coordinator *refreshPersistenceCoordinator) popWaiterLocked() *refreshPersistenceWaiter {
	maintenanceQueued := len(coordinator.queues[int(RefreshPersistencePriorityMaintenance)]) > 0
	importQueued := len(coordinator.queues[int(RefreshPersistencePriorityImport)]) > 0
	sessionQueued := len(coordinator.queues[int(RefreshPersistencePrioritySession)]) > 0

	priority := RefreshPersistencePriorityMaintenance
	switch {
	case maintenanceQueued && coordinator.sinceMaint >= refreshPersistenceMaintenanceBurst:
		priority = RefreshPersistencePriorityMaintenance
	case importQueued && coordinator.sinceImport >= refreshPersistenceSessionBurst:
		priority = RefreshPersistencePriorityImport
	case sessionQueued:
		priority = RefreshPersistencePrioritySession
	case importQueued:
		priority = RefreshPersistencePriorityImport
	case maintenanceQueued:
		priority = RefreshPersistencePriorityMaintenance
	default:
		return nil
	}
	queue := coordinator.queues[int(priority)]
	waiter := queue[0]
	coordinator.queues[int(priority)] = queue[1:]
	switch priority {
	case RefreshPersistencePrioritySession:
		coordinator.sinceImport++
		coordinator.sinceMaint++
	case RefreshPersistencePriorityImport:
		coordinator.sinceImport = 0
		coordinator.sinceMaint++
	case RefreshPersistencePriorityMaintenance:
		coordinator.sinceImport = 0
		coordinator.sinceMaint = 0
	}
	return waiter
}

func (coordinator *refreshPersistenceCoordinator) queuedLocked() int {
	return len(coordinator.queues[0]) + len(coordinator.queues[1]) + len(coordinator.queues[2])
}

func (reservation *refreshPersistenceReservation) release() {
	if reservation == nil || reservation.coordinator == nil {
		return
	}
	reservation.once.Do(func() {
		coordinator := reservation.coordinator
		coordinator.mu.Lock()
		if coordinator.active > 0 {
			coordinator.active--
		}
		if !coordinator.closed {
			if waiter := coordinator.popWaiterLocked(); waiter != nil {
				granted := coordinator.grantLocked(waiter, time.Now())
				waiter.grant <- granted
			}
		}
		coordinator.mu.Unlock()
	})
}

func (reservation *refreshPersistenceReservation) context(ctx context.Context) context.Context {
	if reservation == nil || reservation.info.Generation == 0 {
		return ctx
	}
	ctx = WithRefreshPersistenceBatchInfo(ctx, reservation.info)
	return context.WithValue(ctx, refreshPersistenceReservationContextKey{}, reservation)
}

func refreshPersistenceReservationFromContext(ctx context.Context) *refreshPersistenceReservation {
	if ctx == nil {
		return nil
	}
	reservation, _ := ctx.Value(refreshPersistenceReservationContextKey{}).(*refreshPersistenceReservation)
	return reservation
}

func (coordinator *refreshPersistenceCoordinator) close() {
	if coordinator == nil {
		return
	}
	coordinator.mu.Lock()
	if coordinator.closed {
		coordinator.mu.Unlock()
		return
	}
	coordinator.closed = true
	waiters := make([]*refreshPersistenceWaiter, 0, coordinator.queuedLocked())
	for priority := range coordinator.queues {
		waiters = append(waiters, coordinator.queues[priority]...)
		coordinator.queues[priority] = nil
	}
	for _, waiter := range waiters {
		waiter.queued = false
		waiter.grant <- nil
	}
	coordinator.mu.Unlock()
}

func (coordinator *refreshPersistenceCoordinator) snapshot() RefreshPersistenceMetricsSnapshot {
	if coordinator == nil {
		return RefreshPersistenceMetricsSnapshot{}
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	now := time.Now()
	oldest := time.Duration(0)
	for _, queue := range coordinator.queues {
		if len(queue) == 0 {
			continue
		}
		age := now.Sub(queue[0].queuedAt)
		if age > oldest {
			oldest = age
		}
	}
	return RefreshPersistenceMetricsSnapshot{
		Enabled:                true,
		Concurrency:            coordinator.concurrency,
		TransactionConcurrency: coordinator.transactionConcurrency,
		QueueLimit:             coordinator.queueLimit,
		Queued:                 int64(coordinator.queuedLocked()),
		QueuedSession:          int64(len(coordinator.queues[int(RefreshPersistencePrioritySession)])),
		QueuedImport:           int64(len(coordinator.queues[int(RefreshPersistencePriorityImport)])),
		QueuedMaintenance:      int64(len(coordinator.queues[int(RefreshPersistencePriorityMaintenance)])),
		Active:                 int64(coordinator.active),
		PeakActive:             int64(coordinator.peakActive),
		OldestQueuedNanos:      int64(oldest),
		QueueWaitNanos:         coordinator.queueWait,
		Acquired:               coordinator.acquired,
		BackpressureEvents:     coordinator.backpressure,
		Rejected:               coordinator.rejected,
	}
}

// RefreshPersistenceMetrics returns the current refresh write queue pressure.
func (m *Manager) RefreshPersistenceMetrics() RefreshPersistenceMetricsSnapshot {
	if m == nil {
		return RefreshPersistenceMetricsSnapshot{}
	}
	snapshot := m.refreshPersistence.Load().snapshot()
	if m.resultPersistence != nil {
		snapshot.ResultPersistence = m.resultPersistence.snapshot()
	}
	m.mu.RLock()
	store := m.store
	m.mu.RUnlock()
	if metricsStore, ok := store.(RefreshPersistenceMetricsStore); ok && metricsStore != nil {
		storeSnapshot := metricsStore.RefreshPersistenceStoreMetrics()
		snapshot.Batches = storeSnapshot.Batches
		snapshot.BatchItems = storeSnapshot.BatchItems
		snapshot.Coalesced = storeSnapshot.Coalesced
		snapshot.Pushes = storeSnapshot.Pushes
		snapshot.PushDurationNanos = storeSnapshot.PushDurationNanos
		snapshot.OldestStoreWaitNanos = storeSnapshot.OldestWaitNanos
	}
	return snapshot
}
