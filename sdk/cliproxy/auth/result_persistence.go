package auth

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	resultPersistenceWorkers     = 32
	resultPersistenceQueueLimit  = 4096
	resultPersistenceLockStripes = 256
	resultPersistenceIdleTimeout = time.Second
	resultPersistenceRetryBase   = time.Second
	resultPersistenceRetryMax    = 30 * time.Second
	// One normal Git refresh transaction may use 30 seconds and an uncertain
	// push may need another 10 seconds for bounded remote verification.
	resultPersistenceDrainTimeout          = 45 * time.Second
	resultPersistenceCancelTimeout         = time.Second
	resultPersistenceProducerWaitTimeout   = 5 * time.Second
	resultPersistenceProducerCancelTimeout = time.Second
)

// ResultPersistenceMetricsSnapshot reports background result-state writes.
type ResultPersistenceMetricsSnapshot struct {
	Workers                int    `json:"workers"`
	QueueLimit             int    `json:"queue_limit"`
	Queued                 int64  `json:"queued"`
	Active                 int64  `json:"active"`
	OldestQueuedNanos      int64  `json:"oldest_queued_nanos"`
	Enqueued               uint64 `json:"enqueued"`
	Coalesced              uint64 `json:"coalesced"`
	BackpressureEvents     uint64 `json:"backpressure_events"`
	Rescans                uint64 `json:"rescans"`
	Persisted              uint64 `json:"persisted"`
	Skipped                uint64 `json:"skipped"`
	Retries                uint64 `json:"retries"`
	Failures               uint64 `json:"failures"`
	Terminal               uint64 `json:"terminal"`
	DrainTimeouts          uint64 `json:"drain_timeouts"`
	CancelTimeouts         uint64 `json:"cancel_timeouts"`
	Abandoned              uint64 `json:"abandoned"`
	PersistenceNanos       uint64 `json:"persistence_nanos"`
	ProducerActive         int64  `json:"producer_active"`
	ProducerTimeouts       uint64 `json:"producer_timeouts"`
	ProducerCancelTimeouts uint64 `json:"producer_cancel_timeouts"`
	ProducerAbandoned      uint64 `json:"producer_abandoned"`
}

type resultPersistenceEntry struct {
	authID     string
	generation uint64
	queued     bool
	active     bool
	enqueuedAt time.Time
	retryAt    time.Time
	retryCount int
}

type resultPersistenceWork struct {
	entry      *resultPersistenceEntry
	generation uint64
}

type resultPersistenceProducerContextKey struct{}

type resultPersistenceProducer struct {
	manager *Manager
	claimed atomic.Bool
	once    sync.Once
	cancel  context.CancelFunc
}

type resultPersistenceCoordinator struct {
	manager *Manager

	mu               sync.Mutex
	entries          map[string]*resultPersistenceEntry
	queue            []string
	queueLimit       int
	workerLimit      int
	liveWorkers      int
	active           int
	closing          bool
	closed           bool
	rescanNeeded     bool
	rescanRunning    bool
	wake             chan struct{}
	ctx              context.Context
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	enqueued         uint64
	coalesced        uint64
	backpressure     uint64
	rescans          uint64
	persisted        uint64
	skipped          uint64
	retries          uint64
	failures         uint64
	terminal         uint64
	drainTimeouts    uint64
	cancelTimeouts   uint64
	abandoned        uint64
	persistenceNanos uint64
	drainTimeout     time.Duration
	cancelTimeout    time.Duration

	beforeLock func(string)
}

func (m *Manager) beginResultPersistenceProducer(ctx context.Context) (context.Context, *resultPersistenceProducer, func(), error) {
	if m == nil {
		return ctx, nil, func() {}, errors.New("auth manager is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return ctx, nil, func() {}, err
	}
	producerCtx, cancelProducer := context.WithCancel(ctx)
	producer := &resultPersistenceProducer{manager: m, cancel: cancelProducer}
	m.resultProducerMu.Lock()
	if m.resultProducerClosing {
		m.resultProducerMu.Unlock()
		cancelProducer()
		return ctx, nil, func() {}, &Error{
			Code:       "auth_manager_closing",
			Message:    "auth manager is shutting down",
			Retryable:  true,
			HTTPStatus: http.StatusServiceUnavailable,
		}
	}
	if m.resultProducers == nil {
		m.resultProducers = make(map[*resultPersistenceProducer]struct{})
	}
	m.resultProducers[producer] = struct{}{}
	m.resultProducerMu.Unlock()
	return context.WithValue(producerCtx, resultPersistenceProducerContextKey{}, producer), producer, producer.release, nil
}

func claimResultPersistenceProducer(ctx context.Context) func() {
	if ctx == nil {
		return func() {}
	}
	producer, _ := ctx.Value(resultPersistenceProducerContextKey{}).(*resultPersistenceProducer)
	if producer == nil || !producer.claimed.CompareAndSwap(false, true) {
		return func() {}
	}
	return producer.release
}

func (producer *resultPersistenceProducer) release() {
	if producer == nil || producer.manager == nil {
		return
	}
	producer.once.Do(func() {
		manager := producer.manager
		manager.resultProducerMu.Lock()
		delete(manager.resultProducers, producer)
		manager.resultProducerMu.Unlock()
		if producer.cancel != nil {
			producer.cancel()
		}
	})
}

func (m *Manager) stopAcceptingResultPersistenceProducers() {
	if m == nil {
		return
	}
	m.resultProducerMu.Lock()
	m.resultProducerClosing = true
	m.resultProducerMu.Unlock()
}

func (m *Manager) waitForResultPersistenceProducers() {
	if m == nil {
		return
	}
	waitTimeout := m.resultProducerWaitTimeout
	if waitTimeout <= 0 {
		waitTimeout = resultPersistenceProducerWaitTimeout
	}
	if m.waitForResultPersistenceProducerDrain(waitTimeout) {
		return
	}
	m.resultProducerMu.Lock()
	m.resultProducerWaitTimeouts++
	producers := make([]*resultPersistenceProducer, 0, len(m.resultProducers))
	for producer := range m.resultProducers {
		producers = append(producers, producer)
	}
	m.resultProducerMu.Unlock()
	for _, producer := range producers {
		if producer != nil && producer.cancel != nil {
			producer.cancel()
		}
	}
	cancelWait := m.resultProducerCancelWait
	if cancelWait <= 0 {
		cancelWait = resultPersistenceProducerCancelTimeout
	}
	if m.waitForResultPersistenceProducerDrain(cancelWait) {
		return
	}
	m.resultProducerMu.Lock()
	m.resultProducerCancelLimits++
	producers = producers[:0]
	for producer := range m.resultProducers {
		producers = append(producers, producer)
	}
	m.resultProducerAbandoned += uint64(len(producers))
	m.resultProducerMu.Unlock()
	for _, producer := range producers {
		producer.release()
	}
}

func (m *Manager) waitForResultPersistenceProducerDrain(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		m.resultProducerMu.Lock()
		drained := len(m.resultProducers) == 0
		m.resultProducerMu.Unlock()
		if drained {
			return true
		}
		if !time.Now().Before(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func newResultPersistenceCoordinator(manager *Manager) *resultPersistenceCoordinator {
	ctx, cancel := context.WithCancel(context.Background())
	return &resultPersistenceCoordinator{
		manager:       manager,
		entries:       make(map[string]*resultPersistenceEntry),
		queueLimit:    resultPersistenceQueueLimit,
		workerLimit:   resultPersistenceWorkers,
		wake:          make(chan struct{}, 1),
		ctx:           ctx,
		cancel:        cancel,
		drainTimeout:  resultPersistenceDrainTimeout,
		cancelTimeout: resultPersistenceCancelTimeout,
	}
}

func (coordinator *resultPersistenceCoordinator) enqueue(authID string) bool {
	return coordinator.enqueueInternal(authID, false)
}

func (coordinator *resultPersistenceCoordinator) enqueueInternal(authID string, allowClosing bool) bool {
	if coordinator == nil {
		return false
	}
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return false
	}
	now := time.Now()
	coordinator.mu.Lock()
	if coordinator.closed || coordinator.closing && !allowClosing {
		coordinator.mu.Unlock()
		return false
	}
	if entry := coordinator.entries[authID]; entry != nil {
		entry.generation++
		coordinator.coalesced++
		coordinator.ensureWorkersLocked()
		coordinator.signalLocked()
		coordinator.mu.Unlock()
		return true
	}
	if len(coordinator.entries) >= coordinator.queueLimit {
		coordinator.backpressure++
		coordinator.rescanNeeded = true
		coordinator.ensureWorkersLocked()
		coordinator.signalLocked()
		coordinator.mu.Unlock()
		return true
	}
	entry := &resultPersistenceEntry{
		authID:     authID,
		generation: 1,
		queued:     true,
		enqueuedAt: now,
	}
	coordinator.entries[authID] = entry
	coordinator.queue = append(coordinator.queue, authID)
	coordinator.enqueued++
	coordinator.ensureWorkersLocked()
	coordinator.signalLocked()
	coordinator.mu.Unlock()
	return true
}

func (coordinator *resultPersistenceCoordinator) ensureWorkersLocked() {
	if coordinator.closed {
		return
	}
	workers := coordinator.workerLimit
	if workers <= 0 {
		workers = 1
	}
	desired := min(workers, len(coordinator.entries))
	missing := desired - coordinator.liveWorkers
	if missing <= 0 {
		return
	}
	coordinator.liveWorkers += missing
	coordinator.wg.Add(missing)
	for range missing {
		go coordinator.runWorker()
	}
}

func (coordinator *resultPersistenceCoordinator) signalLocked() {
	select {
	case coordinator.wake <- struct{}{}:
	default:
	}
}

func (coordinator *resultPersistenceCoordinator) runWorker() {
	for {
		if coordinator.runRescanIfNeeded() {
			continue
		}
		work, wait, closed := coordinator.takeReadyWork()
		if closed {
			coordinator.workerExited()
			return
		}
		if work != nil {
			coordinator.persist(work)
			continue
		}
		if wait <= 0 {
			wait = resultPersistenceIdleTimeout
		}
		timer := time.NewTimer(wait)
		select {
		case <-coordinator.ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			coordinator.workerExited()
			return
		case <-coordinator.wake:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
			if wait == resultPersistenceIdleTimeout && coordinator.exitIfIdle() {
				return
			}
		}
	}
}

func (coordinator *resultPersistenceCoordinator) workerExited() {
	coordinator.mu.Lock()
	if coordinator.liveWorkers > 0 {
		coordinator.liveWorkers--
	}
	coordinator.mu.Unlock()
	coordinator.wg.Done()
}

func (coordinator *resultPersistenceCoordinator) exitIfIdle() bool {
	coordinator.mu.Lock()
	if coordinator.closed || len(coordinator.queue) == 0 {
		if coordinator.liveWorkers > 0 {
			coordinator.liveWorkers--
		}
		coordinator.mu.Unlock()
		coordinator.wg.Done()
		return true
	}
	coordinator.mu.Unlock()
	return false
}

func (coordinator *resultPersistenceCoordinator) takeReadyWork() (*resultPersistenceWork, time.Duration, bool) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.closed {
		return nil, 0, true
	}
	now := time.Now()
	readyIndex := -1
	wait := time.Duration(0)
	for index, authID := range coordinator.queue {
		entry := coordinator.entries[authID]
		if entry == nil || !entry.queued || entry.active {
			continue
		}
		if entry.retryAt.IsZero() || !now.Before(entry.retryAt) {
			readyIndex = index
			break
		}
		remaining := entry.retryAt.Sub(now)
		if wait == 0 || remaining < wait {
			wait = remaining
		}
	}
	if readyIndex < 0 {
		return nil, wait, false
	}
	authID := coordinator.queue[readyIndex]
	coordinator.queue = append(coordinator.queue[:readyIndex], coordinator.queue[readyIndex+1:]...)
	entry := coordinator.entries[authID]
	entry.queued = false
	entry.active = true
	entry.retryAt = time.Time{}
	coordinator.active++
	return &resultPersistenceWork{entry: entry, generation: entry.generation}, 0, false
}

func (coordinator *resultPersistenceCoordinator) persist(work *resultPersistenceWork) {
	if coordinator == nil || coordinator.manager == nil || work == nil || work.entry == nil {
		return
	}
	for {
		admission := coordinator.manager.refreshPersistence.Load()
		reservation, errReserve := admission.acquireContext(
			coordinator.ctx,
			RefreshPersistencePriorityMaintenance,
			work.entry.authID,
		)
		if errReserve != nil {
			coordinator.finishFailure(work, errReserve)
			return
		}
		persistCtx := reservation.context(coordinator.ctx)
		if coordinator.beforeLock != nil {
			coordinator.beforeLock(work.entry.authID)
		}
		unlockPersist, errLock := coordinator.manager.lockAuthIDMutationContext(persistCtx, work.entry.authID)
		if errLock != nil {
			reservation.release()
			coordinator.finishFailure(work, errLock)
			return
		}

		retryAdmission := coordinator.manager.refreshPersistence.Load() != admission
		var (
			persisted  bool
			terminal   bool
			errPersist error
		)
		if !retryAdmission {
			snapshot := coordinator.manager.currentResultPersistenceSnapshot(work.entry.authID)
			if snapshot == nil || !isNativeChatGPTWebCredentialAuth(snapshot) || !coordinator.manager.shouldPersistAuth(persistCtx, snapshot) {
				// The current credential no longer has a durable result state.
			} else {
				expectedSourceHash := authSourceHash(snapshot)
				if expectedSourceHash == "" {
					terminal = true
				} else if needed, errNeeded := resultMetadataPersistenceNeeded(snapshot, expectedSourceHash); errNeeded != nil {
					terminal = true
				} else if needed {
					started := time.Now()
					saveCtx := persistCtx
					if coordinator.manager.SupportsSourceConditionalSave() {
						saveCtx = WithSourceHashSavePrecondition(saveCtx, expectedSourceHash)
					}
					errPersist = coordinator.manager.persistWithoutLock(saveCtx, snapshot, false)
					elapsed := max(time.Since(started), 0)
					coordinator.mu.Lock()
					coordinator.persistenceNanos += uint64(elapsed)
					coordinator.mu.Unlock()
					if errPersist == nil {
						coordinator.manager.syncResultPersistenceSourceHash(snapshot, expectedSourceHash)
						persisted = true
					}
				}
			}
		}
		unlockPersist()
		reservation.release()
		if retryAdmission {
			continue
		}
		if errPersist != nil {
			coordinator.finishFailure(work, errPersist)
			return
		}
		if terminal {
			coordinator.finishTerminal(work)
			return
		}
		coordinator.finishSuccess(work.entry, work.generation, persisted)
		return
	}
}

func resultMetadataPersistenceNeeded(auth *Auth, expectedSourceHash string) (bool, error) {
	raw, errRaw := CanonicalMetadataBytes(auth)
	if errRaw != nil {
		return false, errRaw
	}
	if len(raw) == 0 {
		return false, nil
	}
	return !SourceHashMatchesBytes(expectedSourceHash, raw), nil
}

func (coordinator *resultPersistenceCoordinator) finishSuccess(
	entry *resultPersistenceEntry,
	processedGeneration uint64,
	persisted bool,
) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if persisted {
		coordinator.persisted++
	} else {
		coordinator.skipped++
	}
	if entry.generation > processedGeneration && !coordinator.closed {
		entry.retryCount = 0
		entry.active = false
		entry.queued = true
		entry.enqueuedAt = time.Now()
		coordinator.queue = append(coordinator.queue, entry.authID)
		if coordinator.active > 0 {
			coordinator.active--
		}
		coordinator.signalLocked()
		return
	}
	delete(coordinator.entries, entry.authID)
	entry.active = false
	if coordinator.active > 0 {
		coordinator.active--
	}
	coordinator.signalLocked()
}

func (coordinator *resultPersistenceCoordinator) finishFailure(work *resultPersistenceWork, err error) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	entry := work.entry
	if entry == nil {
		return
	}
	coordinator.failures++
	entry.active = false
	if coordinator.active > 0 {
		coordinator.active--
	}
	if coordinator.closed || errors.Is(err, context.Canceled) {
		delete(coordinator.entries, entry.authID)
		coordinator.signalLocked()
		return
	}
	if resultPersistenceTerminalError(err) {
		delete(coordinator.entries, entry.authID)
		coordinator.terminal++
		coordinator.signalLocked()
		return
	}
	entry.retryCount++
	delay := resultPersistenceRetryBase << min(entry.retryCount-1, 5)
	if delay > resultPersistenceRetryMax {
		delay = resultPersistenceRetryMax
	}
	entry.retryAt = time.Now().Add(delay)
	entry.queued = true
	entry.enqueuedAt = time.Now()
	coordinator.queue = append(coordinator.queue, entry.authID)
	coordinator.retries++
	coordinator.signalLocked()
}

func (coordinator *resultPersistenceCoordinator) finishTerminal(work *resultPersistenceWork) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	entry := work.entry
	if entry == nil {
		return
	}
	entry.active = false
	if coordinator.active > 0 {
		coordinator.active--
	}
	delete(coordinator.entries, entry.authID)
	coordinator.failures++
	coordinator.terminal++
	coordinator.signalLocked()
}

func resultPersistenceTerminalError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrAuthMutationIdentityChanged) || isRuntimeAuthInstanceRetiredError(err) {
		return true
	}
	return false
}

func (coordinator *resultPersistenceCoordinator) runRescanIfNeeded() bool {
	if coordinator == nil || coordinator.manager == nil {
		return false
	}
	coordinator.mu.Lock()
	if coordinator.closed || !coordinator.rescanNeeded || coordinator.rescanRunning || len(coordinator.entries) >= coordinator.queueLimit {
		coordinator.mu.Unlock()
		return false
	}
	coordinator.rescanNeeded = false
	coordinator.rescanRunning = true
	coordinator.rescans++
	limit := coordinator.queueLimit - len(coordinator.entries)
	tracked := make(map[string]struct{}, len(coordinator.entries))
	for authID := range coordinator.entries {
		tracked[authID] = struct{}{}
	}
	coordinator.mu.Unlock()

	ids, more := coordinator.manager.dirtyResultPersistenceAuthIDs(limit, tracked)
	for _, authID := range ids {
		coordinator.enqueueInternal(authID, true)
	}
	coordinator.mu.Lock()
	coordinator.rescanRunning = false
	coordinator.rescanNeeded = coordinator.rescanNeeded || more
	coordinator.signalLocked()
	coordinator.mu.Unlock()
	return true
}

func (coordinator *resultPersistenceCoordinator) close() {
	if coordinator == nil {
		return
	}
	coordinator.mu.Lock()
	if coordinator.closed {
		coordinator.mu.Unlock()
		return
	}
	coordinator.closing = true
	coordinator.ensureWorkersLocked()
	coordinator.signalLocked()
	coordinator.mu.Unlock()

	drainTimeout := coordinator.drainTimeout
	if drainTimeout <= 0 {
		drainTimeout = resultPersistenceDrainTimeout
	}
	drainDeadline := time.Now().Add(drainTimeout)
	for {
		coordinator.mu.Lock()
		drained := len(coordinator.entries) == 0 && coordinator.active == 0 &&
			!coordinator.rescanNeeded && !coordinator.rescanRunning
		if drained || !time.Now().Before(drainDeadline) {
			if !drained {
				coordinator.drainTimeouts++
				coordinator.abandoned += uint64(len(coordinator.entries))
			}
			coordinator.closed = true
			coordinator.cancel()
			coordinator.signalLocked()
			coordinator.mu.Unlock()
			break
		}
		coordinator.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}

	cancelTimeout := coordinator.cancelTimeout
	if cancelTimeout <= 0 {
		cancelTimeout = resultPersistenceCancelTimeout
	}
	cancelDeadline := time.Now().Add(cancelTimeout)
	for {
		coordinator.mu.Lock()
		workers := coordinator.liveWorkers
		if workers == 0 || !time.Now().Before(cancelDeadline) {
			if workers > 0 {
				coordinator.cancelTimeouts++
			}
			coordinator.mu.Unlock()
			return
		}
		coordinator.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
}

func (coordinator *resultPersistenceCoordinator) snapshot() ResultPersistenceMetricsSnapshot {
	if coordinator == nil {
		return ResultPersistenceMetricsSnapshot{}
	}
	var producerActive int64
	var producerTimeouts, producerCancelWait, producerAbandoned uint64
	if coordinator.manager != nil {
		coordinator.manager.resultProducerMu.Lock()
		producerActive = int64(len(coordinator.manager.resultProducers))
		producerTimeouts = coordinator.manager.resultProducerWaitTimeouts
		producerCancelWait = coordinator.manager.resultProducerCancelLimits
		producerAbandoned = coordinator.manager.resultProducerAbandoned
		coordinator.manager.resultProducerMu.Unlock()
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	oldest := time.Duration(0)
	now := time.Now()
	for _, authID := range coordinator.queue {
		entry := coordinator.entries[authID]
		if entry == nil || !entry.queued {
			continue
		}
		age := now.Sub(entry.enqueuedAt)
		if age > oldest {
			oldest = age
		}
	}
	return ResultPersistenceMetricsSnapshot{
		Workers:                coordinator.workerLimit,
		QueueLimit:             coordinator.queueLimit,
		Queued:                 int64(len(coordinator.queue)),
		Active:                 int64(coordinator.active),
		OldestQueuedNanos:      int64(oldest),
		Enqueued:               coordinator.enqueued,
		Coalesced:              coordinator.coalesced,
		BackpressureEvents:     coordinator.backpressure,
		Rescans:                coordinator.rescans,
		Persisted:              coordinator.persisted,
		Skipped:                coordinator.skipped,
		Retries:                coordinator.retries,
		Failures:               coordinator.failures,
		Terminal:               coordinator.terminal,
		DrainTimeouts:          coordinator.drainTimeouts,
		CancelTimeouts:         coordinator.cancelTimeouts,
		Abandoned:              coordinator.abandoned,
		PersistenceNanos:       coordinator.persistenceNanos,
		ProducerActive:         producerActive,
		ProducerTimeouts:       producerTimeouts,
		ProducerCancelTimeouts: producerCancelWait,
		ProducerAbandoned:      producerAbandoned,
	}
}

func (m *Manager) lockResultMutation(authID string) func() {
	if m == nil {
		return func() {}
	}
	var hash uint32 = 2166136261
	for index := 0; index < len(authID); index++ {
		hash ^= uint32(authID[index])
		hash *= 16777619
	}
	lock := &m.resultMutationLocks[hash%resultPersistenceLockStripes]
	lock.Lock()
	return lock.Unlock
}

func (m *Manager) canPersistResultAsynchronously(result Result) bool {
	if m == nil || m.resultPersistence == nil || strings.TrimSpace(result.AuthID) == "" {
		return false
	}
	m.mu.RLock()
	current := m.auths[result.AuthID]
	_, conditional := m.store.(SourceConditionalSaveStore)
	eligible := current != nil &&
		isNativeChatGPTWebCredentialAuth(current) &&
		strings.EqualFold(strings.TrimSpace(result.Provider), "chatgpt-web") &&
		authSourceHash(current) != "" &&
		conditional
	m.mu.RUnlock()
	return eligible
}

func (m *Manager) snapshotResultStateForPersistence(ctx context.Context, authID string, asynchronous bool) *Auth {
	if m == nil || strings.TrimSpace(authID) == "" {
		return nil
	}
	if asynchronous {
		m.mu.RLock()
		current := m.auths[authID]
		var snapshot *Auth
		if current != nil {
			snapshot = current.Clone()
		}
		m.mu.RUnlock()
		if snapshot != nil && m.resultPersistence != nil {
			m.resultPersistence.enqueue(authID)
		}
		return snapshot
	}
	snapshot, errPersist := m.snapshotCurrentAuthForPersistenceLocked(ctx, &Auth{ID: authID})
	if errPersist != nil {
		return nil
	}
	return snapshot
}

func (m *Manager) currentResultPersistenceSnapshot(authID string) *Auth {
	if m == nil {
		return nil
	}
	unlockResult := m.lockResultMutation(authID)
	defer unlockResult()
	m.mu.RLock()
	current := m.auths[authID]
	var snapshot *Auth
	if current != nil {
		snapshot = current.Clone()
	}
	m.mu.RUnlock()
	return snapshot
}

func (m *Manager) syncResultPersistenceSourceHash(persisted *Auth, expectedSourceHash string) {
	if m == nil || persisted == nil || authSourceHash(persisted) == "" {
		return
	}
	m.mu.Lock()
	current := m.auths[persisted.ID]
	if current != nil && current.instanceID == persisted.instanceID && current.instanceState == persisted.instanceState &&
		authSourceHash(current) == expectedSourceHash {
		if current.Attributes == nil {
			current.Attributes = make(map[string]string)
		}
		current.Attributes[SourceHashAttributeKey] = authSourceHash(persisted)
	}
	m.mu.Unlock()
}

func (m *Manager) upsertCurrentResultAuthState(snapshot *Auth) {
	if m == nil || m.scheduler == nil || snapshot == nil {
		return
	}
	m.mu.RLock()
	current := m.auths[snapshot.ID]
	currentSnapshot := current != nil &&
		current.instanceID == snapshot.instanceID &&
		current.instanceState == snapshot.instanceState &&
		!current.UpdatedAt.After(snapshot.UpdatedAt)
	if currentSnapshot {
		m.scheduler.upsertAuthState(snapshot)
	}
	m.mu.RUnlock()
}

func (m *Manager) dirtyResultPersistenceAuthIDs(limit int, tracked map[string]struct{}) ([]string, bool) {
	if m == nil || limit <= 0 {
		return nil, false
	}
	m.mu.RLock()
	ids := make([]string, 0, len(m.auths))
	for authID, auth := range m.auths {
		if auth != nil && isNativeChatGPTWebCredentialAuth(auth) && authSourceHash(auth) != "" {
			ids = append(ids, authID)
		}
	}
	m.mu.RUnlock()
	sort.Strings(ids)
	dirty := make([]string, 0, min(limit, len(ids)))
	for _, authID := range ids {
		if _, alreadyTracked := tracked[authID]; alreadyTracked {
			continue
		}
		snapshot := m.currentResultPersistenceSnapshot(authID)
		if snapshot == nil {
			continue
		}
		needed, errNeeded := resultMetadataPersistenceNeeded(snapshot, authSourceHash(snapshot))
		if errNeeded != nil || !needed {
			continue
		}
		if len(dirty) >= limit {
			return dirty, true
		}
		dirty = append(dirty, authID)
	}
	return dirty, false
}
