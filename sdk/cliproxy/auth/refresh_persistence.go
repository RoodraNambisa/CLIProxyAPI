package auth

import (
	"net/http"
	"sync/atomic"
)

const refreshPersistenceQueueLimit = 512

// RefreshPersistenceMetricsSnapshot reports process-local refresh write pressure.
type RefreshPersistenceMetricsSnapshot struct {
	Enabled            bool   `json:"enabled"`
	Concurrency        int    `json:"concurrency"`
	QueueLimit         int    `json:"queue_limit"`
	Queued             int64  `json:"queued"`
	Active             int64  `json:"active"`
	PeakActive         int64  `json:"peak_active"`
	BackpressureEvents uint64 `json:"refresh_persist_backpressure"`
	Rejected           uint64 `json:"rejected"`
}

type refreshPersistenceCoordinator struct {
	slots        chan struct{}
	queueLimit   int64
	queued       atomic.Int64
	active       atomic.Int64
	peakActive   atomic.Int64
	backpressure atomic.Uint64
	rejected     atomic.Uint64
}

func newRefreshPersistenceCoordinator(store Store) *refreshPersistenceCoordinator {
	capable, ok := store.(RefreshPersistenceConcurrencyStore)
	if !ok {
		return nil
	}
	concurrency := capable.RefreshPersistenceConcurrency()
	if concurrency <= 0 {
		return nil
	}
	return &refreshPersistenceCoordinator{
		slots:      make(chan struct{}, concurrency),
		queueLimit: refreshPersistenceQueueLimit,
	}
}

func (coordinator *refreshPersistenceCoordinator) acquire() (func(), error) {
	if coordinator == nil {
		return func() {}, nil
	}
	select {
	case coordinator.slots <- struct{}{}:
		coordinator.recordActive()
		return coordinator.release, nil
	default:
	}
	for {
		queued := coordinator.queued.Load()
		if queued >= coordinator.queueLimit {
			coordinator.rejected.Add(1)
			return nil, &Error{
				Code:       "refresh_persist_backpressure",
				Message:    "refresh persistence queue is full",
				Retryable:  true,
				HTTPStatus: http.StatusServiceUnavailable,
			}
		}
		if coordinator.queued.CompareAndSwap(queued, queued+1) {
			break
		}
	}
	coordinator.backpressure.Add(1)
	coordinator.slots <- struct{}{}
	coordinator.queued.Add(-1)
	coordinator.recordActive()
	return coordinator.release, nil
}

func (coordinator *refreshPersistenceCoordinator) recordActive() {
	active := coordinator.active.Add(1)
	for {
		peak := coordinator.peakActive.Load()
		if active <= peak || coordinator.peakActive.CompareAndSwap(peak, active) {
			return
		}
	}
}

func (coordinator *refreshPersistenceCoordinator) release() {
	coordinator.active.Add(-1)
	<-coordinator.slots
}

func (coordinator *refreshPersistenceCoordinator) snapshot() RefreshPersistenceMetricsSnapshot {
	if coordinator == nil {
		return RefreshPersistenceMetricsSnapshot{}
	}
	return RefreshPersistenceMetricsSnapshot{
		Enabled:            true,
		Concurrency:        cap(coordinator.slots),
		QueueLimit:         int(coordinator.queueLimit),
		Queued:             coordinator.queued.Load(),
		Active:             coordinator.active.Load(),
		PeakActive:         coordinator.peakActive.Load(),
		BackpressureEvents: coordinator.backpressure.Load(),
		Rejected:           coordinator.rejected.Load(),
	}
}

// RefreshPersistenceMetrics returns the current refresh write queue pressure.
func (m *Manager) RefreshPersistenceMetrics() RefreshPersistenceMetricsSnapshot {
	if m == nil {
		return RefreshPersistenceMetricsSnapshot{}
	}
	return m.refreshPersistence.Load().snapshot()
}
