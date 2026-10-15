package auth

import (
	"sync/atomic"
	"time"
)

var selectionRuntime struct {
	schedulerLockWait, providerLockWait, candidateNanos, reservationNanos, cloneNanos          atomic.Uint64
	fastPaths, compatibilityPaths, candidates, reservations, clones, exclusions, indexRebuilds atomic.Uint64
}

// SelectionRuntimeSnapshot contains process-wide cumulative selection costs.
// Candidate time includes reservation time; these nested measurements must not
// be added together. No credential identity or high-cardinality label is used.
type SelectionRuntimeSnapshot struct {
	SchedulerLockWaitNanos uint64 `json:"scheduler_lock_wait_nanos"`
	ProviderLockWaitNanos  uint64 `json:"provider_lock_wait_nanos"`
	CandidateCheckNanos    uint64 `json:"candidate_check_nanos"`
	ReservationNanos       uint64 `json:"reservation_nanos"`
	CloneNanos             uint64 `json:"clone_nanos"`
	FastPaths              uint64 `json:"fast_paths"`
	CompatibilityPaths     uint64 `json:"compatibility_paths"`
	CandidateChecks        uint64 `json:"candidate_checks"`
	Reservations           uint64 `json:"reservations"`
	Clones                 uint64 `json:"clones"`
	Exclusions             uint64 `json:"request_limit_exclusions"`
	IndexRebuilds          uint64 `json:"request_limit_index_rebuilds"`
}

func SelectionMetricsSnapshot() SelectionRuntimeSnapshot {
	return SelectionRuntimeSnapshot{
		SchedulerLockWaitNanos: selectionRuntime.schedulerLockWait.Load(), ProviderLockWaitNanos: selectionRuntime.providerLockWait.Load(),
		CandidateCheckNanos: selectionRuntime.candidateNanos.Load(), ReservationNanos: selectionRuntime.reservationNanos.Load(), CloneNanos: selectionRuntime.cloneNanos.Load(),
		FastPaths: selectionRuntime.fastPaths.Load(), CompatibilityPaths: selectionRuntime.compatibilityPaths.Load(), CandidateChecks: selectionRuntime.candidates.Load(),
		Reservations: selectionRuntime.reservations.Load(), Clones: selectionRuntime.clones.Load(), Exclusions: selectionRuntime.exclusions.Load(), IndexRebuilds: selectionRuntime.indexRebuilds.Load(),
	}
}

func observeSelectionTime(counter *atomic.Uint64, start time.Time) {
	counter.Add(uint64(max(0, time.Since(start).Nanoseconds())))
}
