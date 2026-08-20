package proxypool

import (
	"context"
	"sync"
)

type checkAdmissionWaiter struct {
	ready   chan struct{}
	granted bool
}

type checkAdmission struct {
	mu sync.Mutex

	limit      int
	active     int
	queued     int
	peakActive int
	peakQueued int
	waiters    []*checkAdmissionWaiter

	attempts  uint64
	acquired  uint64
	canceled  uint64
	completed uint64
	succeeded uint64
	failed    uint64
}

type CheckAdmissionSnapshot struct {
	Limit      int    `json:"limit"`
	Active     int    `json:"active"`
	Queued     int    `json:"queued"`
	PeakActive int    `json:"peak_active"`
	PeakQueued int    `json:"peak_queued"`
	Attempts   uint64 `json:"attempts"`
	Acquired   uint64 `json:"acquired"`
	Canceled   uint64 `json:"canceled"`
	Completed  uint64 `json:"completed"`
	Succeeded  uint64 `json:"succeeded"`
	Failed     uint64 `json:"failed"`
}

func (admission *checkAdmission) recordResult(ok bool) {
	if admission == nil {
		return
	}
	admission.mu.Lock()
	admission.completed++
	if ok {
		admission.succeeded++
	} else {
		admission.failed++
	}
	admission.mu.Unlock()
}

func newCheckAdmission(limit int) *checkAdmission {
	if limit < 1 {
		limit = 1
	}
	return &checkAdmission{limit: limit}
}

func (admission *checkAdmission) resize(limit int) {
	if admission == nil {
		return
	}
	if limit < 1 {
		limit = 1
	}
	admission.mu.Lock()
	admission.limit = limit
	admission.grantLocked()
	admission.mu.Unlock()
}

func (admission *checkAdmission) acquire(ctx context.Context) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if errContext := ctx.Err(); errContext != nil {
		return nil, errContext
	}
	admission.mu.Lock()
	admission.attempts++
	if admission.active < admission.limit && len(admission.waiters) == 0 {
		admission.active++
		admission.acquired++
		if admission.active > admission.peakActive {
			admission.peakActive = admission.active
		}
		admission.mu.Unlock()
		return admission.releaseFunc(), nil
	}
	waiter := &checkAdmissionWaiter{ready: make(chan struct{})}
	admission.waiters = append(admission.waiters, waiter)
	admission.queued++
	if admission.queued > admission.peakQueued {
		admission.peakQueued = admission.queued
	}
	admission.mu.Unlock()

	select {
	case <-waiter.ready:
		if errContext := ctx.Err(); errContext != nil {
			admission.release()
			admission.mu.Lock()
			admission.canceled++
			admission.mu.Unlock()
			return nil, errContext
		}
		return admission.releaseFunc(), nil
	case <-ctx.Done():
		admission.mu.Lock()
		if waiter.granted {
			admission.mu.Unlock()
			admission.release()
			admission.mu.Lock()
			admission.canceled++
			admission.mu.Unlock()
			return nil, ctx.Err()
		}
		for index, candidate := range admission.waiters {
			if candidate != waiter {
				continue
			}
			admission.waiters = append(admission.waiters[:index], admission.waiters[index+1:]...)
			admission.queued--
			break
		}
		admission.canceled++
		admission.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (admission *checkAdmission) releaseFunc() func() {
	var once sync.Once
	return func() {
		once.Do(admission.release)
	}
}

func (admission *checkAdmission) release() {
	admission.mu.Lock()
	if admission.active > 0 {
		admission.active--
	}
	admission.grantLocked()
	admission.mu.Unlock()
}

func (admission *checkAdmission) grantLocked() {
	for admission.active < admission.limit && len(admission.waiters) > 0 {
		waiter := admission.waiters[0]
		admission.waiters = admission.waiters[1:]
		admission.queued--
		admission.active++
		admission.acquired++
		if admission.active > admission.peakActive {
			admission.peakActive = admission.active
		}
		waiter.granted = true
		close(waiter.ready)
	}
}

func (admission *checkAdmission) snapshot() CheckAdmissionSnapshot {
	if admission == nil {
		return CheckAdmissionSnapshot{}
	}
	admission.mu.Lock()
	snapshot := CheckAdmissionSnapshot{
		Limit:      admission.limit,
		Active:     admission.active,
		Queued:     admission.queued,
		PeakActive: admission.peakActive,
		PeakQueued: admission.peakQueued,
		Attempts:   admission.attempts,
		Acquired:   admission.acquired,
		Canceled:   admission.canceled,
		Completed:  admission.completed,
		Succeeded:  admission.succeeded,
		Failed:     admission.failed,
	}
	admission.mu.Unlock()
	return snapshot
}
