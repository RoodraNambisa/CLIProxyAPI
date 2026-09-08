package live

import (
	"context"
	"net/http"
	"sync"
)

type liveDisabledError struct{}

func (*liveDisabledError) Error() string        { return "Codex live is disabled" }
func (*liveDisabledError) StatusCode() int      { return http.StatusServiceUnavailable }
func (*liveDisabledError) SkipAuthResult() bool { return true }

var errLiveDisabled = &liveDisabledError{}

const liveDisabledCode = "codex_live_disabled"

// admissionGate separates cancellable setup from already established sessions.
// Its zero value rejects new work. It never owns an established session's lifetime.
type admissionGate struct {
	mu         sync.Mutex
	enabled    bool
	generation uint64
	pending    map[*liveAdmission]struct{}
}

type liveAdmission struct {
	gate       *admissionGate
	ctx        context.Context
	cancel     context.CancelCauseFunc
	stop       func() bool
	generation uint64
	committed  bool
	closed     bool
}

func (g *admissionGate) update(enabled bool) {
	g.mu.Lock()
	if g.enabled == enabled {
		g.mu.Unlock()
		return
	}
	g.enabled = enabled
	g.generation++
	var pending []*liveAdmission
	if !enabled {
		for admission := range g.pending {
			pending = append(pending, admission)
		}
		clear(g.pending)
	}
	g.mu.Unlock()
	// Cancellation may trigger resource cleanup. Never do it under the gate lock.
	for _, admission := range pending {
		admission.cancel(errLiveDisabled)
	}
}

func (g *admissionGate) begin(ctx context.Context) (*liveAdmission, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !g.enabled {
		return nil, errLiveDisabled
	}
	setup, cancel := context.WithCancelCause(ctx)
	a := &liveAdmission{gate: g, ctx: setup, cancel: cancel, generation: g.generation}
	if g.pending == nil {
		g.pending = make(map[*liveAdmission]struct{})
	}
	g.pending[a] = struct{}{}
	a.stop = context.AfterFunc(setup, func() { a.close() })
	return a, nil
}

// commit is the last admission check after setup succeeds. Callers must still
// roll back their allocated resources if commit fails or response delivery fails.
func (a *liveAdmission) commit() error {
	g := a.gate
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := context.Cause(a.ctx); err != nil {
		return err
	}
	if a.closed {
		return context.Canceled
	}
	if a.committed {
		return nil
	}
	if !g.enabled || a.generation != g.generation {
		return errLiveDisabled
	}
	a.committed = true
	delete(g.pending, a)
	return nil
}

// close releases setup bookkeeping and cancellation links on every exit path.
// Established connections must use their own session and credential lifecycle.
func (a *liveAdmission) close() {
	if a == nil {
		return
	}
	g := a.gate
	g.mu.Lock()
	if a.closed {
		g.mu.Unlock()
		return
	}
	a.closed = true
	delete(g.pending, a)
	stop := a.stop
	g.mu.Unlock()
	if stop != nil {
		stop()
	}
	a.cancel(context.Canceled)
}
