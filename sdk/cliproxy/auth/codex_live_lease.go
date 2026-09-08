package auth

import (
	"context"
	"net/http"
	"strings"
	"sync"

	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

// CodexLiveLease retains one selected credential instance and its executor.
// The caller owns the session context and must close the lease when it ends.
// Selection reserves capacity; CommitUpstream consumes it only when connecting.
type CodexLiveLease struct {
	ctx                  context.Context
	auth                 *Auth
	executor             ProviderExecutor
	model                string
	slot                 *core.AuthRequestSlot
	mu                   sync.Mutex
	closed               bool
	websocketDialStarted bool
	stop                 func() bool
	release              func() bool
	cancel               context.CancelFunc
}

// AcquireCodexLive selects once through the existing routing and model rules.
// It performs no upstream request and adds no retry or cooldown policy.
func (m *Manager) AcquireCodexLive(ctx context.Context, model string, opts core.Options) (*CodexLiveLease, error) {
	if m == nil {
		return nil, &Error{Code: "provider_not_found", Message: "manager unavailable", HTTPStatus: http.StatusServiceUnavailable}
	}
	model = strings.TrimSpace(model)
	if model == "" {
		return nil, &Error{Code: "invalid_request", Message: "realtime model is required", HTTPStatus: http.StatusBadRequest}
	}
	ctx = m.WithRoutingPolicySnapshot(ctx)
	ctx, _, releaseProducer, errProducer := m.beginResultPersistenceProducer(ctx)
	if errProducer != nil {
		return nil, errProducer
	}
	opts = ensureRequestedModelMetadata(opts, model)
	opts.SourceFormat = translator.FormatCodexLive
	opts.AuthRequestSlot = newAuthRequestSlot(opts.ExecutionDiagnostics, opts.ExecutionMetrics)
	transferred := false
	defer func() {
		if !transferred {
			opts.AuthRequestSlot.Release()
			releaseProducer()
		}
	}()
	selected, executor, errPick := m.pickNext(ctx, "codex", model, opts, nil)
	if errPick != nil {
		return nil, errPick
	}
	resolved, errProxy := m.ResolveProxyAuth(ctx, selected)
	if errProxy != nil {
		return nil, withAuthErrorResponseSource(errProxy, selected, "codex")
	}
	if rt := m.roundTripperFor(resolved); rt != nil {
		ctx = context.WithValue(ctx, roundTripperContextKey{}, rt)
		ctx = context.WithValue(ctx, "cliproxy.roundtripper", rt)
	}
	prepared, errPrepare := m.prepareRequestAuth(ctx, executor, resolved)
	if errPrepare != nil {
		return nil, withAuthErrorResponseSource(errPrepare, selected, "codex")
	}
	carryRuntimeProxy(resolved, prepared)
	if !SupportsCodexLive(prepared) {
		return nil, &Error{Code: "auth_not_found", Message: "selected realtime credential is unavailable", HTTPStatus: http.StatusServiceUnavailable}
	}
	models, _ := m.preparedExecutionModels(prepared, model, opts)
	if len(models) == 0 {
		return nil, &Error{Code: "auth_not_found", Message: "selected realtime credential has no executable model", HTTPStatus: http.StatusServiceUnavailable}
	}
	leaseCtx, cancel := context.WithCancel(ctx)
	runtimeCtx, release, active := m.beginCurrentAuthExecution(leaseCtx, prepared, executor)
	if !active {
		cancel()
		return nil, runtimeAuthInstanceRetiredError()
	}
	lease := &CodexLiveLease{ctx: runtimeCtx, auth: prepared, executor: executor, model: models[0], slot: opts.AuthRequestSlot, release: release, cancel: cancel}
	lease.release = func() bool {
		retired := release()
		releaseProducer()
		return retired
	}
	lease.mu.Lock()
	lease.stop = context.AfterFunc(runtimeCtx, lease.Close)
	lease.mu.Unlock()
	transferred = true
	if errCtx := context.Cause(runtimeCtx); errCtx != nil {
		lease.Close()
		return nil, errCtx
	}
	return lease, nil
}

func (l *CodexLiveLease) Context() context.Context { return l.ctx }

func (l *CodexLiveLease) CloneAuth() *Auth { return l.auth.Clone() }

func (l *CodexLiveLease) Model() string { return l.model }

// WithErrorSource retains the selected provider and priority for response presentation.
func (l *CodexLiveLease) WithErrorSource(err error) error {
	return withAuthErrorResponseSource(err, l.auth, "codex")
}

// CommitUpstream is idempotent and serializes with Close. A rejected setup
// returns its reservation; an attempted connection keeps its consumed slot.
func (l *CodexLiveLease) CommitUpstream() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if errCtx := context.Cause(l.ctx); errCtx != nil {
		return errCtx
	}
	if l.closed {
		return context.Canceled
	}
	if !l.slot.Commit() && !l.slot.Committed() {
		return context.Canceled
	}
	return nil
}

// Close also runs on cancellation or credential retirement. It never waits
// for network cleanup and may be called concurrently by session owners.
func (l *CodexLiveLease) Close() {
	if l == nil {
		return
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.closed = true
	stop, release := l.stop, l.release
	l.mu.Unlock()
	if stop != nil {
		stop()
	}
	l.cancel()
	l.slot.Release()
	release()
}
