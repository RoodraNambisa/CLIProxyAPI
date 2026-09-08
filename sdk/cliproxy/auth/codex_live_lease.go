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
	return m.acquireCodexLive(ctx, nil, model, opts)
}

// AcquireCodexLiveSession selects using the setup request but retains only the
// caller-supplied lifetime context. It must not contain a Gin context or body.
// The owner must close an uncommitted lease on setup failure or cancellation.
func (m *Manager) AcquireCodexLiveSession(ctx, lifetime context.Context, model string, opts core.Options) (*CodexLiveLease, error) {
	if lifetime == nil {
		return nil, &Error{Code: "invalid_request", Message: "realtime session lifetime is required", HTTPStatus: http.StatusBadRequest}
	}
	if errCtx := context.Cause(lifetime); errCtx != nil {
		return nil, errCtx
	}
	return m.acquireCodexLive(ctx, lifetime, model, opts)
}

func (m *Manager) acquireCodexLive(ctx, lifetime context.Context, model string, opts core.Options) (*CodexLiveLease, error) {
	if m == nil {
		return nil, &Error{Code: "provider_not_found", Message: "manager unavailable", HTTPStatus: http.StatusServiceUnavailable}
	}
	model = strings.TrimSpace(model)
	if model == "" {
		return nil, &Error{Code: "invalid_request", Message: "realtime model is required", HTTPStatus: http.StatusBadRequest}
	}
	ctx = m.WithRoutingPolicySnapshot(ctx)
	producerBase := ctx
	if lifetime != nil {
		producerBase = lifetime
	}
	producerCtx, producer, releaseProducer, errProducer := m.beginResultPersistenceProducer(producerBase)
	if errProducer != nil {
		return nil, errProducer
	}
	if lifetime == nil {
		ctx = producerCtx
	} else {
		// The producer's cancellation closure must not retain the setup request.
		lifetime = producerCtx
		setupCtx, cancelSetup := context.WithCancelCause(ctx)
		stopProducer := context.AfterFunc(producerCtx, func() { cancelSetup(context.Cause(producerCtx)) })
		if errCtx := context.Cause(producerCtx); errCtx != nil {
			cancelSetup(errCtx)
		}
		defer func() { stopProducer(); cancelSetup(nil) }()
		ctx = context.WithValue(setupCtx, resultPersistenceProducerContextKey{}, producer)
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
	rt := m.roundTripperFor(resolved)
	if rt != nil {
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
	if lifetime == nil {
		lifetime = ctx
	} else if rt != nil {
		lifetime = context.WithValue(lifetime, roundTripperContextKey{}, rt)
		lifetime = context.WithValue(lifetime, "cliproxy.roundtripper", rt)
	}
	leaseCtx, cancel := context.WithCancel(lifetime)
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
	if errCtx := context.Cause(ctx); errCtx != nil {
		lease.Close()
		return nil, errCtx
	}
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
