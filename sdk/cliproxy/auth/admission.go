package auth

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/admission"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type admissionContextKey struct{}
type logicalAdmission struct {
	lease        *admission.Lease
	wait         time.Duration
	bytes        int64
	managerOwned bool
	once         sync.Once
	refs         atomic.Int64
}

func logicalFrom(ctx context.Context) *logicalAdmission {
	if ctx == nil {
		return nil
	}
	v, _ := ctx.Value(admissionContextKey{}).(*logicalAdmission)
	return v
}

// BeginClientRequest is shared by handler bootstrap retries and image aggregation.
func (m *Manager) BeginClientRequest(ctx context.Context, body []byte) (context.Context, func(), error) {
	return m.beginClientRequest(ctx, body, false)
}
func (m *Manager) beginClientRequest(ctx context.Context, body []byte, owned bool) (context.Context, func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if m == nil || m.admission == nil || logicalFrom(ctx) != nil {
		return ctx, func() {}, nil
	}
	scope := &logicalAdmission{bytes: int64(cap(body)), managerOwned: owned}
	var keyWait time.Duration
	digest, required := clientKeyScope(ctx)
	id := "anonymous"
	if digest != [32]byte{} {
		id = hex.EncodeToString(digest[:16])
	}
	lease, err := m.admission.AcquireIdentity(ctx, id, required, scope.bytes, &keyWait)
	if err != nil {
		return ctx, func() {}, err
	}
	scope.lease = lease
	scope.refs.Store(1)
	ctx = context.WithValue(ctx, admissionContextKey{}, scope)
	return ctx, func() { scope.once.Do(scope.releaseRef) }, nil
}
func finishManagedLogical(ctx context.Context) {
	if s := logicalFrom(ctx); s != nil && s.managerOwned {
		s.once.Do(s.releaseRef)
	}
}
func (m *Manager) TrafficController() *admission.Controller { return m.admission }
func admissionRouteGroup(priority int) string               { return fmt.Sprintf("route:%d", priority) }

// queueSelection retries only local capacity failures and never changes routing rounds.
func (m *Manager) queueSelection(ctx context.Context, providers []string, model string, opts core.Options, try func() error) error {
	if m.admission == nil || !m.admission.RouteQueueEnabled.Load() {
		return try()
	}
	names := append([]string(nil), providers...)
	sort.Strings(names)
	target := pinnedAuthIDFromMetadata(opts.Metadata)
	if selector, ok := m.selectorForContext(ctx).(*SessionAffinitySelector); ok && !selector.failover {
		target += ":" + selector.cachedStrictAuthID(ctx, strings.Join(names, ","), selectionArgForSelector(selector, model), opts)
	}
	accessScope := ""
	if snapshot, _ := ctx.Value(clientKeyPrioritySnapshotKey{}).(*clientKeyPrioritySnapshot); snapshot != nil {
		accessScope = fmt.Sprintf("%v/%v/%t", snapshot.policy.allowed, snapshot.policy.excluded, snapshot.policy.denyAll)
	}
	key := "route:" + strings.Join(names, ",") + ":" + model + ":" + accessScope + ":" + target + ":" + string(opts.SourceFormat)
	group := m.admission.Queue.Group(key)
	var blocked *authRequestLimitedError
	var initialError error
	if group == "" {
		err := try()
		initialError = err
		blocked = queueCapacityBlock(err)
		if blocked == nil {
			return err
		}
		group = admissionRouteGroup(blocked.priority)
	}
	priority := 0
	_, _ = fmt.Sscanf(group, "route:%d", &priority)
	policy := m.authAdmissionQueue(priority)
	if !policy.Enabled {
		if blocked != nil {
			return initialError
		}
		return try()
	}
	scope := logicalFrom(ctx)
	var spent time.Duration
	bytes := int64(cap(opts.OriginalRequest))
	budget := &spent
	if scope != nil {
		budget = &scope.wait
		bytes = max(bytes, scope.bytes)
	}
	if scope != nil {
		scope.lease.WaitCredential(1)
		defer scope.lease.WaitCredential(-1)
	}
	return m.admission.Queue.Do(ctx, key, group, policy, bytes, budget, func() error {
		err := try()
		b := queueCapacityBlock(err)
		if b == nil {
			return err
		}
		busy := &admission.Busy{Reason: err.Error()}
		if b.concurrency == 0 {
			busy.ReadyAt = time.Now().Add(b.resetIn)
		}
		return busy
	})
}

func (m *Manager) pickNext(ctx context.Context, provider, model string, opts core.Options, tried map[string]struct{}) (a *Auth, e ProviderExecutor, err error) {
	err = m.queueSelection(ctx, []string{provider}, model, opts, func() error { var x error; a, e, x = m.pickNextImmediate(ctx, provider, model, opts, tried); return x })
	return
}
func (m *Manager) pickNextMixed(ctx context.Context, providers []string, model string, opts core.Options, tried map[string]struct{}, allowed ...func(*Auth) bool) (a *Auth, e ProviderExecutor, p string, err error) {
	err = m.queueSelection(ctx, providers, model, opts, func() error {
		var x error
		a, e, p, x = m.pickNextMixedImmediate(ctx, providers, model, opts, tried, allowed...)
		return x
	})
	return
}

// authAdmissionQueue is exposed only as resolved policy metadata.
func (m *Manager) authAdmissionQueue(priority int) config.AdmissionQueue {
	cfg := m.currentConfig()
	if cfg == nil {
		return config.AdmissionQueue{}
	}
	q := cfg.Routing.Queue
	for _, r := range cfg.Routing.PriorityOverrides {
		if r.Priority == priority && r.Queue != nil {
			q = *r.Queue
			break
		}
	}
	return q
}

func (m *Manager) CredentialTraffic(indexes []string, now time.Time) []admission.Snapshot {
	out := make([]admission.Snapshot, 0, len(indexes))
	for _, index := range indexes {
		m.mu.RLock()
		var a *Auth
		for id := range m.authIDsByIndex[index] {
			if current := m.auths[id]; current != nil {
				copyAuth := *current
				a = &copyAuth
				break
			}
		}
		m.mu.RUnlock()
		if a == nil {
			continue
		}
		v := m.admission.Traffic.Lookup("credential", a.ID).Snapshot(now)
		v.ID = index
		p := m.routingAuthRequestLimitPolicyForAuth(a)
		v.Limit = p.concurrency
		v.RPMLimit = p.limit
		v.WindowMinutes = p.windowMinutes
		u := m.authRequestLimiter().usageAt(a.ID, p, now)
		v.WindowUsed = u.limit - u.remaining
		summary := m.AuthRequestLimitSummary(a)
		v.Source = summary.Source
		if p.limit == 0 {
			v.WindowUsed, v.WindowMinutes = int(v.CurrentMinute), 1
			if summary.Source == "fill_first" {
				v.RPMLimit = summary.Limit
				limiter := m.fillFirstLimiter()
				limiter.mu.Lock()
				if limiter.window == fillFirstMinuteWindow(now) {
					v.WindowUsed = limiter.counts[a.ID]
				} else {
					v.WindowUsed = 0
				}
				limiter.mu.Unlock()
			}
		}
		v.ConcurrencySource = "global"
		if cfg := m.currentConfig(); cfg != nil {
			for _, rule := range cfg.Routing.PriorityOverrides {
				if rule.Priority != p.priority {
					continue
				}
				if rule.PerAuthConcurrency != nil {
					v.ConcurrencySource = "priority"
				}
				for layer, index := range routingSubscriptionMatchIndexes(rule.SubscriptionOverrides, a) {
					if index >= 0 && rule.SubscriptionOverrides[index].PerAuthConcurrency != nil {
						v.ConcurrencySource = "subscription"
						if layer == 1 {
							v.ConcurrencySource = "credential"
						}
					}
				}
				break
			}
		}
		v.QueueScope = admissionRouteGroup(p.priority)
		v.Queued, _ = m.admission.Queue.Counts(admissionRouteGroup(p.priority))
		out = append(out, v)
	}
	return out
}

func (m *Manager) reserveTargetConcurrency(a *Auth, opts core.Options) error {
	p := m.routingAuthRequestLimitPolicyForAuth(a)
	p.limit = 0
	p.requestSlot = opts.AuthRequestSlot
	l := m.authRequestLimiter()
	ok, block := l.tryAcquireAt(a.ID, p, l.nowTime())
	if !ok {
		return newAuthRequestLimitedError(block)
	}
	return nil
}

// InheritClientRequest carries a handler-owned lease across derived contexts.
func InheritClientRequest(destination, source context.Context) context.Context {
	if source != nil {
		if scope := logicalFrom(source); scope != nil {
			return context.WithValue(destination, admissionContextKey{}, scope)
		}
	}
	return destination
}

func (s *logicalAdmission) releaseRef() {
	if s.refs.Add(-1) == 0 {
		s.lease.Close()
	}
}
func retainLogicalStream(ctx context.Context) func() {
	if s := logicalFrom(ctx); s != nil {
		s.refs.Add(1)
		return s.releaseRef
	}
	return func() {}
}

func queueCapacityBlock(err error) *authRequestLimitedError {
	var generic *authRequestLimitedError
	if errors.As(err, &generic) {
		return generic
	}
	var legacy *authRPMLimitedError
	if errors.As(err, &legacy) {
		return &authRequestLimitedError{authRequestLimitBlock: authRequestLimitBlock{priority: legacy.priority, resetIn: legacy.resetIn}}
	}
	return nil
}
