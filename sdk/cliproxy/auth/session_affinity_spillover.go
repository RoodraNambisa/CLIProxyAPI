package auth

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/admission"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

var errSpilloverBindingChanged = errors.New("sticky spillover binding changed")

func (p *routingRequestPolicy) spilloverEnabled(priority int) bool {
	if v, ok := p.prioritySpillover[priority]; ok {
		return v
	}
	return p.capacitySpillover
}
func (s *sessionBindingSnapshot) preserve() { s.mu.Lock(); s.preserveBinding = true; s.mu.Unlock() }

func (m *Manager) pickStickyCapacitySpillover(ctx context.Context, providers []string, model string, opts core.Options, tried map[string]struct{}, allowed func(*Auth) bool) (*Auth, ProviderExecutor, string, bool, error) {
	p := m.selectionPolicy(ctx)
	if p == nil || !p.spilloverPossible || m.admission == nil || p.strictAffinity {
		return nil, nil, "", false, nil
	}
	selector, ok := p.selector.(*SessionAffinitySelector)
	if !ok || selector == nil || !selector.failover || pinnedAuthIDFromMetadata(opts.Metadata) != "" || authenticatedModelTarget(ctx, opts) != "" || core.SingleAttempt(ctx) || !requestBodyReplayable(ctx, opts) {
		return nil, nil, "", false, nil
	}
	if gjson.GetBytes(opts.OriginalRequest, "previous_response_id").String() != "" || gjson.GetBytes(opts.OriginalRequest, "input.#(encrypted_content)").Exists() {
		return nil, nil, "", false, nil
	}
	binding := sessionBindingSnapshotFromOptions(opts, selector.cache)
	if binding == nil || !binding.allowSpillover {
		return nil, nil, "", false, nil
	}
	id := selector.cachedAuthID(affinityProviderKey(providers), selectionArgForSelector(selector, model), opts, ctx)
	if id == "" {
		return nil, nil, "", false, nil
	}
	if _, used := tried[id]; used {
		return nil, nil, "", false, nil
	}
	bound, exists := m.GetByID(id)
	if !exists {
		return nil, nil, "", false, nil
	}
	effective := stickySpilloverPolicy{authID: bound.ID, instanceID: bound.instanceID, priority: authPriority(bound), enabled: p.spilloverEnabled(authPriority(bound)), queue: p.queue}
	if q, ok := p.priorityQueues[effective.priority]; ok {
		effective.queue = q
	}
	if logical := logicalFrom(ctx); logical != nil {
		logical.spillover.CompareAndSwap(nil, &effective)
		frozen := logical.spillover.Load()
		if frozen.authID != bound.ID || frozen.instanceID != bound.instanceID {
			return nil, nil, "", false, nil
		}
		effective = *frozen
	}
	if !effective.enabled {
		return nil, nil, "", false, nil
	}
	names := append([]string(nil), providers...)
	sort.Strings(names)
	eligible := func(a *Auth) bool {
		if a == nil || a.Disabled || a.instanceID != bound.instanceID || !slices.Contains(names, a.ExecutionProvider()) || !clientKeyPriorityAllowed(ctx, a) || !credentialSupportsExecutionFormat(a, opts.SourceFormat) {
			return false
		}
		if allowed != nil && !allowed(a) {
			return false
		}
		if m.routingStrategyForPriority(authPriority(a), ctx) == schedulerStrategyWeightedRoundRobin && authWeight(a) <= 0 {
			return false
		}
		if blocked, _, _ := isAuthBlockedForModel(a, m.selectionModelForAuth(a, model), time.Now()); blocked {
			return false
		}
		return model == "" || m.AuthSupportsRouteModel(a, model) && registry.GetGlobalRegistry().ClientModelAvailable(a.ID, m.selectionModelForAuth(a, model))
	}
	if !eligible(bound) || !m.fillRPMAvailable(bound, p) || (!selector.acrossPriorities && m.spilloverHigherPriorityReady(ctx, providers, model, opts, authPriority(bound), tried, allowed)) {
		return nil, nil, "", false, nil
	}
	access := ""
	if v, _ := ctx.Value(clientKeyPrioritySnapshotKey{}).(*clientKeyPrioritySnapshot); v != nil {
		access = fmt.Sprintf("%v/%v/%t", v.policy.allowed, v.policy.excluded, v.policy.denyAll)
	}
	prefix := strings.Join(names, ",") + ":" + model + ":" + access + ":"
	scopeKey := "spillover:" + prefix + bound.instanceID + ":" + string(opts.SourceFormat)
	group := admissionRouteGroup(effective.priority)
	queue := effective.queue
	limiter := m.authRequestLimiter()
	policy := m.routingAuthRequestLimitPolicyForAuth(bound)
	available, block := limiter.availableAt(id, policy, limiter.nowTime())
	if available {
		if policy.concurrency <= 0 || !m.admission.Queue.HasWaiters(scopeKey) {
			return nil, nil, "", false, nil
		}
		block = authRequestLimitBlock{concurrency: policy.concurrency, priority: effective.priority}
	} else if block.concurrency == 0 {
		return nil, nil, "", false, nil
	}
	window := limiter.usageAt(id, policy, limiter.nowTime())
	if policy.limit > 0 && window.remaining <= 0 {
		return nil, nil, "", false, nil
	}
	initial := newAuthRequestLimitedError(block)
	var selected *Auth
	var executor ProviderExecutor
	tryBound := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		current, ok := m.GetByID(id)
		if !ok || !eligible(current) || !m.fillRPMAvailable(current, p) {
			return errSpilloverBindingChanged
		}
		binding.mu.Lock()
		same := selector.cache.bindingVersion(binding.key) == binding.version
		binding.mu.Unlock()
		if !same {
			return errSpilloverBindingChanged
		}
		policy := m.routingAuthRequestLimitPolicyForAuth(current)
		policy.requestSlot = opts.AuthRequestSlot
		if acquired, b := limiter.tryAcquireAt(id, policy, limiter.nowTime()); !acquired {
			if b.concurrency == 0 {
				return errSpilloverBindingChanged
			}
			return &admission.Busy{Kind: "concurrency", Reason: initial.Error()}
		}
		if policy.limit == 0 && p.strategyForPriority(authPriority(current)) == schedulerStrategyFillFirst {
			rpm := p.rpmForPriority(authPriority(current))
			if rpm > 0 && !m.fillFirstLimiter().tryAcquireAt(id, rpm, m.fillFirstLimiter().nowTime()) {
				if opts.AuthRequestSlot != nil {
					opts.AuthRequestSlot.FinishReservation()()
				}
				return errSpilloverBindingChanged
			}
		}
		selected = current
		executor, _ = m.Executor(current.ExecutionProvider())
		return nil
	}
	waitErr := error(initial)
	if queue.Enabled {
		var spent time.Duration
		budget := &spent
		bytes := int64(cap(opts.OriginalRequest))
		if logical := logicalFrom(ctx); logical != nil {
			budget = &logical.wait
			bytes = max(bytes, logical.bytes)
			logical.lease.WaitCredential(1)
			defer logical.lease.WaitCredential(-1)
		}
		waitErr = m.admission.Queue.Do(ctx, scopeKey, group, queue, bytes, budget, tryBound)
		if waitErr == nil {
			return selected, executor, selected.ExecutionProvider(), true, nil
		}
		if errors.Is(waitErr, errSpilloverBindingChanged) {
			return nil, nil, "", false, nil
		}
		var busy *admission.Busy
		if !errors.As(waitErr, &busy) || (busy.Kind != "full" && busy.Kind != "timeout") {
			return nil, nil, "", true, waitErr
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, "", true, err
	}
	fallback := *p
	fallback.selector = baseSelector(selector)
	fallbackCtx := context.WithValue(ctx, routingRequestPolicyKey{}, &fallback)
	excluded := make(map[string]struct{}, len(tried)+1)
	for key := range tried {
		excluded[key] = struct{}{}
	}
	excluded[id] = struct{}{}
	borrowingAllowed := func(a *Auth) bool {
		if allowed != nil && !allowed(a) {
			return false
		}
		return !m.admission.Queue.HasWaiters("spillover:" + prefix + a.instanceID + ":" + string(opts.SourceFormat))
	}
	genericScope := "route:" + prefix + ":" + string(opts.SourceFormat)
	var provider string
	err := m.admission.Queue.TryNow(genericScope, group, func() error {
		var errPick error
		selected, executor, provider, errPick = m.pickNextMixedImmediate(fallbackCtx, providers, model, opts, excluded, borrowingAllowed)
		return errPick
	})
	if err != nil {
		return nil, nil, "", true, waitErr
	}
	binding.preserve()
	return selected, executor, provider, true, nil
}

// Existing priority policy still takes precedence unless cross-priority affinity is enabled.
func (m *Manager) spilloverHigherPriorityReady(ctx context.Context, providers []string, model string, opts core.Options, priority int, tried map[string]struct{}, allowed func(*Auth) bool) bool {
	s := m.scheduler
	if s == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, name := range providers {
		provider := s.providers[name]
		if provider == nil {
			continue
		}
		provider.mu.Lock()
		shard := provider.ensureModelLocked(canonicalModelKey(model), time.Now(), core.ChatGPTWebUploadRequired(opts))
		found := false
		if shard != nil {
			shard.promoteExpiredLocked(time.Now())
			for _, tier := range shard.priorityOrder {
				if tier <= priority {
					break
				}
				bucket := shard.readyByPriority[tier]
				if bucket == nil {
					continue
				}
				predicate := func(e *scheduledAuth) bool {
					_, used := tried[e.auth.ID]
					return !used && clientKeyPriorityAllowed(ctx, e.auth) && credentialSupportsExecutionFormat(e.auth, opts.SourceFormat) && (allowed == nil || allowed(e.auth)) && registry.GetGlobalRegistry().ClientModelAvailable(e.auth.ID, canonicalModelKey(model))
				}
				candidate, _ := bucket.fill.firstFillAvailable(s.requestLimitPolicyForAuthLocked, s.requestLimiter, predicate)
				if candidate != nil {
					found = true
					break
				}
			}
		}
		provider.mu.Unlock()
		if found {
			return true
		}
	}
	return false
}

// One immutable policy is shared by every attempt in a logical request.
type stickySpilloverPolicy struct {
	authID, instanceID string
	priority           int
	enabled            bool
	queue              config.AdmissionQueue
}
