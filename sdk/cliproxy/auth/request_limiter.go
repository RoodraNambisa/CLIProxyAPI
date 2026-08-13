package auth

import (
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type authRequestLimitPolicy struct {
	limit         int
	windowMinutes int
	generation    uint64
	requestSlot   *cliproxyexecutor.AuthRequestSlot
}

func normalizeAuthRequestLimitPolicy(policy authRequestLimitPolicy) authRequestLimitPolicy {
	policy.limit = internalconfig.NormalizePerAuthRequestLimit(policy.limit)
	policy.windowMinutes = internalconfig.NormalizePerAuthRequestWindowMinutes(policy.windowMinutes)
	return policy
}

func authRequestLimitPolicyForRouting(routing internalconfig.RoutingConfig, priority int) authRequestLimitPolicy {
	policy := authRequestLimitPolicy{
		limit:         internalconfig.NormalizePerAuthRequestLimit(routing.PerAuthRequestLimit),
		windowMinutes: internalconfig.NormalizePerAuthRequestWindowMinutes(routing.PerAuthRequestWindowMinutes),
	}
	for _, override := range routing.PriorityOverrides {
		if override.Priority != priority {
			continue
		}
		if override.PerAuthRequestLimit != nil {
			policy.limit = internalconfig.NormalizePerAuthRequestLimit(*override.PerAuthRequestLimit)
		}
		if override.PerAuthRequestWindowMinutes != nil {
			policy.windowMinutes = internalconfig.NormalizePerAuthRequestWindowMinutes(*override.PerAuthRequestWindowMinutes)
		}
		break
	}
	return normalizeAuthRequestLimitPolicy(policy)
}

func routingAuthPlanType(auth *Auth) string {
	if auth == nil {
		return ""
	}
	if auth.Attributes != nil {
		if planType := internalconfig.NormalizeRoutingPlanType(auth.Attributes["plan_type"]); planType != "" {
			return planType
		}
	}
	for _, key := range []string{"plan_type", "planType", "account_type", "accountType", "chatgpt_plan_type"} {
		if value, ok := auth.Metadata[key].(string); ok {
			if planType := internalconfig.NormalizeRoutingPlanType(value); planType != "" {
				return planType
			}
		}
	}
	return ""
}

func routingSubscriptionOverrideMatches(override internalconfig.RoutingSubscriptionOverride, auth *Auth, planType string) bool {
	if auth == nil || planType == "" {
		return false
	}
	planMatched := false
	for _, candidate := range override.PlanTypes {
		if internalconfig.NormalizeRoutingPlanType(candidate) == planType {
			planMatched = true
			break
		}
	}
	if !planMatched {
		return false
	}
	if len(override.Providers) == 0 {
		return true
	}
	provider := strings.ToLower(strings.TrimSpace(auth.Provider))
	for _, candidate := range override.Providers {
		if strings.ToLower(strings.TrimSpace(candidate)) == provider {
			return true
		}
	}
	return false
}

func applyRoutingSubscriptionRequestLimitPolicy(policy authRequestLimitPolicy, auth *Auth, overrides []internalconfig.RoutingSubscriptionOverride) authRequestLimitPolicy {
	if len(overrides) == 0 {
		return normalizeAuthRequestLimitPolicy(policy)
	}
	planType := routingAuthPlanType(auth)
	if planType == "" {
		return normalizeAuthRequestLimitPolicy(policy)
	}
	for _, override := range overrides {
		if !routingSubscriptionOverrideMatches(override, auth, planType) {
			continue
		}
		if override.PerAuthRequestLimit != nil {
			policy.limit = internalconfig.NormalizePerAuthRequestLimit(*override.PerAuthRequestLimit)
		}
		if override.PerAuthRequestWindowMinutes != nil {
			policy.windowMinutes = internalconfig.NormalizePerAuthRequestWindowMinutes(*override.PerAuthRequestWindowMinutes)
		}
		break
	}
	return normalizeAuthRequestLimitPolicy(policy)
}

func authRequestLimitPolicyForRoutingAuth(routing internalconfig.RoutingConfig, auth *Auth) authRequestLimitPolicy {
	priority := authPriority(auth)
	policy := authRequestLimitPolicyForRouting(routing, priority)
	for _, override := range routing.PriorityOverrides {
		if override.Priority == priority {
			return applyRoutingSubscriptionRequestLimitPolicy(policy, auth, override.SubscriptionOverrides)
		}
	}
	return policy
}

type authRequestWindowCount struct {
	window        int64
	windowMinutes int
	limit         int
	count         int
	version       uint64
}

type authRequestWindowLimiter struct {
	mu          sync.Mutex
	now         func() time.Time
	generation  uint64
	nextVersion uint64
	counts      map[string]authRequestWindowCount
}

const (
	authRequestReservationReserved uint32 = iota
	authRequestReservationCommitted
	authRequestReservationReleased
)

type authRequestReservation struct {
	limiter       *authRequestWindowLimiter
	authID        string
	generation    uint64
	window        int64
	windowMinutes int
	limit         int
	entryVersion  uint64
	noOp          bool
	state         atomic.Uint32
}

func (r *authRequestReservation) Commit() bool {
	return r != nil && r.state.CompareAndSwap(authRequestReservationReserved, authRequestReservationCommitted)
}

func (r *authRequestReservation) Release() bool {
	if r == nil || !r.state.CompareAndSwap(authRequestReservationReserved, authRequestReservationReleased) {
		return false
	}
	if r.noOp || r.limiter == nil || r.authID == "" {
		return true
	}
	r.limiter.mu.Lock()
	defer r.limiter.mu.Unlock()
	if r.limiter.generation != r.generation {
		return true
	}
	entry, ok := r.limiter.counts[r.authID]
	if !ok || entry.window != r.window || entry.windowMinutes != r.windowMinutes || entry.limit != r.limit || entry.version != r.entryVersion {
		return true
	}
	if entry.count <= 1 {
		delete(r.limiter.counts, r.authID)
		return true
	}
	entry.count--
	r.limiter.counts[r.authID] = entry
	return true
}

func (r *authRequestReservation) Reserved() bool {
	return r != nil && r.state.Load() == authRequestReservationReserved
}

func (r *authRequestReservation) Committed() bool {
	return r != nil && r.state.Load() == authRequestReservationCommitted
}

func (r *authRequestReservation) Consumed() bool {
	return r != nil && !r.noOp
}

func newAuthRequestWindowLimiter() *authRequestWindowLimiter {
	return &authRequestWindowLimiter{generation: 1, counts: make(map[string]authRequestWindowCount)}
}

func (l *authRequestWindowLimiter) nowTime() time.Time {
	if l != nil && l.now != nil {
		return l.now()
	}
	return time.Now()
}

func authRequestWindowAt(now time.Time, windowMinutes int) (int64, time.Time) {
	windowMinutes = internalconfig.NormalizePerAuthRequestWindowMinutes(windowMinutes)
	windowSeconds := int64(windowMinutes) * int64(time.Minute/time.Second)
	if windowSeconds <= 0 {
		windowSeconds = int64(time.Minute / time.Second)
	}
	now = now.UTC()
	unixSeconds := now.Unix()
	window := unixSeconds / windowSeconds
	remainder := unixSeconds % windowSeconds
	if remainder < 0 {
		remainder += windowSeconds
		window--
	}
	resetIn := time.Duration(windowSeconds-remainder)*time.Second - time.Duration(now.Nanosecond())
	return window, now.Add(resetIn)
}

func (l *authRequestWindowLimiter) reset(generation uint64) {
	if l == nil {
		return
	}
	if generation == 0 {
		generation = 1
	}
	l.mu.Lock()
	l.generation = generation
	clear(l.counts)
	l.mu.Unlock()
}

func (l *authRequestWindowLimiter) remove(authID string) {
	if l == nil || authID == "" {
		return
	}
	l.mu.Lock()
	delete(l.counts, authID)
	l.mu.Unlock()
}

func (l *authRequestWindowLimiter) availableAt(authID string, policy authRequestLimitPolicy, now time.Time) (bool, authRequestLimitBlock) {
	policy = normalizeAuthRequestLimitPolicy(policy)
	if l == nil || authID == "" {
		return true, authRequestLimitBlock{}
	}
	var window int64
	var resetAt time.Time
	if policy.limit > 0 {
		window, resetAt = authRequestWindowAt(now, policy.windowMinutes)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if policy.generation != 0 && policy.generation != l.generation {
		return true, authRequestLimitBlock{}
	}
	if policy.limit == 0 {
		return true, authRequestLimitBlock{}
	}
	entry := l.counts[authID]
	if entry.window != window || entry.windowMinutes != policy.windowMinutes || entry.limit != policy.limit || entry.count < policy.limit {
		return true, authRequestLimitBlock{}
	}
	return false, newAuthRequestLimitBlock(policy, resetAt.Sub(now))
}

func (l *authRequestWindowLimiter) tryAcquireAt(authID string, policy authRequestLimitPolicy, now time.Time) (bool, authRequestLimitBlock) {
	reservation, acquired, block := l.reserveAt(authID, policy, now)
	if !acquired {
		return false, block
	}
	if policy.requestSlot != nil {
		policy.requestSlot.Bind(reservation)
		return true, authRequestLimitBlock{}
	}
	reservation.Commit()
	return true, authRequestLimitBlock{}
}

func (l *authRequestWindowLimiter) reserveAt(authID string, policy authRequestLimitPolicy, now time.Time) (*authRequestReservation, bool, authRequestLimitBlock) {
	policy = normalizeAuthRequestLimitPolicy(policy)
	if l == nil || authID == "" {
		return &authRequestReservation{authID: authID, noOp: true}, true, authRequestLimitBlock{}
	}
	var window int64
	var resetAt time.Time
	if policy.limit > 0 {
		window, resetAt = authRequestWindowAt(now, policy.windowMinutes)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if policy.generation != 0 && policy.generation != l.generation {
		return nil, false, authRequestLimitBlock{stalePolicy: true}
	}
	if policy.limit == 0 {
		return &authRequestReservation{limiter: l, authID: authID, generation: l.generation, windowMinutes: policy.windowMinutes, noOp: true}, true, authRequestLimitBlock{}
	}
	if l.counts == nil {
		l.counts = make(map[string]authRequestWindowCount)
	}
	entry := l.counts[authID]
	if entry.window != window || entry.windowMinutes != policy.windowMinutes || entry.limit != policy.limit {
		l.nextVersion++
		entry = authRequestWindowCount{window: window, windowMinutes: policy.windowMinutes, limit: policy.limit, version: l.nextVersion}
	}
	if entry.count >= policy.limit {
		return nil, false, newAuthRequestLimitBlock(policy, resetAt.Sub(now))
	}
	entry.count++
	l.counts[authID] = entry
	return &authRequestReservation{
		limiter:       l,
		authID:        authID,
		generation:    l.generation,
		window:        window,
		windowMinutes: policy.windowMinutes,
		limit:         policy.limit,
		entryVersion:  entry.version,
	}, true, authRequestLimitBlock{}
}

func (m *Manager) acquireAdditionalAuthRequest(auth *Auth, requestSlot *cliproxyexecutor.AuthRequestSlot) error {
	if m == nil || auth == nil {
		return nil
	}
	if requestSlot != nil {
		requestSlot.Release()
	}
	limiter := m.authRequestLimiter()
	if limiter == nil {
		return nil
	}
	for {
		policy := m.routingAuthRequestLimitPolicyForAuth(auth)
		policy.requestSlot = requestSlot
		acquired, block := limiter.tryAcquireAt(auth.ID, policy, limiter.nowTime())
		if acquired {
			commitImmediateAuthRequestReservation(auth, requestSlot)
			return nil
		}
		if block.stalePolicy {
			continue
		}
		return newAuthRequestLimitedError(block)
	}
}

func commitImmediateAuthRequestReservation(auth *Auth, requestSlot *cliproxyexecutor.AuthRequestSlot) {
	if auth == nil || requestSlot == nil {
		return
	}
	if !strings.EqualFold(strings.TrimSpace(auth.Provider), "chatgpt-web") {
		requestSlot.Commit()
	}
}

type authRequestLimitBlock struct {
	limit         int
	windowMinutes int
	resetIn       time.Duration
	stalePolicy   bool
}

func newAuthRequestLimitBlock(policy authRequestLimitPolicy, resetIn time.Duration) authRequestLimitBlock {
	if resetIn < 0 {
		resetIn = 0
	}
	policy = normalizeAuthRequestLimitPolicy(policy)
	return authRequestLimitBlock{limit: policy.limit, windowMinutes: policy.windowMinutes, resetIn: resetIn}
}

func (b authRequestLimitBlock) limited() bool {
	return b.limit > 0
}

func earlierAuthRequestLimitBlock(current, candidate authRequestLimitBlock) authRequestLimitBlock {
	if !candidate.limited() {
		return current
	}
	if !current.limited() || candidate.resetIn < current.resetIn {
		return candidate
	}
	return current
}

type authRequestLimitedError struct {
	authRequestLimitBlock
}

func newAuthRequestLimitedError(block authRequestLimitBlock) *authRequestLimitedError {
	return &authRequestLimitedError{authRequestLimitBlock: block}
}

func (e *authRequestLimitedError) Error() string {
	resetSeconds := int64(math.Ceil(e.resetIn.Seconds()))
	if resetSeconds < 0 {
		resetSeconds = 0
	}
	payload := map[string]any{
		"error": map[string]any{
			"code":           "auth_request_limited",
			"message":        "All available credentials reached their request limit",
			"limit":          e.limit,
			"window_minutes": e.windowMinutes,
			"reset_seconds":  resetSeconds,
		},
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return `{"error":{"code":"auth_request_limited","message":"All available credentials reached their request limit"}}`
	}
	return string(data)
}

func (e *authRequestLimitedError) StatusCode() int {
	return http.StatusTooManyRequests
}

func (e *authRequestLimitedError) Headers() http.Header {
	headers := make(http.Header)
	headers.Set("Content-Type", "application/json")
	resetSeconds := int64(math.Ceil(e.resetIn.Seconds()))
	if resetSeconds < 0 {
		resetSeconds = 0
	}
	headers.Set("Retry-After", strconv.FormatInt(resetSeconds, 10))
	return headers
}
