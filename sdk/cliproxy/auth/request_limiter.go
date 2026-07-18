package auth

import (
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

type authRequestLimitPolicy struct {
	limit         int
	windowMinutes int
	generation    uint64
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

type authRequestWindowCount struct {
	window        int64
	windowMinutes int
	limit         int
	count         int
}

type authRequestWindowLimiter struct {
	mu         sync.Mutex
	now        func() time.Time
	generation uint64
	counts     map[string]authRequestWindowCount
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
		return false, authRequestLimitBlock{stalePolicy: true}
	}
	if policy.limit == 0 {
		return true, authRequestLimitBlock{}
	}
	if l.counts == nil {
		l.counts = make(map[string]authRequestWindowCount)
	}
	entry := l.counts[authID]
	if entry.window != window || entry.windowMinutes != policy.windowMinutes || entry.limit != policy.limit {
		entry = authRequestWindowCount{window: window, windowMinutes: policy.windowMinutes, limit: policy.limit}
	}
	if entry.count >= policy.limit {
		return false, newAuthRequestLimitBlock(policy, resetAt.Sub(now))
	}
	entry.count++
	l.counts[authID] = entry
	return true, authRequestLimitBlock{}
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
