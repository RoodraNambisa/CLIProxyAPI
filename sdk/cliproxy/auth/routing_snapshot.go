package auth

import (
	"context"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

// routingRequestPolicy freezes selection choices, not credential availability or
// limiter generations. Retired instances and current capacity remain authoritative.
type routingRequestPolicy struct {
	manager       *Manager
	selector      Selector
	strategy      schedulerStrategy
	strategies    map[int]schedulerStrategy
	fillRange     int
	fillRPM       int
	priorityRange map[int]int
	priorityRPM   map[int]int
}

type routingRequestPolicyKey struct{}

// Only owned, fully known selector chains can be retired here. Opaque custom
// selectors may retain a previous selector and keep their existing lifecycle.
func sessionCacheMaintenanceChain(selector Selector) (map[*SessionCache]struct{}, bool) {
	caches := make(map[*SessionCache]struct{})
	visited := make(map[*SessionAffinitySelector]struct{})
	for {
		switch current := selector.(type) {
		case *SessionAffinitySelector:
			if current == nil {
				return caches, false
			}
			if _, exists := visited[current]; exists {
				return caches, false
			}
			visited[current] = struct{}{}
			if current.cache != nil {
				caches[current.cache] = struct{}{}
			}
			selector = current.fallback
		case nil, *RoundRobinSelector, *FillFirstSelector, *RandomSelector, *WeightedRoundRobinSelector:
			return caches, true
		default:
			return caches, false
		}
	}
}

func stopReplacedSessionCacheMaintenance(previous, next Selector) {
	retained, known := sessionCacheMaintenanceChain(next)
	if !known {
		return
	}
	old, _ := sessionCacheMaintenanceChain(previous)
	for cache := range old {
		if _, exists := retained[cache]; !exists {
			cache.Stop()
		}
	}
}

func newRoutingRequestPolicy(m *Manager, selector Selector, routing config.RoutingConfig) *routingRequestPolicy {
	p := &routingRequestPolicy{
		manager: m, selector: selector, strategy: selectorStrategy(selector),
		strategies:    make(map[int]schedulerStrategy),
		fillRange:     normalizeFillFirstRangeValue(routing.FillFirstRange),
		fillRPM:       normalizeFillFirstPerAuthRPMValue(routing.FillFirstPerAuthRPM),
		priorityRange: make(map[int]int), priorityRPM: make(map[int]int),
	}
	for _, rule := range routing.PriorityOverrides {
		if strategy, ok := schedulerStrategyFromName(rule.Strategy); ok && strings.TrimSpace(rule.Strategy) != "" {
			p.strategies[rule.Priority] = strategy
		}
		if rule.FillFirstRange != nil {
			p.priorityRange[rule.Priority] = normalizeFillFirstRangeValue(*rule.FillFirstRange)
		}
		if rule.FillFirstPerAuthRPM != nil {
			p.priorityRPM[rule.Priority] = normalizeFillFirstPerAuthRPMValue(*rule.FillFirstPerAuthRPM)
		}
	}
	return p
}

func routingPolicyFromContext(ctx context.Context) *routingRequestPolicy {
	if ctx == nil {
		return nil
	}
	p, _ := ctx.Value(routingRequestPolicyKey{}).(*routingRequestPolicy)
	return p
}

// WithRoutingPolicySnapshot preserves one logical request's choices across SDK
// attempts and handler bootstrap retries. It retains no credential pool or body.
func (m *Manager) WithRoutingPolicySnapshot(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if m == nil {
		return ctx
	}
	if p := routingPolicyFromContext(ctx); p != nil && p.manager == m {
		return ctx
	}
	if p := m.routingPolicy.Load(); p != nil {
		return context.WithValue(ctx, routingRequestPolicyKey{}, p)
	}
	return ctx
}

func (m *Manager) selectionPolicy(contexts ...context.Context) *routingRequestPolicy {
	if m == nil {
		return nil
	}
	if len(contexts) > 0 {
		if p := routingPolicyFromContext(contexts[0]); p != nil && p.manager == m {
			return p
		}
	}
	return m.routingPolicy.Load()
}

func (m *Manager) selectorForContext(contexts ...context.Context) Selector {
	if p := m.selectionPolicy(contexts...); p != nil {
		return p.selector
	}
	return &RoundRobinSelector{}
}

func (p *routingRequestPolicy) strategyForPriority(priority int) schedulerStrategy {
	if value, ok := p.strategies[priority]; ok {
		return value
	}
	return p.strategy
}

func (p *routingRequestPolicy) rangeForPriority(priority int) int {
	if value, ok := p.priorityRange[priority]; ok {
		return value
	}
	return p.fillRange
}

func (p *routingRequestPolicy) rpmForPriority(priority int) int {
	if value, ok := p.priorityRPM[priority]; ok {
		return value
	}
	return p.fillRPM
}
