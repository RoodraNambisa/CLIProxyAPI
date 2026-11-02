package auth

import (
	"math/rand/v2"
	"sort"
	"time"
)

// firstFillAvailable reads capacity without reserving it, allowing mixed-provider
// selection to reserve exactly one winner. Failed reservations invalidate its index.
func (v *readyView) firstFillAvailable(policy func(*Auth) authRequestLimitPolicy, l *authRequestWindowLimiter, predicate func(*scheduledAuth) bool) (*scheduledAuth, authRequestLimitBlock) {
	now := l.nowTime()
	var index *requestLimitReadyIndex
	if v.limited != nil {
		index = v.requestLimitIndex(l, now)
	}
	var blocked authRequestLimitBlock
	for pos := 0; pos < len(v.flat); {
		if index != nil {
			pos = index.nextOrdered(pos, false)
			if pos < 0 {
				break
			}
		}
		candidate := v.flat[pos]
		pos++
		if predicate != nil && !predicate(candidate) {
			continue
		}
		selectionRuntime.candidates.Add(1)
		available, b := l.availableAt(candidate.auth.ID, policy(candidate.auth), now)
		if available {
			return candidate, blocked
		}
		blocked = earlierAuthRequestLimitBlock(blocked, b)
		if b.limited() {
			index = v.requestLimitIndex(l, now)
			index.exclude(candidate, b, now)
		}
	}
	if index != nil {
		blocked = earlierAuthRequestLimitBlock(blocked, index.earliestBlock(predicate, now))
	}
	return nil, blocked
}
func pickMixedOrderedFillHead(providers []string, shards []*modelScheduler, priority int, policy func(*Auth) authRequestLimitPolicy, l *authRequestWindowLimiter, membership, predicate func(*scheduledAuth) bool) (*Auth, string, bool, authRequestLimitBlock) {
	allowed := func(e *scheduledAuth) bool {
		return (membership == nil || membership(e)) && (predicate == nil || predicate(e))
	}
	var blocked authRequestLimitBlock
	for {
		var chosen *scheduledAuth
		var view *readyView
		provider := ""
		for i, shard := range shards {
			if shard == nil || i >= len(providers) {
				continue
			}
			bucket := shard.readyByPriority[priority]
			if bucket == nil {
				continue
			}
			candidate, b := bucket.fill.firstFillAvailable(policy, l, allowed)
			blocked = earlierAuthRequestLimitBlock(blocked, b)
			if candidate != nil && (chosen == nil || fillOrderLess(candidate.auth, chosen.auth)) {
				chosen, view, provider = candidate, &bucket.fill, providers[i]
			}
		}
		if chosen == nil {
			return nil, "", false, blocked
		}
		now := l.nowTime()
		if ok, b := l.tryAcquireAt(chosen.auth.ID, policy(chosen.auth), now); ok {
			return chosen.auth, provider, false, authRequestLimitBlock{}
		} else {
			if b.stalePolicy {
				return nil, "", false, b
			}
			blocked = earlierAuthRequestLimitBlock(blocked, b)
			view.requestLimitIndex(l, now).exclude(chosen, b, now)
		}
	}
}

// Refresh legacy RPM eligibility without charging a new attempt during queueing.
func (m *Manager) fillRPMAvailable(a *Auth, ctxPolicy *routingRequestPolicy) bool {
	if ctxPolicy.strategyForPriority(authPriority(a)) != schedulerStrategyFillFirst || m.routingAuthRequestLimitPolicyForAuth(a).limit > 0 {
		return true
	}
	limit := ctxPolicy.rpmForPriority(authPriority(a))
	if limit <= 0 {
		return true
	}
	l := m.fillFirstLimiter()
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.window != fillFirstMinuteWindow(l.nowTime()) || l.counts[a.ID] < limit
}

// Legacy fill RPM has its own minute window. This ordered exclusion index is
// rebuilt only at the minute boundary or after a view/configuration change.
type fillRPMIndex struct {
	generation uint64
	window     int64
	limit      int
	order      requestLimitReadyIndex
}

func (v *readyView) rpmIndex(l *fillFirstMinuteLimiter, limit int, generation uint64) *fillRPMIndex {
	window := fillFirstMinuteWindow(l.nowTime())
	if v.fillRPM == nil || v.fillRPM.window != window || v.fillRPM.limit != limit || v.fillRPM.generation != generation {
		v.fillRPM = &fillRPMIndex{window: window, limit: limit, generation: generation}
		v.fillRPM.order.reset(v.flat)
	}
	return v.fillRPM
}
func (v *readyView) excludeFillRPM(entry *scheduledAuth, l *fillFirstMinuteLimiter, limit int, generation uint64) {
	idx := v.rpmIndex(l, limit, generation)
	if _, exists := idx.order.positions[entry.auth.ID]; !exists {
		return
	}
	delete(idx.order.positions, entry.auth.ID)
	idx.order.updateOrder(idx.order.order[entry.auth.ID], -1)
}
func (l *fillFirstMinuteLimiter) availableAt(id string, limit int, now time.Time) bool {
	if l == nil || limit <= 0 {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.window != fillFirstMinuteWindow(now) || l.counts[id] < limit
}
func (v *readyView) firstFillGroupAvailable(policy func(*Auth) authRequestLimitPolicy, l *authRequestWindowLimiter, predicate func(*scheduledAuth) bool, rpm *fillFirstMinuteLimiter, rpmLimit int) (*scheduledAuth, authRequestLimitBlock, bool) {
	now := l.nowTime()
	var idx *requestLimitReadyIndex
	if v.limited != nil {
		idx = v.requestLimitIndex(l, now)
	}
	var ri *fillRPMIndex
	if rpm != nil && rpmLimit > 0 {
		ri = v.rpmIndex(rpm, rpmLimit, l.generationAtomic.Load())
	}
	var block authRequestLimitBlock
	rpmLimited := ri != nil && len(ri.order.positions) < len(v.flat)
	for pos := 0; pos < len(v.flat); {
		if idx != nil {
			pos = idx.nextOrdered(pos, false)
			if pos < 0 {
				break
			}
		}
		if ri != nil {
			next := ri.order.nextOrdered(pos, false)
			if next < 0 {
				break
			}
			if next != pos {
				pos = next
				continue
			}
		}
		e := v.flat[pos]
		pos++
		if predicate != nil && !predicate(e) {
			continue
		}
		selectionRuntime.candidates.Add(1)
		p := policy(e.auth)
		if ok, b := l.availableAt(e.auth.ID, p, now); !ok {
			block = earlierAuthRequestLimitBlock(block, b)
			if b.stalePolicy {
				return nil, b, rpmLimited
			}
			if b.limited() {
				idx = v.requestLimitIndex(l, now)
				idx.exclude(e, b, now)
			}
			continue
		}
		if p.limit == 0 && rpm != nil && rpmLimit > 0 && !rpm.availableAt(e.auth.ID, rpmLimit, rpm.nowTime()) {
			v.excludeFillRPM(e, rpm, rpmLimit, l.generationAtomic.Load())
			rpmLimited = true
			continue
		}
		return e, block, rpmLimited
	}
	if idx != nil {
		block = earlierAuthRequestLimitBlock(block, idx.earliestBlock(predicate, now))
	}
	return nil, block, rpmLimited
}

// Find the first group with capacity using ordered heads. Binary searches locate
// its ordinal across providers; only that group's members are merged, never the
// complete credential pool. Group boundaries are stable across capacity changes.
func pickOrderedFillGroup(providers []string, shards []*modelScheduler, priority int, ws bool, width, rpmLimit int, rpm *fillFirstMinuteLimiter, policy func(*Auth) authRequestLimitPolicy, l *authRequestWindowLimiter, membership, predicate func(*scheduledAuth) bool) (*Auth, string, bool, authRequestLimitBlock) {
	width = normalizeFillFirstRangeValue(width)
	rpmLimit = normalizeFillFirstPerAuthRPMValue(rpmLimit)
	views := make([]*readyView, len(shards))
	members := make([][]*scheduledAuth, len(shards))
	for i, s := range shards {
		if s == nil {
			continue
		}
		members[i] = s.fillMembers[priority]
		if ws {
			members[i] = s.fillMembersWS[priority]
		}
		if b := s.readyByPriority[priority]; b != nil {
			views[i] = &b.fill
			if ws {
				views[i] = &b.fillWS
			}
		}
	}
	allowed := func(e *scheduledAuth) bool {
		return (membership == nil || membership(e)) && (predicate == nil || predicate(e))
	}
	var blocked authRequestLimitBlock
	rpmLimited := false
	for {
		var first *scheduledAuth
		for _, v := range views {
			if v == nil {
				continue
			}
			e, b, r := v.firstFillGroupAvailable(policy, l, allowed, rpm, rpmLimit)
			blocked = earlierAuthRequestLimitBlock(blocked, b)
			rpmLimited = rpmLimited || r
			if b.stalePolicy {
				return nil, "", rpmLimited, b
			}
			if e != nil && (first == nil || fillOrderLess(e.auth, first.auth)) {
				first = e
			}
		}
		if first == nil {
			return nil, "", rpmLimited, blocked
		}
		positions := make([]int, len(members))
		ordinal := 0
		for i, entries := range members {
			positions[i] = sort.Search(len(entries), func(j int) bool { return !fillOrderLess(entries[j].auth, first.auth) })
			ordinal += positions[i]
		}
		// Walk backwards by at most one group to its fixed starting position.
		for back := ordinal % width; back > 0; back-- {
			best := -1
			for i, entries := range members {
				pos := positions[i] - 1
				if pos >= 0 && (best < 0 || fillOrderLess(members[best][positions[best]-1].auth, entries[pos].auth)) {
					best = i
				}
			}
			if best < 0 {
				break
			}
			positions[best]--
		}
		candidates := make([]mixedFillFirstEntry, 0, min(width, 64))
		candidateViews := make(map[string]*readyView, len(providers))
		for count := 0; count < width; count++ {
			best := -1
			for i, entries := range members {
				if positions[i] < len(entries) && (best < 0 || fillOrderLess(entries[positions[i]].auth, members[best][positions[best]].auth)) {
					best = i
				}
			}
			if best < 0 {
				break
			}
			e := members[best][positions[best]]
			positions[best]++
			if e.state != scheduledStateReady || !allowed(e) {
				continue
			}
			selectionRuntime.candidates.Add(1)
			p := policy(e.auth)
			now := l.nowTime()
			if ok, b := l.availableAt(e.auth.ID, p, now); !ok {
				blocked = earlierAuthRequestLimitBlock(blocked, b)
				if b.stalePolicy {
					return nil, "", rpmLimited, b
				}
				if views[best] != nil && b.limited() {
					views[best].requestLimitIndex(l, now).exclude(e, b, now)
				}
				continue
			}
			if p.limit == 0 && rpm != nil && rpmLimit > 0 && !rpm.availableAt(e.auth.ID, rpmLimit, rpm.nowTime()) {
				views[best].excludeFillRPM(e, rpm, rpmLimit, l.generationAtomic.Load())
				rpmLimited = true
				continue
			}
			candidates = append(candidates, mixedFillFirstEntry{entry: e, providerKey: providers[best]})
			candidateViews[providers[best]] = views[best]
		}
		for len(candidates) > 0 {
			n := rand.IntN(len(candidates))
			c := candidates[n]
			candidates[n] = candidates[len(candidates)-1]
			candidates = candidates[:len(candidates)-1]
			p := policy(c.entry.auth)
			now := l.nowTime()
			if ok, b := l.tryAcquireAt(c.entry.auth.ID, p, now); !ok {
				if b.stalePolicy {
					return nil, "", rpmLimited, b
				}
				blocked = earlierAuthRequestLimitBlock(blocked, b)
				if b.limited() {
					candidateViews[c.providerKey].requestLimitIndex(l, now).exclude(c.entry, b, now)
				}
				continue
			}
			if p.limit == 0 && rpm != nil && rpmLimit > 0 && !rpm.tryAcquireAt(c.entry.auth.ID, rpmLimit, rpm.nowTime()) {
				if p.requestSlot != nil {
					p.requestSlot.FinishReservation()()
				}
				candidateViews[c.providerKey].excludeFillRPM(c.entry, rpm, rpmLimit, l.generationAtomic.Load())
				rpmLimited = true
				continue
			}
			return c.entry.auth, c.providerKey, false, authRequestLimitBlock{}
		}
	}
}
