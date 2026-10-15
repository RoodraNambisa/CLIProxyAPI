package auth

import (
	"container/heap"
	"math/rand/v2"
	"time"
)

const requestLimitChangeHistory = 4096

// Availability changes are copied by readers; the limiter never calls back into
// a provider scheduler while holding its mutex. A lagging reader rebuilds once.
type requestLimitChange struct{ authID string }

func (l *authRequestWindowLimiter) changedLocked(authID string) {
	l.changeSequence++
	l.changes[l.changeSequence%requestLimitChangeHistory] = requestLimitChange{authID: authID}
}

func (l *authRequestWindowLimiter) changesAfter(sequence uint64) (uint64, []requestLimitChange, bool) {
	if l == nil {
		return 0, nil, false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	current := l.changeSequence
	if current == sequence {
		return current, nil, false
	}
	if sequence > current || current-sequence > requestLimitChangeHistory {
		return current, nil, true
	}
	changes := make([]requestLimitChange, 0, current-sequence)
	for next := sequence + 1; next <= current; next++ {
		change := l.changes[next%requestLimitChangeHistory]
		if change.authID == "" {
			return current, nil, true
		}
		changes = append(changes, change)
	}
	return current, changes, false
}

type requestLimitExclusion struct {
	entry     *scheduledAuth
	until     time.Time
	block     authRequestLimitBlock
	heapIndex int
}

type requestLimitExpiryHeap []*requestLimitExclusion

func (h requestLimitExpiryHeap) Len() int           { return len(h) }
func (h requestLimitExpiryHeap) Less(i, j int) bool { return h[i].until.Before(h[j].until) }
func (h requestLimitExpiryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}
func (h *requestLimitExpiryHeap) Push(v any) {
	e := v.(*requestLimitExclusion)
	e.heapIndex = len(*h)
	*h = append(*h, e)
}
func (h *requestLimitExpiryHeap) Pop() any {
	old := *h
	e := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	e.heapIndex = -1
	return e
}

// A view excludes only authoritative request-limit failures. Request-local
// predicates never remove a credential from this shared candidate index.
type requestLimitReadyIndex struct {
	limiter   *authRequestWindowLimiter
	sequence  uint64
	available []*scheduledAuth
	positions map[string]int
	excluded  map[string]*requestLimitExclusion
	expiry    requestLimitExpiryHeap
}

func (v *readyView) requestLimitIndex(limiter *authRequestWindowLimiter, now time.Time) *requestLimitReadyIndex {
	index := v.limited
	if index == nil || index.limiter != limiter {
		index = &requestLimitReadyIndex{limiter: limiter}
		v.limited = index
		index.reset(v.flat)
	}
	sequence, changes, reset := limiter.changesAfter(index.sequence)
	index.sequence = sequence
	if reset {
		index.reset(v.flat)
	} else {
		for _, change := range changes {
			index.restore(change.authID)
		}
	}
	for len(index.expiry) > 0 && !index.expiry[0].until.After(now) {
		index.restore(index.expiry[0].entry.auth.ID)
	}
	return index
}

func (index *requestLimitReadyIndex) reset(entries []*scheduledAuth) {
	selectionRuntime.indexRebuilds.Add(1)
	index.available = append(index.available[:0], entries...)
	if index.positions == nil {
		index.positions = make(map[string]int, len(entries))
	} else {
		clear(index.positions)
	}
	if index.excluded == nil {
		index.excluded = make(map[string]*requestLimitExclusion)
	} else {
		clear(index.excluded)
	}
	clear(index.expiry)
	index.expiry = index.expiry[:0]
	for i, entry := range index.available {
		if entry != nil && entry.auth != nil {
			index.positions[entry.auth.ID] = i
		}
	}
}

func (index *requestLimitReadyIndex) exclude(entry *scheduledAuth, block authRequestLimitBlock, now time.Time) {
	if index == nil || entry == nil || entry.auth == nil || !block.limited() {
		return
	}
	if _, exists := index.excluded[entry.auth.ID]; exists {
		return
	}
	position, exists := index.positions[entry.auth.ID]
	if !exists {
		return
	}
	last := len(index.available) - 1
	index.available[position] = index.available[last]
	if index.available[position] != nil {
		index.positions[index.available[position].auth.ID] = position
	}
	index.available[last] = nil
	index.available = index.available[:last]
	delete(index.positions, entry.auth.ID)
	exclusion := &requestLimitExclusion{entry: entry, block: block, until: now.Add(block.resetIn)}
	index.excluded[entry.auth.ID] = exclusion
	heap.Push(&index.expiry, exclusion)
	selectionRuntime.exclusions.Add(1)
}

func (index *requestLimitReadyIndex) restore(authID string) {
	if index == nil {
		return
	}
	exclusion := index.excluded[authID]
	if exclusion == nil {
		return
	}
	heap.Remove(&index.expiry, exclusion.heapIndex)
	delete(index.excluded, authID)
	index.positions[authID] = len(index.available)
	index.available = append(index.available, exclusion.entry)
}

func (index *requestLimitReadyIndex) earliestBlock(predicate func(*scheduledAuth) bool, now time.Time) authRequestLimitBlock {
	if index == nil || len(index.expiry) == 0 {
		return authRequestLimitBlock{}
	}
	first := index.expiry[0]
	if predicate == nil || predicate(first.entry) {
		block := first.block
		block.resetIn = max(0, first.until.Sub(now))
		return block
	}
	var result authRequestLimitBlock
	for _, entry := range index.expiry {
		if predicate != nil && !predicate(entry.entry) {
			continue
		}
		block := entry.block
		block.resetIn = max(0, entry.until.Sub(now))
		result = earlierAuthRequestLimitBlock(result, block)
	}
	return result
}

func (v *readyView) invalidateRequestLimit(authID string) {
	if v != nil && v.limited != nil {
		v.limited.restore(authID)
	}
}

type mixedRandomReadyView struct {
	provider string
	view     *readyView
}

func randomAvailableEntries(view *readyView) []*scheduledAuth {
	if view.limited != nil {
		return view.limited.available
	}
	return view.flat
}

func pickMixedRandomWithRequestLimits(views []mixedRandomReadyView, predicate func(*scheduledAuth) bool, policyFor func(*Auth) authRequestLimitPolicy, limiter *authRequestWindowLimiter) (*Auth, string, authRequestLimitBlock) {
	started := time.Now()
	defer observeSelectionTime(&selectionRuntime.candidateNanos, started)
	now := limiter.nowTime()
	var blocked authRequestLimitBlock
	for _, v := range views {
		if v.view.limited != nil {
			v.view.requestLimitIndex(limiter, now)
		}
	}
	try := func(v mixedRandomReadyView, entry *scheduledAuth) bool {
		selectionRuntime.candidates.Add(1)
		if entry == nil || entry.auth == nil || predicate != nil && !predicate(entry) {
			return false
		}
		policy := policyFor(entry.auth)
		ok, block, until := limiter.tryAcquireIndexedAt(entry.auth.ID, policy, now)
		if !ok {
			blocked = earlierAuthRequestLimitBlock(blocked, block)
			if block.limited() {
				v.view.requestLimitIndex(limiter, now).exclude(entry, block, now)
			}
		} else if !until.IsZero() {
			v.view.requestLimitIndex(limiter, now).exclude(entry, newAuthRequestLimitBlock(policy, until.Sub(now)), now)
		}
		return ok
	}
	for attemptsWithoutExclusion := 0; attemptsWithoutExclusion < 8; {
		total := 0
		for _, v := range views {
			total += len(randomAvailableEntries(v.view))
		}
		if total == 0 {
			break
		}
		choice := rand.IntN(total)
		for _, v := range views {
			entries := randomAvailableEntries(v.view)
			if choice >= len(entries) {
				choice -= len(entries)
				continue
			}
			entry := entries[choice]
			if try(v, entry) {
				return entry.auth, v.provider, authRequestLimitBlock{}
			}
			if len(randomAvailableEntries(v.view)) >= len(entries) {
				attemptsWithoutExclusion++
			}
			break
		}
	}
	type candidate struct {
		owner mixedRandomReadyView
		entry *scheduledAuth
	}
	var candidates []candidate
	for _, v := range views {
		for _, entry := range randomAvailableEntries(v.view) {
			if entry != nil && entry.auth != nil && (predicate == nil || predicate(entry)) {
				candidates = append(candidates, candidate{v, entry})
			}
		}
	}
	for len(candidates) > 0 {
		i := rand.IntN(len(candidates))
		selected := candidates[i]
		if try(selected.owner, selected.entry) {
			return selected.entry.auth, selected.owner.provider, authRequestLimitBlock{}
		}
		candidates[i] = candidates[len(candidates)-1]
		candidates = candidates[:len(candidates)-1]
	}
	for _, v := range views {
		blocked = earlierAuthRequestLimitBlock(blocked, v.view.limited.earliestBlock(predicate, now))
	}
	return nil, "", blocked
}
