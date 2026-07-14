package auth

import (
	"context"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

// schedulerStrategy identifies which built-in routing semantics the scheduler should apply.
type schedulerStrategy int

const (
	schedulerStrategyCustom schedulerStrategy = iota
	schedulerStrategyRoundRobin
	schedulerStrategyFillFirst
	schedulerStrategyRandom
)

// scheduledState describes how an auth currently participates in a model shard.
type scheduledState int

const (
	scheduledStateReady scheduledState = iota
	scheduledStateCooldown
	scheduledStateBlocked
	scheduledStateDisabled
)

// authScheduler keeps the incremental provider/model scheduling state used by Manager.
type authScheduler struct {
	mu                        sync.RWMutex
	strategy                  schedulerStrategy
	globalFillFirstRange      int
	globalFillFirstPerAuthRPM int
	priorityRules             map[int]schedulerStrategy
	priorityFillFirstRanges   map[int]int
	priorityFillFirstRPMs     map[int]int
	providers                 map[string]*providerScheduler
	authProviders             map[string]string
	blockedAuths              map[string]struct{}
	fillFirstLimiter          *fillFirstMinuteLimiter
	mixedCursorMu             sync.Mutex
	mixedCursors              map[string]int
}

// providerScheduler stores auth metadata and model shards for a single provider.
type providerScheduler struct {
	mu          sync.Mutex
	providerKey string
	auths       map[string]*scheduledAuthMeta
	modelShards map[string]*modelScheduler
}

// scheduledAuthMeta stores the immutable scheduling fields derived from an auth snapshot.
type scheduledAuthMeta struct {
	auth              *Auth
	providerKey       string
	priority          int
	websocketEnabled  bool
	supportedModelSet map[string]struct{}
}

// modelScheduler tracks ready and blocked auths for one provider/model combination.
type modelScheduler struct {
	modelKey        string
	entries         map[string]*scheduledAuth
	priorityOrder   []int
	readyByPriority map[int]*readyBucket
	blocked         cooldownQueue
}

// scheduledAuth stores the runtime scheduling state for a single auth inside a model shard.
type scheduledAuth struct {
	meta        *scheduledAuthMeta
	auth        *Auth
	state       scheduledState
	nextRetryAt time.Time
}

// readyBucket keeps the ready views for one priority level.
type readyBucket struct {
	all readyView
	ws  readyView
}

// readyView holds the selection order for selection traversal.
type readyView struct {
	flat   []*scheduledAuth
	cursor int
}

// cooldownQueue is the blocked auth collection ordered by next retry time during rebuilds.
type cooldownQueue []*scheduledAuth

type readyViewCursorState struct {
	cursor int
}

type readyBucketCursorState struct {
	all readyViewCursorState
	ws  readyViewCursorState
}

func snapshotReadyViewCursors(view readyView) readyViewCursorState {
	return readyViewCursorState{cursor: view.cursor}
}

func restoreReadyViewCursors(view *readyView, state readyViewCursorState) {
	if view == nil {
		return
	}
	if len(view.flat) > 0 {
		view.cursor = normalizeCursor(state.cursor, len(view.flat))
	}
}

func normalizeCursor(cursor, size int) int {
	if size <= 0 || cursor <= 0 {
		return 0
	}
	cursor = cursor % size
	if cursor < 0 {
		cursor += size
	}
	return cursor
}

// newAuthScheduler constructs an empty scheduler configured for the supplied selector strategy.
func newAuthScheduler(selector Selector) *authScheduler {
	return &authScheduler{
		strategy:                  selectorStrategy(selector),
		globalFillFirstRange:      fillFirstRangeFromSelector(selector),
		globalFillFirstPerAuthRPM: 0,
		priorityRules:             make(map[int]schedulerStrategy),
		priorityFillFirstRanges:   make(map[int]int),
		priorityFillFirstRPMs:     make(map[int]int),
		providers:                 make(map[string]*providerScheduler),
		authProviders:             make(map[string]string),
		blockedAuths:              make(map[string]struct{}),
		fillFirstLimiter:          newFillFirstMinuteLimiter(),
		mixedCursors:              make(map[string]int),
	}
}

// selectorStrategy maps a selector implementation to the scheduler semantics it should emulate.
func selectorStrategy(selector Selector) schedulerStrategy {
	selector = baseSelector(selector)
	switch selector.(type) {
	case *FillFirstSelector:
		return schedulerStrategyFillFirst
	case *RandomSelector:
		return schedulerStrategyRandom
	case nil, *RoundRobinSelector:
		return schedulerStrategyRoundRobin
	default:
		return schedulerStrategyCustom
	}
}

func baseSelector(selector Selector) Selector {
	if sessionSelector, ok := selector.(*SessionAffinitySelector); ok && sessionSelector != nil {
		return sessionSelector.fallback
	}
	return selector
}

func fillFirstRangeFromSelector(selector Selector) int {
	selector = baseSelector(selector)
	if selector, ok := selector.(*FillFirstSelector); ok && selector != nil {
		return normalizeFillFirstRangeValue(selector.Range)
	}
	return 1
}

func schedulerStrategyFromName(strategy string) (schedulerStrategy, bool) {
	normalized, ok := internalconfig.NormalizeRoutingStrategy(strategy)
	if !ok {
		return schedulerStrategyRoundRobin, false
	}
	switch normalized {
	case "fill-first":
		return schedulerStrategyFillFirst, true
	case "random":
		return schedulerStrategyRandom, true
	default:
		return schedulerStrategyRoundRobin, true
	}
}

func (s *authScheduler) setRoutingPriorityOverrides(overrides []internalconfig.RoutingPriorityOverride) {
	s.setRoutingConfig(internalconfig.RoutingConfig{FillFirstRange: 1, PriorityOverrides: overrides})
}

func (s *authScheduler) setRoutingConfig(routing internalconfig.RoutingConfig) {
	if s == nil {
		return
	}
	rules := make(map[int]schedulerStrategy, len(routing.PriorityOverrides))
	ranges := make(map[int]int, len(routing.PriorityOverrides))
	rpms := make(map[int]int, len(routing.PriorityOverrides))
	for _, override := range routing.PriorityOverrides {
		if strings.TrimSpace(override.Strategy) == "" {
			if override.FillFirstRange != nil {
				ranges[override.Priority] = normalizeFillFirstRangeValue(*override.FillFirstRange)
			}
			if override.FillFirstPerAuthRPM != nil {
				rpms[override.Priority] = normalizeFillFirstPerAuthRPMValue(*override.FillFirstPerAuthRPM)
			}
			continue
		}
		if strategy, ok := schedulerStrategyFromName(override.Strategy); ok {
			rules[override.Priority] = strategy
		}
		if override.FillFirstRange != nil {
			ranges[override.Priority] = normalizeFillFirstRangeValue(*override.FillFirstRange)
		}
		if override.FillFirstPerAuthRPM != nil {
			rpms[override.Priority] = normalizeFillFirstPerAuthRPMValue(*override.FillFirstPerAuthRPM)
		}
	}
	s.mu.Lock()
	s.globalFillFirstRange = normalizeFillFirstRangeValue(routing.FillFirstRange)
	s.globalFillFirstPerAuthRPM = normalizeFillFirstPerAuthRPMValue(routing.FillFirstPerAuthRPM)
	s.priorityRules = rules
	s.priorityFillFirstRanges = ranges
	s.priorityFillFirstRPMs = rpms
	s.mu.Unlock()
}

func (s *authScheduler) strategyForPriorityLocked(priority int) schedulerStrategy {
	if s == nil {
		return schedulerStrategyRoundRobin
	}
	if strategy, ok := s.priorityRules[priority]; ok {
		return strategy
	}
	return s.strategy
}

func (s *authScheduler) fillFirstRangeForPriorityLocked(priority int) int {
	if s == nil {
		return 1
	}
	if value, ok := s.priorityFillFirstRanges[priority]; ok {
		return normalizeFillFirstRangeValue(value)
	}
	return normalizeFillFirstRangeValue(s.globalFillFirstRange)
}

func (s *authScheduler) fillFirstPerAuthRPMForPriorityLocked(priority int) int {
	if s == nil {
		return 0
	}
	if value, ok := s.priorityFillFirstRPMs[priority]; ok {
		return normalizeFillFirstPerAuthRPMValue(value)
	}
	return normalizeFillFirstPerAuthRPMValue(s.globalFillFirstPerAuthRPM)
}

// setSelector updates the active built-in strategy and resets mixed-provider cursors.
func (s *authScheduler) setSelector(selector Selector) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.strategy = selectorStrategy(selector)
	s.globalFillFirstRange = fillFirstRangeFromSelector(selector)
	s.mu.Unlock()
	s.mixedCursorMu.Lock()
	defer s.mixedCursorMu.Unlock()
	clear(s.mixedCursors)
}

// rebuild recreates the complete scheduler state from an auth snapshot.
func (s *authScheduler) rebuild(auths []*Auth) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.providers = make(map[string]*providerScheduler)
	s.authProviders = make(map[string]string)
	s.mixedCursorMu.Lock()
	s.mixedCursors = make(map[string]int)
	s.mixedCursorMu.Unlock()
	now := time.Now()
	for _, auth := range auths {
		s.upsertAuthLocked(auth, now, true)
	}
}

// upsertAuth incrementally synchronizes one auth into the scheduler.
func (s *authScheduler) upsertAuth(auth *Auth) {
	s.upsertAuthWithModelRefresh(auth, true)
}

// upsertAuthState incrementally synchronizes auth runtime state without
// rebuilding the registered supported model set from the global registry.
func (s *authScheduler) upsertAuthState(auth *Auth) {
	s.upsertAuthWithModelRefresh(auth, false)
}

func (s *authScheduler) upsertAuthWithModelRefresh(auth *Auth, refreshSupportedModels bool) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.upsertAuthLocked(auth, time.Now(), refreshSupportedModels)
}

// removeAuth deletes one auth from every scheduler shard that references it.
func (s *authScheduler) removeAuth(authID string) {
	if s == nil {
		return
	}
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removeAuthLocked(authID)
}

func (s *authScheduler) blockAuth(authID string) {
	if s == nil {
		return
	}
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return
	}
	s.mu.Lock()
	if s.blockedAuths == nil {
		s.blockedAuths = make(map[string]struct{})
	}
	s.blockedAuths[authID] = struct{}{}
	s.removeAuthLocked(authID)
	s.mu.Unlock()
}

func (s *authScheduler) unblockAuth(authID string, auth *Auth) {
	if s == nil {
		return
	}
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return
	}
	s.mu.Lock()
	delete(s.blockedAuths, authID)
	if auth != nil {
		s.upsertAuthLocked(auth, time.Now(), true)
	}
	s.mu.Unlock()
}

// pickSingle returns the next auth for a single provider/model request using scheduler state.
func (s *authScheduler) pickSingle(ctx context.Context, provider, model string, opts cliproxyexecutor.Options, tried map[string]struct{}, authAllowed ...func(*Auth) bool) (*Auth, error) {
	if s == nil {
		return nil, &Error{Code: "auth_not_found", Message: "no auth available"}
	}
	providerKey := strings.ToLower(strings.TrimSpace(provider))
	modelKey := canonicalModelKey(model)
	pinnedAuthID := pinnedAuthIDFromMetadata(opts.Metadata)
	preferWebsocket := shouldPreferCodexWebsocket(ctx, providerKey) && pinnedAuthID == ""
	selectionAttempt := 0
	s.mu.RLock()
	defer s.mu.RUnlock()
	providerState := s.providers[providerKey]
	strategyForPriority := s.strategyForPriorityLocked
	fillFirstRangeForPriority := s.fillFirstRangeForPriorityLocked
	fillFirstPerAuthRPMForPriority := s.fillFirstPerAuthRPMForPriorityLocked
	fillFirstLimiter := s.fillFirstLimiter
	selectionAttempt = selectionAttemptFromMetadata(opts.Metadata)
	if providerState == nil {
		return nil, &Error{Code: "auth_not_found", Message: "no auth available"}
	}
	providerState.mu.Lock()
	defer providerState.mu.Unlock()
	shard := providerState.ensureModelLocked(modelKey, time.Now())
	if shard == nil {
		return nil, &Error{Code: "auth_not_found", Message: "no auth available"}
	}
	priorityPredicate := func(entry *scheduledAuth) bool {
		if entry == nil || entry.auth == nil {
			return false
		}
		if pinnedAuthID != "" && entry.auth.ID != pinnedAuthID {
			return false
		}
		return true
	}
	pickPredicate := func(entry *scheduledAuth) bool {
		if !priorityPredicate(entry) {
			return false
		}
		if len(tried) > 0 {
			if _, ok := tried[entry.auth.ID]; ok {
				return false
			}
		}
		if len(authAllowed) > 0 && authAllowed[0] != nil && !authAllowed[0](entry.auth) {
			return false
		}
		return true
	}
	if picked, errPick := shard.pickReadyLocked(preferWebsocket, strategyForPriority, fillFirstRangeForPriority, fillFirstPerAuthRPMForPriority, fillFirstLimiter, selectionAttempt, priorityPredicate, pickPredicate, provider, model); errPick != nil {
		return nil, errPick
	} else if picked != nil {
		return picked, nil
	}
	return nil, shard.unavailableErrorForAttemptLocked(provider, model, preferWebsocket, selectionAttempt, priorityPredicate, pickPredicate)
}

// pickMixed returns the next auth and provider for a mixed-provider request.
func (s *authScheduler) pickMixed(ctx context.Context, providers []string, model string, opts cliproxyexecutor.Options, tried map[string]struct{}, authAllowed ...func(*Auth) bool) (*Auth, string, error) {
	if s == nil {
		return nil, "", &Error{Code: "auth_not_found", Message: "no auth available"}
	}
	normalized := normalizeProviderKeys(providers)
	if len(normalized) == 0 {
		return nil, "", &Error{Code: "provider_not_found", Message: "no provider supplied"}
	}
	if len(normalized) == 1 {
		// When a single provider is eligible, reuse pickSingle so provider-specific preferences
		// (for example Codex websocket transport) are applied consistently.
		providerKey := normalized[0]
		picked, errPick := s.pickSingle(ctx, providerKey, model, opts, tried, authAllowed...)
		if errPick != nil {
			return nil, "", errPick
		}
		if picked == nil {
			return nil, "", &Error{Code: "auth_not_found", Message: "no auth available"}
		}
		return picked, providerKey, nil
	}
	pinnedAuthID := pinnedAuthIDFromMetadata(opts.Metadata)
	modelKey := canonicalModelKey(model)
	selectionAttempt := 0
	s.mu.RLock()
	defer s.mu.RUnlock()
	strategyForPriority := s.strategyForPriorityLocked
	fillFirstRangeForPriority := s.fillFirstRangeForPriorityLocked
	fillFirstPerAuthRPMForPriority := s.fillFirstPerAuthRPMForPriorityLocked
	fillFirstLimiter := s.fillFirstLimiter
	selectionAttempt = selectionAttemptFromMetadata(opts.Metadata)
	if pinnedAuthID != "" {
		providerKey := s.authProviders[pinnedAuthID]
		providerState := s.providers[providerKey]
		if providerKey == "" || !containsProvider(normalized, providerKey) {
			return nil, "", &Error{Code: "auth_not_found", Message: "no auth available"}
		}
		if providerState == nil {
			return nil, "", &Error{Code: "auth_not_found", Message: "no auth available"}
		}
		providerState.mu.Lock()
		defer providerState.mu.Unlock()
		shard := providerState.ensureModelLocked(modelKey, time.Now())
		priorityPredicate := func(entry *scheduledAuth) bool {
			if entry == nil || entry.auth == nil {
				return false
			}
			return entry.auth.ID == pinnedAuthID
		}
		pickPredicate := func(entry *scheduledAuth) bool {
			if !priorityPredicate(entry) {
				return false
			}
			if len(tried) == 0 {
				return len(authAllowed) == 0 || authAllowed[0] == nil || authAllowed[0](entry.auth)
			}
			_, ok := tried[pinnedAuthID]
			if ok {
				return false
			}
			return len(authAllowed) == 0 || authAllowed[0] == nil || authAllowed[0](entry.auth)
		}
		if picked, errPick := shard.pickReadyLocked(false, strategyForPriority, fillFirstRangeForPriority, fillFirstPerAuthRPMForPriority, fillFirstLimiter, selectionAttempt, priorityPredicate, pickPredicate, "mixed", model); errPick != nil {
			return nil, "", errPick
		} else if picked != nil {
			return picked, providerKey, nil
		}
		return nil, "", shard.unavailableErrorForAttemptLocked("mixed", model, false, selectionAttempt, priorityPredicate, pickPredicate)
	}
	providerStates := make(map[string]*providerScheduler, len(normalized))
	for _, providerKey := range normalized {
		if providerState := s.providers[providerKey]; providerState != nil {
			providerStates[providerKey] = providerState
		}
	}

	priorityPredicate := func(entry *scheduledAuth) bool {
		return entry != nil && entry.auth != nil
	}
	pickPredicate := triedPredicate(tried, authAllowed...)
	candidateShards := make([]*modelScheduler, len(normalized))
	prioritySet := make(map[int]struct{})
	now := time.Now()
	lockedProviders := lockProviderSchedulers(providerStates)
	defer unlockProviderSchedulers(lockedProviders)
	for providerIndex, providerKey := range normalized {
		providerState := providerStates[providerKey]
		if providerState == nil {
			continue
		}
		shard := providerState.ensureModelLocked(modelKey, now)
		candidateShards[providerIndex] = shard
		if shard == nil {
			continue
		}
		for _, priority := range shard.candidatePrioritiesLocked(false, priorityPredicate) {
			prioritySet[priority] = struct{}{}
		}
	}
	if len(prioritySet) == 0 {
		return nil, "", mixedUnavailableErrorFromShards(normalized, candidateShards, model, tried)
	}

	priorities := make([]int, 0, len(prioritySet))
	for priority := range prioritySet {
		priorities = append(priorities, priority)
	}
	sort.Slice(priorities, func(i, j int) bool {
		return priorities[i] > priorities[j]
	})
	prioritiesToTry := selectionPrioritiesForAttempt(priorities, selectionAttempt)
	if len(prioritiesToTry) == 0 {
		return nil, "", mixedUnavailableErrorFromShards(normalized, candidateShards, model, tried)
	}

	type randomMixedCandidate struct {
		auth        *Auth
		providerKey string
	}
	cursorKey := strings.Join(normalized, ",") + ":" + modelKey
	rpmLimited := false
	for _, targetPriority := range prioritiesToTry {
		switch strategyForPriority(targetPriority) {
		case schedulerStrategyRandom:
			candidates := make([]randomMixedCandidate, 0)
			for providerIndex, providerKey := range normalized {
				shard := candidateShards[providerIndex]
				if shard == nil {
					continue
				}
				for _, entry := range shard.readyEntriesAtPriorityLocked(false, targetPriority, pickPredicate) {
					if entry == nil || entry.auth == nil {
						continue
					}
					candidates = append(candidates, randomMixedCandidate{
						auth:        entry.auth,
						providerKey: providerKey,
					})
				}
			}
			if len(candidates) > 0 {
				picked := candidates[rand.IntN(len(candidates))]
				return picked.auth, picked.providerKey, nil
			}
		case schedulerStrategyFillFirst:
			picked, providerKey, limited := pickMixedFillFirstAtPriorityLocked(normalized, candidateShards, targetPriority, fillFirstRangeForPriority(targetPriority), fillFirstPerAuthRPMForPriority(targetPriority), fillFirstLimiter, priorityPredicate, pickPredicate)
			if picked != nil {
				return picked, providerKey, nil
			}
			rpmLimited = rpmLimited || limited
		default:
			weights := make([]int, len(normalized))
			segmentStarts := make([]int, len(normalized))
			segmentEnds := make([]int, len(normalized))
			totalWeight := 0
			for providerIndex, shard := range candidateShards {
				segmentStarts[providerIndex] = totalWeight
				if shard != nil {
					weights[providerIndex] = shard.readyCountAtPriorityLocked(false, targetPriority, pickPredicate)
				}
				totalWeight += weights[providerIndex]
				segmentEnds[providerIndex] = totalWeight
			}
			if totalWeight == 0 {
				continue
			}

			s.mixedCursorMu.Lock()
			startSlot := s.mixedCursors[cursorKey] % totalWeight
			startProviderIndex := -1
			for providerIndex := range normalized {
				if weights[providerIndex] == 0 {
					continue
				}
				if startSlot < segmentEnds[providerIndex] {
					startProviderIndex = providerIndex
					break
				}
			}
			if startProviderIndex < 0 {
				s.mixedCursorMu.Unlock()
				continue
			}

			slot := startSlot
			for offset := 0; offset < len(normalized); offset++ {
				providerIndex := (startProviderIndex + offset) % len(normalized)
				if weights[providerIndex] == 0 {
					continue
				}
				if providerIndex != startProviderIndex {
					slot = segmentStarts[providerIndex]
				}
				providerKey := normalized[providerIndex]
				shard := candidateShards[providerIndex]
				if shard == nil {
					continue
				}
				picked, _ := shard.pickReadyAtPriorityLocked(false, targetPriority, schedulerStrategyRoundRobin, 1, 0, nil, pickPredicate, pickPredicate)
				if picked == nil {
					continue
				}
				s.mixedCursors[cursorKey] = slot + 1
				s.mixedCursorMu.Unlock()
				return picked, providerKey, nil
			}
			s.mixedCursorMu.Unlock()
		}
	}
	if rpmLimited {
		rpmNow := fillFirstLimiterNow(fillFirstLimiter)
		rpmRetryAfter := fillFirstRPMRetryAfterAt(fillFirstLimiter, rpmNow)
		if earliest := mixedBlockedEarliestAtPrioritiesLocked(candidateShards, prioritiesToTry, pickPredicate); cooldownBeforeRPMReset(earliest, rpmRetryAfter, rpmNow) {
			return nil, "", newModelCooldownErrorUntil(model, "", earliest, rpmNow)
		}
		return nil, "", newAuthRPMLimitedError(rpmRetryAfter)
	}
	return nil, "", mixedUnavailableErrorFromShardsForAttempt(normalized, candidateShards, model, priorityPredicate, pickPredicate, selectionAttempt)
}

type mixedFillFirstEntry struct {
	entry       *scheduledAuth
	providerKey string
}

func pickMixedFillFirstAtPriorityLocked(providers []string, candidateShards []*modelScheduler, priority int, fillFirstRange int, fillFirstPerAuthRPM int, rpmLimiter *fillFirstMinuteLimiter, membershipPredicate func(*scheduledAuth) bool, pickPredicate func(*scheduledAuth) bool) (*Auth, string, bool) {
	fillFirstRange = normalizeFillFirstRangeValue(fillFirstRange)
	fillFirstPerAuthRPM = normalizeFillFirstPerAuthRPMValue(fillFirstPerAuthRPM)
	members := make([]mixedFillFirstEntry, 0)
	for providerIndex, providerKey := range providers {
		if providerIndex >= len(candidateShards) {
			continue
		}
		shard := candidateShards[providerIndex]
		if shard == nil {
			continue
		}
		for _, entry := range shard.entries {
			if entry == nil || entry.auth == nil || entry.meta == nil || entry.meta.priority != priority {
				continue
			}
			if !entryCandidateForPriority(entry, membershipPredicate) {
				continue
			}
			members = append(members, mixedFillFirstEntry{entry: entry, providerKey: providerKey})
		}
	}
	if len(members) == 0 {
		return nil, "", false
	}
	sort.Slice(members, func(i, j int) bool {
		leftID := members[i].entry.auth.ID
		rightID := members[j].entry.auth.ID
		if leftID == rightID {
			return members[i].providerKey < members[j].providerKey
		}
		return leftID < rightID
	})
	if fillFirstPerAuthRPM > 0 {
		now := time.Now()
		if rpmLimiter != nil {
			now = rpmLimiter.nowTime()
		}
		rpmLimited := false
		for _, member := range members {
			if member.entry.state != scheduledStateReady {
				continue
			}
			if pickPredicate != nil && !pickPredicate(member.entry) {
				continue
			}
			if rpmLimiter != nil && !rpmLimiter.tryAcquireAt(member.entry.auth.ID, fillFirstPerAuthRPM, now) {
				rpmLimited = true
				continue
			}
			return member.entry.auth, member.providerKey, false
		}
		return nil, "", rpmLimited
	}
	for start := 0; start < len(members); start += fillFirstRange {
		end := start + fillFirstRange
		if end > len(members) {
			end = len(members)
		}
		candidates := make([]mixedFillFirstEntry, 0, end-start)
		for _, member := range members[start:end] {
			if member.entry.state != scheduledStateReady {
				continue
			}
			if pickPredicate != nil && !pickPredicate(member.entry) {
				continue
			}
			candidates = append(candidates, member)
		}
		if len(candidates) == 1 {
			return candidates[0].entry.auth, candidates[0].providerKey, false
		}
		if len(candidates) > 1 {
			picked := candidates[rand.IntN(len(candidates))]
			return picked.entry.auth, picked.providerKey, false
		}
	}
	return nil, "", false
}

func mixedBlockedEarliestAtPrioritiesLocked(candidateShards []*modelScheduler, priorities []int, predicate func(*scheduledAuth) bool) time.Time {
	var earliest time.Time
	for _, priority := range priorities {
		for _, shard := range candidateShards {
			if shard == nil {
				continue
			}
			if next, okNext := shard.blockedEarliestAtPriorityLocked(false, priority, predicate); okNext && (earliest.IsZero() || next.Before(earliest)) {
				earliest = next
			}
		}
	}
	return earliest
}

// mixedUnavailableErrorFromShards synthesizes the mixed-provider cooldown or unavailable error.
func mixedUnavailableErrorFromShards(providers []string, candidateShards []*modelScheduler, model string, tried map[string]struct{}) error {
	return mixedUnavailableErrorFromShardsWithPredicate(providers, candidateShards, model, triedPredicate(tried))
}

func mixedUnavailableErrorFromShardsWithPredicate(providers []string, candidateShards []*modelScheduler, model string, predicate func(*scheduledAuth) bool) error {
	now := time.Now()
	total := 0
	cooldownCount := 0
	earliest := time.Time{}
	for providerIndex, providerKey := range providers {
		_ = providerKey
		var shard *modelScheduler
		if providerIndex < len(candidateShards) {
			shard = candidateShards[providerIndex]
		}
		if shard == nil {
			continue
		}
		localTotal, localCooldownCount, localEarliest := shard.availabilitySummaryLocked(predicate)
		total += localTotal
		cooldownCount += localCooldownCount
		if !localEarliest.IsZero() && (earliest.IsZero() || localEarliest.Before(earliest)) {
			earliest = localEarliest
		}
	}
	if total == 0 {
		return &Error{Code: "auth_not_found", Message: "no auth available"}
	}
	if cooldownCount == total && !earliest.IsZero() {
		resetIn := earliest.Sub(now)
		if resetIn < 0 {
			resetIn = 0
		}
		return newModelCooldownError(model, "", resetIn)
	}
	return &Error{Code: "auth_unavailable", Message: "no auth available"}
}

func mixedUnavailableErrorFromShardsForAttempt(providers []string, candidateShards []*modelScheduler, model string, priorityPredicate func(*scheduledAuth) bool, pickPredicate func(*scheduledAuth) bool, selectionAttempt int) error {
	prioritySet := make(map[int]struct{})
	for _, shard := range candidateShards {
		if shard == nil {
			continue
		}
		for _, priority := range shard.candidatePrioritiesLocked(false, priorityPredicate) {
			prioritySet[priority] = struct{}{}
		}
	}
	if len(prioritySet) == 0 {
		return mixedUnavailableErrorFromShardsWithPredicate(providers, candidateShards, model, pickPredicate)
	}
	priorities := make([]int, 0, len(prioritySet))
	for priority := range prioritySet {
		priorities = append(priorities, priority)
	}
	sort.Slice(priorities, func(i, j int) bool {
		return priorities[i] > priorities[j]
	})
	prioritiesToTry := selectionPrioritiesForAttempt(priorities, selectionAttempt)
	if len(prioritiesToTry) == 0 {
		return mixedUnavailableErrorFromShardsWithPredicate(providers, candidateShards, model, pickPredicate)
	}
	now := time.Now()
	var earliest time.Time
	for _, priority := range prioritiesToTry {
		for _, shard := range candidateShards {
			if shard == nil {
				continue
			}
			if next, okNext := shard.blockedEarliestAtPriorityLocked(false, priority, pickPredicate); okNext && (earliest.IsZero() || next.Before(earliest)) {
				earliest = next
			}
		}
	}
	if !earliest.IsZero() {
		resetIn := earliest.Sub(now)
		if resetIn < 0 {
			resetIn = 0
		}
		return newModelCooldownError(model, "", resetIn)
	}
	return &Error{Code: "auth_unavailable", Message: "no auth available"}
}

// triedPredicate builds a filter that excludes auths already attempted for the current request.
func triedPredicate(tried map[string]struct{}, authAllowed ...func(*Auth) bool) func(*scheduledAuth) bool {
	allowed := func(auth *Auth) bool {
		return len(authAllowed) == 0 || authAllowed[0] == nil || authAllowed[0](auth)
	}
	if len(tried) == 0 {
		return func(entry *scheduledAuth) bool { return entry != nil && entry.auth != nil && allowed(entry.auth) }
	}
	return func(entry *scheduledAuth) bool {
		if entry == nil || entry.auth == nil {
			return false
		}
		_, ok := tried[entry.auth.ID]
		return !ok && allowed(entry.auth)
	}
}

// normalizeProviderKeys lowercases, trims, and de-duplicates provider keys while preserving order.
func normalizeProviderKeys(providers []string) []string {
	seen := make(map[string]struct{}, len(providers))
	out := make([]string, 0, len(providers))
	for _, provider := range providers {
		providerKey := strings.ToLower(strings.TrimSpace(provider))
		if providerKey == "" {
			continue
		}
		if _, ok := seen[providerKey]; ok {
			continue
		}
		seen[providerKey] = struct{}{}
		out = append(out, providerKey)
	}
	return out
}

// containsProvider reports whether provider is present in the normalized provider list.
func containsProvider(providers []string, provider string) bool {
	for _, candidate := range providers {
		if candidate == provider {
			return true
		}
	}
	return false
}

// upsertAuthLocked updates one auth in-place while the scheduler mutex is held.
func (s *authScheduler) upsertAuthLocked(auth *Auth, now time.Time, refreshSupportedModels bool) {
	if auth == nil {
		return
	}
	authID := strings.TrimSpace(auth.ID)
	providerKey := strings.ToLower(strings.TrimSpace(auth.Provider))
	if _, blocked := s.blockedAuths[authID]; blocked {
		s.removeAuthLocked(authID)
		return
	}
	if authID == "" || providerKey == "" || auth.Disabled {
		s.removeAuthLocked(authID)
		return
	}
	var preservedModelSet map[string]struct{}
	preservedSupportedModels := false
	if previousProvider := s.authProviders[authID]; previousProvider != "" {
		if previousState := s.providers[previousProvider]; previousState != nil {
			previousState.mu.Lock()
			if existing := previousState.auths[authID]; existing != nil {
				preservedModelSet = existing.supportedModelSet
				preservedSupportedModels = true
			}
			if previousProvider != providerKey {
				previousState.removeAuthLocked(authID)
			}
			previousState.mu.Unlock()
		}
	}
	s.authProviders[authID] = providerKey
	providerState := s.ensureProviderLocked(providerKey)
	providerState.mu.Lock()
	defer providerState.mu.Unlock()
	if !preservedSupportedModels {
		if existing := providerState.auths[authID]; existing != nil {
			preservedModelSet = existing.supportedModelSet
			preservedSupportedModels = true
		}
	}
	supportedModelSet := preservedModelSet
	if refreshSupportedModels || !preservedSupportedModels {
		supportedModelSet = supportedModelSetForAuth(authID)
	}
	meta := buildScheduledAuthMetaWithSupportedModels(auth, supportedModelSet)
	providerState.upsertAuthLocked(meta, now)
}

// removeAuthLocked removes one auth from the scheduler while the scheduler mutex is held.
func (s *authScheduler) removeAuthLocked(authID string) {
	if authID == "" {
		return
	}
	if providerKey := s.authProviders[authID]; providerKey != "" {
		if providerState := s.providers[providerKey]; providerState != nil {
			providerState.mu.Lock()
			providerState.removeAuthLocked(authID)
			providerState.mu.Unlock()
		}
		delete(s.authProviders, authID)
	}
}

// ensureProviderLocked returns the provider scheduler for providerKey, creating it when needed.
func (s *authScheduler) ensureProviderLocked(providerKey string) *providerScheduler {
	if s.providers == nil {
		s.providers = make(map[string]*providerScheduler)
	}
	providerState := s.providers[providerKey]
	if providerState == nil {
		providerState = &providerScheduler{
			providerKey: providerKey,
			auths:       make(map[string]*scheduledAuthMeta),
			modelShards: make(map[string]*modelScheduler),
		}
		s.providers[providerKey] = providerState
	}
	return providerState
}

func lockProviderSchedulers(providerStates map[string]*providerScheduler) []*providerScheduler {
	if len(providerStates) == 0 {
		return nil
	}
	ordered := make([]*providerScheduler, 0, len(providerStates))
	for _, providerState := range providerStates {
		if providerState != nil {
			ordered = append(ordered, providerState)
		}
	}
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].providerKey < ordered[j].providerKey
	})
	for _, providerState := range ordered {
		providerState.mu.Lock()
	}
	return ordered
}

func unlockProviderSchedulers(providerStates []*providerScheduler) {
	for index := len(providerStates) - 1; index >= 0; index-- {
		providerStates[index].mu.Unlock()
	}
}

// buildScheduledAuthMeta extracts the scheduling metadata needed for shard bookkeeping.
func buildScheduledAuthMeta(auth *Auth) *scheduledAuthMeta {
	return buildScheduledAuthMetaWithSupportedModels(auth, supportedModelSetForAuth(auth.ID))
}

func buildScheduledAuthMetaWithSupportedModels(auth *Auth, supportedModelSet map[string]struct{}) *scheduledAuthMeta {
	providerKey := strings.ToLower(strings.TrimSpace(auth.Provider))
	return &scheduledAuthMeta{
		auth:              auth,
		providerKey:       providerKey,
		priority:          authPriority(auth),
		websocketEnabled:  authWebsocketsEnabled(auth),
		supportedModelSet: supportedModelSet,
	}
}

// supportedModelSetForAuth snapshots the registry models currently registered for an auth.
func supportedModelSetForAuth(authID string) map[string]struct{} {
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return nil
	}
	models := registry.GetGlobalRegistry().GetModelsForClient(authID)
	if len(models) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(models))
	for _, model := range models {
		if model == nil {
			continue
		}
		modelKey := canonicalModelKey(model.ID)
		if modelKey == "" {
			continue
		}
		set[modelKey] = struct{}{}
	}
	return set
}

// upsertAuthLocked updates every existing model shard that can reference the auth metadata.
func (p *providerScheduler) upsertAuthLocked(meta *scheduledAuthMeta, now time.Time) {
	if p == nil || meta == nil || meta.auth == nil {
		return
	}
	p.auths[meta.auth.ID] = meta
	for modelKey, shard := range p.modelShards {
		if shard == nil {
			continue
		}
		if !meta.supportsModel(modelKey) {
			shard.removeEntryLocked(meta.auth.ID)
			continue
		}
		shard.upsertEntryLocked(meta, now)
	}
}

// removeAuthLocked removes an auth from all model shards owned by the provider scheduler.
func (p *providerScheduler) removeAuthLocked(authID string) {
	if p == nil || authID == "" {
		return
	}
	delete(p.auths, authID)
	for _, shard := range p.modelShards {
		if shard != nil {
			shard.removeEntryLocked(authID)
		}
	}
}

// ensureModelLocked returns the shard for modelKey, building it lazily from provider auths.
func (p *providerScheduler) ensureModelLocked(modelKey string, now time.Time) *modelScheduler {
	if p == nil {
		return nil
	}
	modelKey = canonicalModelKey(modelKey)
	if shard, ok := p.modelShards[modelKey]; ok && shard != nil {
		shard.promoteExpiredLocked(now)
		return shard
	}
	shard := &modelScheduler{
		modelKey:        modelKey,
		entries:         make(map[string]*scheduledAuth),
		readyByPriority: make(map[int]*readyBucket),
	}
	for _, meta := range p.auths {
		if meta == nil || !meta.supportsModel(modelKey) {
			continue
		}
		if entry := buildScheduledAuth(meta, modelKey, now); entry != nil && entry.auth != nil {
			shard.entries[entry.auth.ID] = entry
		}
	}
	if len(shard.entries) > 0 {
		shard.rebuildIndexesLocked()
	}
	p.modelShards[modelKey] = shard
	return shard
}

// supportsModel reports whether the auth metadata currently supports modelKey.
func (m *scheduledAuthMeta) supportsModel(modelKey string) bool {
	modelKey = canonicalModelKey(modelKey)
	if modelKey == "" {
		return true
	}
	if len(m.supportedModelSet) == 0 {
		return false
	}
	_, ok := m.supportedModelSet[modelKey]
	return ok
}

// upsertEntryLocked updates or inserts one auth entry and rebuilds indexes when ordering changes.
func (m *modelScheduler) upsertEntryLocked(meta *scheduledAuthMeta, now time.Time) {
	if m == nil || meta == nil || meta.auth == nil {
		return
	}
	entry, ok := m.entries[meta.auth.ID]
	if !ok || entry == nil {
		entry = &scheduledAuth{}
		m.entries[meta.auth.ID] = entry
	}
	previousState := entry.state
	previousNextRetryAt := entry.nextRetryAt
	previousPriority := 0
	previousWebsocketEnabled := false
	if entry.meta != nil {
		previousPriority = entry.meta.priority
		previousWebsocketEnabled = entry.meta.websocketEnabled
	}

	entry.applyMeta(meta, m.modelKey, now)

	if ok && previousState == entry.state && previousNextRetryAt.Equal(entry.nextRetryAt) && previousPriority == meta.priority && previousWebsocketEnabled == meta.websocketEnabled {
		return
	}
	m.rebuildIndexesLocked()
}

func buildScheduledAuth(meta *scheduledAuthMeta, modelKey string, now time.Time) *scheduledAuth {
	if meta == nil || meta.auth == nil {
		return nil
	}
	entry := &scheduledAuth{}
	entry.applyMeta(meta, modelKey, now)
	return entry
}

func (e *scheduledAuth) applyMeta(meta *scheduledAuthMeta, modelKey string, now time.Time) {
	if e == nil || meta == nil || meta.auth == nil {
		return
	}
	e.meta = meta
	e.auth = meta.auth
	e.nextRetryAt = time.Time{}
	blocked, reason, next := isAuthBlockedForModel(meta.auth, modelKey, now)
	switch {
	case !blocked:
		e.state = scheduledStateReady
	case reason == blockReasonCooldown:
		e.state = scheduledStateCooldown
		e.nextRetryAt = next
	case reason == blockReasonDisabled:
		e.state = scheduledStateDisabled
	default:
		e.state = scheduledStateBlocked
		e.nextRetryAt = next
	}
}

// removeEntryLocked deletes one auth entry and rebuilds the shard indexes if needed.
func (m *modelScheduler) removeEntryLocked(authID string) {
	if m == nil || authID == "" {
		return
	}
	if _, ok := m.entries[authID]; !ok {
		return
	}
	delete(m.entries, authID)
	m.rebuildIndexesLocked()
}

// promoteExpiredLocked reevaluates blocked auths whose retry time has elapsed.
func (m *modelScheduler) promoteExpiredLocked(now time.Time) {
	if m == nil || len(m.blocked) == 0 {
		return
	}
	changed := false
	for _, entry := range m.blocked {
		if entry == nil || entry.auth == nil {
			continue
		}
		if entry.nextRetryAt.IsZero() || entry.nextRetryAt.After(now) {
			continue
		}
		blocked, reason, next := isAuthBlockedForModel(entry.auth, m.modelKey, now)
		switch {
		case !blocked:
			entry.state = scheduledStateReady
			entry.nextRetryAt = time.Time{}
		case reason == blockReasonCooldown:
			entry.state = scheduledStateCooldown
			entry.nextRetryAt = next
		case reason == blockReasonDisabled:
			entry.state = scheduledStateDisabled
			entry.nextRetryAt = time.Time{}
		default:
			entry.state = scheduledStateBlocked
			entry.nextRetryAt = next
		}
		changed = true
	}
	if changed {
		m.rebuildIndexesLocked()
	}
}

// pickReadyLocked selects the next ready auth from the target priority bucket for the request attempt.
func (m *modelScheduler) pickReadyLocked(preferWebsocket bool, strategyForPriority func(int) schedulerStrategy, fillFirstRangeForPriority func(int) int, fillFirstPerAuthRPMForPriority func(int) int, rpmLimiter *fillFirstMinuteLimiter, selectionAttempt int, priorityPredicate func(*scheduledAuth) bool, pickPredicate func(*scheduledAuth) bool, provider, model string) (*Auth, error) {
	if m == nil {
		return nil, nil
	}
	now := time.Now()
	m.promoteExpiredLocked(now)
	priorities, restrictWebsocket := m.candidatePrioritiesAndWebsocketRestrictionLocked(preferWebsocket, priorityPredicate, pickPredicate)
	if len(priorities) == 0 {
		return nil, nil
	}
	pickFromPriorities := func(candidatePriorities []int, onlyWebsocket bool) (*Auth, bool) {
		rpmLimited := false
		for _, priorityReady := range selectionPrioritiesForAttempt(candidatePriorities, selectionAttempt) {
			strategy := schedulerStrategyRoundRobin
			if strategyForPriority != nil {
				strategy = strategyForPriority(priorityReady)
			}
			fillFirstRange := 1
			if fillFirstRangeForPriority != nil {
				fillFirstRange = fillFirstRangeForPriority(priorityReady)
			}
			fillFirstPerAuthRPM := 0
			if fillFirstPerAuthRPMForPriority != nil {
				fillFirstPerAuthRPM = fillFirstPerAuthRPMForPriority(priorityReady)
			}
			picked, limited := m.pickReadyAtPriorityLocked(onlyWebsocket, priorityReady, strategy, fillFirstRange, fillFirstPerAuthRPM, rpmLimiter, priorityPredicate, pickPredicate)
			if picked != nil {
				return picked, false
			}
			rpmLimited = rpmLimited || limited
		}
		return nil, rpmLimited
	}
	picked, rpmLimited := pickFromPriorities(priorities, restrictWebsocket)
	if picked != nil {
		return picked, nil
	}
	if restrictWebsocket && rpmLimited {
		fallbackPriorities := m.candidatePrioritiesForAllLocked(priorityPredicate)
		picked, fallbackRPMLimited := pickFromPriorities(fallbackPriorities, false)
		if picked != nil {
			return picked, nil
		}
		priorities = fallbackPriorities
		restrictWebsocket = false
		rpmLimited = rpmLimited || fallbackRPMLimited
	}
	if rpmLimited {
		rpmNow := fillFirstLimiterNow(rpmLimiter)
		rpmRetryAfter := fillFirstRPMRetryAfterAt(rpmLimiter, rpmNow)
		if earliest := m.blockedEarliestForAttemptLocked(restrictWebsocket, priorities, selectionAttempt, pickPredicate); cooldownBeforeRPMReset(earliest, rpmRetryAfter, rpmNow) {
			return nil, newModelCooldownErrorUntil(model, provider, earliest, rpmNow)
		}
		return nil, newAuthRPMLimitedError(rpmRetryAfter)
	}
	return nil, nil
}

func (m *modelScheduler) readyPrioritiesLocked(preferWebsocket bool, predicate func(*scheduledAuth) bool) []int {
	if m == nil {
		return nil
	}
	if preferWebsocket {
		priorities := make([]int, 0, len(m.priorityOrder))
		// When downstream is websocket and Codex supports websocket transport, prefer websocket-enabled
		// credentials even if they are in a lower priority tier than HTTP-only credentials.
		for _, priority := range m.priorityOrder {
			bucket := m.readyByPriority[priority]
			if bucket == nil {
				continue
			}
			if bucket.ws.pickFirst(predicate) != nil {
				priorities = append(priorities, priority)
			}
		}
		if len(priorities) > 0 {
			return priorities
		}
	}
	priorities := make([]int, 0, len(m.priorityOrder))
	for _, priority := range m.priorityOrder {
		bucket := m.readyByPriority[priority]
		if bucket == nil {
			continue
		}
		if bucket.all.pickFirst(predicate) != nil {
			priorities = append(priorities, priority)
		}
	}
	return priorities
}

func (m *modelScheduler) candidatePrioritiesLocked(preferWebsocket bool, predicate func(*scheduledAuth) bool) []int {
	priorities, _ := m.candidatePrioritiesAndWebsocketRestrictionLocked(preferWebsocket, predicate, predicate)
	return priorities
}

func (m *modelScheduler) candidatePrioritiesAndWebsocketRestrictionLocked(preferWebsocket bool, priorityPredicate func(*scheduledAuth) bool, pickPredicate func(*scheduledAuth) bool) ([]int, bool) {
	if m == nil {
		return nil, false
	}
	if preferWebsocket {
		if readyPriorities := m.readyWebsocketPrioritiesLocked(pickPredicate); len(readyPriorities) > 0 {
			priorities := m.candidatePrioritiesForWebsocketLocked(pickPredicate)
			if len(priorities) > 0 {
				return priorities, true
			}
		}
	}
	return m.candidatePrioritiesForAllLocked(priorityPredicate), false
}

func (m *modelScheduler) readyWebsocketPrioritiesLocked(predicate func(*scheduledAuth) bool) []int {
	if m == nil {
		return nil
	}
	prioritySet := make(map[int]struct{})
	for _, entry := range m.entries {
		if entry == nil || entry.meta == nil || !entry.meta.websocketEnabled || entry.state != scheduledStateReady {
			continue
		}
		if predicate != nil && !predicate(entry) {
			continue
		}
		prioritySet[entry.meta.priority] = struct{}{}
	}
	return sortedPrioritySet(prioritySet)
}

func (m *modelScheduler) candidatePrioritiesForWebsocketLocked(predicate func(*scheduledAuth) bool) []int {
	prioritySet := make(map[int]struct{})
	for _, entry := range m.entries {
		if entry == nil || entry.meta == nil || !entry.meta.websocketEnabled {
			continue
		}
		if !entryCandidateForPriority(entry, predicate) {
			continue
		}
		prioritySet[entry.meta.priority] = struct{}{}
	}
	return sortedPrioritySet(prioritySet)
}

func (m *modelScheduler) candidatePrioritiesForAllLocked(predicate func(*scheduledAuth) bool) []int {
	prioritySet := make(map[int]struct{})
	for _, entry := range m.entries {
		if !entryCandidateForPriority(entry, predicate) {
			continue
		}
		prioritySet[entry.meta.priority] = struct{}{}
	}
	return sortedPrioritySet(prioritySet)
}

func entryCandidateForPriority(entry *scheduledAuth, predicate func(*scheduledAuth) bool) bool {
	if entry == nil || entry.auth == nil || entry.meta == nil {
		return false
	}
	if predicate != nil && !predicate(entry) {
		return false
	}
	switch entry.state {
	case scheduledStateReady:
		return true
	case scheduledStateCooldown, scheduledStateBlocked:
		return !entry.nextRetryAt.IsZero()
	default:
		return false
	}
}

func sortedPrioritySet(prioritySet map[int]struct{}) []int {
	if len(prioritySet) == 0 {
		return nil
	}
	priorities := make([]int, 0, len(prioritySet))
	for priority := range prioritySet {
		priorities = append(priorities, priority)
	}
	sort.Slice(priorities, func(i, j int) bool {
		return priorities[i] > priorities[j]
	})
	return priorities
}

// pickReadyAtPriorityLocked selects the next ready auth from a specific priority bucket.
// The caller must ensure expired entries are already promoted when needed.
func (m *modelScheduler) pickReadyAtPriorityLocked(preferWebsocket bool, priority int, strategy schedulerStrategy, fillFirstRange int, fillFirstPerAuthRPM int, rpmLimiter *fillFirstMinuteLimiter, membershipPredicate func(*scheduledAuth) bool, pickPredicate func(*scheduledAuth) bool) (*Auth, bool) {
	if m == nil {
		return nil, false
	}
	if strategy == schedulerStrategyFillFirst && (normalizeFillFirstPerAuthRPMValue(fillFirstPerAuthRPM) > 0 || normalizeFillFirstRangeValue(fillFirstRange) > 1) {
		return m.pickFillFirstAtPriorityLocked(preferWebsocket, priority, fillFirstRange, fillFirstPerAuthRPM, rpmLimiter, membershipPredicate, pickPredicate)
	}
	bucket := m.readyByPriority[priority]
	if bucket == nil {
		return nil, false
	}
	view := &bucket.all
	if preferWebsocket {
		view = &bucket.ws
	}
	var picked *scheduledAuth
	switch strategy {
	case schedulerStrategyFillFirst:
		picked = view.pickFirst(pickPredicate)
	case schedulerStrategyRandom:
		picked = view.pickRandom(pickPredicate)
	default:
		picked = view.pickRoundRobin(pickPredicate)
	}
	if picked == nil || picked.auth == nil {
		return nil, false
	}
	return picked.auth, false
}

func (m *modelScheduler) pickFillFirstAtPriorityLocked(preferWebsocket bool, priority int, fillFirstRange int, fillFirstPerAuthRPM int, rpmLimiter *fillFirstMinuteLimiter, membershipPredicate func(*scheduledAuth) bool, pickPredicate func(*scheduledAuth) bool) (*Auth, bool) {
	if m == nil {
		return nil, false
	}
	fillFirstRange = normalizeFillFirstRangeValue(fillFirstRange)
	fillFirstPerAuthRPM = normalizeFillFirstPerAuthRPMValue(fillFirstPerAuthRPM)
	members := make([]*scheduledAuth, 0)
	for _, entry := range m.entries {
		if entry == nil || entry.auth == nil || entry.meta == nil || entry.meta.priority != priority {
			continue
		}
		if preferWebsocket && !entry.meta.websocketEnabled {
			continue
		}
		if !entryCandidateForPriority(entry, membershipPredicate) {
			continue
		}
		members = append(members, entry)
	}
	if len(members) == 0 {
		return nil, false
	}
	sort.Slice(members, func(i, j int) bool {
		return members[i].auth.ID < members[j].auth.ID
	})
	if fillFirstPerAuthRPM > 0 {
		now := time.Now()
		if rpmLimiter != nil {
			now = rpmLimiter.nowTime()
		}
		rpmLimited := false
		for _, entry := range members {
			if entry.state != scheduledStateReady {
				continue
			}
			if pickPredicate != nil && !pickPredicate(entry) {
				continue
			}
			if rpmLimiter != nil && !rpmLimiter.tryAcquireAt(entry.auth.ID, fillFirstPerAuthRPM, now) {
				rpmLimited = true
				continue
			}
			return entry.auth, false
		}
		return nil, rpmLimited
	}
	for start := 0; start < len(members); start += fillFirstRange {
		end := start + fillFirstRange
		if end > len(members) {
			end = len(members)
		}
		candidates := make([]*scheduledAuth, 0, end-start)
		for _, entry := range members[start:end] {
			if entry.state != scheduledStateReady {
				continue
			}
			if pickPredicate != nil && !pickPredicate(entry) {
				continue
			}
			candidates = append(candidates, entry)
		}
		if len(candidates) == 1 {
			return candidates[0].auth, false
		}
		if len(candidates) > 1 {
			return candidates[rand.IntN(len(candidates))].auth, false
		}
	}
	return nil, false
}

func (m *modelScheduler) readyCountAtPriorityLocked(preferWebsocket bool, priority int, predicate func(*scheduledAuth) bool) int {
	if m == nil {
		return 0
	}
	bucket := m.readyByPriority[priority]
	if bucket == nil {
		return 0
	}
	view := &bucket.all
	if preferWebsocket {
		view = &bucket.ws
	}
	count := 0
	for _, entry := range view.flat {
		if predicate == nil || predicate(entry) {
			count++
		}
	}
	return count
}

func (m *modelScheduler) readyEntriesAtPriorityLocked(preferWebsocket bool, priority int, predicate func(*scheduledAuth) bool) []*scheduledAuth {
	if m == nil {
		return nil
	}
	bucket := m.readyByPriority[priority]
	if bucket == nil {
		return nil
	}
	view := &bucket.all
	if preferWebsocket {
		view = &bucket.ws
	}
	out := make([]*scheduledAuth, 0, len(view.flat))
	for _, entry := range view.flat {
		if predicate != nil && !predicate(entry) {
			continue
		}
		out = append(out, entry)
	}
	return out
}

// unavailableErrorLocked returns the correct unavailable or cooldown error for the shard.
func (m *modelScheduler) unavailableErrorLocked(provider, model string, predicate func(*scheduledAuth) bool) error {
	now := time.Now()
	total, cooldownCount, earliest := m.availabilitySummaryLocked(predicate)
	if total == 0 {
		return &Error{Code: "auth_not_found", Message: "no auth available"}
	}
	if cooldownCount == total && !earliest.IsZero() {
		providerForError := provider
		if providerForError == "mixed" {
			providerForError = ""
		}
		resetIn := earliest.Sub(now)
		if resetIn < 0 {
			resetIn = 0
		}
		return newModelCooldownError(model, providerForError, resetIn)
	}
	return &Error{Code: "auth_unavailable", Message: "no auth available"}
}

func (m *modelScheduler) unavailableErrorForAttemptLocked(provider, model string, preferWebsocket bool, selectionAttempt int, priorityPredicate func(*scheduledAuth) bool, pickPredicate func(*scheduledAuth) bool) error {
	now := time.Now()
	priorities, restrictWebsocket := m.candidatePrioritiesAndWebsocketRestrictionLocked(preferWebsocket, priorityPredicate, pickPredicate)
	if len(priorities) == 0 {
		return m.unavailableErrorLocked(provider, model, pickPredicate)
	}
	earliest := m.blockedEarliestForAttemptLocked(restrictWebsocket, priorities, selectionAttempt, pickPredicate)
	if !earliest.IsZero() {
		return newModelCooldownErrorUntil(model, provider, earliest, now)
	}
	return &Error{Code: "auth_unavailable", Message: "no auth available"}
}

func (m *modelScheduler) blockedEarliestForAttemptLocked(preferWebsocket bool, priorities []int, selectionAttempt int, predicate func(*scheduledAuth) bool) time.Time {
	var earliest time.Time
	for _, priority := range selectionPrioritiesForAttempt(priorities, selectionAttempt) {
		if next, okNext := m.blockedEarliestAtPriorityLocked(preferWebsocket, priority, predicate); okNext && (earliest.IsZero() || next.Before(earliest)) {
			earliest = next
		}
	}
	return earliest
}

func (m *modelScheduler) blockedEarliestAtPriorityLocked(preferWebsocket bool, priority int, predicate func(*scheduledAuth) bool) (time.Time, bool) {
	if m == nil {
		return time.Time{}, false
	}
	var earliest time.Time
	for _, entry := range m.entries {
		if entry == nil || entry.auth == nil || entry.meta == nil {
			continue
		}
		if entry.meta.priority != priority {
			continue
		}
		if preferWebsocket && !entry.meta.websocketEnabled {
			continue
		}
		if predicate != nil && !predicate(entry) {
			continue
		}
		if entry.state != scheduledStateCooldown && entry.state != scheduledStateBlocked {
			continue
		}
		if entry.nextRetryAt.IsZero() {
			continue
		}
		if earliest.IsZero() || entry.nextRetryAt.Before(earliest) {
			earliest = entry.nextRetryAt
		}
	}
	if !earliest.IsZero() {
		return earliest, true
	}
	return time.Time{}, false
}

// availabilitySummaryLocked summarizes total candidates, cooldown count, and earliest retry time.
func (m *modelScheduler) availabilitySummaryLocked(predicate func(*scheduledAuth) bool) (int, int, time.Time) {
	if m == nil {
		return 0, 0, time.Time{}
	}
	total := 0
	cooldownCount := 0
	earliest := time.Time{}
	for _, entry := range m.entries {
		if predicate != nil && !predicate(entry) {
			continue
		}
		total++
		if entry == nil || entry.auth == nil {
			continue
		}
		if entry.state != scheduledStateCooldown {
			continue
		}
		cooldownCount++
		if !entry.nextRetryAt.IsZero() && (earliest.IsZero() || entry.nextRetryAt.Before(earliest)) {
			earliest = entry.nextRetryAt
		}
	}
	return total, cooldownCount, earliest
}

// rebuildIndexesLocked reconstructs ready and blocked views from the current entry map.
func (m *modelScheduler) rebuildIndexesLocked() {
	cursorStates := make(map[int]readyBucketCursorState, len(m.readyByPriority))
	for priority, bucket := range m.readyByPriority {
		if bucket == nil {
			continue
		}
		cursorStates[priority] = readyBucketCursorState{
			all: snapshotReadyViewCursors(bucket.all),
			ws:  snapshotReadyViewCursors(bucket.ws),
		}
	}

	m.readyByPriority = make(map[int]*readyBucket)
	m.priorityOrder = m.priorityOrder[:0]
	m.blocked = m.blocked[:0]
	priorityBuckets := make(map[int][]*scheduledAuth)
	for _, entry := range m.entries {
		if entry == nil || entry.auth == nil {
			continue
		}
		switch entry.state {
		case scheduledStateReady:
			priority := entry.meta.priority
			priorityBuckets[priority] = append(priorityBuckets[priority], entry)
		case scheduledStateCooldown, scheduledStateBlocked:
			m.blocked = append(m.blocked, entry)
		}
	}
	for priority, entries := range priorityBuckets {
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].auth.ID < entries[j].auth.ID
		})
		bucket := buildReadyBucket(entries)
		if cursorState, ok := cursorStates[priority]; ok && bucket != nil {
			restoreReadyViewCursors(&bucket.all, cursorState.all)
			restoreReadyViewCursors(&bucket.ws, cursorState.ws)
		}
		m.readyByPriority[priority] = bucket
		m.priorityOrder = append(m.priorityOrder, priority)
	}
	sort.Slice(m.priorityOrder, func(i, j int) bool {
		return m.priorityOrder[i] > m.priorityOrder[j]
	})
	sort.Slice(m.blocked, func(i, j int) bool {
		left := m.blocked[i]
		right := m.blocked[j]
		if left == nil || right == nil {
			return left != nil
		}
		if left.nextRetryAt.Equal(right.nextRetryAt) {
			return left.auth.ID < right.auth.ID
		}
		if left.nextRetryAt.IsZero() {
			return false
		}
		if right.nextRetryAt.IsZero() {
			return true
		}
		return left.nextRetryAt.Before(right.nextRetryAt)
	})
}

// buildReadyBucket prepares the general and websocket-only ready views for one priority bucket.
func buildReadyBucket(entries []*scheduledAuth) *readyBucket {
	bucket := &readyBucket{}
	bucket.all = buildReadyView(entries)
	wsEntries := make([]*scheduledAuth, 0, len(entries))
	for _, entry := range entries {
		if entry != nil && entry.meta != nil && entry.meta.websocketEnabled {
			wsEntries = append(wsEntries, entry)
		}
	}
	bucket.ws = buildReadyView(wsEntries)
	return bucket
}

// buildReadyView creates a flat selection view.
func buildReadyView(entries []*scheduledAuth) readyView {
	return readyView{flat: append([]*scheduledAuth(nil), entries...)}
}

// pickFirst returns the first ready entry that satisfies predicate without advancing cursors.
func (v *readyView) pickFirst(predicate func(*scheduledAuth) bool) *scheduledAuth {
	for _, entry := range v.flat {
		if predicate == nil || predicate(entry) {
			return entry
		}
	}
	return nil
}

// pickRoundRobin returns the next ready entry using flat round-robin traversal.
func (v *readyView) pickRoundRobin(predicate func(*scheduledAuth) bool) *scheduledAuth {
	if len(v.flat) == 0 {
		return nil
	}
	start := 0
	if len(v.flat) > 0 {
		start = v.cursor % len(v.flat)
	}
	for offset := 0; offset < len(v.flat); offset++ {
		index := (start + offset) % len(v.flat)
		entry := v.flat[index]
		if predicate != nil && !predicate(entry) {
			continue
		}
		v.cursor = index + 1
		return entry
	}
	return nil
}

func (v *readyView) pickRandom(predicate func(*scheduledAuth) bool) *scheduledAuth {
	if len(v.flat) == 0 {
		return nil
	}
	candidates := make([]*scheduledAuth, 0, len(v.flat))
	for _, entry := range v.flat {
		if predicate != nil && !predicate(entry) {
			continue
		}
		candidates = append(candidates, entry)
	}
	if len(candidates) == 0 {
		return nil
	}
	return candidates[rand.IntN(len(candidates))]
}
