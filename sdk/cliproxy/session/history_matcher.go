package session

import (
	"container/list"
	"crypto/sha256"
	"strings"
	"sync"
	"time"
)

const historyMaxGroups = 4096
const historyMaxPrefixes = 262144

type historyIndexKey struct {
	scope  [sha256.Size]byte
	prefix [sha256.Size]byte
}

type historyEntry struct {
	key     historyIndexKey
	history History
	authID  string
	expires time.Time
	version uint64
	lru     *list.Element
}

// HistoryMatcher holds bounded, caller-scoped credential preferences. It has
// no background goroutine and stores neither prompt text nor response state.
type HistoryMatcher struct {
	mu                     sync.Mutex
	ttl                    time.Duration
	now                    func() time.Time
	maxGroups, maxPrefixes int
	groups                 map[historyIndexKey]*historyEntry
	prefixes               map[historyIndexKey]map[*historyEntry]struct{}
	lru                    list.List
	prefixCount            int
	version, operations    uint64
}

type HistoryMatch struct {
	AuthID       string
	PrefixLength int
}

func NewHistoryMatcher(ttl time.Duration) *HistoryMatcher {
	return newHistoryMatcher(ttl, historyMaxGroups, historyMaxPrefixes, time.Now)
}

func newHistoryMatcher(ttl time.Duration, groups, prefixes int, now func() time.Time) *HistoryMatcher {
	if ttl <= 0 {
		ttl = time.Hour
	}
	if groups <= 0 {
		groups = historyMaxGroups
	}
	if prefixes <= 0 {
		prefixes = historyMaxPrefixes
	}
	if now == nil {
		now = time.Now
	}
	return &HistoryMatcher{ttl: ttl, now: now, maxGroups: groups, maxPrefixes: prefixes, groups: make(map[historyIndexKey]*historyEntry), prefixes: make(map[historyIndexKey]map[*historyEntry]struct{})}
}

// Match uses the longest eligible common prefix. Conflicting credentials at
// that prefix are ambiguous; it must not pick one based on map iteration order.
func (m *HistoryMatcher) Match(namespace string, history History) (HistoryMatch, bool) {
	if m == nil || namespace == "" || !history.Usable() {
		return HistoryMatch{}, false
	}
	scope := sha256.Sum256([]byte(namespace))
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.now == nil {
		return HistoryMatch{}, false
	}
	now := m.now()
	m.pruneLocked(now)
	for index := len(history.prefixes) - 1; index >= history.minimum-1; index-- {
		bucket := m.prefixes[historyIndexKey{scope, history.prefixes[index]}]
		var best *historyEntry
		for entry := range bucket {
			if !now.Before(entry.expires) {
				continue
			}
			if best != nil && best.authID != entry.authID {
				return HistoryMatch{}, false
			}
			if best == nil || entry.version > best.version {
				best = entry
			}
		}
		if best != nil {
			best.expires = now.Add(m.ttl)
			m.lru.MoveToBack(best.lru)
			return HistoryMatch{AuthID: best.authID, PrefixLength: index + 1}, true
		}
	}
	return HistoryMatch{}, false
}

// Bind publishes a completed request's credential preference and returns its
// revision. A zero revision means the history was ineligible or over budget.
func (m *HistoryMatcher) Bind(namespace string, history History, authID string) uint64 {
	if m == nil || namespace == "" || authID == "" || !history.Usable() {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.now == nil || m.groups == nil || len(history.prefixes)-history.minimum+1 > m.maxPrefixes {
		return 0
	}
	now := m.now()
	m.pruneLocked(now)
	key := historyIndexKey{sha256.Sum256([]byte(namespace)), history.prefixes[len(history.prefixes)-1]}
	if previous := m.groups[key]; previous != nil {
		m.removeLocked(previous)
	}
	needed := len(history.prefixes) - history.minimum + 1
	for len(m.groups) >= m.maxGroups || m.prefixCount+needed > m.maxPrefixes {
		m.removeLocked(m.lru.Front().Value.(*historyEntry))
	}
	m.version++
	entry := &historyEntry{key: key, history: history, authID: strings.Clone(authID), expires: now.Add(m.ttl), version: m.version}
	m.addLocked(entry)
	return entry.version
}

func (m *HistoryMatcher) addLocked(entry *historyEntry) {
	m.groups[entry.key] = entry
	entry.lru = m.lru.PushBack(entry)
	for _, prefix := range entry.history.prefixes[entry.history.minimum-1:] {
		key := historyIndexKey{entry.key.scope, prefix}
		bucket := m.prefixes[key]
		if bucket == nil {
			bucket = make(map[*historyEntry]struct{})
			m.prefixes[key] = bucket
		}
		bucket[entry] = struct{}{}
		m.prefixCount++
	}
}

func (m *HistoryMatcher) removeLocked(entry *historyEntry) {
	delete(m.groups, entry.key)
	m.lru.Remove(entry.lru)
	for _, prefix := range entry.history.prefixes[entry.history.minimum-1:] {
		key := historyIndexKey{entry.key.scope, prefix}
		bucket := m.prefixes[key]
		delete(bucket, entry)
		if len(bucket) == 0 {
			delete(m.prefixes, key)
		}
		m.prefixCount--
	}
}

func (m *HistoryMatcher) pruneLocked(now time.Time) {
	m.operations++
	if m.operations%128 != 0 {
		return
	}
	for _, entry := range m.groups {
		if !now.Before(entry.expires) {
			m.removeLocked(entry)
		}
	}
}

func (m *HistoryMatcher) InvalidateAuth(authID string) {
	if m == nil || authID == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, entry := range m.groups {
		if entry.authID == authID {
			m.removeLocked(entry)
		}
	}
}
