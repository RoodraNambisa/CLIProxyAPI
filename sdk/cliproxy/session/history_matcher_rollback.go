package session

type historyMutation struct {
	key           historyIndexKey
	version       uint64
	entry         *historyEntry
	previous      *historyEntry
	invalidations uint64
}

// BindWithRollback publishes a preference and returns an idempotent rollback
// for a credential that retires before its execution result can be committed.
func (m *HistoryMatcher) BindWithRollback(namespace string, history History, authID string) func() {
	mutation := m.bind(namespace, history, authID)
	if mutation.version == 0 {
		return nil
	}
	return func() { m.rollback(mutation) }
}

func (m *HistoryMatcher) rollback(mutation historyMutation) {
	m.mu.Lock()
	defer m.mu.Unlock()
	mutation.entry.discarded = true
	current := m.groups[mutation.key]
	if current == nil || current.version != mutation.version {
		return
	}
	m.removeLocked(current)
	previous := mutation.previous
	// A global epoch conservatively prevents restoring an invalidated prior
	// credential without retaining an unbounded credential-generation map.
	if previous == nil || previous.discarded || mutation.invalidations != m.invalidations || !m.now().Before(previous.expires) {
		return
	}
	needed := len(previous.history.prefixes) - previous.history.minimum + 1
	if len(m.groups) >= m.maxGroups || m.prefixCount+needed > m.maxPrefixes {
		return
	}
	m.addLocked(previous)
}
