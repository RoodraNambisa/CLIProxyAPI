package auth

import "container/heap"

// tokenExpiryQueue retains exactly one entry per ready credential with known
// expiry. Updates replace the heap key, rather than accumulating stale tokens.
type tokenExpiryQueue []*scheduledAuth

func (q tokenExpiryQueue) Len() int { return len(q) }
func (q tokenExpiryQueue) Less(i, j int) bool {
	return q[i].meta.accessTokenExpiresAt.Before(q[j].meta.accessTokenExpiresAt)
}
func (q tokenExpiryQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].expiryIndex, q[j].expiryIndex = i, j
}
func (q *tokenExpiryQueue) Push(value any) {
	entry := value.(*scheduledAuth)
	entry.expiryIndex = len(*q)
	*q = append(*q, entry)
}
func (q *tokenExpiryQueue) Pop() any {
	last := len(*q) - 1
	entry := (*q)[last]
	(*q)[last] = nil
	*q = (*q)[:last]
	entry.expiryIndex = -1
	return entry
}

func (m *modelScheduler) updateTokenExpiryLocked(entry *scheduledAuth) {
	index := entry.expiryIndex
	indexed := index >= 0 && index < len(m.expiring) && m.expiring[index] == entry
	if entry.state == scheduledStateReady && entry.meta.hasAccessTokenExpiry {
		if indexed {
			heap.Fix(&m.expiring, index)
		} else {
			heap.Push(&m.expiring, entry)
		}
	} else if indexed {
		heap.Remove(&m.expiring, index)
	}
}
