package usage

import "time"

// HistoryGeneration returns the destructive-mutation generation used to
// prevent a delayed restore from resurrecting cleared or pruned history.
func (s *RequestStatistics) HistoryGeneration() uint64 {
	if s == nil {
		return 0
	}
	s.flushPending()
	s.mu.RLock()
	generation := s.historyGeneration
	s.mu.RUnlock()
	return generation
}

// DetailCount returns the number of retained request detail rows.
func (s *RequestStatistics) DetailCount() int {
	if s == nil {
		return 0
	}
	s.flushPending()
	s.mu.RLock()
	count := len(s.detailLocations)
	s.mu.RUnlock()
	return count
}

// PruneBefore removes request details older than cutoff and rebuilds all
// aggregates from the retained window.
func (s *RequestStatistics) PruneBefore(cutoff time.Time) int {
	if s == nil || cutoff.IsZero() {
		return 0
	}
	s.flushPending()
	cutoff = cutoff.UTC()
	s.mu.Lock()
	removed := s.pruneDetailsLocked(func(detail RequestDetail) bool {
		return detail.Timestamp.Before(cutoff)
	})
	if removed > 0 {
		s.historyGeneration++
		s.markChangedLocked()
	}
	s.mu.Unlock()
	return removed
}

// PruneOldest removes at most limit oldest detail rows and rebuilds all
// aggregates from the retained window.
func (s *RequestStatistics) PruneOldest(limit int) int {
	if s == nil || limit <= 0 {
		return 0
	}
	s.flushPending()
	s.mu.Lock()
	s.sortDetailIndexLocked()
	removeIDs := make(map[uint64]struct{}, limit)
	for _, ref := range s.detailIndex {
		if len(removeIDs) >= limit {
			break
		}
		if _, exists := s.detailLocations[ref.ID]; exists {
			removeIDs[ref.ID] = struct{}{}
		}
	}
	removed := s.pruneDetailsLocked(func(detail RequestDetail) bool {
		_, remove := removeIDs[detail.internalID]
		return remove
	})
	if removed > 0 {
		s.historyGeneration++
		s.markChangedLocked()
	}
	s.mu.Unlock()
	return removed
}

func (s *RequestStatistics) pruneDetailsLocked(remove func(RequestDetail) bool) int {
	if remove == nil {
		return 0
	}
	removed := 0
	for apiName, stats := range s.apis {
		if stats == nil {
			delete(s.apis, apiName)
			continue
		}
		for modelName, modelStatsValue := range stats.Models {
			if modelStatsValue == nil {
				delete(stats.Models, modelName)
				continue
			}
			details := modelStatsValue.Details
			kept := details[:0]
			for _, detail := range details {
				if remove(detail) {
					removed++
					continue
				}
				kept = append(kept, detail)
			}
			for index := len(kept); index < len(details); index++ {
				details[index] = RequestDetail{}
			}
			modelStatsValue.Details = kept
			if len(kept) == 0 {
				delete(stats.Models, modelName)
			}
		}
		if len(stats.Models) == 0 {
			delete(s.apis, apiName)
		}
	}
	if removed > 0 {
		s.rebuildLocked()
	}
	return removed
}
