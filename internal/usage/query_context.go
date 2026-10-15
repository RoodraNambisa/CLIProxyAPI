package usage

import (
	"context"
	"slices"
	"sort"
	"strings"
	"time"
)

const usageQueryBatchSize = 1024

func usageQueryContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// Waiting for a statistics lock must not outlive a disconnected management
// request. No waiter goroutine is created, and no lock is acquired after exit.
func lockUsageQuery(ctx context.Context, try func() bool) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if try() {
			return nil
		}
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *RequestStatistics) queryReadLock(ctx context.Context) error {
	if err := s.batch.flushContext(ctx); err != nil {
		return err
	}
	for {
		if err := lockUsageQuery(ctx, s.mu.TryRLock); err != nil {
			return err
		}
		if !s.detailIndexDirty {
			return nil
		}
		s.mu.RUnlock()
		if err := lockUsageQuery(ctx, s.mu.TryLock); err != nil {
			return err
		}
		err := s.sortDetailIndexContextLocked(ctx)
		s.mu.Unlock()
		if err != nil {
			return err
		}
	}
}

func detailRefCompare(a, b usageDetailRef) int {
	if n := a.Timestamp.Compare(b.Timestamp); n != 0 {
		return n
	}
	if a.ID < b.ID {
		return -1
	}
	if a.ID > b.ID {
		return 1
	}
	return 0
}

// Each merge is prepared separately before publishing. Cancellation therefore
// leaves every ID present exactly once, even if some runs are still unsorted.
func sortUsageQuerySlice[T any](ctx context.Context, values []T, compare func(T, T) int) error {
	for start := 0; start < len(values); start += usageQueryBatchSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		slices.SortFunc(values[start:min(len(values), start+usageQueryBatchSize)], compare)
	}
	var scratch []T
	for width := usageQueryBatchSize; width < len(values); {
		for start := 0; start < len(values)-width; start += 2 * width {
			if err := ctx.Err(); err != nil {
				return err
			}
			middle, end := start+width, min(len(values), start+2*width)
			if compare(values[middle-1], values[middle]) <= 0 {
				continue
			}
			if cap(scratch) < end-start {
				scratch = make([]T, end-start)
			} else {
				scratch = scratch[:end-start]
			}
			left, right := start, middle
			for i := range scratch {
				if i%usageQueryBatchSize == 0 {
					if err := ctx.Err(); err != nil {
						return err
					}
				}
				if right >= end || left < middle && compare(values[left], values[right]) <= 0 {
					scratch[i] = values[left]
					left++
				} else {
					scratch[i] = values[right]
					right++
				}
			}
			copy(values[start:end], scratch)
		}
		if width > len(values)/2 {
			break
		}
		width *= 2
	}
	return ctx.Err()
}

func (s *RequestStatistics) sortDetailIndexContextLocked(ctx context.Context) error {
	if !s.detailIndexDirty {
		return ctx.Err()
	}
	prefix := min(max(s.detailIndexSorted, 0), len(s.detailIndex))
	if err := sortUsageQuerySlice(ctx, s.detailIndex[prefix:], detailRefCompare); err != nil {
		return err
	}
	if prefix > 0 && prefix < len(s.detailIndex) {
		first := s.detailIndex[prefix]
		start := sort.Search(prefix, func(i int) bool { return detailRefCompare(s.detailIndex[i], first) > 0 })
		// Only overlapping history needs merging; recent out-of-order completions
		// normally touch a short suffix instead of the complete retained history.
		if start < prefix {
			merged := make([]usageDetailRef, len(s.detailIndex)-start)
			left, right := start, prefix
			for i := range merged {
				if i%usageQueryBatchSize == 0 {
					if err := ctx.Err(); err != nil {
						return err
					}
				}
				if right == len(s.detailIndex) || left < prefix && detailRefCompare(s.detailIndex[left], s.detailIndex[right]) <= 0 {
					merged[i] = s.detailIndex[left]
					left++
				} else {
					merged[i] = s.detailIndex[right]
					right++
				}
			}
			copy(s.detailIndex[start:], merged)
		}
	}
	s.detailIndexSorted = len(s.detailIndex)
	s.detailIndexDirty = false
	return ctx.Err()
}

type usageQueryCandidate struct {
	id               uint64
	at               time.Time
	api, model, auth string
	tokens           int64
}

func compareUsageCandidates(a, b usageQueryCandidate, by, order string) int {
	n := 0
	switch by {
	case SortByAPI:
		n = strings.Compare(a.api, b.api)
	case SortByModel:
		n = strings.Compare(a.model, b.model)
	case SortByAuthIndex:
		n = strings.Compare(strings.TrimSpace(a.auth), strings.TrimSpace(b.auth))
	case SortByTokens:
		n = compareInt64(a.tokens, b.tokens)
	default:
		n = a.at.Compare(b.at)
	}
	if n == 0 {
		n = a.at.Compare(b.at)
	}
	if n == 0 {
		n = strings.Compare(a.api+"\x00"+a.model+"\x00"+a.auth, b.api+"\x00"+b.model+"\x00"+b.auth)
	}
	if order == SortOrderDesc {
		return -n
	}
	return n
}

// retainQueryCandidate keeps the best K lightweight references in a max heap.
func retainQueryCandidate(values []usageQueryCandidate, candidate usageQueryCandidate, k int, compare func(usageQueryCandidate, usageQueryCandidate) int) []usageQueryCandidate {
	if k <= 0 {
		return values
	}
	if len(values) < k {
		values = append(values, candidate)
		for i := len(values) - 1; i > 0; {
			parent := (i - 1) / 2
			if compare(values[parent], values[i]) >= 0 {
				break
			}
			values[parent], values[i] = values[i], values[parent]
			i = parent
		}
		return values
	}
	if compare(candidate, values[0]) >= 0 {
		return values
	}
	values[0] = candidate
	for i := 0; ; {
		child := 2*i + 1
		if child >= len(values) {
			break
		}
		if child+1 < len(values) && compare(values[child+1], values[child]) > 0 {
			child++
		}
		if compare(values[i], values[child]) >= 0 {
			break
		}
		values[i], values[child] = values[child], values[i]
		i = child
	}
	return values
}

// DetailsContext returns an exact page without copying every matching detail.
func (s *RequestStatistics) DetailsContext(ctx context.Context, query DetailQuery) (DetailPage, error) {
	ctx = usageQueryContext(ctx)
	query = normalizeDetailQuery(query)
	empty := newDetailPage(nil, 0, query.Offset, query.Limit)
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	if s == nil {
		return empty, nil
	}
	if err := s.queryReadLock(ctx); err != nil {
		return empty, err
	}
	defer s.mu.RUnlock()
	start, end := s.queryIndexBoundsLocked(query.TimeRange)
	maxInt := int(^uint(0) >> 1)
	k := min(query.Offset, maxInt-query.Limit) + query.Limit
	k = min(k, len(s.detailLocations))
	selected := make([]usageQueryCandidate, 0, min(k, query.Limit))
	compare := func(a, b usageQueryCandidate) int { return compareUsageCandidates(a, b, query.SortBy, query.SortOrder) }
	unfiltered := query.API == "" && query.Model == "" && query.AuthIndex == "" && query.Source == "" && query.ClientIP == "" && query.RequestPath == "" && query.Failed == nil
	knownCount := unfiltered && s.detailIndexTombstones == 0
	total := 0
	if knownCount {
		total = end - start
	}
	visit := func(id uint64) bool {
		detail, location, ok := s.detailForIDLocked(id)
		if !ok || query.API != "" && query.API != location.API || query.Model != "" && query.Model != location.Model || !detailMatchesQuery(detail, query) {
			return false
		}
		if !knownCount {
			total++
		}
		if query.SortBy == SortByCreatedAt && len(selected) == k && k > 0 {
			cmp := detail.Timestamp.Compare(selected[0].at)
			if query.SortOrder == SortOrderDesc && cmp < 0 || query.SortOrder == SortOrderAsc && cmp > 0 {
				return knownCount
			}
		}
		selected = retainQueryCandidate(selected, usageQueryCandidate{id: id, at: detail.Timestamp, api: location.API, model: location.Model, auth: detail.AuthIndex, tokens: detail.Tokens.TotalTokens}, k, compare)
		return false
	}
	if query.AuthIndex != "" {
		for i, id := range s.authDetailIDs[query.AuthIndex] {
			if i%usageQueryBatchSize == 0 {
				if err := ctx.Err(); err != nil {
					return empty, err
				}
			}
			visit(id)
		}
	} else {
		for i := 0; i < end-start; i++ {
			if i%usageQueryBatchSize == 0 {
				if err := ctx.Err(); err != nil {
					return empty, err
				}
			}
			index := start + i
			if query.SortOrder == SortOrderDesc {
				index = end - 1 - i
			}
			if visit(s.detailIndex[index].ID) {
				break
			}
		}
	}
	if err := sortUsageQuerySlice(ctx, selected, compare); err != nil {
		return empty, err
	}
	rows := make([]DetailEntry, 0, min(query.Limit, len(selected)))
	for i := min(query.Offset, len(selected)); i < len(selected); i++ {
		detail, location, ok := s.detailForIDLocked(selected[i].id)
		if ok {
			rows = append(rows, DetailEntry{API: location.API, Model: location.Model, RequestDetail: publicRequestDetail(detail)})
		}
	}
	return newDetailPage(rows, total, query.Offset, query.Limit), ctx.Err()
}

func (s *RequestStatistics) queryIndexBoundsLocked(r TimeRange) (int, int) {
	start, end := 0, len(s.detailIndex)
	if !r.From.IsZero() {
		start = sort.Search(end, func(i int) bool { return !s.detailIndex[i].Timestamp.Before(r.From) })
	}
	if !r.To.IsZero() {
		end = sort.Search(end, func(i int) bool { return !s.detailIndex[i].Timestamp.Before(r.To) })
	}
	return start, max(start, end)
}
