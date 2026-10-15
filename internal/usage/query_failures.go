package usage

import (
	"context"
	"slices"
	"strings"
	"sync"
	"time"
)

type usageFailureCounts struct {
	total, main, auxiliary         int64
	boundaries                     FailureBoundarySummary
	codes, stages, models, sources map[string]int64
	hours                          map[int64]int64
}

func failureDimension(value string) string {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return "unknown"
}

func adjustFailureCount[K comparable](values map[K]int64, key K, delta int64) {
	values[key] += delta
	if values[key] == 0 {
		delete(values, key)
	}
}

func (v *usageFailureCounts) init() {
	if v.codes != nil {
		return
	}
	v.codes, v.stages, v.models, v.sources = make(map[string]int64), make(map[string]int64), make(map[string]int64), make(map[string]int64)
	v.hours = make(map[int64]int64)
}

func (v *usageFailureCounts) add(model string, detail RequestDetail, delta int64) {
	if !detail.Failed {
		return
	}
	v.init()
	v.total += delta
	if detail.Auxiliary {
		v.auxiliary += delta
	} else {
		v.main += delta
	}
	if detail.CredentialSelected {
		v.boundaries.CredentialSelected += delta
	}
	if detail.UpstreamCommitted {
		v.boundaries.UpstreamCommitted += delta
	}
	if detail.AuthRequestSlotConsumed {
		v.boundaries.AuthRequestSlotConsumed += delta
	}
	adjustFailureCount(v.codes, failureDimension(detail.ErrorCode), delta)
	adjustFailureCount(v.stages, failureDimension(detail.FailureStage), delta)
	adjustFailureCount(v.models, failureDimension(model), delta)
	adjustFailureCount(v.sources, failureDimension(detail.Source), delta)
	adjustFailureCount(v.hours, detail.Timestamp.UTC().Truncate(time.Hour).Unix(), delta)
}

func mergeFailureDimension[K comparable](ctx context.Context, target, source map[K]int64) error {
	visited := 0
	for key, count := range source {
		if visited%usageQueryBatchSize == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		visited++
		target[key] += count
	}
	return ctx.Err()
}

func (v *usageFailureCounts) merge(ctx context.Context, other *usageFailureCounts) error {
	if other == nil || other.total == 0 {
		return ctx.Err()
	}
	v.init()
	v.total += other.total
	v.main += other.main
	v.auxiliary += other.auxiliary
	v.boundaries.CredentialSelected += other.boundaries.CredentialSelected
	v.boundaries.UpstreamCommitted += other.boundaries.UpstreamCommitted
	v.boundaries.AuthRequestSlotConsumed += other.boundaries.AuthRequestSlotConsumed
	for _, pair := range [][2]map[string]int64{{v.codes, other.codes}, {v.stages, other.stages}, {v.models, other.models}, {v.sources, other.sources}} {
		if err := mergeFailureDimension(ctx, pair[0], pair[1]); err != nil {
			return err
		}
	}
	return mergeFailureDimension(ctx, v.hours, other.hours)
}

func (v *usageFailureCounts) snapshot() FailureSummary {
	result, _ := v.snapshotContext(context.Background())
	return result
}

func (v *usageFailureCounts) snapshotContext(ctx context.Context) (FailureSummary, error) {
	hours := make(map[string]int64, len(v.hours))
	for hour, n := range v.hours {
		if len(hours)%usageQueryBatchSize == 0 {
			if err := ctx.Err(); err != nil {
				return FailureSummary{}, err
			}
		}
		hours[time.Unix(hour, 0).UTC().Format(time.RFC3339)] = n
	}
	result := FailureSummary{AsOf: time.Now().UTC(), Total: v.total, Main: v.main, Auxiliary: v.auxiliary, Boundaries: v.boundaries}
	for _, dimension := range []struct {
		values map[string]int64
		output *[]FailureDimensionCount
	}{
		{v.codes, &result.ByErrorCode}, {v.stages, &result.ByFailureStage}, {v.models, &result.ByModel}, {v.sources, &result.BySource}, {hours, &result.ByHour},
	} {
		rows := make([]FailureDimensionCount, 0, len(dimension.values))
		for value, count := range dimension.values {
			if len(rows)%usageQueryBatchSize == 0 {
				if err := ctx.Err(); err != nil {
					return FailureSummary{}, err
				}
			}
			percent := float64(0)
			if v.total > 0 {
				percent = float64(count) * 100 / float64(v.total)
			}
			rows = append(rows, FailureDimensionCount{Value: value, Count: count, Percent: percent})
		}
		if err := sortUsageQuerySlice(ctx, rows, func(a, b FailureDimensionCount) int {
			if a.Count == b.Count {
				return strings.Compare(a.Value, b.Value)
			}
			return -compareInt64(a.Count, b.Count)
		}); err != nil {
			return FailureSummary{}, err
		}
		*dimension.output = rows
	}
	return result, ctx.Err()
}

type failureQueryKey struct {
	query               FailureSummaryQuery
	version, generation uint64
}

type failureQueryFlight struct {
	done   chan struct{}
	result FailureSummary
	err    error
}

type failureQueryCache struct {
	mu      sync.Mutex
	entries map[failureQueryKey]*failureQueryFlight
	order   []failureQueryKey
}

const usageFailureQueryCacheLimit = 64

func (c *failureQueryCache) begin(key failureQueryKey) (*failureQueryFlight, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if found := c.entries[key]; found != nil {
		return found, false
	}
	if c.entries == nil {
		c.entries = make(map[failureQueryKey]*failureQueryFlight)
	}
	if len(c.entries) >= usageFailureQueryCacheLimit {
		for i, old := range c.order {
			select {
			case <-c.entries[old].done:
				delete(c.entries, old)
				c.order = append(c.order[:i], c.order[i+1:]...)
			default:
				continue
			}
			break
		}
	}
	flight := &failureQueryFlight{done: make(chan struct{})}
	if len(c.entries) < usageFailureQueryCacheLimit {
		c.entries[key] = flight
		c.order = append(c.order, key)
	}
	return flight, true
}

func (c *failureQueryCache) finish(key failureQueryKey, flight *failureQueryFlight, result FailureSummary, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	flight.result, flight.err = result, err
	close(flight.done)
	if err != nil && c.entries[key] == flight {
		delete(c.entries, key)
		for i, k := range c.order {
			if k == key {
				c.order = append(c.order[:i], c.order[i+1:]...)
				break
			}
		}
	}
}

func cloneFailureSummary(v FailureSummary) FailureSummary {
	v.ByErrorCode = slices.Clone(v.ByErrorCode)
	v.ByFailureStage = slices.Clone(v.ByFailureStage)
	v.ByModel = slices.Clone(v.ByModel)
	v.BySource = slices.Clone(v.BySource)
	v.ByHour = slices.Clone(v.ByHour)
	return v
}

// FailureSummaryContext merges complete buckets and scans only boundary or
// filtered buckets. Cache entries are tied to an exact history version.
func (s *RequestStatistics) FailureSummaryContext(ctx context.Context, query FailureSummaryQuery) (FailureSummary, error) {
	ctx = usageQueryContext(ctx)
	empty := (&usageFailureCounts{}).snapshot()
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	if s == nil {
		return empty, nil
	}
	query.API, query.Model, query.Source = strings.TrimSpace(query.API), strings.TrimSpace(query.Model), strings.TrimSpace(query.Source)
	query.TimeRange.From, query.TimeRange.To = query.TimeRange.From.Round(0).UTC(), query.TimeRange.To.Round(0).UTC()
	for {
		if err := s.batch.flushContext(ctx); err != nil {
			return empty, err
		}
		if err := lockUsageQuery(ctx, s.mu.TryRLock); err != nil {
			return empty, err
		}
		key := failureQueryKey{query: query, version: s.changeCount, generation: s.queryGeneration}
		flight, leader := s.failureQueries.begin(key)
		if !leader {
			s.mu.RUnlock()
			select {
			case <-ctx.Done():
				return empty, ctx.Err()
			case <-flight.done:
			}
			if flight.err != nil {
				continue
			}
			return cloneFailureSummary(flight.result), nil
		}
		result, err := s.failureSummaryLocked(ctx, query)
		s.mu.RUnlock()
		s.failureQueries.finish(key, flight, result, err)
		return cloneFailureSummary(result), err
	}
}

func (s *RequestStatistics) failureSummaryLocked(ctx context.Context, query FailureSummaryQuery) (FailureSummary, error) {
	var counts usageFailureCounts
	buckets, step := s.bucketsForRangeLocked(query.TimeRange, time.Now().UTC())
	unfiltered := query.API == "" && query.Model == "" && query.Source == ""
	scanned := 0
	for key, bucket := range buckets {
		if err := ctx.Err(); err != nil {
			return FailureSummary{}, err
		}
		start := time.Unix(key, 0).UTC()
		end := start.Add(step)
		if !query.TimeRange.From.IsZero() && !end.After(query.TimeRange.From) || !query.TimeRange.To.IsZero() && !start.Before(query.TimeRange.To) {
			continue
		}
		if unfiltered && (query.TimeRange.From.IsZero() || !start.Before(query.TimeRange.From)) && (query.TimeRange.To.IsZero() || !end.After(query.TimeRange.To)) {
			if err := counts.merge(ctx, &bucket.failures); err != nil {
				return FailureSummary{}, err
			}
			continue
		}
		for _, id := range bucket.detailIDs {
			scanned++
			if scanned%usageQueryBatchSize == 0 {
				if err := ctx.Err(); err != nil {
					return FailureSummary{}, err
				}
			}
			detail, location, ok := s.detailForIDLocked(id)
			if !ok || !detail.Failed || !query.TimeRange.contains(detail.Timestamp) || query.API != "" && query.API != location.API || query.Model != "" && query.Model != location.Model || query.Source != "" && query.Source != strings.TrimSpace(detail.Source) {
				continue
			}
			counts.add(location.Model, detail, 1)
		}
	}
	return counts.snapshotContext(ctx)
}
