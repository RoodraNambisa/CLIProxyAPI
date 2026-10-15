package usage

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func legacyFailureSummary(s *RequestStatistics, query FailureSummaryQuery) FailureSummary {
	result := FailureSummary{}
	codes, stages, models, sources, hours := map[string]int64{}, map[string]int64{}, map[string]int64{}, map[string]int64{}, map[string]int64{}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for api, a := range s.apis {
		if query.API != "" && api != query.API {
			continue
		}
		for model, m := range a.Models {
			if query.Model != "" && model != query.Model {
				continue
			}
			for _, d := range m.Details {
				if !d.Failed || !query.TimeRange.contains(d.Timestamp) || query.Source != "" && query.Source != d.Source {
					continue
				}
				result.Total++
				if d.Auxiliary {
					result.Auxiliary++
				} else {
					result.Main++
				}
				if d.CredentialSelected {
					result.Boundaries.CredentialSelected++
				}
				if d.UpstreamCommitted {
					result.Boundaries.UpstreamCommitted++
				}
				if d.AuthRequestSlotConsumed {
					result.Boundaries.AuthRequestSlotConsumed++
				}
				incrementFailureDimension(codes, d.ErrorCode, "unknown")
				incrementFailureDimension(stages, d.FailureStage, "unknown")
				incrementFailureDimension(models, model, "unknown")
				incrementFailureDimension(sources, d.Source, "unknown")
				incrementFailureDimension(hours, d.Timestamp.UTC().Truncate(time.Hour).Format(time.RFC3339), "unknown")
			}
		}
	}
	result.ByErrorCode = sortedFailureDimensionCounts(codes, result.Total)
	result.ByFailureStage = sortedFailureDimensionCounts(stages, result.Total)
	result.ByModel = sortedFailureDimensionCounts(models, result.Total)
	result.BySource = sortedFailureDimensionCounts(sources, result.Total)
	result.ByHour = sortedFailureDimensionCounts(hours, result.Total)
	return result
}

func assertFailureQueryParity(t *testing.T, s *RequestStatistics) {
	t.Helper()
	for _, q := range []FailureSummaryQuery{{}, {API: "api-1"}, {Model: "model-1"}, {Source: "source-1"}, {TimeRange: TimeRange{From: s.oldestAt.Add(time.Millisecond), To: s.newestAt}}, {API: "api-1", Model: "model-1", TimeRange: TimeRange{From: s.oldestAt, To: s.oldestAt.Add(time.Second)}}} {
		got := s.FailureSummary(q)
		got.AsOf = time.Time{}
		if want := legacyFailureSummary(s, q); !reflect.DeepEqual(got, want) {
			t.Fatalf("failure query %+v mismatch: got %+v want %+v", q, got, want)
		}
	}
}

func TestUsageFailureBucketsAndCacheFollowHistoryMutations(t *testing.T) {
	s := queryPerformanceFixture(3000)
	assertFailureQueryParity(t, s)
	first := s.FailureSummary(FailureSummaryQuery{})
	first.ByErrorCode[0].Count = -999
	assertFailureQueryParity(t, s)
	s.RemoveAuthIndexes([]string{"auth-3", "auth-4"})
	assertFailureQueryParity(t, s)
	s.PruneOldest(50)
	assertFailureQueryParity(t, s)
	s.PruneBefore(s.oldestAt.Add(time.Second))
	assertFailureQueryParity(t, s)
	s.Clear()
	assertFailureQueryParity(t, s)
	prepared := queryPerformanceFixture(100)
	if _, ok := s.ApplyPreparedRestore(prepared, s.HistoryGeneration()); !ok {
		t.Fatal("restore rejected")
	}
	assertFailureQueryParity(t, s)
	for i := 0; i < 100; i++ {
		s.FailureSummary(FailureSummaryQuery{TimeRange: TimeRange{From: s.oldestAt.Add(time.Duration(i) * time.Millisecond)}})
	}
	s.failureQueries.mu.Lock()
	defer s.failureQueries.mu.Unlock()
	if len(s.failureQueries.entries) > usageFailureQueryCacheLimit {
		t.Fatal("unbounded cache")
	}
}

func TestUsageQueriesCancelWhileBatchOrLockIsBlocked(t *testing.T) {
	for _, kind := range []string{"batch_send", "batch_barrier", "read_lock"} {
		t.Run(kind, func(t *testing.T) {
			s := NewRequestStatistics()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			entered := make(chan struct{})
			switch kind {
			case "batch_send":
				s.batch = &usageBatcher{queue: make(chan usageBatchItem)}
				close(entered)
			case "batch_barrier":
				s.batch = &usageBatcher{queue: make(chan usageBatchItem)}
				go func() { <-s.batch.queue; close(entered) }()
			case "read_lock":
				s.mu.Lock()
				defer s.mu.Unlock()
				close(entered)
			}
			done := make(chan error, 1)
			go func() { _, err := s.DetailsContext(ctx, DetailQuery{}); done <- err }()
			<-entered
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("error=%v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("query ignored cancellation")
			}
		})
	}
}

type cancelQueryAfterChecks struct {
	context.Context
	remaining atomic.Int64
}

func (c *cancelQueryAfterChecks) Err() error {
	if c.remaining.Add(-1) <= 0 {
		return context.Canceled
	}
	return nil
}

func TestUsageIndexCancellationPreservesEveryRecord(t *testing.T) {
	s := queryPerformanceFixture(5000)
	s.mu.Lock()
	slices.Reverse(s.detailIndex)
	s.detailIndexDirty = true
	s.detailIndexSorted = 0
	ctx := &cancelQueryAfterChecks{Context: context.Background()}
	ctx.remaining.Store(9)
	if err := s.sortDetailIndexContextLocked(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	seen := map[uint64]bool{}
	for _, ref := range s.detailIndex {
		if seen[ref.ID] {
			t.Fatal("duplicate index ID")
		}
		seen[ref.ID] = true
	}
	if len(seen) != len(s.detailLocations) {
		t.Fatal("record lost during canceled merge")
	}
	s.mu.Unlock()
	if got, want := s.Details(DetailQuery{Limit: 50}), legacyQueryDetails(s, DetailQuery{Limit: 50}); !reflect.DeepEqual(got, want) {
		t.Fatal("canceled sort changed query results")
	}
}

func TestUsageQueryOutOfOrderSameTimestampAndDeletion(t *testing.T) {
	s := NewRequestStatistics()
	base := time.Now().UTC()
	for i := 0; i < 3000; i++ {
		s.recordPreparedLocked(preparedUsageRecord{apiName: "api", modelName: "model", detail: RequestDetail{Timestamp: base.Add(time.Duration(i%5) * time.Second), AuthIndex: time.Duration(i).String(), Tokens: TokenStats{TotalTokens: int64(i)}}}, base)
	}
	for _, order := range []string{SortOrderAsc, SortOrderDesc} {
		for _, offset := range []int{0, 1, 60, 2800} {
			q := DetailQuery{SortOrder: order, Limit: 57, Offset: offset}
			if got, want := s.Details(q), legacyQueryDetails(s, q); !reflect.DeepEqual(got, want) {
				t.Fatalf("same timestamp pagination mismatch %s/%d", order, offset)
			}
		}
	}
	s.RemoveAuthIndexes([]string{"1ns", "2ns"})
	if got, want := s.Details(DetailQuery{}), legacyQueryDetails(s, DetailQuery{}); !reflect.DeepEqual(got, want) {
		t.Fatal("tombstones changed count/page")
	}
}

func TestUsageQueriesConcurrentWithWriters(t *testing.T) {
	s := queryPerformanceFixture(1000)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 40 {
				p, err := s.DetailsContext(t.Context(), DetailQuery{Limit: 30})
				if err != nil || p.Total < len(p.Items) {
					t.Error("invalid page")
				}
				f, err := s.FailureSummaryContext(t.Context(), FailureSummaryQuery{})
				if err != nil || f.Total != f.Main+f.Auxiliary {
					t.Error("inconsistent failure snapshot")
				}
			}
		})
	}
	wg.Go(func() {
		for i := 0; i < 100; i++ {
			s.mu.Lock()
			s.recordPreparedLocked(preparedUsageRecord{apiName: "live", modelName: "model", detail: RequestDetail{Timestamp: time.Now(), Failed: true}}, time.Now())
			s.mu.Unlock()
		}
	})
	wg.Wait()
	assertFailureQueryParity(t, s)
}
