package usage

import (
	"context"
	"testing"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

func TestAggregateRangeUsesExactBoundaryDetails(t *testing.T) {
	stats := NewRequestStatistics()
	base := time.Now().UTC().Truncate(time.Minute).Add(-10 * time.Minute)
	recordAggregateTestUsage(stats, base.Add(10*time.Second), "api-a", "model-a", "auth-a", "source-a", 10, false)
	recordAggregateTestUsage(stats, base.Add(time.Minute+10*time.Second), "api-a", "model-a", "auth-a", "source-a", 20, true)
	recordAggregateTestUsage(stats, base.Add(2*time.Minute+10*time.Second), "api-b", "model-b", "auth-b", "source-b", 30, false)

	stats.mu.RLock()
	result := stats.aggregateRangeLocked(TimeRange{
		From: base.Add(30 * time.Second),
		To:   base.Add(2*time.Minute + 5*time.Second),
	})
	stats.mu.RUnlock()

	if result.Total.TotalRequests != 1 || result.Total.SuccessCount != 0 || result.Total.FailureCount != 1 || result.Total.TotalTokens != 20 {
		t.Fatalf("range total = %+v, want only middle failed request", result.Total)
	}
	if result.APIs["api-a"].TotalRequests != 1 || result.Models["model-a"].TotalTokens != 20 {
		t.Fatalf("range dimensions = APIs=%+v models=%+v", result.APIs, result.Models)
	}
	if result.Auths["auth-a"].FailureCount != 1 || result.Sources["source-a"].TotalRequests != 1 {
		t.Fatalf("range auth/source = auths=%+v sources=%+v", result.Auths, result.Sources)
	}
	if result.AuthSources["auth-a"]["source-a"].FailureCount != 1 {
		t.Fatalf("range auth/source cross aggregate = %+v", result.AuthSources)
	}
}

func TestRealtimeAggregateRetentionLayers(t *testing.T) {
	stats := NewRequestStatistics()
	now := time.Now().UTC()
	recent := now.Add(-time.Hour)
	oldMinute := now.Add(-9 * 24 * time.Hour)
	oldHour := now.Add(-401 * 24 * time.Hour)
	recordAggregateTestUsage(stats, recent, "api-a", "model-a", "auth-a", "source-a", 10, false)
	recordAggregateTestUsage(stats, oldMinute, "api-a", "model-a", "auth-a", "source-a", 20, false)
	recordAggregateTestUsage(stats, oldHour, "api-a", "model-a", "auth-a", "source-a", 30, false)

	stats.mu.RLock()
	defer stats.mu.RUnlock()
	if _, ok := stats.minuteBuckets[recent.Truncate(time.Minute).Unix()]; !ok {
		t.Fatal("recent minute bucket missing")
	}
	if _, ok := stats.minuteBuckets[oldMinute.Truncate(time.Minute).Unix()]; ok {
		t.Fatal("minute bucket older than eight days was retained")
	}
	if _, ok := stats.hourBuckets[oldMinute.Truncate(time.Hour).Unix()]; !ok {
		t.Fatal("hour bucket within 400 days missing")
	}
	if _, ok := stats.hourBuckets[oldHour.Truncate(time.Hour).Unix()]; ok {
		t.Fatal("hour bucket older than 400 days was retained")
	}
	for _, timestamp := range []time.Time{recent, oldMinute, oldHour} {
		if _, ok := stats.dayBuckets[truncateAggregateTime(timestamp, 24*time.Hour).Unix()]; !ok {
			t.Fatalf("day bucket missing for %s", timestamp)
		}
	}
}

func TestFutureUsageOnlyEntersPermanentDayAggregation(t *testing.T) {
	stats := NewRequestStatistics()
	timestamp := time.Now().UTC().Add(24 * time.Hour)
	recordAggregateTestUsage(stats, timestamp, "api-a", "model-a", "auth-a", "source-a", 10, false)

	stats.mu.RLock()
	defer stats.mu.RUnlock()
	if _, ok := stats.minuteBuckets[timestamp.Truncate(time.Minute).Unix()]; ok {
		t.Fatal("future request was retained in minute aggregation")
	}
	if _, ok := stats.hourBuckets[timestamp.Truncate(time.Hour).Unix()]; ok {
		t.Fatal("future request was retained in hour aggregation")
	}
	if _, ok := stats.dayBuckets[truncateAggregateTime(timestamp, 24*time.Hour).Unix()]; !ok {
		t.Fatal("future request missing from permanent day aggregation")
	}
}

func TestFutureUsageQueriesFallBackToPermanentDayAggregation(t *testing.T) {
	stats := NewRequestStatistics()
	now := time.Now().UTC()
	recent := now.Add(-time.Minute)
	future := now.Add(20 * 365 * 24 * time.Hour)
	recordAggregateTestUsage(stats, recent, "api-a", "model-a", "auth-a", "source-a", 10, false)
	recordAggregateTestUsage(stats, future, "api-a", "model-a", "auth-a", "source-a", 20, false)

	futureResult := stats.SummaryForRange(TimeRange{
		From: future.Truncate(time.Minute),
		To:   future.Truncate(time.Minute).Add(time.Minute),
	})
	if futureResult.TotalRequests != 1 || futureResult.TotalTokens != 20 {
		t.Fatalf("future summary = %+v, want future request from day fallback", futureResult)
	}
	combined := stats.SummaryForRange(TimeRange{From: recent.Truncate(time.Minute)})
	if combined.TotalRequests != 2 || combined.TotalTokens != 30 {
		t.Fatalf("open-ended summary = %+v, want recent and future requests", combined)
	}

	stats.mu.RLock()
	_, stepAfterClockCatchup := stats.bucketsForRangeLocked(TimeRange{
		From: future.Truncate(time.Minute),
		To:   future.Truncate(time.Minute).Add(time.Minute),
	}, future.Add(time.Hour))
	stats.mu.RUnlock()
	if stepAfterClockCatchup != 24*time.Hour {
		t.Fatalf("bucket step after clock catch-up = %s, want permanent day fallback", stepAfterClockCatchup)
	}
}

func TestFutureDayFallbackKeepsNarrowRangeExact(t *testing.T) {
	stats := NewRequestStatistics()
	day := time.Now().UTC().Add(24 * time.Hour).Truncate(24 * time.Hour)
	recordAggregateTestUsage(stats, day.Add(10*time.Hour), "api-a", "model-a", "auth-a", "source-a", 10, false)
	recordAggregateTestUsage(stats, day.Add(20*time.Hour), "api-a", "model-a", "auth-a", "source-a", 20, false)

	result := stats.SummaryForRange(TimeRange{From: day.Add(20 * time.Hour), To: day.Add(21 * time.Hour)})
	if result.TotalRequests != 1 || result.TotalTokens != 20 {
		t.Fatalf("narrow future summary = %+v, want only the later request", result)
	}
}

func TestAggregateReadPrunesExpiredHighResolutionBuckets(t *testing.T) {
	stats := NewRequestStatistics()
	now := time.Now().UTC()
	stats.mu.Lock()
	stats.minuteBuckets[now.Add(-usageMinuteRetention-time.Hour).Truncate(time.Minute).Unix()] = newUsageAggregateBucket()
	stats.hourBuckets[now.Add(-usageHourRetention-time.Hour).Truncate(time.Hour).Unix()] = newUsageAggregateBucket()
	stats.lastPrunedMinute = 0
	stats.mu.Unlock()

	_ = stats.Summary()
	stats.mu.RLock()
	defer stats.mu.RUnlock()
	if len(stats.minuteBuckets) != 0 || len(stats.hourBuckets) != 0 {
		t.Fatalf("expired buckets remain after read: minute=%d hour=%d", len(stats.minuteBuckets), len(stats.hourBuckets))
	}
}

func TestAggregateReadSortsOutOfOrderDetailIndexOnce(t *testing.T) {
	stats := NewRequestStatistics()
	base := time.Now().UTC().Truncate(time.Minute).Add(-10 * time.Minute)
	recordAggregateTestUsage(stats, base.Add(2*time.Minute), "api-a", "model-a", "auth-a", "source-a", 20, false)
	recordAggregateTestUsage(stats, base.Add(time.Minute), "api-a", "model-a", "auth-a", "source-a", 10, false)
	stats.mu.RLock()
	dirtyBeforeRead := stats.detailIndexDirty
	stats.mu.RUnlock()
	if !dirtyBeforeRead {
		t.Fatal("out-of-order detail index was not marked dirty")
	}

	result := stats.SummaryForRange(TimeRange{From: base.Add(30 * time.Second), To: base.Add(3 * time.Minute)})
	if result.TotalRequests != 2 || result.TotalTokens != 30 {
		t.Fatalf("out-of-order summary = %+v", result)
	}
	stats.mu.RLock()
	defer stats.mu.RUnlock()
	if stats.detailIndexDirty || stats.detailIndex[0].Timestamp.After(stats.detailIndex[1].Timestamp) {
		t.Fatalf("detail index was not sorted before aggregate read: %+v", stats.detailIndex)
	}
}

func TestAggregateRangeAcrossMinuteRetentionUsesHourlyLayer(t *testing.T) {
	stats := NewRequestStatistics()
	now := time.Now().UTC().Truncate(time.Hour)
	recordAggregateTestUsage(stats, now.Add(-9*24*time.Hour), "api-a", "model-a", "auth-a", "source-a", 10, false)
	recordAggregateTestUsage(stats, now.Add(-7*24*time.Hour), "api-a", "model-a", "auth-a", "source-a", 20, false)

	result := stats.SummaryForRange(TimeRange{From: now.Add(-10 * 24 * time.Hour), To: now})
	if result.TotalRequests != 2 || result.TotalTokens != 30 {
		t.Fatalf("cross-retention summary = %+v, want both hourly-backed records", result)
	}
}

func TestMergeSnapshotRebuildsRealtimeAggregatesAndIndex(t *testing.T) {
	stats := NewRequestStatistics()
	timestamp := time.Now().UTC().Add(-time.Hour)
	snapshot := StatisticsSnapshot{
		APIs: map[string]APISnapshot{
			"api-a": {
				Models: map[string]ModelSnapshot{
					"model-a": {
						Details: []RequestDetail{{
							Timestamp: timestamp,
							AuthIndex: "auth-a",
							Source:    "source-a",
							Tokens:    TokenStats{InputTokens: 7, TotalTokens: 7},
						}},
					},
				},
			},
		},
	}
	if result := stats.MergeSnapshot(snapshot); result.Added != 1 {
		t.Fatalf("MergeSnapshot() = %+v, want added=1", result)
	}

	stats.mu.RLock()
	defer stats.mu.RUnlock()
	if len(stats.detailIndex) != 1 || stats.detailIndex[0].API != "api-a" || stats.detailIndex[0].Model != "model-a" {
		t.Fatalf("detail index = %+v", stats.detailIndex)
	}
	if stats.tokens.InputTokens != 7 || stats.models["model-a"].TotalTokens != 7 || stats.sources["source-a"].TotalRequests != 1 {
		t.Fatalf("all-time aggregates not rebuilt: tokens=%+v models=%+v sources=%+v", stats.tokens, stats.models, stats.sources)
	}
	hour := stats.hourBuckets[timestamp.Truncate(time.Hour).Unix()]
	if hour == nil || hour.Auths["auth-a"].TotalRequests != 1 || hour.APIModels["api-a"]["model-a"].TotalTokens != 7 {
		t.Fatalf("hour aggregate = %+v", hour)
	}
}

func TestAlignedSummaryUsesRealtimeBuckets(t *testing.T) {
	stats := NewRequestStatistics()
	base := time.Now().UTC().Truncate(time.Minute).Add(-5 * time.Minute)
	recordAggregateTestUsage(stats, base.Add(10*time.Second), "api-a", "model-a", "auth-a", "source-a", 10, false)
	recordAggregateTestUsage(stats, base.Add(time.Minute+10*time.Second), "api-a", "model-a", "auth-a", "source-a", 20, true)

	stats.mu.Lock()
	stats.detailIndex = nil
	stats.mu.Unlock()
	result := stats.SummaryForRange(TimeRange{From: base, To: base.Add(2 * time.Minute)})
	if result.TotalRequests != 2 || result.SuccessCount != 1 || result.FailureCount != 1 || result.TotalTokens != 30 {
		t.Fatalf("summary = %+v, want realtime bucket totals", result)
	}
	model := result.APIs["api-a"].Models["model-a"]
	if model.TotalRequests != 2 || model.SuccessCount != 1 || model.FailureCount != 1 || model.Tokens.InputTokens != 30 {
		t.Fatalf("model summary = %+v, want realtime bucket breakdown", model)
	}
}

func TestLegacyRangeSummaryPreservesLocalDayAndHourBeyondHourlyRetention(t *testing.T) {
	stats := NewRequestStatistics()
	location := time.FixedZone("test-offset", 5*60*60+30*60)
	timestamp := time.Now().In(location).Add(-500 * 24 * time.Hour)
	timestamp = time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), 9, 15, 0, 0, location)
	recordAggregateTestUsage(stats, timestamp, "api-a", "model-a", "auth-a", "source-a", 17, false)

	stats.mu.Lock()
	stats.detailIndex = nil
	stats.mu.Unlock()
	result := stats.SummaryForRange(TimeRange{
		From: timestamp.Add(-time.Minute),
		To:   timestamp.Add(time.Minute),
	})
	day := timestamp.Format("2006-01-02")
	if result.RequestsByDay[day] != 1 || result.TokensByDay[day] != 17 {
		t.Fatalf("legacy day summary = requests=%v tokens=%v", result.RequestsByDay, result.TokensByDay)
	}
	if result.RequestsByHour["09"] != 1 || result.TokensByHour["09"] != 17 {
		t.Fatalf("legacy hour summary = requests=%v tokens=%v", result.RequestsByHour, result.TokensByHour)
	}
}

func recordAggregateTestUsage(stats *RequestStatistics, timestamp time.Time, apiName, modelName, authIndex, source string, tokens int64, failed bool) {
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      apiName,
		Model:       modelName,
		AuthIndex:   authIndex,
		Source:      source,
		RequestedAt: timestamp,
		Failed:      failed,
		Detail: coreusage.Detail{
			InputTokens: tokens,
			TotalTokens: tokens,
		},
	})
}
