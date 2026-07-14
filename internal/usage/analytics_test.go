package usage

import (
	"testing"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

func TestHealthReturnsContinuousNoDataBuckets(t *testing.T) {
	stats := NewRequestStatistics()
	base := time.Now().UTC().Truncate(15 * time.Minute).Add(-30 * time.Minute)
	recordAggregateTestUsage(stats, base.Add(time.Minute), "api-a", "model-a", "auth-a", "source-a", 10, false)

	result := stats.healthAt(HealthQuery{
		TimeRange: TimeRange{From: base, To: base.Add(30 * time.Minute)},
		Bucket:    Bucket15Min,
		GroupBy:   GroupByNone,
	}, base.Add(30*time.Minute))
	if len(result.Items) != 2 {
		t.Fatalf("health items = %d, want 2: %+v", len(result.Items), result.Items)
	}
	if result.Items[0].State != HealthStateHealthy || result.Items[0].SuccessRate == nil || *result.Items[0].SuccessRate != 1 {
		t.Fatalf("first health item = %+v, want healthy rate 1", result.Items[0])
	}
	if result.Items[1].State != HealthStateNoData || result.Items[1].SuccessRate != nil || result.Items[1].Requests != 0 {
		t.Fatalf("second health item = %+v, want no_data with null rate", result.Items[1])
	}
}

func TestHealthFiltersAndGroupsAuthIndexes(t *testing.T) {
	stats := NewRequestStatistics()
	base := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	recordAggregateTestUsage(stats, base.Add(time.Minute), "api-a", "model-a", "auth-a", "source-a", 10, false)
	recordAggregateTestUsage(stats, base.Add(2*time.Minute), "api-a", "model-a", "auth-b", "source-b", 20, true)
	stats.mu.Lock()
	stats.detailIndex = nil
	stats.mu.Unlock()

	result := stats.healthAt(HealthQuery{
		TimeRange:   TimeRange{From: base, To: base.Add(time.Hour)},
		Bucket:      BucketHour,
		GroupBy:     GroupByAuthIndex,
		AuthIndexes: []string{"auth-a", "missing-auth"},
	}, base.Add(time.Hour))
	if len(result.Items) != 2 {
		t.Fatalf("health auth items = %+v, want selected auth and no-data auth", result.Items)
	}
	if result.Items[0].Group != "auth-a" || result.Items[0].SuccessCount != 1 {
		t.Fatalf("first auth health = %+v", result.Items[0])
	}
	if result.Items[1].Group != "missing-auth" || result.Items[1].State != HealthStateNoData {
		t.Fatalf("second auth health = %+v", result.Items[1])
	}

	result = stats.healthAt(HealthQuery{
		TimeRange:   TimeRange{From: base, To: base.Add(time.Hour)},
		Bucket:      BucketHour,
		GroupBy:     GroupByAuthIndex,
		AuthIndexes: []string{"auth-a"},
		Sources:     []string{"source-b"},
	}, base.Add(time.Hour))
	if len(result.Items) != 1 || result.Items[0].Group != "auth-a" || result.Items[0].State != HealthStateNoData {
		t.Fatalf("cross-filtered health = %+v, want auth-a no_data", result.Items)
	}
}

func TestHealthKeepsDiscoveredAuthGroupsContinuous(t *testing.T) {
	stats := NewRequestStatistics()
	base := time.Now().UTC().Truncate(2 * time.Hour).Add(-2 * time.Hour)
	recordAggregateTestUsage(stats, base.Add(time.Minute), "api-a", "model-a", "auth-a", "source-a", 10, false)

	result := stats.healthAt(HealthQuery{
		TimeRange: TimeRange{From: base, To: base.Add(2 * time.Hour)},
		Bucket:    BucketHour,
		GroupBy:   GroupByAuthIndex,
	}, base.Add(2*time.Hour))
	if len(result.Items) != 2 {
		t.Fatalf("health items = %+v, want auth-a in both buckets", result.Items)
	}
	if result.Items[0].Group != "auth-a" || result.Items[0].State != HealthStateHealthy {
		t.Fatalf("first health item = %+v", result.Items[0])
	}
	if result.Items[1].Group != "auth-a" || result.Items[1].State != HealthStateNoData {
		t.Fatalf("second health item = %+v, want continuous no_data", result.Items[1])
	}
}

func TestHealthTreatsBlankRequestedGroupsAsDiscovery(t *testing.T) {
	stats := NewRequestStatistics()
	base := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	recordAggregateTestUsage(stats, base.Add(time.Minute), "api-a", "model-a", "auth-a", "source-a", 10, false)

	result := stats.healthAt(HealthQuery{
		TimeRange:   TimeRange{From: base, To: base.Add(time.Hour)},
		Bucket:      BucketHour,
		GroupBy:     GroupByAuthIndex,
		AuthIndexes: []string{" ", ""},
	}, base.Add(time.Hour))
	if len(result.Items) != 1 || result.Items[0].Group != "auth-a" {
		t.Fatalf("health items = %+v, want discovered auth-a", result.Items)
	}
}

func TestRatesUsesTrailingWindowAndMinuteSparkline(t *testing.T) {
	stats := NewRequestStatistics()
	now := time.Now().UTC().Truncate(time.Minute).Add(30 * time.Second)
	recordAggregateTestUsage(stats, now.Add(-29*time.Minute), "api-a", "model-a", "auth-a", "source-a", 300, false)
	recordAggregateTestUsage(stats, now.Add(-31*time.Minute), "api-a", "model-a", "auth-a", "source-a", 900, false)
	recordAggregateTestUsage(stats, now.Add(-10*time.Second), "api-a", "model-a", "auth-a", "source-a", 60, false)

	result := stats.ratesAt(RatesQuery{WindowMinutes: 30, SparklineMinutes: 60}, now)
	if result.RequestCount != 2 || result.TokenCount != 360 || result.RPM != float64(2)/30 || result.TPM != 12 {
		t.Fatalf("rates = %+v, want two requests and 360 tokens in trailing window", result)
	}
	if len(result.Items) != 60 {
		t.Fatalf("sparkline items = %d, want 60", len(result.Items))
	}
	last := result.Items[len(result.Items)-1]
	if last.Requests != 1 || last.TotalTokens != 60 || last.RPM != 1 || last.TPM != 60 {
		t.Fatalf("last sparkline item = %+v", last)
	}
}

func TestRatesReturnsFullSparklineAtMinuteBoundary(t *testing.T) {
	stats := NewRequestStatistics()
	now := time.Now().UTC().Truncate(time.Minute)
	result := stats.ratesAt(RatesQuery{SparklineMinutes: 60}, now)
	if len(result.Items) != 60 {
		t.Fatalf("sparkline items = %d, want 60 at exact minute boundary", len(result.Items))
	}
	if got := result.Items[len(result.Items)-1].Bucket; !got.Equal(now.Add(-time.Minute)) {
		t.Fatalf("last bucket = %s, want previous complete minute", got)
	}
}

func TestUsageAnalyticsRangeLimitKeepsLatestBuckets(t *testing.T) {
	to := time.Date(2026, 7, 14, 12, 34, 30, 0, time.UTC)
	timeRange, truncated := limitUsageRangeBuckets(TimeRange{From: to.Add(-20000 * time.Minute), To: to}, time.Minute, 10000)
	if !truncated {
		t.Fatal("range was not truncated")
	}
	firstBucket := truncateAggregateTime(timeRange.From, time.Minute)
	lastBucket := truncateAggregateTime(timeRange.To.Add(-time.Nanosecond), time.Minute)
	if got := int(lastBucket.Sub(firstBucket)/time.Minute) + 1; got != 10000 {
		t.Fatalf("retained buckets = %d, want 10000", got)
	}
}

func TestTokensForQueryGroupsCompleteBreakdown(t *testing.T) {
	stats := NewRequestStatistics()
	base := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
	recordAggregateTestUsageWithBreakdown(stats, base.Add(time.Minute), "model-a", TokenStats{
		InputTokens:         10,
		OutputTokens:        5,
		ReasoningTokens:     2,
		CachedTokens:        3,
		CacheCreationTokens: 4,
		TotalTokens:         24,
	})
	recordAggregateTestUsageWithBreakdown(stats, base.Add(time.Hour+time.Minute), "model-b", TokenStats{
		InputTokens:  20,
		OutputTokens: 10,
		TotalTokens:  30,
	})

	result := stats.TokensForQuery(TokenQuery{
		TimeRange: TimeRange{From: base, To: base.Add(2 * time.Hour)},
		Bucket:    BucketHour,
		GroupBy:   GroupByModel,
	})
	if len(result.Items) != 2 || result.TotalTokens != 54 {
		t.Fatalf("token result = %+v, want two model buckets and 54 tokens", result)
	}
	first := result.Items[0]
	if first.Group != "model-a" || first.Tokens.InputTokens != 10 || first.Tokens.ReasoningTokens != 2 || first.Tokens.CacheCreationTokens != 4 {
		t.Fatalf("first token item = %+v", first)
	}
	if result.Tokens.CachedTokens != 3 || result.Tokens.OutputTokens != 15 {
		t.Fatalf("token totals = %+v", result.Tokens)
	}
}

func TestTokensForQueryOnEmptyStatisticsHasNoSyntheticRange(t *testing.T) {
	result := NewRequestStatistics().TokensForQuery(TokenQuery{})
	if result.From != nil || result.To != nil || len(result.Items) != 0 || result.TotalTokens != 0 {
		t.Fatalf("empty token result = %+v", result)
	}
}

func TestAggregateEntryAppendHonorsItemLimit(t *testing.T) {
	aggregate := newUsageAggregateBucket()
	ensureAggregate(aggregate.Models, "model-a").addDetail(RequestDetail{Tokens: TokenStats{TotalTokens: 1}})
	ensureAggregate(aggregate.Models, "model-b").addDetail(RequestDetail{Tokens: TokenStats{TotalTokens: 1}})
	aggregate.Total.TotalRequests = 2

	series := []SeriesEntry{}
	if !appendSeriesAggregate(&series, time.Now().UTC(), GroupByModel, aggregate, 1) || len(series) != 1 {
		t.Fatalf("series limit result = truncated=%v items=%+v", len(series) == 1, series)
	}
	tokens := []TokenEntry{}
	if !appendTokenEntries(&tokens, time.Now().UTC(), GroupByModel, aggregate, 1) || len(tokens) != 1 {
		t.Fatalf("token limit result = truncated=%v items=%+v", len(tokens) == 1, tokens)
	}
}

func recordAggregateTestUsageWithBreakdown(stats *RequestStatistics, timestamp time.Time, model string, tokens TokenStats) {
	stats.Record(nil, coreusage.Record{
		APIKey:      "api-a",
		Model:       model,
		AuthIndex:   "auth-a",
		Source:      "source-a",
		RequestedAt: timestamp,
		Detail: coreusage.Detail{
			InputTokens:         tokens.InputTokens,
			OutputTokens:        tokens.OutputTokens,
			ReasoningTokens:     tokens.ReasoningTokens,
			CachedTokens:        tokens.CachedTokens,
			CacheCreationTokens: tokens.CacheCreationTokens,
			TotalTokens:         tokens.TotalTokens,
		},
	})
}
