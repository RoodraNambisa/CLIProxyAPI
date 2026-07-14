package usage

import (
	"math"
	"testing"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

func TestCostsUsesConfiguredTokenFormula(t *testing.T) {
	stats := NewRequestStatistics()
	base := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	recordCostTestUsage(stats, base.Add(time.Minute), "api-a", "GPT-5.4", false, TokenStats{
		InputTokens:         1000,
		OutputTokens:        500,
		ReasoningTokens:     100,
		CachedTokens:        200,
		CacheCreationTokens: 50,
		TotalTokens:         1850,
	})

	result := stats.Costs(CostQuery{
		TimeRange: TimeRange{From: base, To: base.Add(time.Hour)},
		Bucket:    BucketHour,
		Prices: map[string]ModelPrice{"gpt-5.4": {
			InputPerMillion:       2,
			OutputPerMillion:      10,
			CachedInputPerMillion: 0.5,
		}},
	})
	if result.Total.AmountMicros != 6700 || result.Total.Amount != 0.0067 {
		t.Fatalf("total = %+v, want 6700 micros", result.Total)
	}
	if len(result.ByModel) != 1 || !result.ByModel[0].Priced || result.ByModel[0].Cost.AmountMicros != 6700 {
		t.Fatalf("by_model = %+v", result.ByModel)
	}
	if len(result.ByAPI) != 1 || result.ByAPI[0].Cost.AmountMicros != 6700 {
		t.Fatalf("by_api = %+v", result.ByAPI)
	}
	if len(result.Series) != 1 || result.Series[0].Cost.AmountMicros != 6700 {
		t.Fatalf("series = %+v", result.Series)
	}
	if result.Tokens.ReasoningTokens != 100 || result.Tokens.CacheCreationTokens != 50 {
		t.Fatalf("tokens = %+v", result.Tokens)
	}
}

func TestCostsReportsCoverageAndUnpricedModels(t *testing.T) {
	stats := NewRequestStatistics()
	base := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	recordCostTestUsage(stats, base.Add(time.Minute), "api-a", "model-a", false, TokenStats{InputTokens: 100, TotalTokens: 100})
	recordCostTestUsage(stats, base.Add(2*time.Minute), "api-a", "model-b", true, TokenStats{InputTokens: 300, TotalTokens: 300})

	result := stats.Costs(CostQuery{
		TimeRange: TimeRange{From: base, To: base.Add(time.Hour)},
		Bucket:    BucketHour,
		Prices:    map[string]ModelPrice{"model-a": {InputPerMillion: 1}},
	})
	if result.Total.AmountMicros != 100 {
		t.Fatalf("total micros = %d, want 100", result.Total.AmountMicros)
	}
	if result.Coverage.TotalRequests != 2 || result.Coverage.PricedRequests != 1 || result.Coverage.RequestCoverage == nil || *result.Coverage.RequestCoverage != 0.5 {
		t.Fatalf("request coverage = %+v", result.Coverage)
	}
	if result.Coverage.TotalTokens != 400 || result.Coverage.PricedTokens != 100 || result.Coverage.TokenCoverage == nil || *result.Coverage.TokenCoverage != 0.25 {
		t.Fatalf("token coverage = %+v", result.Coverage)
	}
	if len(result.UnpricedModels) != 1 || result.UnpricedModels[0].Model != "model-b" || result.UnpricedModels[0].FailureCount != 1 {
		t.Fatalf("unpriced models = %+v", result.UnpricedModels)
	}
	if len(result.ByAPI) != 1 || result.ByAPI[0].Coverage.PricedTokens != 100 {
		t.Fatalf("api coverage = %+v", result.ByAPI)
	}
}

func TestCostsReportsPricedLegacyUsageWithoutBreakdownAsUncalculated(t *testing.T) {
	stats := NewRequestStatistics()
	base := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	recordCostTestUsage(stats, base.Add(time.Minute), "api-a", "model-a", false, TokenStats{TotalTokens: 100})
	recordCostTestUsage(stats, base.Add(2*time.Minute), "api-a", "model-a", false, TokenStats{InputTokens: 50, TotalTokens: 50})

	result := stats.Costs(CostQuery{
		TimeRange: TimeRange{From: base, To: base.Add(time.Hour)},
		Bucket:    BucketHour,
		Prices:    map[string]ModelPrice{"model-a": {InputPerMillion: 1}},
	})
	if result.Total.AmountMicros != 50 {
		t.Fatalf("total micros = %d, want 50 from the calculable request", result.Total.AmountMicros)
	}
	if result.Coverage.PricedRequests != 2 || result.Coverage.CalculatedRequests != 1 {
		t.Fatalf("request coverage = %+v", result.Coverage)
	}
	if result.Coverage.PricedTokens != 150 || result.Coverage.CalculatedTokens != 50 {
		t.Fatalf("token coverage = %+v", result.Coverage)
	}
	if result.Coverage.CalculationRequestCoverage == nil || *result.Coverage.CalculationRequestCoverage != 0.5 {
		t.Fatalf("calculation request coverage = %+v", result.Coverage.CalculationRequestCoverage)
	}
	if len(result.UncalculatedModels) != 1 || result.UncalculatedModels[0].Model != "model-a" || result.UncalculatedModels[0].Requests != 1 || result.UncalculatedModels[0].TotalTokens != 100 {
		t.Fatalf("uncalculated models = %+v", result.UncalculatedModels)
	}
	if len(result.ByModel) != 1 || result.ByModel[0].CalculatedRequests != 1 || result.ByModel[0].CalculatedTokens != 50 {
		t.Fatalf("by_model = %+v", result.ByModel)
	}
}

func TestCostsUsesRangeAndContinuousBuckets(t *testing.T) {
	stats := NewRequestStatistics()
	base := time.Now().UTC().Truncate(3 * time.Hour).Add(-3 * time.Hour)
	recordCostTestUsage(stats, base.Add(time.Minute), "api-a", "model-a", false, TokenStats{InputTokens: 10, TotalTokens: 10})
	recordCostTestUsage(stats, base.Add(2*time.Hour+time.Minute), "api-b", "model-a", false, TokenStats{InputTokens: 30, TotalTokens: 30})

	result := stats.Costs(CostQuery{
		TimeRange: TimeRange{From: base, To: base.Add(2 * time.Hour)},
		Bucket:    BucketHour,
		Prices:    map[string]ModelPrice{"model-a": {InputPerMillion: 1}},
	})
	if result.Total.AmountMicros != 10 {
		t.Fatalf("total micros = %d, want first request only", result.Total.AmountMicros)
	}
	if len(result.Series) != 2 || result.Series[0].Requests != 1 || result.Series[1].Requests != 0 {
		t.Fatalf("series = %+v, want one used and one empty bucket", result.Series)
	}
}

func TestModelCostMicrosSaturatesOverflow(t *testing.T) {
	got := modelCostMicros(TokenStats{InputTokens: math.MaxInt64}, ModelPrice{InputPerMillion: math.MaxFloat64})
	if got != math.MaxInt64 {
		t.Fatalf("modelCostMicros() = %d, want MaxInt64", got)
	}
}

func TestModelCostMicrosClampsNonCachedInputAtZero(t *testing.T) {
	got := modelCostMicros(
		TokenStats{InputTokens: 100, CachedTokens: 200},
		ModelPrice{InputPerMillion: 10, CachedInputPerMillion: 1},
	)
	if got != 200 {
		t.Fatalf("modelCostMicros() = %d, want cached cost only", got)
	}
}

func TestZeroPriceStillCountsAsPricedCoverage(t *testing.T) {
	stats := &costAggregateStats{TotalRequests: 1, TotalTokens: 10, CalculableRequests: 1, CalculableTokens: 10, NonCachedInput: 10, Tokens: TokenStats{InputTokens: 10, TotalTokens: 10}}
	micros, coverage := aggregateModelCosts(map[string]*costAggregateStats{"model-a": stats}, map[string]ModelPrice{"model-a": {}})
	if micros != 0 || coverage.PricedRequests != 1 || coverage.CalculatedRequests != 1 || coverage.PricedTokens != 10 || coverage.CalculatedTokens != 10 || coverage.RequestCoverage == nil || *coverage.RequestCoverage != 1 {
		t.Fatalf("zero-price coverage = micros=%d coverage=%+v", micros, coverage)
	}
}

func TestCostsPreservesPerRequestNonCachedInputBasis(t *testing.T) {
	stats := NewRequestStatistics()
	base := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	recordCostTestUsage(stats, base.Add(time.Minute), "api-a", "model-a", false, TokenStats{CachedTokens: 100, TotalTokens: 100})
	recordCostTestUsage(stats, base.Add(2*time.Minute), "api-a", "model-a", false, TokenStats{InputTokens: 100, TotalTokens: 100})

	result := stats.Costs(CostQuery{
		TimeRange: TimeRange{From: base, To: base.Add(time.Hour)},
		Bucket:    BucketHour,
		Prices: map[string]ModelPrice{"model-a": {
			InputPerMillion:       10,
			CachedInputPerMillion: 1,
		}},
	})
	if result.Total.AmountMicros != 1100 {
		t.Fatalf("total micros = %d, want 1100 from per-request billing bases", result.Total.AmountMicros)
	}
}

func TestNormaliseTokenStatsUsesInclusiveParentFallbacks(t *testing.T) {
	tokens := normaliseTokenStats(TokenStats{
		InputTokens:         1,
		ReasoningTokens:     5,
		CachedTokens:        100,
		CacheCreationTokens: 80,
	})
	if tokens.TotalTokens != 105 {
		t.Fatalf("total tokens = %d, want max input subtype 100 plus reasoning 5", tokens.TotalTokens)
	}
}

func TestCostsReconcilesGroupedRoundingToTotal(t *testing.T) {
	stats := NewRequestStatistics()
	base := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
	recordCostTestUsage(stats, base.Add(time.Minute), "api-a", "model-a", false, TokenStats{InputTokens: 1, TotalTokens: 1})
	recordCostTestUsage(stats, base.Add(time.Hour+time.Minute), "api-b", "model-a", false, TokenStats{InputTokens: 1, TotalTokens: 1})

	result := stats.Costs(CostQuery{
		TimeRange: TimeRange{From: base, To: base.Add(2 * time.Hour)},
		Bucket:    BucketHour,
		Prices:    map[string]ModelPrice{"model-a": {InputPerMillion: 0.6}},
	})
	if result.Total.AmountMicros != 1 {
		t.Fatalf("total micros = %d, want 1", result.Total.AmountMicros)
	}
	var apiMicros, apiAdjustments int64
	for _, entry := range result.ByAPI {
		apiMicros += entry.Cost.AmountMicros
		apiAdjustments += entry.RoundingAdjustmentMicros
	}
	if apiMicros != result.Total.AmountMicros || apiAdjustments != -1 {
		t.Fatalf("API rounding = sum %d adjustments %d, want total %d and adjustment -1", apiMicros, apiAdjustments, result.Total.AmountMicros)
	}
	var seriesMicros, seriesAdjustments int64
	for _, entry := range result.Series {
		seriesMicros += entry.Cost.AmountMicros
		seriesAdjustments += entry.RoundingAdjustmentMicros
	}
	if seriesMicros != result.Total.AmountMicros || seriesAdjustments != -1 {
		t.Fatalf("series rounding = sum %d adjustments %d, want total %d and adjustment -1", seriesMicros, seriesAdjustments, result.Total.AmountMicros)
	}
}

func TestCostsDoesNotAssignPositiveRoundingToEmptyBucket(t *testing.T) {
	stats := NewRequestStatistics()
	base := time.Now().UTC().Truncate(time.Hour).Add(-3 * time.Hour)
	recordCostTestUsage(stats, base.Add(time.Minute), "api-a", "model-a", false, TokenStats{InputTokens: 1, TotalTokens: 1})
	recordCostTestUsage(stats, base.Add(2*time.Hour+time.Minute), "api-a", "model-a", false, TokenStats{InputTokens: 1, TotalTokens: 1})

	result := stats.Costs(CostQuery{
		TimeRange: TimeRange{From: base, To: base.Add(3 * time.Hour)},
		Bucket:    BucketHour,
		Prices:    map[string]ModelPrice{"model-a": {InputPerMillion: 0.4}},
	})
	if len(result.Series) != 3 || result.Total.AmountMicros != 1 {
		t.Fatalf("series=%+v total=%+v", result.Series, result.Total)
	}
	if result.Series[1].Requests != 0 || result.Series[1].Cost.AmountMicros != 0 || result.Series[1].RoundingAdjustmentMicros != 0 {
		t.Fatalf("empty bucket received rounding adjustment: %+v", result.Series[1])
	}
	if result.Series[0].Cost.AmountMicros+result.Series[2].Cost.AmountMicros != 1 || result.Series[0].RoundingAdjustmentMicros+result.Series[2].RoundingAdjustmentMicros != 1 {
		t.Fatalf("used buckets did not receive rounding adjustment: %+v", result.Series)
	}
}

func TestAggregateStatsSaturatesCountersAndTokens(t *testing.T) {
	stats := aggregateStats{
		TotalRequests:      math.MaxInt64,
		TotalTokens:        math.MaxInt64 - 1,
		CalculableRequests: math.MaxInt64,
		CalculableTokens:   math.MaxInt64 - 1,
		NonCachedInput:     math.MaxInt64 - 1,
		Tokens:             TokenStats{InputTokens: math.MaxInt64 - 1, TotalTokens: math.MaxInt64 - 1},
	}
	stats.merge(aggregateStats{
		TotalRequests:      1,
		TotalTokens:        10,
		CalculableRequests: 1,
		CalculableTokens:   10,
		NonCachedInput:     10,
		Tokens:             TokenStats{InputTokens: 10, TotalTokens: 10},
	})
	if stats.TotalRequests != math.MaxInt64 || stats.TotalTokens != math.MaxInt64 || stats.CalculableRequests != math.MaxInt64 || stats.CalculableTokens != math.MaxInt64 || stats.NonCachedInput != math.MaxInt64 {
		t.Fatalf("saturated counters = %+v", stats)
	}
	if stats.Tokens.InputTokens != math.MaxInt64 || stats.Tokens.TotalTokens != math.MaxInt64 {
		t.Fatalf("saturated tokens = %+v", stats.Tokens)
	}
}

func TestRecordSaturatesTopLevelAndLegacyCounters(t *testing.T) {
	stats := NewRequestStatistics()
	timestamp := time.Now().UTC().Truncate(time.Second)
	dayKey := timestamp.Format("2006-01-02")
	hourKey := timestamp.Hour()
	legacyKey := legacyHourKey{StartUnix: timestamp.Truncate(time.Hour).Unix(), Day: dayKey, Hour: hourKey}
	stats.totalRequests = math.MaxInt64
	stats.successCount = math.MaxInt64
	stats.totalTokens = math.MaxInt64 - 1
	stats.requestsByDay[dayKey] = math.MaxInt64
	stats.requestsByHour[hourKey] = math.MaxInt64
	stats.tokensByDay[dayKey] = math.MaxInt64 - 1
	stats.tokensByHour[hourKey] = math.MaxInt64 - 1
	stats.legacyHourBuckets[legacyKey] = &legacyHourStats{Requests: math.MaxInt64, Tokens: math.MaxInt64 - 1}

	recordCostTestUsage(stats, timestamp, "api-a", "model-a", false, TokenStats{InputTokens: 10, TotalTokens: 10})
	if stats.totalRequests != math.MaxInt64 || stats.successCount != math.MaxInt64 || stats.totalTokens != math.MaxInt64 {
		t.Fatalf("top-level counters requests=%d success=%d tokens=%d", stats.totalRequests, stats.successCount, stats.totalTokens)
	}
	if stats.requestsByDay[dayKey] != math.MaxInt64 || stats.requestsByHour[hourKey] != math.MaxInt64 || stats.tokensByDay[dayKey] != math.MaxInt64 || stats.tokensByHour[hourKey] != math.MaxInt64 {
		t.Fatalf("time counters requests=%v/%v tokens=%v/%v", stats.requestsByDay, stats.requestsByHour, stats.tokensByDay, stats.tokensByHour)
	}
	if bucket := stats.legacyHourBuckets[legacyKey]; bucket.Requests != math.MaxInt64 || bucket.Tokens != math.MaxInt64 {
		t.Fatalf("legacy bucket = %+v", bucket)
	}
}

func TestReconcileCostAmountsRedistributesSaturatedGroups(t *testing.T) {
	amounts := []int64{math.MaxInt64, math.MaxInt64}
	adjustments := []int64{0, 0}
	reconcileCostAmounts(math.MaxInt64, len(amounts), func(index int) int64 {
		return amounts[index]
	}, func(int) int64 {
		return 1
	}, func(index int, amount, adjustment int64) {
		amounts[index] = amount
		adjustments[index] = adjustment
	})
	if amounts[0] != math.MaxInt64 || amounts[1] != 0 || adjustments[1] != -math.MaxInt64 {
		t.Fatalf("amounts=%v adjustments=%v", amounts, adjustments)
	}
}

func TestCachedOnlyUsageIsCalculable(t *testing.T) {
	stats := aggregateStats{}
	stats.addDetail(RequestDetail{Tokens: TokenStats{CachedTokens: 100, TotalTokens: 100}})
	if stats.CalculableRequests != 1 || stats.CalculableTokens != 100 {
		t.Fatalf("calculable cached-only stats = %+v", stats)
	}
}

func TestCostsCapsSeriesBuckets(t *testing.T) {
	stats := NewRequestStatistics()
	to := time.Now().UTC().Truncate(time.Hour)
	from := to.Add(-1500 * time.Hour)
	recordCostTestUsage(stats, from.Add(time.Minute), "api-a", "model-a", false, TokenStats{InputTokens: 1, TotalTokens: 1})
	recordCostTestUsage(stats, to.Add(-time.Hour+time.Minute), "api-a", "model-a", false, TokenStats{InputTokens: 1, TotalTokens: 1})

	result := stats.Costs(CostQuery{
		TimeRange: TimeRange{From: from, To: to},
		Bucket:    BucketHour,
		Prices:    map[string]ModelPrice{"model-a": {InputPerMillion: 1}},
	})
	if !result.Truncated || len(result.Series) != maxUsageCostBuckets {
		t.Fatalf("truncated=%t series=%d, want true and %d", result.Truncated, len(result.Series), maxUsageCostBuckets)
	}
}

func TestCostsOnEmptyStatisticsHasNoSyntheticRange(t *testing.T) {
	result := NewRequestStatistics().Costs(CostQuery{})
	if result.From != nil || result.To != nil || len(result.Series) != 0 || result.Total.AmountMicros != 0 {
		t.Fatalf("empty cost result = %+v", result)
	}
}

func recordCostTestUsage(stats *RequestStatistics, timestamp time.Time, apiName, model string, failed bool, tokens TokenStats) {
	stats.Record(nil, coreusage.Record{
		APIKey:      apiName,
		Model:       model,
		AuthIndex:   "auth-a",
		Source:      "source-a",
		RequestedAt: timestamp,
		Failed:      failed,
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
