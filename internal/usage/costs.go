package usage

import (
	"math"
	"sort"
	"strings"
	"time"
)

// CostCurrencyUSD is the currency used by usage price and cost responses.
const CostCurrencyUSD = "USD"

const maxUsageCostBuckets = 1000

// ModelPrice stores USD prices per one million tokens.
type ModelPrice struct {
	InputPerMillion       float64 `json:"input-per-million"`
	OutputPerMillion      float64 `json:"output-per-million"`
	CachedInputPerMillion float64 `json:"cached-input-per-million"`
}

// CostQuery controls a usage cost aggregation.
type CostQuery struct {
	TimeRange TimeRange
	Bucket    string
	Prices    map[string]ModelPrice
}

// Money is a USD amount represented both as integer micros and a display value.
type Money struct {
	Currency     string  `json:"currency"`
	AmountMicros int64   `json:"amount_micros"`
	Amount       float64 `json:"amount"`
}

// CostCoverage reports how much usage was covered by configured model prices.
type CostCoverage struct {
	TotalRequests              int64    `json:"total_requests"`
	PricedRequests             int64    `json:"priced_requests"`
	CalculatedRequests         int64    `json:"calculated_requests"`
	RequestCoverage            *float64 `json:"request_coverage"`
	CalculationRequestCoverage *float64 `json:"calculation_request_coverage"`
	TotalTokens                int64    `json:"total_tokens"`
	PricedTokens               int64    `json:"priced_tokens"`
	CalculatedTokens           int64    `json:"calculated_tokens"`
	TokenCoverage              *float64 `json:"token_coverage"`
	CalculationTokenCoverage   *float64 `json:"calculation_token_coverage"`
}

// CostResult contains server-side cost totals and breakdowns.
type CostResult struct {
	AsOf               time.Time                `json:"as_of"`
	From               *time.Time               `json:"from"`
	To                 *time.Time               `json:"to"`
	Bucket             string                   `json:"bucket"`
	Truncated          bool                     `json:"truncated"`
	Total              Money                    `json:"total"`
	TotalTokens        int64                    `json:"total_tokens"`
	Tokens             TokenStats               `json:"tokens"`
	Coverage           CostCoverage             `json:"coverage"`
	ByModel            []ModelCostEntry         `json:"by_model"`
	ByAPI              []APICostEntry           `json:"by_api"`
	Series             []CostSeriesEntry        `json:"series"`
	UnpricedModels     []UnpricedModelEntry     `json:"unpriced_models"`
	UncalculatedModels []UncalculatedModelEntry `json:"uncalculated_models"`
}

// ModelCostEntry contains one model's usage and cost.
type ModelCostEntry struct {
	Model              string     `json:"model"`
	Priced             bool       `json:"priced"`
	Requests           int64      `json:"requests"`
	SuccessCount       int64      `json:"success_count"`
	FailureCount       int64      `json:"failure_count"`
	TotalTokens        int64      `json:"total_tokens"`
	CalculatedRequests int64      `json:"calculated_requests"`
	CalculatedTokens   int64      `json:"calculated_tokens"`
	Tokens             TokenStats `json:"tokens"`
	Cost               Money      `json:"cost"`
}

// APICostEntry contains one API's usage, coverage, and cost.
type APICostEntry struct {
	API                      string       `json:"api"`
	Requests                 int64        `json:"requests"`
	SuccessCount             int64        `json:"success_count"`
	FailureCount             int64        `json:"failure_count"`
	TotalTokens              int64        `json:"total_tokens"`
	Tokens                   TokenStats   `json:"tokens"`
	Coverage                 CostCoverage `json:"coverage"`
	Cost                     Money        `json:"cost"`
	RoundingAdjustmentMicros int64        `json:"rounding_adjustment_micros"`
}

// CostSeriesEntry contains one time bucket's usage, coverage, and cost.
type CostSeriesEntry struct {
	Bucket                   time.Time    `json:"bucket"`
	Requests                 int64        `json:"requests"`
	SuccessCount             int64        `json:"success_count"`
	FailureCount             int64        `json:"failure_count"`
	TotalTokens              int64        `json:"total_tokens"`
	Tokens                   TokenStats   `json:"tokens"`
	Coverage                 CostCoverage `json:"coverage"`
	Cost                     Money        `json:"cost"`
	RoundingAdjustmentMicros int64        `json:"rounding_adjustment_micros"`
}

// UnpricedModelEntry describes usage excluded from cost totals.
type UnpricedModelEntry struct {
	Model        string     `json:"model"`
	Requests     int64      `json:"requests"`
	SuccessCount int64      `json:"success_count"`
	FailureCount int64      `json:"failure_count"`
	TotalTokens  int64      `json:"total_tokens"`
	Tokens       TokenStats `json:"tokens"`
}

// UncalculatedModelEntry describes priced usage whose legacy details lack a token breakdown.
type UncalculatedModelEntry struct {
	Model       string `json:"model"`
	Requests    int64  `json:"requests"`
	TotalTokens int64  `json:"total_tokens"`
}

// Costs uses realtime aggregates with indexed corrections for partial bucket edges.
func (s *RequestStatistics) Costs(query CostQuery) CostResult {
	query.Bucket = normalizeCostBucket(query.Bucket)
	result := CostResult{
		AsOf:               time.Now().UTC(),
		Bucket:             query.Bucket,
		Total:              newUSDMoney(0),
		ByModel:            []ModelCostEntry{},
		ByAPI:              []APICostEntry{},
		Series:             []CostSeriesEntry{},
		UnpricedModels:     []UnpricedModelEntry{},
		UncalculatedModels: []UncalculatedModelEntry{},
	}
	if s == nil {
		return result
	}

	s.pruneExpiredBuckets(result.AsOf)
	s.mu.RLock()
	timeRange := query.TimeRange
	if timeRange.From.IsZero() {
		timeRange.From = s.oldestAt
	}
	if timeRange.To.IsZero() && !s.newestAt.IsZero() {
		timeRange.To = s.newestAt.Add(time.Nanosecond)
	}
	timeRange = clampUsageRangeToBounds(timeRange, s.oldestAt, s.newestAt)
	step := usageBucketDuration(query.Bucket)
	timeRange, result.Truncated = limitUsageRangeBuckets(timeRange, step, maxUsageCostBuckets)
	result.From = usageTimePointer(timeRange.From)
	result.To = usageTimePointer(timeRange.To)
	if timeRange.From.IsZero() || timeRange.To.IsZero() || !timeRange.From.Before(timeRange.To) {
		s.mu.RUnlock()
		return result
	}

	overall := s.aggregateCostRangeLocked(timeRange, true)
	type seriesAggregate struct {
		bucket    time.Time
		aggregate *costRangeAggregate
	}
	seriesAggregates := make([]seriesAggregate, 0)
	for bucketStart := truncateAggregateTime(timeRange.From, step); bucketStart.Before(timeRange.To); bucketStart = bucketStart.Add(step) {
		bucketEnd := bucketStart.Add(step)
		interval := TimeRange{From: bucketStart, To: bucketEnd}
		if timeRange.From.After(interval.From) {
			interval.From = timeRange.From
		}
		if timeRange.To.Before(interval.To) {
			interval.To = timeRange.To
		}
		seriesAggregates = append(seriesAggregates, seriesAggregate{
			bucket:    bucketStart,
			aggregate: s.aggregateCostRangeLocked(interval, false),
		})
	}
	s.mu.RUnlock()

	result.TotalTokens = nonNegativeInt64(overall.Total.TotalTokens)
	result.Tokens = overall.Total.Tokens
	totalMicros, coverage := aggregateModelCosts(overall.Models, query.Prices)
	result.Total = newUSDMoney(totalMicros)
	result.Coverage = coverage
	var modelTruncated bool
	result.ByModel, result.UnpricedModels, result.UncalculatedModels, modelTruncated = modelCostEntries(overall.Models, query.Prices)
	result.Truncated = result.Truncated || modelTruncated
	var apiTruncated bool
	result.ByAPI, apiTruncated = apiCostEntries(overall, query.Prices)
	result.Truncated = result.Truncated || apiTruncated
	if !apiTruncated {
		reconcileAPICosts(result.ByAPI, totalMicros)
	}

	for _, item := range seriesAggregates {
		aggregate := item.aggregate
		micros, bucketCoverage := aggregateModelCosts(aggregate.Models, query.Prices)
		result.Series = append(result.Series, CostSeriesEntry{
			Bucket:       item.bucket,
			Requests:     aggregate.Total.TotalRequests,
			SuccessCount: aggregate.Total.SuccessCount,
			FailureCount: aggregate.Total.FailureCount,
			TotalTokens:  nonNegativeInt64(aggregate.Total.TotalTokens),
			Tokens:       aggregate.Total.Tokens,
			Coverage:     bucketCoverage,
			Cost:         newUSDMoney(micros),
		})
	}
	reconcileSeriesCosts(result.Series, totalMicros)
	return result
}

type costRangeAggregate struct {
	Total     costAggregateStats
	Models    map[string]*costAggregateStats
	APIs      map[string]*costAggregateStats
	APIModels map[string]map[string]*costAggregateStats
}

type costAggregateStats struct {
	TotalRequests      int64
	SuccessCount       int64
	FailureCount       int64
	TotalTokens        int64
	CalculableRequests int64
	CalculableTokens   int64
	NonCachedInput     int64
	Tokens             TokenStats
}

func newCostRangeAggregate(includeAPIs bool) *costRangeAggregate {
	result := &costRangeAggregate{Models: make(map[string]*costAggregateStats)}
	if includeAPIs {
		result.APIs = make(map[string]*costAggregateStats)
		result.APIModels = make(map[string]map[string]*costAggregateStats)
	}
	return result
}

func (a *costAggregateStats) addDetail(detail RequestDetail) {
	if a == nil {
		return
	}
	updateUsageAggregate(
		&a.TotalRequests,
		&a.SuccessCount,
		&a.FailureCount,
		&a.TotalTokens,
		&a.Tokens,
		detail,
	)
	tokens := normaliseTokenStats(detail.Tokens)
	if hasCostBreakdown(tokens) {
		a.CalculableRequests = saturatingAddInt64(a.CalculableRequests, 1)
		a.CalculableTokens = saturatingAddInt64(a.CalculableTokens, nonNegativeInt64(tokens.TotalTokens))
		a.NonCachedInput = saturatingAddInt64(a.NonCachedInput, nonCachedInputTokens(tokens))
	}
}

func (a *costAggregateStats) mergeAggregate(other aggregateStats) {
	if a == nil {
		return
	}
	a.TotalRequests = saturatingAddInt64(a.TotalRequests, other.TotalRequests)
	a.SuccessCount = saturatingAddInt64(a.SuccessCount, other.SuccessCount)
	a.FailureCount = saturatingAddInt64(a.FailureCount, other.FailureCount)
	a.TotalTokens = saturatingAddInt64(a.TotalTokens, other.TotalTokens)
	a.CalculableRequests = saturatingAddInt64(a.CalculableRequests, other.CalculableRequests)
	a.CalculableTokens = saturatingAddInt64(a.CalculableTokens, other.CalculableTokens)
	a.NonCachedInput = saturatingAddInt64(a.NonCachedInput, other.NonCachedInput)
	addTokenStats(&a.Tokens, other.Tokens)
}

func ensureCostAggregate(values map[string]*costAggregateStats, key string) *costAggregateStats {
	if value := values[key]; value != nil {
		return value
	}
	value := &costAggregateStats{}
	values[key] = value
	return value
}

func ensureNestedCostAggregate(values map[string]map[string]*costAggregateStats, outer, inner string) *costAggregateStats {
	nested := values[outer]
	if nested == nil {
		nested = make(map[string]*costAggregateStats)
		values[outer] = nested
	}
	return ensureCostAggregate(nested, inner)
}

func (a *costRangeAggregate) add(apiName, modelName string, detail RequestDetail) {
	if a == nil {
		return
	}
	modelName = strings.TrimSpace(modelName)
	if modelName == "" {
		modelName = "unknown"
	}
	a.Total.addDetail(detail)
	ensureCostAggregate(a.Models, modelName).addDetail(detail)
	if a.APIs != nil {
		apiName = strings.TrimSpace(apiName)
		if apiName == "" {
			apiName = "unknown"
		}
		ensureCostAggregate(a.APIs, apiName).addDetail(detail)
		ensureNestedCostAggregate(a.APIModels, apiName, modelName).addDetail(detail)
	}
}

func (a *costRangeAggregate) merge(bucket *usageAggregateBucket) {
	if a == nil || bucket == nil {
		return
	}
	a.Total.mergeAggregate(bucket.Total)
	for model, stats := range bucket.Models {
		if stats != nil {
			ensureCostAggregate(a.Models, model).mergeAggregate(*stats)
		}
	}
	if a.APIs != nil {
		for apiName, stats := range bucket.APIs {
			if stats != nil {
				ensureCostAggregate(a.APIs, apiName).mergeAggregate(*stats)
			}
		}
		for apiName, models := range bucket.APIModels {
			for model, stats := range models {
				if stats != nil {
					ensureNestedCostAggregate(a.APIModels, apiName, model).mergeAggregate(*stats)
				}
			}
		}
	}
}

func (s *RequestStatistics) aggregateCostRangeLocked(timeRange TimeRange, includeAPIs bool) *costRangeAggregate {
	result := newCostRangeAggregate(includeAPIs)
	if timeRange.IsZero() {
		for _, bucket := range s.dayBuckets {
			result.merge(bucket)
		}
		return result
	}
	if s.oldestAt.IsZero() || s.newestAt.IsZero() {
		return result
	}
	now := time.Now().UTC()
	buckets, step := s.bucketsForRangeLocked(timeRange, now)
	timeRange = clampUsageRangeToBucketBounds(timeRange, s.oldestAt, s.newestAt, step)
	if timeRange.From.IsZero() || timeRange.To.IsZero() || !timeRange.From.Before(timeRange.To) {
		return result
	}
	from := timeRange.From.UTC()
	to := timeRange.To.UTC()
	fullFrom := ceilAggregateTime(from, step)
	fullTo := truncateAggregateTime(to, step)
	if !fullFrom.Before(fullTo) {
		s.addIndexedCostDetailsLocked(result, timeRange)
		return result
	}
	if from.Before(fullFrom) {
		edgeTo := fullFrom
		if to.Before(edgeTo) {
			edgeTo = to
		}
		s.addIndexedCostDetailsLocked(result, TimeRange{From: from, To: edgeTo})
	}
	s.mergeCostBucketsLocked(result, buckets, fullFrom, fullTo, step)
	if fullTo.Before(to) {
		edgeFrom := fullTo
		if edgeFrom.Before(from) {
			edgeFrom = from
		}
		s.addIndexedCostDetailsLocked(result, TimeRange{From: edgeFrom, To: to})
	}
	return result
}

func (s *RequestStatistics) addIndexedCostDetailsLocked(target *costRangeAggregate, timeRange TimeRange) {
	s.forEachIndexedDetailLocked(timeRange, func(apiName, modelName string, detail RequestDetail) {
		target.add(apiName, modelName, detail)
	})
}

func (s *RequestStatistics) mergeCostBucketsLocked(target *costRangeAggregate, buckets map[int64]*usageAggregateBucket, from, to time.Time, step time.Duration) {
	if target == nil || !from.Before(to) {
		return
	}
	steps := int64(to.Sub(from) / step)
	if steps <= int64(len(buckets))+32 {
		for bucketTime := from; bucketTime.Before(to); bucketTime = bucketTime.Add(step) {
			target.merge(buckets[bucketTime.Unix()])
		}
		return
	}
	for key, bucket := range buckets {
		bucketTime := time.Unix(key, 0).UTC()
		if bucketTime.Before(from) || !bucketTime.Before(to) {
			continue
		}
		target.merge(bucket)
	}
}

func reconcileAPICosts(entries []APICostEntry, totalMicros int64) {
	reconcileCostAmounts(totalMicros, len(entries), func(index int) int64 {
		return entries[index].Cost.AmountMicros
	}, func(index int) int64 {
		return entries[index].Coverage.CalculatedTokens
	}, func(index int, amount, adjustment int64) {
		entries[index].Cost = newUSDMoney(amount)
		entries[index].RoundingAdjustmentMicros = adjustment
	})
}

func reconcileSeriesCosts(entries []CostSeriesEntry, totalMicros int64) {
	reconcileCostAmounts(totalMicros, len(entries), func(index int) int64 {
		return entries[index].Cost.AmountMicros
	}, func(index int) int64 {
		return entries[index].Coverage.CalculatedTokens
	}, func(index int, amount, adjustment int64) {
		entries[index].Cost = newUSDMoney(amount)
		entries[index].RoundingAdjustmentMicros = adjustment
	})
}

func reconcileCostAmounts(totalMicros int64, count int, amountAt, rankAt func(int) int64, apply func(int, int64, int64)) {
	if count == 0 || amountAt == nil || rankAt == nil || apply == nil {
		return
	}
	var groupedMicros int64
	overflow := false
	indices := make([]int, count)
	for index := 0; index < count; index++ {
		indices[index] = index
		amount := nonNegativeInt64(amountAt(index))
		if groupedMicros > math.MaxInt64-amount {
			overflow = true
			continue
		}
		groupedMicros += amount
	}
	sort.SliceStable(indices, func(i, j int) bool {
		left, right := indices[i], indices[j]
		if amountAt(left) != amountAt(right) {
			return amountAt(left) > amountAt(right)
		}
		return rankAt(left) > rankAt(right)
	})
	if overflow {
		remaining := nonNegativeInt64(totalMicros)
		for _, index := range indices {
			amount := nonNegativeInt64(amountAt(index))
			assigned := amount
			if assigned > remaining {
				assigned = remaining
			}
			if assigned != amount {
				apply(index, assigned, assigned-amount)
			}
			remaining -= assigned
		}
		return
	}
	delta := totalMicros - groupedMicros
	if delta == 0 {
		return
	}
	if delta > 0 {
		index := indices[0]
		apply(index, saturatingAddInt64(amountAt(index), delta), delta)
		return
	}
	remaining := -delta
	for _, index := range indices {
		amount := nonNegativeInt64(amountAt(index))
		adjustment := amount
		if adjustment > remaining {
			adjustment = remaining
		}
		if adjustment == 0 {
			continue
		}
		apply(index, amount-adjustment, -adjustment)
		remaining -= adjustment
		if remaining == 0 {
			return
		}
	}
}

func modelCostEntries(models map[string]*costAggregateStats, prices map[string]ModelPrice) ([]ModelCostEntry, []UnpricedModelEntry, []UncalculatedModelEntry, bool) {
	keys := sortedCostAggregateKeys(models)
	truncated := false
	if len(keys) > MaxUsageAnalyticsItems {
		keys = keys[:MaxUsageAnalyticsItems]
		truncated = true
	}
	entries := make([]ModelCostEntry, 0, len(keys))
	unpriced := make([]UnpricedModelEntry, 0)
	uncalculated := make([]UncalculatedModelEntry, 0)
	for _, model := range keys {
		stats := models[model]
		if stats == nil {
			continue
		}
		price, priced := modelPrice(prices, model)
		requests := nonNegativeInt64(stats.TotalRequests)
		totalTokens := nonNegativeInt64(stats.TotalTokens)
		breakdownRequests := boundedCount(stats.CalculableRequests, requests)
		breakdownTokens := boundedCount(stats.CalculableTokens, totalTokens)
		calculatedRequests := int64(0)
		calculatedTokens := int64(0)
		micros := int64(0)
		if priced {
			calculatedRequests = breakdownRequests
			calculatedTokens = breakdownTokens
			micros = aggregateCostMicros(stats, price)
			if calculatedRequests < requests || calculatedTokens < totalTokens {
				uncalculated = append(uncalculated, UncalculatedModelEntry{
					Model:       model,
					Requests:    requests - calculatedRequests,
					TotalTokens: totalTokens - calculatedTokens,
				})
			}
		} else {
			unpriced = append(unpriced, UnpricedModelEntry{
				Model:        model,
				Requests:     stats.TotalRequests,
				SuccessCount: stats.SuccessCount,
				FailureCount: stats.FailureCount,
				TotalTokens:  totalTokens,
				Tokens:       stats.Tokens,
			})
		}
		entries = append(entries, ModelCostEntry{
			Model:              model,
			Priced:             priced,
			Requests:           requests,
			SuccessCount:       stats.SuccessCount,
			FailureCount:       stats.FailureCount,
			TotalTokens:        totalTokens,
			CalculatedRequests: calculatedRequests,
			CalculatedTokens:   calculatedTokens,
			Tokens:             stats.Tokens,
			Cost:               newUSDMoney(micros),
		})
	}
	return entries, unpriced, uncalculated, truncated
}

func apiCostEntries(aggregate *costRangeAggregate, prices map[string]ModelPrice) ([]APICostEntry, bool) {
	if aggregate == nil {
		return []APICostEntry{}, false
	}
	keys := sortedCostAggregateKeys(aggregate.APIs)
	truncated := false
	if len(keys) > MaxUsageAnalyticsItems {
		keys = keys[:MaxUsageAnalyticsItems]
		truncated = true
	}
	entries := make([]APICostEntry, 0, len(keys))
	for _, apiName := range keys {
		stats := aggregate.APIs[apiName]
		if stats == nil {
			continue
		}
		micros, coverage := aggregateModelCosts(aggregate.APIModels[apiName], prices)
		entries = append(entries, APICostEntry{
			API:          apiName,
			Requests:     stats.TotalRequests,
			SuccessCount: stats.SuccessCount,
			FailureCount: stats.FailureCount,
			TotalTokens:  nonNegativeInt64(stats.TotalTokens),
			Tokens:       stats.Tokens,
			Coverage:     coverage,
			Cost:         newUSDMoney(micros),
		})
	}
	return entries, truncated
}

func aggregateModelCosts(models map[string]*costAggregateStats, prices map[string]ModelPrice) (int64, CostCoverage) {
	var micros int64
	coverage := CostCoverage{}
	for model, stats := range models {
		if stats == nil {
			continue
		}
		requests := nonNegativeInt64(stats.TotalRequests)
		tokens := nonNegativeInt64(stats.TotalTokens)
		coverage.TotalRequests = saturatingAddInt64(coverage.TotalRequests, requests)
		coverage.TotalTokens = saturatingAddInt64(coverage.TotalTokens, tokens)
		price, priced := modelPrice(prices, model)
		if !priced {
			continue
		}
		coverage.PricedRequests = saturatingAddInt64(coverage.PricedRequests, requests)
		coverage.PricedTokens = saturatingAddInt64(coverage.PricedTokens, tokens)
		calculatedRequests := boundedCount(stats.CalculableRequests, requests)
		calculatedTokens := boundedCount(stats.CalculableTokens, tokens)
		coverage.CalculatedRequests = saturatingAddInt64(coverage.CalculatedRequests, calculatedRequests)
		coverage.CalculatedTokens = saturatingAddInt64(coverage.CalculatedTokens, calculatedTokens)
		micros = saturatingAddInt64(micros, aggregateCostMicros(stats, price))
	}
	coverage.RequestCoverage = coverageRatio(coverage.PricedRequests, coverage.TotalRequests)
	coverage.CalculationRequestCoverage = coverageRatio(coverage.CalculatedRequests, coverage.TotalRequests)
	coverage.TokenCoverage = coverageRatio(coverage.PricedTokens, coverage.TotalTokens)
	coverage.CalculationTokenCoverage = coverageRatio(coverage.CalculatedTokens, coverage.TotalTokens)
	return micros, coverage
}

func boundedCount(value, upper int64) int64 {
	value = nonNegativeInt64(value)
	upper = nonNegativeInt64(upper)
	if value > upper {
		return upper
	}
	return value
}

func modelPrice(prices map[string]ModelPrice, model string) (ModelPrice, bool) {
	price, ok := prices[strings.ToLower(strings.TrimSpace(model))]
	if !ok || !validModelPrice(price) {
		return ModelPrice{}, false
	}
	return price, true
}

func validModelPrice(price ModelPrice) bool {
	values := []float64{price.InputPerMillion, price.OutputPerMillion, price.CachedInputPerMillion}
	for _, value := range values {
		if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return false
		}
	}
	return true
}

func sortedCostAggregateKeys(values map[string]*costAggregateStats) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func aggregateCostMicros(stats *costAggregateStats, price ModelPrice) int64 {
	if stats == nil {
		return 0
	}
	return costMicrosFromBases(stats.NonCachedInput, stats.Tokens.CachedTokens, stats.Tokens.OutputTokens, price)
}

func modelCostMicros(tokens TokenStats, price ModelPrice) int64 {
	return costMicrosFromBases(nonCachedInputTokens(tokens), tokens.CachedTokens, tokens.OutputTokens, price)
}

func costMicrosFromBases(nonCachedInput, cached, output int64, price ModelPrice) int64 {
	nonCachedInput = nonNegativeInt64(nonCachedInput)
	cached = nonNegativeInt64(cached)
	output = nonNegativeInt64(output)
	value := float64(nonCachedInput)*price.InputPerMillion +
		float64(cached)*price.CachedInputPerMillion +
		float64(output)*price.OutputPerMillion
	if math.IsInf(value, 1) || value >= float64(math.MaxInt64) {
		return math.MaxInt64
	}
	if value <= 0 || math.IsNaN(value) {
		return 0
	}
	return int64(math.Round(value))
}

func newUSDMoney(micros int64) Money {
	return Money{
		Currency:     CostCurrencyUSD,
		AmountMicros: micros,
		Amount:       float64(micros) / 1_000_000,
	}
}

func coverageRatio(numerator, denominator int64) *float64 {
	if denominator <= 0 {
		return nil
	}
	ratio := float64(numerator) / float64(denominator)
	return &ratio
}

func normalizeCostBucket(bucket string) string {
	if strings.TrimSpace(bucket) == BucketHour {
		return BucketHour
	}
	return BucketDay
}
