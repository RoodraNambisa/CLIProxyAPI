package usage

import (
	"math"
	"sort"
	"strings"
	"time"
)

const (
	usageMinuteRetention = 8 * 24 * time.Hour
	usageHourRetention   = 400 * 24 * time.Hour
	usageFutureTolerance = 5 * time.Minute
)

type aggregateStats struct {
	TotalRequests      int64
	SuccessCount       int64
	FailureCount       int64
	TotalTokens        int64
	CalculableRequests int64
	CalculableTokens   int64
	NonCachedInput     int64
	Tokens             TokenStats
	SuccessTokens      TokenStats
	FailureTokens      TokenStats
	LastUsedAt         time.Time
}

type usageAggregateBucket struct {
	Total       aggregateStats
	APIs        map[string]*aggregateStats
	Models      map[string]*aggregateStats
	APIModels   map[string]map[string]*aggregateStats
	Auths       map[string]*aggregateStats
	AuthModels  map[string]map[string]*aggregateStats
	AuthSources map[string]map[string]*aggregateStats
	Sources     map[string]*aggregateStats
}

type usageDetailRef struct {
	Timestamp time.Time
	API       string
	Model     string
	Offset    int
}

func newUsageAggregateBucket() *usageAggregateBucket {
	return &usageAggregateBucket{
		APIs:        make(map[string]*aggregateStats),
		Models:      make(map[string]*aggregateStats),
		APIModels:   make(map[string]map[string]*aggregateStats),
		Auths:       make(map[string]*aggregateStats),
		AuthModels:  make(map[string]map[string]*aggregateStats),
		AuthSources: make(map[string]map[string]*aggregateStats),
		Sources:     make(map[string]*aggregateStats),
	}
}

func (a *aggregateStats) addDetail(detail RequestDetail) {
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
	if detail.Failed {
		addTokenStats(&a.FailureTokens, detail.Tokens)
	} else {
		addTokenStats(&a.SuccessTokens, detail.Tokens)
	}
	normalizedTokens := normaliseTokenStats(detail.Tokens)
	if hasCostBreakdown(normalizedTokens) {
		a.CalculableRequests = saturatingAddInt64(a.CalculableRequests, 1)
		a.CalculableTokens = saturatingAddInt64(a.CalculableTokens, nonNegativeInt64(normalizedTokens.TotalTokens))
		a.NonCachedInput = saturatingAddInt64(a.NonCachedInput, nonCachedInputTokens(normalizedTokens))
	}
	if a.LastUsedAt.IsZero() || detail.Timestamp.After(a.LastUsedAt) {
		a.LastUsedAt = detail.Timestamp
	}
}

func (a *aggregateStats) merge(other aggregateStats) {
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
	addTokenStats(&a.SuccessTokens, other.SuccessTokens)
	addTokenStats(&a.FailureTokens, other.FailureTokens)
	if a.LastUsedAt.IsZero() || other.LastUsedAt.After(a.LastUsedAt) {
		a.LastUsedAt = other.LastUsedAt
	}
}

func addTokenStats(target *TokenStats, value TokenStats) {
	if target == nil {
		return
	}
	value = normaliseTokenStats(value)
	target.InputTokens = saturatingAddInt64(target.InputTokens, value.InputTokens)
	target.OutputTokens = saturatingAddInt64(target.OutputTokens, value.OutputTokens)
	target.ReasoningTokens = saturatingAddInt64(target.ReasoningTokens, value.ReasoningTokens)
	target.CachedTokens = saturatingAddInt64(target.CachedTokens, value.CachedTokens)
	target.CacheCreationTokens = saturatingAddInt64(target.CacheCreationTokens, value.CacheCreationTokens)
	target.TotalTokens = saturatingAddInt64(target.TotalTokens, value.TotalTokens)
}

func hasCostBreakdown(tokens TokenStats) bool {
	if tokens.TotalTokens <= 0 {
		return true
	}
	return tokens.InputTokens > 0 || tokens.OutputTokens > 0 || tokens.CachedTokens > 0
}

func nonCachedInputTokens(tokens TokenStats) int64 {
	input := nonNegativeInt64(tokens.InputTokens)
	cached := nonNegativeInt64(tokens.CachedTokens)
	if cached >= input {
		return 0
	}
	return input - cached
}

func (b *usageAggregateBucket) add(apiName, modelName string, detail RequestDetail) {
	if b == nil {
		return
	}
	apiName = strings.TrimSpace(apiName)
	if apiName == "" {
		apiName = "unknown"
	}
	modelName = strings.TrimSpace(modelName)
	if modelName == "" {
		modelName = "unknown"
	}
	b.Total.addDetail(detail)
	ensureAggregate(b.APIs, apiName).addDetail(detail)
	ensureAggregate(b.Models, modelName).addDetail(detail)
	ensureNestedAggregate(b.APIModels, apiName, modelName).addDetail(detail)

	authIndex := strings.TrimSpace(detail.AuthIndex)
	if authIndex == "" {
		authIndex = "unknown"
	}
	ensureAggregate(b.Auths, authIndex).addDetail(detail)
	ensureNestedAggregate(b.AuthModels, authIndex, modelName).addDetail(detail)
	source := strings.TrimSpace(detail.Source)
	if source == "" {
		source = "unknown"
	}
	ensureAggregate(b.Sources, source).addDetail(detail)
	ensureNestedAggregate(b.AuthSources, authIndex, source).addDetail(detail)
}

func (b *usageAggregateBucket) merge(other *usageAggregateBucket) {
	if b == nil || other == nil {
		return
	}
	b.Total.merge(other.Total)
	mergeAggregateMap(b.APIs, other.APIs)
	mergeAggregateMap(b.Models, other.Models)
	mergeNestedAggregateMap(b.APIModels, other.APIModels)
	mergeAggregateMap(b.Auths, other.Auths)
	mergeNestedAggregateMap(b.AuthModels, other.AuthModels)
	mergeNestedAggregateMap(b.AuthSources, other.AuthSources)
	mergeAggregateMap(b.Sources, other.Sources)
}

func ensureAggregate(values map[string]*aggregateStats, key string) *aggregateStats {
	value := values[key]
	if value == nil {
		value = &aggregateStats{}
		values[key] = value
	}
	return value
}

func ensureNestedAggregate(values map[string]map[string]*aggregateStats, first, second string) *aggregateStats {
	nested := values[first]
	if nested == nil {
		nested = make(map[string]*aggregateStats)
		values[first] = nested
	}
	return ensureAggregate(nested, second)
}

func mergeAggregateMap(target, source map[string]*aggregateStats) {
	for key, value := range source {
		if value != nil {
			ensureAggregate(target, key).merge(*value)
		}
	}
}

func mergeNestedAggregateMap(target, source map[string]map[string]*aggregateStats) {
	for first, values := range source {
		for second, value := range values {
			if value != nil {
				ensureNestedAggregate(target, first, second).merge(*value)
			}
		}
	}
}

func (s *RequestStatistics) updateRealtimeAggregatesLocked(apiName, modelName string, detail RequestDetail, now time.Time) {
	addTokenStats(&s.tokens, detail.Tokens)
	ensureAggregate(s.models, modelName).addDetail(detail)
	source := strings.TrimSpace(detail.Source)
	if source == "" {
		source = "unknown"
	}
	ensureAggregate(s.sources, source).addDetail(detail)
	if s.oldestAt.IsZero() || detail.Timestamp.Before(s.oldestAt) {
		s.oldestAt = detail.Timestamp
	}
	if s.newestAt.IsZero() || detail.Timestamp.After(s.newestAt) {
		s.newestAt = detail.Timestamp
	}

	s.recordTimeBucketLocked(s.dayBuckets, detail.Timestamp, 24*time.Hour, apiName, modelName, detail)
	minuteCutoff := now.Add(-usageMinuteRetention).UTC().Truncate(time.Minute)
	withinFutureTolerance := !detail.Timestamp.After(now.Add(usageFutureTolerance))
	if !withinFutureTolerance {
		if s.dayOnlyFutureOldest.IsZero() || detail.Timestamp.Before(s.dayOnlyFutureOldest) {
			s.dayOnlyFutureOldest = detail.Timestamp
		}
		if s.dayOnlyFutureNewest.IsZero() || detail.Timestamp.After(s.dayOnlyFutureNewest) {
			s.dayOnlyFutureNewest = detail.Timestamp
		}
	}
	if withinFutureTolerance && !detail.Timestamp.UTC().Truncate(time.Minute).Before(minuteCutoff) {
		s.recordTimeBucketLocked(s.minuteBuckets, detail.Timestamp, time.Minute, apiName, modelName, detail)
	}
	hourCutoff := now.Add(-usageHourRetention).UTC().Truncate(time.Hour)
	if withinFutureTolerance && !detail.Timestamp.UTC().Truncate(time.Hour).Before(hourCutoff) {
		s.recordTimeBucketLocked(s.hourBuckets, detail.Timestamp, time.Hour, apiName, modelName, detail)
	}
}

func (s *RequestStatistics) recordTimeBucketLocked(values map[int64]*usageAggregateBucket, timestamp time.Time, step time.Duration, apiName, modelName string, detail RequestDetail) {
	key := truncateAggregateTime(timestamp, step).Unix()
	bucket := values[key]
	if bucket == nil {
		bucket = newUsageAggregateBucket()
		values[key] = bucket
	}
	bucket.add(apiName, modelName, detail)
}

func (s *RequestStatistics) pruneTimeBucketsLocked(now time.Time) {
	minuteKey := now.UTC().Truncate(time.Minute).Unix()
	if s.lastPrunedMinute == minuteKey {
		return
	}
	s.lastPrunedMinute = minuteKey
	minuteCutoff := now.Add(-usageMinuteRetention).UTC().Truncate(time.Minute).Unix()
	for key := range s.minuteBuckets {
		if key < minuteCutoff {
			delete(s.minuteBuckets, key)
		}
	}
	hourCutoff := now.Add(-usageHourRetention).UTC().Truncate(time.Hour).Unix()
	for key := range s.hourBuckets {
		if key < hourCutoff {
			delete(s.hourBuckets, key)
		}
	}
}

func (s *RequestStatistics) pruneExpiredBuckets(now time.Time) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.pruneTimeBucketsLocked(now)
	s.sortDetailIndexLocked()
	s.mu.Unlock()
}

func (s *RequestStatistics) insertDetailRefLocked(ref usageDetailRef) {
	if len(s.detailIndex) > 0 && ref.Timestamp.Before(s.detailIndex[len(s.detailIndex)-1].Timestamp) {
		s.detailIndexDirty = true
	}
	s.detailIndex = append(s.detailIndex, ref)
}

func (s *RequestStatistics) rebuildDetailIndexLocked() {
	s.detailIndex = make([]usageDetailRef, 0)
	for apiName, stats := range s.apis {
		if stats == nil {
			continue
		}
		for modelName, modelStatsValue := range stats.Models {
			if modelStatsValue == nil {
				continue
			}
			for offset, detail := range modelStatsValue.Details {
				s.detailIndex = append(s.detailIndex, usageDetailRef{
					Timestamp: detail.Timestamp,
					API:       apiName,
					Model:     modelName,
					Offset:    offset,
				})
			}
		}
	}
	s.detailIndexDirty = true
	s.sortDetailIndexLocked()
}

func (s *RequestStatistics) sortDetailIndexLocked() {
	if !s.detailIndexDirty {
		return
	}
	sort.Slice(s.detailIndex, func(i, j int) bool {
		left, right := s.detailIndex[i], s.detailIndex[j]
		if !left.Timestamp.Equal(right.Timestamp) {
			return left.Timestamp.Before(right.Timestamp)
		}
		if left.API != right.API {
			return left.API < right.API
		}
		if left.Model != right.Model {
			return left.Model < right.Model
		}
		return left.Offset < right.Offset
	})
	s.detailIndexDirty = false
}

func (s *RequestStatistics) aggregateRangeLocked(timeRange TimeRange) *usageAggregateBucket {
	result := newUsageAggregateBucket()
	if timeRange.IsZero() {
		return s.allTimeAggregateLocked()
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

	fullFrom := from
	if !from.IsZero() {
		fullFrom = ceilAggregateTime(from, step)
	}
	fullTo := to
	if !to.IsZero() {
		fullTo = truncateAggregateTime(to, step)
	}
	if !from.IsZero() && !to.IsZero() && !fullFrom.Before(fullTo) {
		s.addIndexedDetailsLocked(result, timeRange, nil)
		return result
	}

	if !from.IsZero() && from.Before(fullFrom) {
		edgeTo := fullFrom
		if !to.IsZero() && to.Before(edgeTo) {
			edgeTo = to
		}
		s.addIndexedDetailsLocked(result, TimeRange{From: from, To: edgeTo}, nil)
	}
	s.mergeAggregateBucketsLocked(result, buckets, fullFrom, fullTo, step)
	if !to.IsZero() && fullTo.Before(to) {
		edgeFrom := fullTo
		if !from.IsZero() && edgeFrom.Before(from) {
			edgeFrom = from
		}
		s.addIndexedDetailsLocked(result, TimeRange{From: edgeFrom, To: to}, nil)
	}
	return result
}

func (s *RequestStatistics) mergeAggregateBucketsLocked(result *usageAggregateBucket, buckets map[int64]*usageAggregateBucket, from, to time.Time, step time.Duration) {
	if result == nil || !from.Before(to) {
		return
	}
	steps := int64(to.Sub(from) / step)
	if steps <= int64(len(buckets))+32 {
		for bucketTime := from; bucketTime.Before(to); bucketTime = bucketTime.Add(step) {
			result.merge(buckets[bucketTime.Unix()])
		}
		return
	}
	for key, bucket := range buckets {
		bucketTime := time.Unix(key, 0).UTC()
		if bucketTime.Before(from) || !bucketTime.Before(to) {
			continue
		}
		result.merge(bucket)
	}
}

func clampUsageRangeToBucketBounds(timeRange TimeRange, oldest, newest time.Time, step time.Duration) TimeRange {
	if oldest.IsZero() || newest.IsZero() || newest.Before(oldest) {
		return TimeRange{}
	}
	result := timeRange
	lowerBound := truncateAggregateTime(oldest, step)
	upperBound := truncateAggregateTime(newest, step).Add(step)
	if result.From.IsZero() || result.From.Before(lowerBound) {
		result.From = lowerBound
	}
	if result.To.IsZero() || result.To.After(upperBound) {
		result.To = upperBound
	}
	if !result.From.Before(result.To) {
		return TimeRange{}
	}
	return result
}

func (s *RequestStatistics) bucketsForRangeLocked(timeRange TimeRange, now time.Time) (map[int64]*usageAggregateBucket, time.Duration) {
	actualFrom := timeRange.From
	if actualFrom.IsZero() || actualFrom.Before(s.oldestAt) {
		actualFrom = s.oldestAt
	}
	actualTo := timeRange.To
	newestExclusive := s.newestAt.Add(time.Nanosecond)
	if actualTo.IsZero() || actualTo.After(newestExclusive) {
		actualTo = newestExclusive
	}
	intersectsDayOnlyFuture := !s.dayOnlyFutureOldest.IsZero() &&
		actualTo.After(s.dayOnlyFutureOldest) &&
		!actualFrom.After(s.dayOnlyFutureNewest)
	if actualTo.After(now.Add(usageFutureTolerance)) || intersectsDayOnlyFuture {
		return s.dayBuckets, 24 * time.Hour
	}
	from := timeRange.From.UTC()
	if !from.IsZero() && !from.Before(now.Add(-usageMinuteRetention).Truncate(time.Minute)) {
		return s.minuteBuckets, time.Minute
	}
	if !from.IsZero() && !from.Before(now.Add(-usageHourRetention).Truncate(time.Hour)) {
		return s.hourBuckets, time.Hour
	}
	return s.dayBuckets, 24 * time.Hour
}

func (s *RequestStatistics) addIndexedDetailsLocked(target *usageAggregateBucket, timeRange TimeRange, include func(string, string, RequestDetail) bool) {
	s.forEachIndexedDetailLocked(timeRange, func(apiName, modelName string, detail RequestDetail) {
		if include == nil || include(apiName, modelName, detail) {
			target.add(apiName, modelName, detail)
		}
	})
}

func (s *RequestStatistics) forEachIndexedDetailLocked(timeRange TimeRange, visit func(string, string, RequestDetail)) {
	if visit == nil {
		return
	}
	start := 0
	if !timeRange.From.IsZero() {
		start = sort.Search(len(s.detailIndex), func(i int) bool {
			return !s.detailIndex[i].Timestamp.Before(timeRange.From)
		})
	}
	for index := start; index < len(s.detailIndex); index++ {
		ref := s.detailIndex[index]
		if !timeRange.To.IsZero() && !ref.Timestamp.Before(timeRange.To) {
			break
		}
		detail, ok := s.detailForRefLocked(ref)
		if !ok {
			continue
		}
		visit(ref.API, ref.Model, detail)
	}
}

func (s *RequestStatistics) detailForRefLocked(ref usageDetailRef) (RequestDetail, bool) {
	stats := s.apis[ref.API]
	if stats == nil {
		return RequestDetail{}, false
	}
	modelStatsValue := stats.Models[ref.Model]
	if modelStatsValue == nil || ref.Offset < 0 || ref.Offset >= len(modelStatsValue.Details) {
		return RequestDetail{}, false
	}
	return modelStatsValue.Details[ref.Offset], true
}

func (s *RequestStatistics) allTimeAggregateLocked() *usageAggregateBucket {
	result := newUsageAggregateBucket()
	for _, bucket := range s.dayBuckets {
		result.merge(bucket)
	}
	return result
}

func (s *RequestStatistics) updateLegacyHourBucketLocked(detail RequestDetail) {
	if s == nil {
		return
	}
	timestamp := detail.Timestamp
	start := timestamp.Add(-time.Duration(timestamp.Minute()) * time.Minute).
		Add(-time.Duration(timestamp.Second()) * time.Second).
		Add(-time.Duration(timestamp.Nanosecond()))
	key := legacyHourKey{
		StartUnix: start.Unix(),
		Day:       timestamp.Format("2006-01-02"),
		Hour:      timestamp.Hour(),
	}
	stats := s.legacyHourBuckets[key]
	if stats == nil {
		stats = &legacyHourStats{}
		s.legacyHourBuckets[key] = stats
	}
	stats.Requests = saturatingAddInt64(stats.Requests, 1)
	totalTokens := normaliseTokenStats(detail.Tokens).TotalTokens
	stats.Tokens = saturatingAddInt64(stats.Tokens, nonNegativeInt64(totalTokens))
	stats.Entries = append(stats.Entries, legacyUsageEntry{
		Timestamp: timestamp,
		Tokens:    totalTokens,
	})
}

func truncateAggregateTime(timestamp time.Time, step time.Duration) time.Time {
	timestamp = timestamp.UTC()
	if step == 24*time.Hour {
		return time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), 0, 0, 0, 0, time.UTC)
	}
	return timestamp.Truncate(step)
}

func ceilAggregateTime(timestamp time.Time, step time.Duration) time.Time {
	truncated := truncateAggregateTime(timestamp, step)
	if timestamp.Equal(truncated) {
		return truncated
	}
	return truncated.Add(step)
}

func saturatingAddInt64(left, right int64) int64 {
	if right > 0 && left > math.MaxInt64-right {
		return math.MaxInt64
	}
	if right < 0 && left < math.MinInt64-right {
		return math.MinInt64
	}
	return left + right
}

func nonNegativeInt64(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}
