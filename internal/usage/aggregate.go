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
	DetailCount        int64
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
	detailIDs   []uint64
}

type usageAggregateProjection struct {
	add   func(*usageAggregateBucket, string, string, RequestDetail)
	merge func(*usageAggregateBucket, *usageAggregateBucket)
}

type usageDetailRef struct {
	Timestamp time.Time
	ID        uint64
}

type usageDetailLocation struct {
	API    string
	Model  string
	Offset int
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
	a.DetailCount = saturatingAddInt64(a.DetailCount, 1)
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
		if !detail.Auxiliary {
			a.CalculableRequests = saturatingAddInt64(a.CalculableRequests, 1)
		}
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
	a.DetailCount = saturatingAddInt64(a.DetailCount, other.DetailCount)
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

func (a *aggregateStats) removeDetail(detail RequestDetail) {
	if a == nil {
		return
	}
	a.DetailCount = subtractNonNegativeInt64(a.DetailCount, 1)
	subtractUsageAggregate(
		&a.TotalRequests,
		&a.SuccessCount,
		&a.FailureCount,
		&a.TotalTokens,
		&a.Tokens,
		detail,
	)
	if detail.Failed {
		subtractTokenStats(&a.FailureTokens, detail.Tokens)
	} else {
		subtractTokenStats(&a.SuccessTokens, detail.Tokens)
	}
	normalizedTokens := normaliseTokenStats(detail.Tokens)
	if hasCostBreakdown(normalizedTokens) {
		if !detail.Auxiliary {
			a.CalculableRequests = subtractNonNegativeInt64(a.CalculableRequests, 1)
		}
		a.CalculableTokens = subtractNonNegativeInt64(a.CalculableTokens, normalizedTokens.TotalTokens)
		a.NonCachedInput = subtractNonNegativeInt64(a.NonCachedInput, nonCachedInputTokens(normalizedTokens))
	}
	if a.LastUsedAt.Equal(detail.Timestamp) {
		a.LastUsedAt = time.Time{}
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

func subtractTokenStats(target *TokenStats, value TokenStats) {
	if target == nil {
		return
	}
	value = normaliseTokenStats(value)
	target.InputTokens = subtractNonNegativeInt64(target.InputTokens, value.InputTokens)
	target.OutputTokens = subtractNonNegativeInt64(target.OutputTokens, value.OutputTokens)
	target.ReasoningTokens = subtractNonNegativeInt64(target.ReasoningTokens, value.ReasoningTokens)
	target.CachedTokens = subtractNonNegativeInt64(target.CachedTokens, value.CachedTokens)
	target.CacheCreationTokens = subtractNonNegativeInt64(target.CacheCreationTokens, value.CacheCreationTokens)
	target.TotalTokens = subtractNonNegativeInt64(target.TotalTokens, value.TotalTokens)
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
	if detail.internalID != 0 {
		b.detailIDs = append(b.detailIDs, detail.internalID)
	}
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

func (b *usageAggregateBucket) remove(apiName, modelName string, detail RequestDetail) {
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
	b.Total.removeDetail(detail)
	removeAggregateDetail(b.APIs, apiName, detail)
	removeAggregateDetail(b.Models, modelName, detail)
	removeNestedAggregateDetail(b.APIModels, apiName, modelName, detail)
	authIndex := strings.TrimSpace(detail.AuthIndex)
	if authIndex == "" {
		authIndex = "unknown"
	}
	removeAggregateDetail(b.Auths, authIndex, detail)
	removeNestedAggregateDetail(b.AuthModels, authIndex, modelName, detail)
	source := strings.TrimSpace(detail.Source)
	if source == "" {
		source = "unknown"
	}
	removeAggregateDetail(b.Sources, source, detail)
	removeNestedAggregateDetail(b.AuthSources, authIndex, source, detail)
}

func removeAggregateDetail(values map[string]*aggregateStats, key string, detail RequestDetail) {
	value := values[key]
	if value == nil {
		return
	}
	value.removeDetail(detail)
	if value.empty() {
		delete(values, key)
	}
}

func removeNestedAggregateDetail(values map[string]map[string]*aggregateStats, first, second string, detail RequestDetail) {
	nested := values[first]
	if nested == nil {
		return
	}
	removeAggregateDetail(nested, second, detail)
	if len(nested) == 0 {
		delete(values, first)
	}
}

func (a *aggregateStats) empty() bool {
	return a == nil || a.DetailCount == 0
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
	s.detailLocations = make(map[uint64]usageDetailLocation)
	s.detailIndexTombstones = 0
	s.authDetailIDs = make(map[string][]uint64)
	clearAggregateBucketDetailRefs(s.minuteBuckets)
	clearAggregateBucketDetailRefs(s.hourBuckets)
	clearAggregateBucketDetailRefs(s.dayBuckets)
	seen := make(map[uint64]struct{})
	for apiName, stats := range s.apis {
		if stats == nil {
			continue
		}
		for modelName, modelStatsValue := range stats.Models {
			if modelStatsValue == nil {
				continue
			}
			for offset, detail := range modelStatsValue.Details {
				if detail.internalID == 0 {
					detail.internalID = s.allocateDetailIDLocked()
				} else if _, duplicate := seen[detail.internalID]; duplicate {
					detail.internalID = s.allocateDetailIDLocked()
				} else if detail.internalID > s.nextDetailID {
					s.nextDetailID = detail.internalID
				}
				seen[detail.internalID] = struct{}{}
				modelStatsValue.Details[offset] = detail
				s.detailLocations[detail.internalID] = usageDetailLocation{API: apiName, Model: modelName, Offset: offset}
				s.indexAuthDetailLocked(detail.AuthIndex, detail.internalID)
				ref := usageDetailRef{
					Timestamp: detail.Timestamp,
					ID:        detail.internalID,
				}
				s.detailIndex = append(s.detailIndex, ref)
				appendAggregateBucketDetailID(s.minuteBuckets, time.Minute, ref.Timestamp, ref.ID)
				appendAggregateBucketDetailID(s.hourBuckets, time.Hour, ref.Timestamp, ref.ID)
				appendAggregateBucketDetailID(s.dayBuckets, 24*time.Hour, ref.Timestamp, ref.ID)
			}
		}
	}
	s.detailIndexDirty = true
	s.sortDetailIndexLocked()
}

func clearAggregateBucketDetailRefs(buckets map[int64]*usageAggregateBucket) {
	for _, bucket := range buckets {
		if bucket != nil {
			bucket.detailIDs = nil
		}
	}
}

func appendAggregateBucketDetailID(buckets map[int64]*usageAggregateBucket, step time.Duration, timestamp time.Time, detailID uint64) {
	bucket := buckets[truncateAggregateTime(timestamp, step).Unix()]
	if bucket != nil {
		bucket.detailIDs = append(bucket.detailIDs, detailID)
	}
}

func (s *RequestStatistics) compactDetailIndexLocked() {
	if s.detailIndexTombstones == 0 {
		return
	}
	if s.detailIndexTombstones < 4096 && s.detailIndexTombstones*2 < len(s.detailIndex) {
		return
	}
	kept := s.detailIndex[:0]
	for _, ref := range s.detailIndex {
		if _, ok := s.detailLocations[ref.ID]; ok {
			kept = append(kept, ref)
		}
	}
	clear(s.detailIndex[len(kept):])
	s.detailIndex = kept
	s.detailIndexTombstones = 0
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
		return left.ID < right.ID
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

func (s *RequestStatistics) aggregateProjectedRangeLocked(timeRange TimeRange, projection usageAggregateProjection) *usageAggregateBucket {
	result := newUsageAggregateBucket()
	if projection.add == nil || projection.merge == nil {
		return result
	}
	if timeRange.IsZero() {
		for _, bucket := range s.dayBuckets {
			projection.merge(result, bucket)
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
		s.addIndexedDetailsProjectedLocked(result, timeRange, projection)
		return result
	}
	if from.Before(fullFrom) {
		edgeTo := fullFrom
		if to.Before(edgeTo) {
			edgeTo = to
		}
		s.addIndexedDetailsProjectedLocked(result, TimeRange{From: from, To: edgeTo}, projection)
	}
	s.mergeAggregateBucketsProjectedLocked(result, buckets, fullFrom, fullTo, step, projection)
	if fullTo.Before(to) {
		edgeFrom := fullTo
		if edgeFrom.Before(from) {
			edgeFrom = from
		}
		s.addIndexedDetailsProjectedLocked(result, TimeRange{From: edgeFrom, To: to}, projection)
	}
	return result
}

func (s *RequestStatistics) mergeAggregateBucketsProjectedLocked(result *usageAggregateBucket, buckets map[int64]*usageAggregateBucket, from, to time.Time, step time.Duration, projection usageAggregateProjection) {
	if result == nil || !from.Before(to) {
		return
	}
	steps := int64(to.Sub(from) / step)
	if steps <= int64(len(buckets))+32 {
		for bucketTime := from; bucketTime.Before(to); bucketTime = bucketTime.Add(step) {
			projection.merge(result, buckets[bucketTime.Unix()])
		}
		return
	}
	for key, bucket := range buckets {
		bucketTime := time.Unix(key, 0).UTC()
		if bucketTime.Before(from) || !bucketTime.Before(to) {
			continue
		}
		projection.merge(result, bucket)
	}
}

func (s *RequestStatistics) addIndexedDetailsProjectedLocked(target *usageAggregateBucket, timeRange TimeRange, projection usageAggregateProjection) {
	s.forEachIndexedDetailLocked(timeRange, func(apiName, modelName string, detail RequestDetail) {
		projection.add(target, apiName, modelName, detail)
	})
}

func summaryAggregateProjection(includeSources bool) usageAggregateProjection {
	return usageAggregateProjection{
		add: func(target *usageAggregateBucket, apiName, modelName string, detail RequestDetail) {
			target.Total.addDetail(detail)
			ensureAggregate(target.APIs, apiName).addDetail(detail)
			ensureAggregate(target.Models, modelName).addDetail(detail)
			ensureNestedAggregate(target.APIModels, apiName, modelName).addDetail(detail)
			if includeSources {
				source := strings.TrimSpace(detail.Source)
				if source == "" {
					source = "unknown"
				}
				ensureAggregate(target.Sources, source).addDetail(detail)
			}
		},
		merge: func(target, source *usageAggregateBucket) {
			if target == nil || source == nil {
				return
			}
			target.Total.merge(source.Total)
			mergeAggregateMap(target.APIs, source.APIs)
			mergeAggregateMap(target.Models, source.Models)
			mergeNestedAggregateMap(target.APIModels, source.APIModels)
			if includeSources {
				mergeAggregateMap(target.Sources, source.Sources)
			}
		},
	}
}

func authAggregateProjection() usageAggregateProjection {
	return usageAggregateProjection{
		add: func(target *usageAggregateBucket, _ string, modelName string, detail RequestDetail) {
			authIndex := strings.TrimSpace(detail.AuthIndex)
			if authIndex == "" {
				authIndex = "unknown"
			}
			ensureAggregate(target.Auths, authIndex).addDetail(detail)
			ensureNestedAggregate(target.AuthModels, authIndex, modelName).addDetail(detail)
		},
		merge: func(target, source *usageAggregateBucket) {
			if target == nil || source == nil {
				return
			}
			mergeAggregateMap(target.Auths, source.Auths)
			mergeNestedAggregateMap(target.AuthModels, source.AuthModels)
		},
	}
}

func sourceAggregateProjection() usageAggregateProjection {
	return usageAggregateProjection{
		add: func(target *usageAggregateBucket, _, _ string, detail RequestDetail) {
			source := strings.TrimSpace(detail.Source)
			if source == "" {
				source = "unknown"
			}
			ensureAggregate(target.Sources, source).addDetail(detail)
		},
		merge: func(target, source *usageAggregateBucket) {
			if target != nil && source != nil {
				mergeAggregateMap(target.Sources, source.Sources)
			}
		},
	}
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
		detail, location, ok := s.detailForIDLocked(ref.ID)
		if !ok {
			continue
		}
		visit(location.API, location.Model, detail)
	}
}

func (s *RequestStatistics) detailForIDLocked(detailID uint64) (RequestDetail, usageDetailLocation, bool) {
	location, ok := s.detailLocations[detailID]
	if !ok {
		return RequestDetail{}, usageDetailLocation{}, false
	}
	stats := s.apis[location.API]
	if stats == nil {
		return RequestDetail{}, usageDetailLocation{}, false
	}
	modelStatsValue := stats.Models[location.Model]
	if modelStatsValue == nil || location.Offset < 0 || location.Offset >= len(modelStatsValue.Details) {
		return RequestDetail{}, usageDetailLocation{}, false
	}
	detail := modelStatsValue.Details[location.Offset]
	if detail.internalID != detailID {
		return RequestDetail{}, usageDetailLocation{}, false
	}
	return detail, location, true
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
	requests := int64(0)
	if !detail.Auxiliary {
		requests = 1
		stats.Requests = saturatingAddInt64(stats.Requests, requests)
	}
	totalTokens := normaliseTokenStats(detail.Tokens).TotalTokens
	stats.Tokens = saturatingAddInt64(stats.Tokens, nonNegativeInt64(totalTokens))
	stats.Entries = append(stats.Entries, legacyUsageEntry{
		Timestamp: timestamp,
		Tokens:    totalTokens,
		Requests:  requests,
		AuthIndex: strings.TrimSpace(detail.AuthIndex),
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

func subtractNonNegativeInt64(value, decrement int64) int64 {
	decrement = nonNegativeInt64(decrement)
	if decrement >= value {
		return 0
	}
	return value - decrement
}
