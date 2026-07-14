package usage

import (
	"sort"
	"strings"
	"time"
)

const (
	DefaultRatesWindowMinutes = 30
	DefaultSparklineMinutes   = 60
	MaxUsageAnalyticsMinutes  = 1440
	MaxUsageAnalyticsBuckets  = 10000
	MaxUsageAnalyticsItems    = 10000
	DefaultHealthRange        = 7 * 24 * time.Hour
	DefaultTokenRange         = 30 * 24 * time.Hour
	HealthStateNoData         = "no_data"
	HealthStateHealthy        = "healthy"
	HealthStateDegraded       = "degraded"
	HealthStateUnhealthy      = "unhealthy"
)

type HealthQuery struct {
	TimeRange   TimeRange
	Bucket      string
	GroupBy     string
	AuthIndexes []string
	Sources     []string
}

type HealthResult struct {
	AsOf      time.Time     `json:"as_of"`
	From      time.Time     `json:"from"`
	To        time.Time     `json:"to"`
	Bucket    string        `json:"bucket"`
	GroupBy   string        `json:"group_by"`
	Truncated bool          `json:"truncated"`
	Items     []HealthEntry `json:"items"`
}

type HealthEntry struct {
	Bucket       time.Time `json:"bucket"`
	Group        string    `json:"group"`
	Requests     int64     `json:"requests"`
	SuccessCount int64     `json:"success_count"`
	FailureCount int64     `json:"failure_count"`
	SuccessRate  *float64  `json:"success_rate"`
	State        string    `json:"state"`
}

type RatesQuery struct {
	WindowMinutes    int
	SparklineMinutes int
}

type RatesResult struct {
	AsOf             time.Time   `json:"as_of"`
	WindowMinutes    int         `json:"window_minutes"`
	SparklineMinutes int         `json:"sparkline_minutes"`
	RequestCount     int64       `json:"request_count"`
	TokenCount       int64       `json:"token_count"`
	RPM              float64     `json:"rpm"`
	TPM              float64     `json:"tpm"`
	Items            []RateEntry `json:"items"`
}

type RateEntry struct {
	Bucket      time.Time `json:"bucket"`
	Requests    int64     `json:"requests"`
	TotalTokens int64     `json:"total_tokens"`
	RPM         float64   `json:"rpm"`
	TPM         float64   `json:"tpm"`
}

type TokenQuery struct {
	TimeRange TimeRange
	Bucket    string
	GroupBy   string
}

type TokenResult struct {
	AsOf        time.Time    `json:"as_of"`
	From        *time.Time   `json:"from"`
	To          *time.Time   `json:"to"`
	Bucket      string       `json:"bucket"`
	GroupBy     string       `json:"group_by"`
	Truncated   bool         `json:"truncated"`
	TotalTokens int64        `json:"total_tokens"`
	Tokens      TokenStats   `json:"tokens"`
	Items       []TokenEntry `json:"items"`
}

type TokenEntry struct {
	Bucket       time.Time  `json:"bucket"`
	Group        string     `json:"group"`
	Requests     int64      `json:"requests"`
	SuccessCount int64      `json:"success_count"`
	FailureCount int64      `json:"failure_count"`
	TotalTokens  int64      `json:"total_tokens"`
	Tokens       TokenStats `json:"tokens"`
}

func (s *RequestStatistics) Health(query HealthQuery) HealthResult {
	return s.healthAt(query, time.Now().UTC())
}

func (s *RequestStatistics) healthAt(query HealthQuery, now time.Time) HealthResult {
	query.Bucket = normalizeHealthBucket(query.Bucket)
	query.GroupBy = normalizeHealthGroupBy(query.GroupBy)
	step := healthBucketDuration(query.Bucket)
	if query.TimeRange.IsZero() {
		end := truncateAggregateTime(now, step).Add(step)
		query.TimeRange = TimeRange{From: end.Add(-DefaultHealthRange), To: end}
	} else {
		if query.TimeRange.To.IsZero() {
			query.TimeRange.To = now.UTC()
		}
		if query.TimeRange.From.IsZero() {
			query.TimeRange.From = query.TimeRange.To.Add(-DefaultHealthRange)
		}
	}
	result := HealthResult{
		AsOf:    now.UTC(),
		From:    query.TimeRange.From.UTC(),
		To:      query.TimeRange.To.UTC(),
		Bucket:  query.Bucket,
		GroupBy: query.GroupBy,
		Items:   []HealthEntry{},
	}
	if s == nil || result.From.IsZero() || result.To.IsZero() || !result.From.Before(result.To) {
		return result
	}

	authSet := stringSet(query.AuthIndexes)
	sourceSet := stringSet(query.Sources)
	s.pruneExpiredBuckets(now)
	s.mu.RLock()
	limitedRange, truncated := limitUsageRangeBuckets(TimeRange{From: result.From, To: result.To}, step, MaxUsageAnalyticsBuckets)
	result.From = limitedRange.From
	result.To = limitedRange.To
	result.Truncated = truncated
	resolvedAuths := sortedStringSet(authSet)
	resolvedSources := sortedStringSet(sourceSet)
	if query.GroupBy == GroupByAuthIndex && len(resolvedAuths) == 0 {
		overall := s.filteredAggregateLocked(limitedRange, authSet, sourceSet)
		resolvedAuths = sortedAggregateKeys(overall.Auths)
	}
	if query.GroupBy == GroupBySource && len(resolvedSources) == 0 {
		overall := s.filteredAggregateLocked(limitedRange, authSet, sourceSet)
		resolvedSources = sortedAggregateKeys(overall.Sources)
	}
	groupCount := 1
	if query.GroupBy == GroupByAuthIndex {
		groupCount = len(resolvedAuths)
	} else if query.GroupBy == GroupBySource {
		groupCount = len(resolvedSources)
	}
	if groupCount > MaxUsageAnalyticsItems {
		if query.GroupBy == GroupByAuthIndex {
			resolvedAuths = resolvedAuths[:MaxUsageAnalyticsItems]
		} else {
			resolvedSources = resolvedSources[:MaxUsageAnalyticsItems]
		}
		groupCount = MaxUsageAnalyticsItems
		result.Truncated = true
	}
	if groupCount > 0 {
		maxBuckets := MaxUsageAnalyticsItems / groupCount
		if maxBuckets > MaxUsageAnalyticsBuckets {
			maxBuckets = MaxUsageAnalyticsBuckets
		}
		limitedRange, truncated = limitUsageRangeBuckets(limitedRange, step, maxBuckets)
		result.From = limitedRange.From
		result.To = limitedRange.To
		result.Truncated = result.Truncated || truncated
	}
	for bucketStart := truncateAggregateTime(result.From, step); bucketStart.Before(result.To); bucketStart = bucketStart.Add(step) {
		bucketEnd := bucketStart.Add(step)
		interval := TimeRange{From: bucketStart, To: bucketEnd}
		if result.From.After(interval.From) {
			interval.From = result.From
		}
		if result.To.Before(interval.To) {
			interval.To = result.To
		}
		aggregate := s.filteredAggregateLocked(interval, authSet, sourceSet)
		appendHealthEntries(&result.Items, bucketStart, query.GroupBy, aggregate, resolvedAuths, resolvedSources)
	}
	s.mu.RUnlock()
	return result
}

func (s *RequestStatistics) Rates(query RatesQuery) RatesResult {
	return s.ratesAt(query, time.Now().UTC())
}

func (s *RequestStatistics) ratesAt(query RatesQuery, now time.Time) RatesResult {
	query.WindowMinutes = normalizeAnalyticsMinutes(query.WindowMinutes, DefaultRatesWindowMinutes)
	query.SparklineMinutes = normalizeAnalyticsMinutes(query.SparklineMinutes, DefaultSparklineMinutes)
	result := RatesResult{
		AsOf:             now.UTC(),
		WindowMinutes:    query.WindowMinutes,
		SparklineMinutes: query.SparklineMinutes,
		Items:            make([]RateEntry, 0, query.SparklineMinutes),
	}
	if s == nil {
		return result
	}
	now = now.UTC()

	s.pruneExpiredBuckets(now)
	s.mu.RLock()
	window := s.aggregateRangeLocked(TimeRange{
		From: now.Add(-time.Duration(query.WindowMinutes) * time.Minute),
		To:   now,
	})
	result.RequestCount = window.Total.TotalRequests
	result.TokenCount = window.Total.TotalTokens
	result.RPM = float64(result.RequestCount) / float64(query.WindowMinutes)
	result.TPM = float64(result.TokenCount) / float64(query.WindowMinutes)
	minuteStart := now.Truncate(time.Minute)
	sparklineStart := minuteStart.Add(-time.Duration(query.SparklineMinutes-1) * time.Minute)
	if now.Equal(minuteStart) {
		sparklineStart = now.Add(-time.Duration(query.SparklineMinutes) * time.Minute)
	}
	for bucketStart := sparklineStart; bucketStart.Before(now); bucketStart = bucketStart.Add(time.Minute) {
		bucketEnd := bucketStart.Add(time.Minute)
		if bucketEnd.After(now) {
			bucketEnd = now
		}
		bucket := s.aggregateRangeLocked(TimeRange{From: bucketStart, To: bucketEnd})
		result.Items = append(result.Items, RateEntry{
			Bucket:      bucketStart,
			Requests:    bucket.Total.TotalRequests,
			TotalTokens: bucket.Total.TotalTokens,
			RPM:         float64(bucket.Total.TotalRequests),
			TPM:         float64(bucket.Total.TotalTokens),
		})
	}
	s.mu.RUnlock()
	return result
}

func (s *RequestStatistics) TokensForQuery(query TokenQuery) TokenResult {
	query.Bucket = normalizeTokenBucket(query.Bucket)
	query.GroupBy = normalizeTokenGroupBy(query.GroupBy)
	result := TokenResult{
		AsOf:    time.Now().UTC(),
		Bucket:  query.Bucket,
		GroupBy: query.GroupBy,
		Items:   []TokenEntry{},
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
	timeRange, result.Truncated = limitUsageRangeBuckets(timeRange, step, MaxUsageAnalyticsBuckets)
	result.From = usageTimePointer(timeRange.From)
	result.To = usageTimePointer(timeRange.To)
	if timeRange.From.IsZero() || timeRange.To.IsZero() || !timeRange.From.Before(timeRange.To) {
		s.mu.RUnlock()
		return result
	}

	overall := s.aggregateRangeLocked(timeRange)
	result.Tokens = overall.Total.Tokens
	result.TotalTokens = result.Tokens.TotalTokens
	for bucketStart := truncateAggregateTime(timeRange.From, step); bucketStart.Before(timeRange.To); bucketStart = bucketStart.Add(step) {
		bucketEnd := bucketStart.Add(step)
		interval := TimeRange{From: bucketStart, To: bucketEnd}
		if timeRange.From.After(interval.From) {
			interval.From = timeRange.From
		}
		if timeRange.To.Before(interval.To) {
			interval.To = timeRange.To
		}
		aggregate := s.aggregateRangeLocked(interval)
		if appendTokenEntries(&result.Items, bucketStart, query.GroupBy, aggregate, MaxUsageAnalyticsItems) {
			result.Truncated = true
			break
		}
	}
	s.mu.RUnlock()
	return result
}

func (s *RequestStatistics) filteredAggregateLocked(timeRange TimeRange, authSet, sourceSet map[string]struct{}) *usageAggregateBucket {
	aggregate := s.aggregateRangeLocked(timeRange)
	if len(authSet) == 0 && len(sourceSet) == 0 {
		return aggregate
	}
	result := newUsageAggregateBucket()
	for authIndex, sources := range aggregate.AuthSources {
		if len(authSet) > 0 {
			if _, ok := authSet[authIndex]; !ok {
				continue
			}
		}
		for source, stats := range sources {
			if stats == nil {
				continue
			}
			if len(sourceSet) > 0 {
				if _, ok := sourceSet[source]; !ok {
					continue
				}
			}
			result.Total.merge(*stats)
			ensureAggregate(result.Auths, authIndex).merge(*stats)
			ensureAggregate(result.Sources, source).merge(*stats)
			ensureNestedAggregate(result.AuthSources, authIndex, source).merge(*stats)
		}
	}
	return result
}

func appendHealthEntries(items *[]HealthEntry, bucket time.Time, groupBy string, aggregate *usageAggregateBucket, requestedAuths, requestedSources []string) {
	if items == nil || aggregate == nil {
		return
	}
	if groupBy == GroupByNone {
		*items = append(*items, healthEntry(bucket, "", &aggregate.Total))
		return
	}
	groups := aggregate.Auths
	requested := requestedAuths
	if groupBy == GroupBySource {
		groups = aggregate.Sources
		requested = requestedSources
	}
	keys := make([]string, 0, len(groups)+len(requested))
	seen := make(map[string]struct{}, len(groups)+len(requested))
	for _, key := range requested {
		key = strings.TrimSpace(key)
		if key != "" {
			seen[key] = struct{}{}
		}
	}
	if len(seen) == 0 {
		for key := range groups {
			seen[key] = struct{}{}
		}
	}
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, group := range keys {
		*items = append(*items, healthEntry(bucket, group, groups[group]))
	}
}

func sortedAggregateKeys(values map[string]*aggregateStats) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedStringSet(values map[string]struct{}) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func healthEntry(bucket time.Time, group string, stats *aggregateStats) HealthEntry {
	entry := HealthEntry{Bucket: bucket, Group: group, State: HealthStateNoData}
	if stats == nil || stats.TotalRequests == 0 {
		return entry
	}
	entry.Requests = stats.TotalRequests
	entry.SuccessCount = stats.SuccessCount
	entry.FailureCount = stats.FailureCount
	rate := float64(stats.SuccessCount) / float64(stats.TotalRequests)
	entry.SuccessRate = &rate
	switch {
	case stats.FailureCount == 0:
		entry.State = HealthStateHealthy
	case stats.SuccessCount == 0:
		entry.State = HealthStateUnhealthy
	default:
		entry.State = HealthStateDegraded
	}
	return entry
}

func appendTokenEntries(items *[]TokenEntry, bucket time.Time, groupBy string, aggregate *usageAggregateBucket, maxItems int) bool {
	if items == nil || aggregate == nil || aggregate.Total.TotalRequests == 0 {
		return false
	}
	if groupBy == GroupByNone {
		if len(*items) >= maxItems {
			return true
		}
		*items = append(*items, tokenEntry(bucket, "", &aggregate.Total))
		return false
	}
	groups := aggregate.Models
	if groupBy == GroupByAPI {
		groups = aggregate.APIs
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, group := range keys {
		if stats := groups[group]; stats != nil && stats.TotalRequests > 0 {
			if len(*items) >= maxItems {
				return true
			}
			*items = append(*items, tokenEntry(bucket, group, stats))
		}
	}
	return false
}

func tokenEntry(bucket time.Time, group string, stats *aggregateStats) TokenEntry {
	if stats == nil {
		return TokenEntry{Bucket: bucket, Group: group}
	}
	return TokenEntry{
		Bucket:       bucket,
		Group:        group,
		Requests:     stats.TotalRequests,
		SuccessCount: stats.SuccessCount,
		FailureCount: stats.FailureCount,
		TotalTokens:  stats.TotalTokens,
		Tokens:       stats.Tokens,
	}
}

func normalizeAnalyticsMinutes(value, fallback int) int {
	if value <= 0 {
		return fallback
	}
	if value > MaxUsageAnalyticsMinutes {
		return MaxUsageAnalyticsMinutes
	}
	return value
}

func normalizeHealthBucket(bucket string) string {
	switch strings.TrimSpace(bucket) {
	case BucketHour:
		return BucketHour
	case BucketDay:
		return BucketDay
	default:
		return Bucket15Min
	}
}

func normalizeHealthGroupBy(groupBy string) string {
	switch strings.TrimSpace(groupBy) {
	case GroupByAuthIndex:
		return GroupByAuthIndex
	case GroupBySource:
		return GroupBySource
	default:
		return GroupByNone
	}
}

func normalizeTokenBucket(bucket string) string {
	if strings.TrimSpace(bucket) == BucketHour {
		return BucketHour
	}
	return BucketDay
}

func normalizeTokenGroupBy(groupBy string) string {
	switch strings.TrimSpace(groupBy) {
	case GroupByModel:
		return GroupByModel
	case GroupByAPI:
		return GroupByAPI
	default:
		return GroupByNone
	}
}

func healthBucketDuration(bucket string) time.Duration {
	switch bucket {
	case BucketHour:
		return time.Hour
	case BucketDay:
		return 24 * time.Hour
	default:
		return 15 * time.Minute
	}
}

func limitUsageRangeBuckets(timeRange TimeRange, step time.Duration, maxBuckets int) (TimeRange, bool) {
	if maxBuckets <= 0 || step <= 0 || timeRange.From.IsZero() || timeRange.To.IsZero() || !timeRange.From.Before(timeRange.To) {
		return timeRange, false
	}
	lastBucket := truncateAggregateTime(timeRange.To.Add(-time.Nanosecond), step)
	earliest := lastBucket.Add(-time.Duration(maxBuckets-1) * step)
	if !timeRange.From.Before(earliest) {
		return timeRange, false
	}
	timeRange.From = earliest
	return timeRange, true
}
