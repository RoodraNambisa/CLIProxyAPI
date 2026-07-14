package usage

import (
	"sort"
	"strings"
	"time"
)

const (
	DefaultDetailsLimit = 200
	MaxDetailsLimit     = 1000

	SortByCreatedAt = "created_at"
	SortByTokens    = "tokens"
	SortByModel     = "model"
	SortByAPI       = "api"
	SortByAuthIndex = "auth_index"

	SortOrderDesc = "desc"
	SortOrderAsc  = "asc"

	BucketMinute = "minute"
	Bucket15Min  = "15m"
	BucketHour   = "hour"
	BucketDay    = "day"

	GroupByAPI       = "api"
	GroupByNone      = "none"
	GroupByModel     = "model"
	GroupByAuthIndex = "auth_index"
	GroupBySource    = "source"
	GroupByFailed    = "failed"
)

// TimeRange limits usage queries by request timestamp.
type TimeRange struct {
	From time.Time
	To   time.Time
}

// IsZero reports whether the range has no lower or upper bound.
func (r TimeRange) IsZero() bool {
	return r.From.IsZero() && r.To.IsZero()
}

func (r TimeRange) contains(timestamp time.Time) bool {
	if !r.From.IsZero() && timestamp.Before(r.From) {
		return false
	}
	if !r.To.IsZero() && !timestamp.Before(r.To) {
		return false
	}
	return true
}

// MetaSnapshot is the smallest usage statistics view for change detection.
type MetaSnapshot struct {
	Version       uint64     `json:"version"`
	Enabled       bool       `json:"enabled"`
	Available     bool       `json:"available"`
	AsOf          time.Time  `json:"as_of"`
	OldestAt      *time.Time `json:"oldest_at"`
	NewestAt      *time.Time `json:"newest_at"`
	TotalRequests int64      `json:"total_requests"`
	SuccessCount  int64      `json:"success_count"`
	FailureCount  int64      `json:"failure_count"`
	TotalTokens   int64      `json:"total_tokens"`
}

// SummarySnapshot mirrors StatisticsSnapshot without per-request details.
type SummarySnapshot struct {
	Version       uint64     `json:"version"`
	TotalRequests int64      `json:"total_requests"`
	SuccessCount  int64      `json:"success_count"`
	FailureCount  int64      `json:"failure_count"`
	TotalTokens   int64      `json:"total_tokens"`
	Tokens        TokenStats `json:"tokens"`

	APIs    map[string]APISummarySnapshot   `json:"apis"`
	Models  map[string]ModelSummarySnapshot `json:"models"`
	Sources map[string]ModelSummarySnapshot `json:"sources"`

	RequestsByDay  map[string]int64 `json:"requests_by_day"`
	RequestsByHour map[string]int64 `json:"requests_by_hour"`
	TokensByDay    map[string]int64 `json:"tokens_by_day"`
	TokensByHour   map[string]int64 `json:"tokens_by_hour"`
}

// APISummarySnapshot summarises metrics for a single API without details.
type APISummarySnapshot struct {
	TotalRequests int64                           `json:"total_requests"`
	SuccessCount  int64                           `json:"success_count"`
	FailureCount  int64                           `json:"failure_count"`
	TotalTokens   int64                           `json:"total_tokens"`
	Tokens        TokenStats                      `json:"tokens"`
	LastUsedAt    *time.Time                      `json:"last_used_at"`
	Models        map[string]ModelSummarySnapshot `json:"models"`
}

// ModelSummarySnapshot summarises metrics for a model without details.
type ModelSummarySnapshot struct {
	TotalRequests int64      `json:"total_requests"`
	SuccessCount  int64      `json:"success_count"`
	FailureCount  int64      `json:"failure_count"`
	TotalTokens   int64      `json:"total_tokens"`
	Tokens        TokenStats `json:"tokens"`
	LastUsedAt    *time.Time `json:"last_used_at"`
}

// AuthUsageSnapshot summarises metrics for a single auth credential.
type AuthUsageSnapshot struct {
	AuthIndex     string     `json:"auth_index"`
	TotalRequests int64      `json:"total_requests"`
	SuccessCount  int64      `json:"success_count"`
	FailureCount  int64      `json:"failure_count"`
	TotalTokens   int64      `json:"total_tokens"`
	Tokens        TokenStats `json:"tokens"`
	LastUsedAt    *time.Time `json:"last_used_at"`
	ModelCount    int        `json:"model_count"`
}

// AuthModelUsageSnapshot summarises one model under a single auth credential.
type AuthModelUsageSnapshot struct {
	Model         string     `json:"model"`
	TotalRequests int64      `json:"total_requests"`
	SuccessCount  int64      `json:"success_count"`
	FailureCount  int64      `json:"failure_count"`
	TotalTokens   int64      `json:"total_tokens"`
	Tokens        TokenStats `json:"tokens"`
	LastUsedAt    *time.Time `json:"last_used_at"`
}

// AuthUsageQuery filters auth usage summaries.
type AuthUsageQuery struct {
	TimeRange   TimeRange
	AuthIndexes []string
}

// DetailQuery filters usage request details.
type DetailQuery struct {
	API       string
	Model     string
	AuthIndex string
	Source    string
	ClientIP  string
	Failed    *bool
	TimeRange TimeRange
	SortBy    string
	SortOrder string
	Offset    int
	Limit     int
}

// DetailEntry is a request detail with its API and model keys.
type DetailEntry struct {
	API   string `json:"api"`
	Model string `json:"model"`
	RequestDetail
}

// DetailPage is a filtered and paginated detail response.
type DetailPage struct {
	Items        []DetailEntry `json:"items"`
	Details      []DetailEntry `json:"details"`
	Total        int           `json:"total"`
	Offset       int           `json:"offset"`
	Limit        int           `json:"limit"`
	NextOffset   int           `json:"next_offset,omitempty"`
	HasMore      bool          `json:"has_more"`
	TotalMatched int           `json:"total_matched"`
}

// SeriesQuery filters and groups time-series usage data.
type SeriesQuery struct {
	TimeRange TimeRange
	Bucket    string
	GroupBy   string
}

// SeriesResult is a flat list of time bucket aggregates.
type SeriesResult struct {
	Bucket    string        `json:"bucket"`
	GroupBy   string        `json:"group_by"`
	Truncated bool          `json:"truncated"`
	Items     []SeriesEntry `json:"items"`
}

// SeriesEntry is one grouped aggregate inside one time bucket.
type SeriesEntry struct {
	Bucket        time.Time  `json:"bucket"`
	Group         string     `json:"group"`
	TotalRequests int64      `json:"requests"`
	SuccessCount  int64      `json:"success_count"`
	FailureCount  int64      `json:"failure_count"`
	TotalTokens   int64      `json:"total_tokens"`
	Tokens        TokenStats `json:"tokens"`
}

// Meta returns a lightweight statistics metadata snapshot.
func (s *RequestStatistics) Meta() MetaSnapshot {
	now := time.Now().UTC()
	if s == nil {
		return MetaSnapshot{Enabled: StatisticsEnabled(), AsOf: now}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return MetaSnapshot{
		Version:       s.changeCount,
		Enabled:       StatisticsEnabled(),
		Available:     true,
		AsOf:          now,
		OldestAt:      usageTimePointer(s.oldestAt),
		NewestAt:      usageTimePointer(s.newestAt),
		TotalRequests: s.totalRequests,
		SuccessCount:  s.successCount,
		FailureCount:  s.failureCount,
		TotalTokens:   s.totalTokens,
	}
}

// Summary returns aggregated usage statistics without request details.
func (s *RequestStatistics) Summary() SummarySnapshot {
	result := newSummarySnapshot(0)
	if s == nil {
		return result
	}
	s.pruneExpiredBuckets(time.Now().UTC())
	s.mu.RLock()
	defer s.mu.RUnlock()

	result = summarySnapshotFromAggregate(s.changeCount, s.allTimeAggregateLocked())
	result.RequestsByDay = cloneStringIntMap(s.requestsByDay)
	result.RequestsByHour = cloneHourIntMap(s.requestsByHour)
	result.TokensByDay = cloneStringIntMap(s.tokensByDay)
	result.TokensByHour = cloneHourIntMap(s.tokensByHour)
	return result
}

// SummaryForRange returns aggregated usage statistics within a time range.
func (s *RequestStatistics) SummaryForRange(timeRange TimeRange) SummarySnapshot {
	if timeRange.IsZero() {
		return s.Summary()
	}
	result := newSummarySnapshot(0)
	if s == nil {
		return result
	}

	s.pruneExpiredBuckets(time.Now().UTC())
	s.mu.RLock()
	result = summarySnapshotFromAggregate(s.changeCount, s.aggregateRangeLocked(timeRange))
	s.fillLegacyRangeMapsLocked(&result, timeRange)
	s.mu.RUnlock()
	return result
}

func (s *RequestStatistics) fillLegacyRangeMapsLocked(result *SummarySnapshot, timeRange TimeRange) {
	if result == nil || s.oldestAt.IsZero() || s.newestAt.IsZero() {
		return
	}
	if (timeRange.From.IsZero() || !timeRange.From.After(s.oldestAt)) &&
		(timeRange.To.IsZero() || timeRange.To.After(s.newestAt)) {
		result.RequestsByDay = cloneStringIntMap(s.requestsByDay)
		result.RequestsByHour = cloneHourIntMap(s.requestsByHour)
		result.TokensByDay = cloneStringIntMap(s.tokensByDay)
		result.TokensByHour = cloneHourIntMap(s.tokensByHour)
		return
	}
	effective := clampUsageRangeToBounds(timeRange, s.oldestAt, s.newestAt)
	if effective.From.IsZero() || effective.To.IsZero() || !effective.From.Before(effective.To) {
		return
	}
	for key, stats := range s.legacyHourBuckets {
		if stats == nil {
			continue
		}
		bucketStart := time.Unix(key.StartUnix, 0)
		bucketEnd := bucketStart.Add(time.Hour)
		if !bucketStart.Before(effective.From) && !bucketEnd.After(effective.To) {
			addLegacyRangeStats(result, key.Day, key.Hour, stats.Requests, stats.Tokens)
			continue
		}
		if !bucketStart.Before(effective.To) || !bucketEnd.After(effective.From) {
			continue
		}
		for _, entry := range stats.Entries {
			if effective.contains(entry.Timestamp) {
				addLegacyRangeStats(result, key.Day, key.Hour, 1, entry.Tokens)
			}
		}
	}
}

func addLegacyRangeStats(result *SummarySnapshot, day string, hour int, requests, tokens int64) {
	result.RequestsByDay[day] += requests
	result.RequestsByHour[formatHour(hour)] += requests
	result.TokensByDay[day] += tokens
	result.TokensByHour[formatHour(hour)] += tokens
}

func clampUsageRangeToBounds(timeRange TimeRange, oldest, newest time.Time) TimeRange {
	if oldest.IsZero() || newest.IsZero() || newest.Before(oldest) {
		return TimeRange{}
	}
	result := timeRange
	if result.From.IsZero() || result.From.Before(oldest) {
		result.From = oldest
	}
	newestExclusive := newest.Add(time.Nanosecond)
	if result.To.IsZero() || result.To.After(newestExclusive) {
		result.To = newestExclusive
	}
	if !result.From.Before(result.To) {
		return TimeRange{}
	}
	return result
}

// AuthSummaries returns usage summaries for auth indexes with recorded usage.
func (s *RequestStatistics) AuthSummaries() []AuthUsageSnapshot {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]AuthUsageSnapshot, 0, len(s.auths))
	for authIndex, stats := range s.auths {
		if stats == nil {
			continue
		}
		out = append(out, authUsageSnapshot(authIndex, stats))
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].AuthIndex < out[j].AuthIndex
	})
	return out
}

// AuthSummariesForQuery returns usage summaries for auth indexes matching the query.
func (s *RequestStatistics) AuthSummariesForQuery(query AuthUsageQuery) []AuthUsageSnapshot {
	if s == nil {
		return nil
	}
	authSet := stringSet(query.AuthIndexes)
	if query.TimeRange.IsZero() && len(authSet) == 0 {
		return s.AuthSummaries()
	}

	s.pruneExpiredBuckets(time.Now().UTC())
	s.mu.RLock()
	aggregates := s.aggregateRangeLocked(query.TimeRange)
	out := make([]AuthUsageSnapshot, 0, len(aggregates.Auths))
	for authIndex, stats := range aggregates.Auths {
		if stats == nil || authIndex == "unknown" {
			continue
		}
		if len(authSet) > 0 {
			if _, ok := authSet[authIndex]; !ok {
				continue
			}
		}
		out = append(out, authUsageSnapshotFromAggregate(authIndex, stats, aggregates.AuthModels[authIndex]))
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		return out[i].AuthIndex < out[j].AuthIndex
	})
	return out
}

// AuthSummary returns the usage summary for one auth index.
func (s *RequestStatistics) AuthSummary(authIndex string) (AuthUsageSnapshot, bool) {
	authIndex = strings.TrimSpace(authIndex)
	if s == nil || authIndex == "" {
		return AuthUsageSnapshot{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	stats, ok := s.auths[authIndex]
	if !ok || stats == nil {
		return AuthUsageSnapshot{}, false
	}
	return authUsageSnapshot(authIndex, stats), true
}

// AuthModelSummaries returns per-model usage summaries for one auth index.
func (s *RequestStatistics) AuthModelSummaries(authIndex string) ([]AuthModelUsageSnapshot, bool) {
	authIndex = strings.TrimSpace(authIndex)
	if s == nil || authIndex == "" {
		return nil, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	stats, ok := s.auths[authIndex]
	if !ok || stats == nil {
		return nil, false
	}
	out := make([]AuthModelUsageSnapshot, 0, len(stats.Models))
	for model, modelStatsValue := range stats.Models {
		if modelStatsValue == nil {
			continue
		}
		out = append(out, AuthModelUsageSnapshot{
			Model:         model,
			TotalRequests: modelStatsValue.TotalRequests,
			SuccessCount:  modelStatsValue.SuccessCount,
			FailureCount:  modelStatsValue.FailureCount,
			TotalTokens:   modelStatsValue.TotalTokens,
			Tokens:        modelStatsValue.Tokens,
			LastUsedAt:    usageTimePointer(modelStatsValue.LastUsedAt),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Model < out[j].Model
	})
	return out, true
}

// Details returns filtered request details in reverse chronological order.
func (s *RequestStatistics) Details(query DetailQuery) DetailPage {
	if s == nil {
		query = normalizeDetailQuery(query)
		return newDetailPage(nil, 0, query.Offset, query.Limit)
	}
	query = normalizeDetailQuery(query)

	s.mu.RLock()
	matches := make([]DetailEntry, 0)
	for apiName, stats := range s.apis {
		if stats == nil {
			continue
		}
		if query.API != "" && apiName != query.API {
			continue
		}
		for modelName, modelStatsValue := range stats.Models {
			if modelStatsValue == nil {
				continue
			}
			if query.Model != "" && modelName != query.Model {
				continue
			}
			for _, detail := range modelStatsValue.Details {
				if !detailMatchesQuery(detail, query) {
					continue
				}
				matches = append(matches, DetailEntry{
					API:           apiName,
					Model:         modelName,
					RequestDetail: detail,
				})
			}
		}
	}
	s.mu.RUnlock()

	sortDetailEntries(matches, query.SortBy, query.SortOrder)

	total := len(matches)
	if query.Offset >= total {
		return newDetailPage([]DetailEntry{}, total, query.Offset, query.Limit)
	}
	end := query.Offset + query.Limit
	if end > total {
		end = total
	}
	return newDetailPage(matches[query.Offset:end], total, query.Offset, query.Limit)
}

// Series returns grouped time-series usage aggregates.
func (s *RequestStatistics) Series(query SeriesQuery) SeriesResult {
	query.Bucket = normalizeSeriesBucket(query.Bucket)
	query.GroupBy = normalizeSeriesGroupBy(query.GroupBy)
	result := SeriesResult{
		Bucket:  query.Bucket,
		GroupBy: query.GroupBy,
		Items:   []SeriesEntry{},
	}
	if s == nil {
		return result
	}

	s.pruneExpiredBuckets(time.Now().UTC())
	s.mu.RLock()
	timeRange := query.TimeRange
	if timeRange.From.IsZero() {
		timeRange.From = s.oldestAt
	}
	if timeRange.To.IsZero() && !s.newestAt.IsZero() {
		timeRange.To = s.newestAt.Add(time.Nanosecond)
	}
	timeRange = clampUsageRangeToBounds(timeRange, s.oldestAt, s.newestAt)
	if timeRange.From.IsZero() || timeRange.To.IsZero() || !timeRange.From.Before(timeRange.To) {
		s.mu.RUnlock()
		return result
	}
	step := usageBucketDuration(query.Bucket)
	timeRange, result.Truncated = limitUsageRangeBuckets(timeRange, step, MaxUsageAnalyticsBuckets)
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
		if appendSeriesAggregate(&result.Items, bucketStart, query.GroupBy, aggregate, MaxUsageAnalyticsItems) {
			result.Truncated = true
			break
		}
	}
	s.mu.RUnlock()
	return result
}

func appendSeriesAggregate(items *[]SeriesEntry, bucket time.Time, groupBy string, aggregate *usageAggregateBucket, maxItems int) bool {
	if items == nil || aggregate == nil || aggregate.Total.TotalRequests == 0 {
		return false
	}
	if groupBy == GroupByFailed {
		if aggregate.Total.FailureCount > 0 {
			if len(*items) >= maxItems {
				return true
			}
			*items = append(*items, SeriesEntry{
				Bucket:        bucket,
				Group:         "failed",
				TotalRequests: aggregate.Total.FailureCount,
				FailureCount:  aggregate.Total.FailureCount,
				TotalTokens:   aggregate.Total.FailureTokens.TotalTokens,
				Tokens:        aggregate.Total.FailureTokens,
			})
		}
		if aggregate.Total.SuccessCount > 0 {
			if len(*items) >= maxItems {
				return true
			}
			*items = append(*items, SeriesEntry{
				Bucket:        bucket,
				Group:         "success",
				TotalRequests: aggregate.Total.SuccessCount,
				SuccessCount:  aggregate.Total.SuccessCount,
				TotalTokens:   aggregate.Total.SuccessTokens.TotalTokens,
				Tokens:        aggregate.Total.SuccessTokens,
			})
		}
		return false
	}
	groups := aggregate.Models
	switch groupBy {
	case GroupByAPI:
		groups = aggregate.APIs
	case GroupByAuthIndex:
		groups = aggregate.Auths
	case GroupBySource:
		groups = aggregate.Sources
	}
	keys := make([]string, 0, len(groups))
	for group := range groups {
		keys = append(keys, group)
	}
	sort.Strings(keys)
	for _, group := range keys {
		stats := groups[group]
		if stats == nil || stats.TotalRequests == 0 {
			continue
		}
		if len(*items) >= maxItems {
			return true
		}
		*items = append(*items, SeriesEntry{
			Bucket:        bucket,
			Group:         group,
			TotalRequests: stats.TotalRequests,
			SuccessCount:  stats.SuccessCount,
			FailureCount:  stats.FailureCount,
			TotalTokens:   stats.TotalTokens,
			Tokens:        stats.Tokens,
		})
	}
	return false
}

func usageBucketDuration(bucket string) time.Duration {
	switch bucket {
	case BucketMinute:
		return time.Minute
	case BucketDay:
		return 24 * time.Hour
	default:
		return time.Hour
	}
}

func authUsageSnapshot(authIndex string, stats *authStats) AuthUsageSnapshot {
	return AuthUsageSnapshot{
		AuthIndex:     authIndex,
		TotalRequests: stats.TotalRequests,
		SuccessCount:  stats.SuccessCount,
		FailureCount:  stats.FailureCount,
		TotalTokens:   stats.TotalTokens,
		Tokens:        stats.Tokens,
		LastUsedAt:    usageTimePointer(stats.LastUsedAt),
		ModelCount:    len(stats.Models),
	}
}

func detailMatchesQuery(detail RequestDetail, query DetailQuery) bool {
	if !query.TimeRange.contains(detail.Timestamp) {
		return false
	}
	if query.AuthIndex != "" && strings.TrimSpace(detail.AuthIndex) != query.AuthIndex {
		return false
	}
	if query.Source != "" && strings.TrimSpace(detail.Source) != query.Source {
		return false
	}
	if query.ClientIP != "" && strings.TrimSpace(detail.ClientIP) != query.ClientIP {
		return false
	}
	if query.Failed != nil && detail.Failed != *query.Failed {
		return false
	}
	return true
}

func newSummarySnapshot(version uint64) SummarySnapshot {
	return SummarySnapshot{
		Version:        version,
		APIs:           map[string]APISummarySnapshot{},
		Models:         map[string]ModelSummarySnapshot{},
		Sources:        map[string]ModelSummarySnapshot{},
		RequestsByDay:  map[string]int64{},
		RequestsByHour: map[string]int64{},
		TokensByDay:    map[string]int64{},
		TokensByHour:   map[string]int64{},
	}
}

func summarySnapshotFromAggregate(version uint64, aggregate *usageAggregateBucket) SummarySnapshot {
	result := newSummarySnapshot(version)
	if aggregate == nil {
		return result
	}
	result.TotalRequests = aggregate.Total.TotalRequests
	result.SuccessCount = aggregate.Total.SuccessCount
	result.FailureCount = aggregate.Total.FailureCount
	result.TotalTokens = aggregate.Total.TotalTokens
	result.Tokens = aggregate.Total.Tokens
	for apiName, stats := range aggregate.APIs {
		if stats == nil {
			continue
		}
		apiSnapshot := APISummarySnapshot{
			TotalRequests: stats.TotalRequests,
			SuccessCount:  stats.SuccessCount,
			FailureCount:  stats.FailureCount,
			TotalTokens:   stats.TotalTokens,
			Tokens:        stats.Tokens,
			LastUsedAt:    usageTimePointer(stats.LastUsedAt),
			Models:        map[string]ModelSummarySnapshot{},
		}
		for modelName, modelStatsValue := range aggregate.APIModels[apiName] {
			if modelStatsValue != nil {
				apiSnapshot.Models[modelName] = modelSummarySnapshotFromAggregate(modelStatsValue)
			}
		}
		result.APIs[apiName] = apiSnapshot
	}
	for modelName, stats := range aggregate.Models {
		if stats != nil {
			result.Models[modelName] = modelSummarySnapshotFromAggregate(stats)
		}
	}
	for source, stats := range aggregate.Sources {
		if stats != nil {
			result.Sources[source] = modelSummarySnapshotFromAggregate(stats)
		}
	}
	return result
}

func modelSummarySnapshotFromAggregate(stats *aggregateStats) ModelSummarySnapshot {
	if stats == nil {
		return ModelSummarySnapshot{}
	}
	return ModelSummarySnapshot{
		TotalRequests: stats.TotalRequests,
		SuccessCount:  stats.SuccessCount,
		FailureCount:  stats.FailureCount,
		TotalTokens:   stats.TotalTokens,
		Tokens:        stats.Tokens,
		LastUsedAt:    usageTimePointer(stats.LastUsedAt),
	}
}

func authUsageSnapshotFromAggregate(authIndex string, stats *aggregateStats, models map[string]*aggregateStats) AuthUsageSnapshot {
	if stats == nil {
		return AuthUsageSnapshot{AuthIndex: authIndex}
	}
	return AuthUsageSnapshot{
		AuthIndex:     authIndex,
		TotalRequests: stats.TotalRequests,
		SuccessCount:  stats.SuccessCount,
		FailureCount:  stats.FailureCount,
		TotalTokens:   stats.TotalTokens,
		Tokens:        stats.Tokens,
		LastUsedAt:    usageTimePointer(stats.LastUsedAt),
		ModelCount:    len(models),
	}
}

func usageTimePointer(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	result := value.UTC()
	return &result
}

func updateAuthStatsMap(auths map[string]*authStats, model string, detail RequestDetail) {
	if auths == nil {
		return
	}
	authIndex := strings.TrimSpace(detail.AuthIndex)
	if authIndex == "" {
		return
	}
	authStatsValue, ok := auths[authIndex]
	if !ok {
		authStatsValue = &authStats{Models: make(map[string]*authModelStats)}
		auths[authIndex] = authStatsValue
	}
	updateUsageAggregate(
		&authStatsValue.TotalRequests,
		&authStatsValue.SuccessCount,
		&authStatsValue.FailureCount,
		&authStatsValue.TotalTokens,
		&authStatsValue.Tokens,
		detail,
	)
	if authStatsValue.LastUsedAt.IsZero() || detail.Timestamp.After(authStatsValue.LastUsedAt) {
		authStatsValue.LastUsedAt = detail.Timestamp
	}

	model = strings.TrimSpace(model)
	if model == "" {
		model = "unknown"
	}
	modelStatsValue, ok := authStatsValue.Models[model]
	if !ok {
		modelStatsValue = &authModelStats{}
		authStatsValue.Models[model] = modelStatsValue
	}
	updateUsageAggregate(
		&modelStatsValue.TotalRequests,
		&modelStatsValue.SuccessCount,
		&modelStatsValue.FailureCount,
		&modelStatsValue.TotalTokens,
		&modelStatsValue.Tokens,
		detail,
	)
	if modelStatsValue.LastUsedAt.IsZero() || detail.Timestamp.After(modelStatsValue.LastUsedAt) {
		modelStatsValue.LastUsedAt = detail.Timestamp
	}
}

func normalizeDetailQuery(query DetailQuery) DetailQuery {
	query.Offset = normalizeDetailsOffset(query.Offset)
	query.Limit = normalizeDetailsLimit(query.Limit)
	query.API = strings.TrimSpace(query.API)
	query.Model = strings.TrimSpace(query.Model)
	query.AuthIndex = strings.TrimSpace(query.AuthIndex)
	query.Source = strings.TrimSpace(query.Source)
	query.ClientIP = strings.TrimSpace(query.ClientIP)
	query.SortBy = normalizeDetailsSortBy(query.SortBy)
	query.SortOrder = normalizeDetailsSortOrder(query.SortOrder)
	return query
}

func normalizeDetailsOffset(offset int) int {
	if offset < 0 {
		return 0
	}
	return offset
}

func normalizeDetailsLimit(limit int) int {
	if limit <= 0 {
		return DefaultDetailsLimit
	}
	if limit > MaxDetailsLimit {
		return MaxDetailsLimit
	}
	return limit
}

func normalizeDetailsSortBy(sortBy string) string {
	switch strings.TrimSpace(sortBy) {
	case SortByTokens:
		return SortByTokens
	case SortByModel:
		return SortByModel
	case SortByAPI:
		return SortByAPI
	case SortByAuthIndex:
		return SortByAuthIndex
	default:
		return SortByCreatedAt
	}
}

func normalizeDetailsSortOrder(sortOrder string) string {
	if strings.TrimSpace(sortOrder) == SortOrderAsc {
		return SortOrderAsc
	}
	return SortOrderDesc
}

func sortDetailEntries(items []DetailEntry, sortBy, sortOrder string) {
	sort.Slice(items, func(i, j int) bool {
		cmp := compareDetailEntries(items[i], items[j], sortBy)
		if cmp == 0 {
			cmp = compareDetailEntries(items[i], items[j], SortByCreatedAt)
			if cmp == 0 {
				cmp = strings.Compare(items[i].API+"\x00"+items[i].Model+"\x00"+items[i].AuthIndex, items[j].API+"\x00"+items[j].Model+"\x00"+items[j].AuthIndex)
			}
		}
		if sortOrder == SortOrderAsc {
			return cmp < 0
		}
		return cmp > 0
	})
}

func compareDetailEntries(left, right DetailEntry, sortBy string) int {
	switch sortBy {
	case SortByTokens:
		return compareInt64(left.Tokens.TotalTokens, right.Tokens.TotalTokens)
	case SortByModel:
		return strings.Compare(left.Model, right.Model)
	case SortByAPI:
		return strings.Compare(left.API, right.API)
	case SortByAuthIndex:
		return strings.Compare(strings.TrimSpace(left.AuthIndex), strings.TrimSpace(right.AuthIndex))
	default:
		return left.Timestamp.Compare(right.Timestamp)
	}
}

func compareInt64(left, right int64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func newDetailPage(items []DetailEntry, total, offset, limit int) DetailPage {
	if items == nil {
		items = []DetailEntry{}
	}
	end := offset + len(items)
	hasMore := end < total
	nextOffset := 0
	if hasMore {
		nextOffset = end
	}
	return DetailPage{
		Items:        items,
		Details:      items,
		Total:        total,
		Offset:       offset,
		Limit:        limit,
		NextOffset:   nextOffset,
		HasMore:      hasMore,
		TotalMatched: total,
	}
}

func normalizeSeriesBucket(bucket string) string {
	switch strings.TrimSpace(bucket) {
	case BucketMinute:
		return BucketMinute
	case BucketDay:
		return BucketDay
	default:
		return BucketHour
	}
}

func normalizeSeriesGroupBy(groupBy string) string {
	switch strings.TrimSpace(groupBy) {
	case GroupByAPI:
		return GroupByAPI
	case GroupByAuthIndex:
		return GroupByAuthIndex
	case GroupBySource:
		return GroupBySource
	case GroupByFailed:
		return GroupByFailed
	default:
		return GroupByModel
	}
}

func truncateUsageBucket(timestamp time.Time, bucket string) time.Time {
	timestamp = timestamp.UTC()
	switch bucket {
	case BucketMinute:
		return timestamp.Truncate(time.Minute)
	case BucketDay:
		return time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), 0, 0, 0, 0, time.UTC)
	default:
		return timestamp.Truncate(time.Hour)
	}
}

func usageSeriesGroup(apiName, modelName string, detail RequestDetail, groupBy string) string {
	var group string
	switch groupBy {
	case GroupByAPI:
		group = apiName
	case GroupByAuthIndex:
		group = strings.TrimSpace(detail.AuthIndex)
	case GroupBySource:
		group = strings.TrimSpace(detail.Source)
	case GroupByFailed:
		if detail.Failed {
			return "failed"
		}
		return "success"
	default:
		group = modelName
	}
	if group == "" {
		return "unknown"
	}
	return group
}

func stringSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		out[value] = struct{}{}
	}
	return out
}

func cloneStringIntMap(values map[string]int64) map[string]int64 {
	if len(values) == 0 {
		return map[string]int64{}
	}
	out := make(map[string]int64, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

func cloneHourIntMap(values map[int]int64) map[string]int64 {
	if len(values) == 0 {
		return map[string]int64{}
	}
	out := make(map[string]int64, len(values))
	for hour, value := range values {
		out[formatHour(hour)] = value
	}
	return out
}
