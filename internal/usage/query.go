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
	BucketHour   = "hour"
	BucketDay    = "day"

	GroupByAPI       = "api"
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
	Version       uint64 `json:"version"`
	TotalRequests int64  `json:"total_requests"`
	SuccessCount  int64  `json:"success_count"`
	FailureCount  int64  `json:"failure_count"`
	TotalTokens   int64  `json:"total_tokens"`
}

// SummarySnapshot mirrors StatisticsSnapshot without per-request details.
type SummarySnapshot struct {
	Version       uint64 `json:"version"`
	TotalRequests int64  `json:"total_requests"`
	SuccessCount  int64  `json:"success_count"`
	FailureCount  int64  `json:"failure_count"`
	TotalTokens   int64  `json:"total_tokens"`

	APIs map[string]APISummarySnapshot `json:"apis"`

	RequestsByDay  map[string]int64 `json:"requests_by_day"`
	RequestsByHour map[string]int64 `json:"requests_by_hour"`
	TokensByDay    map[string]int64 `json:"tokens_by_day"`
	TokensByHour   map[string]int64 `json:"tokens_by_hour"`
}

// APISummarySnapshot summarises metrics for a single API without details.
type APISummarySnapshot struct {
	TotalRequests int64                           `json:"total_requests"`
	TotalTokens   int64                           `json:"total_tokens"`
	Models        map[string]ModelSummarySnapshot `json:"models"`
}

// ModelSummarySnapshot summarises metrics for a model without details.
type ModelSummarySnapshot struct {
	TotalRequests int64 `json:"total_requests"`
	TotalTokens   int64 `json:"total_tokens"`
}

// AuthUsageSnapshot summarises metrics for a single auth credential.
type AuthUsageSnapshot struct {
	AuthIndex     string     `json:"auth_index"`
	TotalRequests int64      `json:"total_requests"`
	SuccessCount  int64      `json:"success_count"`
	FailureCount  int64      `json:"failure_count"`
	TotalTokens   int64      `json:"total_tokens"`
	Tokens        TokenStats `json:"tokens"`
}

// AuthModelUsageSnapshot summarises one model under a single auth credential.
type AuthModelUsageSnapshot struct {
	Model         string     `json:"model"`
	TotalRequests int64      `json:"total_requests"`
	SuccessCount  int64      `json:"success_count"`
	FailureCount  int64      `json:"failure_count"`
	TotalTokens   int64      `json:"total_tokens"`
	Tokens        TokenStats `json:"tokens"`
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
	Bucket  string        `json:"bucket"`
	GroupBy string        `json:"group_by"`
	Items   []SeriesEntry `json:"items"`
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
	if s == nil {
		return MetaSnapshot{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return MetaSnapshot{
		Version:       s.changeCount,
		TotalRequests: s.totalRequests,
		SuccessCount:  s.successCount,
		FailureCount:  s.failureCount,
		TotalTokens:   s.totalTokens,
	}
}

// Summary returns aggregated usage statistics without request details.
func (s *RequestStatistics) Summary() SummarySnapshot {
	result := SummarySnapshot{}
	if s == nil {
		return result
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	result.Version = s.changeCount
	result.TotalRequests = s.totalRequests
	result.SuccessCount = s.successCount
	result.FailureCount = s.failureCount
	result.TotalTokens = s.totalTokens
	result.APIs = make(map[string]APISummarySnapshot, len(s.apis))
	for apiName, stats := range s.apis {
		if stats == nil {
			continue
		}
		apiSnapshot := APISummarySnapshot{
			TotalRequests: stats.TotalRequests,
			TotalTokens:   stats.TotalTokens,
			Models:        make(map[string]ModelSummarySnapshot, len(stats.Models)),
		}
		for modelName, modelStatsValue := range stats.Models {
			if modelStatsValue == nil {
				continue
			}
			apiSnapshot.Models[modelName] = ModelSummarySnapshot{
				TotalRequests: modelStatsValue.TotalRequests,
				TotalTokens:   modelStatsValue.TotalTokens,
			}
		}
		result.APIs[apiName] = apiSnapshot
	}

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
	result := SummarySnapshot{}
	if s == nil {
		return result
	}

	s.mu.RLock()
	result = newSummarySnapshot(s.changeCount)
	for apiName, stats := range s.apis {
		if stats == nil {
			continue
		}
		for modelName, modelStatsValue := range stats.Models {
			if modelStatsValue == nil {
				continue
			}
			for _, detail := range modelStatsValue.Details {
				if !timeRange.contains(detail.Timestamp) {
					continue
				}
				addSummaryDetail(&result, apiName, modelName, detail)
			}
		}
	}
	s.mu.RUnlock()
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

	s.mu.RLock()
	auths := make(map[string]*authStats)
	for _, stats := range s.apis {
		if stats == nil {
			continue
		}
		for modelName, modelStatsValue := range stats.Models {
			if modelStatsValue == nil {
				continue
			}
			for _, detail := range modelStatsValue.Details {
				authIndex := strings.TrimSpace(detail.AuthIndex)
				if authIndex == "" {
					continue
				}
				if len(authSet) > 0 {
					if _, ok := authSet[authIndex]; !ok {
						continue
					}
				}
				if !query.TimeRange.contains(detail.Timestamp) {
					continue
				}
				updateAuthStatsMap(auths, modelName, detail)
			}
		}
	}
	s.mu.RUnlock()

	out := make([]AuthUsageSnapshot, 0, len(auths))
	for authIndex, stats := range auths {
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

	s.mu.RLock()
	byKey := make(map[string]*SeriesEntry)
	for apiName, stats := range s.apis {
		if stats == nil {
			continue
		}
		for modelName, modelStatsValue := range stats.Models {
			if modelStatsValue == nil {
				continue
			}
			for _, detail := range modelStatsValue.Details {
				if !query.TimeRange.contains(detail.Timestamp) {
					continue
				}
				bucket := truncateUsageBucket(detail.Timestamp, query.Bucket)
				group := usageSeriesGroup(apiName, modelName, detail, query.GroupBy)
				key := bucket.Format(time.RFC3339Nano) + "\x00" + group
				entry, ok := byKey[key]
				if !ok {
					entry = &SeriesEntry{Bucket: bucket, Group: group}
					byKey[key] = entry
				}
				updateUsageAggregate(
					&entry.TotalRequests,
					&entry.SuccessCount,
					&entry.FailureCount,
					&entry.TotalTokens,
					&entry.Tokens,
					detail,
				)
			}
		}
	}
	s.mu.RUnlock()

	result.Items = make([]SeriesEntry, 0, len(byKey))
	for _, entry := range byKey {
		if entry == nil {
			continue
		}
		result.Items = append(result.Items, *entry)
	}
	sort.Slice(result.Items, func(i, j int) bool {
		if result.Items[i].Bucket.Equal(result.Items[j].Bucket) {
			return result.Items[i].Group < result.Items[j].Group
		}
		return result.Items[i].Bucket.Before(result.Items[j].Bucket)
	})
	return result
}

func authUsageSnapshot(authIndex string, stats *authStats) AuthUsageSnapshot {
	return AuthUsageSnapshot{
		AuthIndex:     authIndex,
		TotalRequests: stats.TotalRequests,
		SuccessCount:  stats.SuccessCount,
		FailureCount:  stats.FailureCount,
		TotalTokens:   stats.TotalTokens,
		Tokens:        stats.Tokens,
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
		RequestsByDay:  map[string]int64{},
		RequestsByHour: map[string]int64{},
		TokensByDay:    map[string]int64{},
		TokensByHour:   map[string]int64{},
	}
}

func addSummaryDetail(result *SummarySnapshot, apiName, modelName string, detail RequestDetail) {
	if result == nil {
		return
	}
	tokens := normaliseTokenStats(detail.Tokens)
	result.TotalRequests++
	if detail.Failed {
		result.FailureCount++
	} else {
		result.SuccessCount++
	}
	result.TotalTokens += tokens.TotalTokens

	apiSnapshot := result.APIs[apiName]
	if apiSnapshot.Models == nil {
		apiSnapshot.Models = map[string]ModelSummarySnapshot{}
	}
	apiSnapshot.TotalRequests++
	apiSnapshot.TotalTokens += tokens.TotalTokens
	modelSnapshot := apiSnapshot.Models[modelName]
	modelSnapshot.TotalRequests++
	modelSnapshot.TotalTokens += tokens.TotalTokens
	apiSnapshot.Models[modelName] = modelSnapshot
	result.APIs[apiName] = apiSnapshot

	dayKey := detail.Timestamp.Format("2006-01-02")
	hourKey := formatHour(detail.Timestamp.Hour())
	result.RequestsByDay[dayKey]++
	result.RequestsByHour[hourKey]++
	result.TokensByDay[dayKey] += tokens.TotalTokens
	result.TokensByHour[hourKey] += tokens.TotalTokens
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
