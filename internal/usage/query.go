package usage

import (
	"sort"
	"strings"
)

const (
	DefaultDetailsLimit = 200
	MaxDetailsLimit     = 1000
)

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

// DetailQuery filters usage request details.
type DetailQuery struct {
	API       string
	Model     string
	AuthIndex string
	Source    string
	ClientIP  string
	Failed    *bool
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
	Details      []DetailEntry `json:"details"`
	Offset       int           `json:"offset"`
	Limit        int           `json:"limit"`
	NextOffset   int           `json:"next_offset,omitempty"`
	HasMore      bool          `json:"has_more"`
	TotalMatched int           `json:"total_matched"`
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
		return DetailPage{Limit: normalizeDetailsLimit(query.Limit)}
	}
	query.Offset = normalizeDetailsOffset(query.Offset)
	query.Limit = normalizeDetailsLimit(query.Limit)
	query.API = strings.TrimSpace(query.API)
	query.Model = strings.TrimSpace(query.Model)
	query.AuthIndex = strings.TrimSpace(query.AuthIndex)
	query.Source = strings.TrimSpace(query.Source)
	query.ClientIP = strings.TrimSpace(query.ClientIP)

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

	sort.Slice(matches, func(i, j int) bool {
		left := matches[i].Timestamp
		right := matches[j].Timestamp
		if left.Equal(right) {
			if matches[i].API == matches[j].API {
				return matches[i].Model < matches[j].Model
			}
			return matches[i].API < matches[j].API
		}
		return left.After(right)
	})

	total := len(matches)
	if query.Offset >= total {
		return DetailPage{
			Details:      []DetailEntry{},
			Offset:       query.Offset,
			Limit:        query.Limit,
			TotalMatched: total,
		}
	}
	end := query.Offset + query.Limit
	if end > total {
		end = total
	}
	page := matches[query.Offset:end]
	hasMore := end < total
	nextOffset := 0
	if hasMore {
		nextOffset = end
	}
	return DetailPage{
		Details:      page,
		Offset:       query.Offset,
		Limit:        query.Limit,
		NextOffset:   nextOffset,
		HasMore:      hasMore,
		TotalMatched: total,
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
	}
}

func detailMatchesQuery(detail RequestDetail, query DetailQuery) bool {
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
