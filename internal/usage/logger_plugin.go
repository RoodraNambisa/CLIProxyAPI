// Package usage provides usage tracking and logging functionality for the CLI Proxy API server.
// It includes plugins for monitoring API usage, token consumption, and other metrics
// to help with observability and billing purposes.
package usage

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

var statisticsEnabled atomic.Bool

func init() {
	statisticsEnabled.Store(true)
	coreusage.RegisterPlugin(NewLoggerPlugin())
}

// LoggerPlugin collects in-memory request statistics for usage analysis.
// It implements coreusage.Plugin to receive usage records emitted by the runtime.
type LoggerPlugin struct {
	stats *RequestStatistics
}

// NewLoggerPlugin constructs a new logger plugin instance.
//
// Returns:
//   - *LoggerPlugin: A new logger plugin instance wired to the shared statistics store.
func NewLoggerPlugin() *LoggerPlugin { return &LoggerPlugin{stats: defaultRequestStatistics} }

// HandleUsage implements coreusage.Plugin.
// It updates the in-memory statistics store whenever a usage record is received.
//
// Parameters:
//   - ctx: The context for the usage record
//   - record: The usage record to aggregate
func (p *LoggerPlugin) HandleUsage(ctx context.Context, record coreusage.Record) {
	if !statisticsEnabled.Load() {
		return
	}
	if p == nil || p.stats == nil {
		return
	}
	p.stats.Record(ctx, record)
}

// SetStatisticsEnabled toggles whether in-memory statistics are recorded.
func SetStatisticsEnabled(enabled bool) { statisticsEnabled.Store(enabled) }

// StatisticsEnabled reports the current recording state.
func StatisticsEnabled() bool { return statisticsEnabled.Load() }

// RequestStatistics maintains aggregated request metrics in memory.
type RequestStatistics struct {
	mu sync.RWMutex

	batch *usageBatcher

	totalRequests  int64
	successCount   int64
	failureCount   int64
	totalTokens    int64
	tokens         TokenStats
	changeCount    uint64
	persistedCount uint64

	apis map[string]*apiStats

	auths   map[string]*authStats
	models  map[string]*aggregateStats
	sources map[string]*aggregateStats

	oldestAt            time.Time
	newestAt            time.Time
	dayOnlyFutureOldest time.Time
	dayOnlyFutureNewest time.Time

	requestsByDay   map[string]int64
	requestsByHour  map[int]int64
	tokensByDay     map[string]int64
	tokensByHour    map[int]int64
	tokenRowsByDay  map[string]int64
	tokenRowsByHour map[int]int64

	minuteBuckets     map[int64]*usageAggregateBucket
	hourBuckets       map[int64]*usageAggregateBucket
	dayBuckets        map[int64]*usageAggregateBucket
	detailIndex       []usageDetailRef
	detailIndexDirty  bool
	authSeries        map[string]map[usageSeriesKey]struct{}
	stringPool        map[string]string
	lastPrunedMinute  int64
	legacyHourBuckets map[legacyHourKey]*legacyHourStats
}

type legacyHourKey struct {
	StartUnix int64
	Day       string
	Hour      int
}

type legacyHourStats struct {
	Requests int64
	Tokens   int64
	Entries  []legacyUsageEntry
}

type legacyUsageEntry struct {
	Timestamp time.Time
	Tokens    int64
	Requests  int64
	AuthIndex string
}

type usageSeriesKey struct {
	API   string
	Model string
}

// apiStats holds aggregated metrics for a single API key.
type apiStats struct {
	TotalRequests int64
	SuccessCount  int64
	FailureCount  int64
	TotalTokens   int64
	Tokens        TokenStats
	LastUsedAt    time.Time
	Models        map[string]*modelStats
}

// modelStats holds aggregated metrics for a specific model within an API.
type modelStats struct {
	TotalRequests int64
	SuccessCount  int64
	FailureCount  int64
	TotalTokens   int64
	Tokens        TokenStats
	LastUsedAt    time.Time
	Details       []RequestDetail
}

type authStats struct {
	TotalRequests int64
	SuccessCount  int64
	FailureCount  int64
	TotalTokens   int64
	Tokens        TokenStats
	LastUsedAt    time.Time
	Models        map[string]*authModelStats
}

type authModelStats struct {
	TotalRequests int64
	SuccessCount  int64
	FailureCount  int64
	TotalTokens   int64
	Tokens        TokenStats
	LastUsedAt    time.Time
}

// RequestDetail stores the timestamp, latency, and token usage for a single request.
type RequestDetail struct {
	Timestamp           time.Time  `json:"timestamp"`
	LatencyMs           int64      `json:"latency_ms"`
	Source              string     `json:"source"`
	ClientIP            string     `json:"client_ip"`
	AuthIndex           string     `json:"auth_index"`
	RequestServiceTier  string     `json:"request_service_tier,omitempty"`
	ResponseServiceTier string     `json:"response_service_tier,omitempty"`
	Tokens              TokenStats `json:"tokens"`
	Failed              bool       `json:"failed"`
	Auxiliary           bool       `json:"auxiliary,omitempty"`
}

// TokenStats captures the token usage breakdown for a request.
type TokenStats struct {
	InputTokens         int64 `json:"input_tokens"`
	OutputTokens        int64 `json:"output_tokens"`
	ReasoningTokens     int64 `json:"reasoning_tokens"`
	CachedTokens        int64 `json:"cached_tokens"`
	CacheCreationTokens int64 `json:"cache_creation_tokens"`
	TotalTokens         int64 `json:"total_tokens"`
}

// StatisticsSnapshot represents an immutable view of the aggregated metrics.
type StatisticsSnapshot struct {
	TotalRequests int64 `json:"total_requests"`
	SuccessCount  int64 `json:"success_count"`
	FailureCount  int64 `json:"failure_count"`
	TotalTokens   int64 `json:"total_tokens"`

	APIs map[string]APISnapshot `json:"apis"`

	RequestsByDay  map[string]int64 `json:"requests_by_day"`
	RequestsByHour map[string]int64 `json:"requests_by_hour"`
	TokensByDay    map[string]int64 `json:"tokens_by_day"`
	TokensByHour   map[string]int64 `json:"tokens_by_hour"`
}

// APISnapshot summarises metrics for a single API key.
type APISnapshot struct {
	TotalRequests int64                    `json:"total_requests"`
	TotalTokens   int64                    `json:"total_tokens"`
	Models        map[string]ModelSnapshot `json:"models"`
}

// ModelSnapshot summarises metrics for a specific model.
type ModelSnapshot struct {
	TotalRequests int64           `json:"total_requests"`
	TotalTokens   int64           `json:"total_tokens"`
	Details       []RequestDetail `json:"details"`
}

var defaultRequestStatistics = newRequestStatistics(true)

// GetRequestStatistics returns the shared statistics store.
func GetRequestStatistics() *RequestStatistics { return defaultRequestStatistics }

// NewRequestStatistics constructs an empty statistics store.
func NewRequestStatistics() *RequestStatistics {
	return newRequestStatistics(false)
}

func newRequestStatistics(async bool) *RequestStatistics {
	statistics := &RequestStatistics{
		apis:              make(map[string]*apiStats),
		auths:             make(map[string]*authStats),
		models:            make(map[string]*aggregateStats),
		sources:           make(map[string]*aggregateStats),
		requestsByDay:     make(map[string]int64),
		requestsByHour:    make(map[int]int64),
		tokensByDay:       make(map[string]int64),
		tokensByHour:      make(map[int]int64),
		tokenRowsByDay:    make(map[string]int64),
		tokenRowsByHour:   make(map[int]int64),
		minuteBuckets:     make(map[int64]*usageAggregateBucket),
		hourBuckets:       make(map[int64]*usageAggregateBucket),
		dayBuckets:        make(map[int64]*usageAggregateBucket),
		legacyHourBuckets: make(map[legacyHourKey]*legacyHourStats),
		authSeries:        make(map[string]map[usageSeriesKey]struct{}),
		stringPool:        make(map[string]string),
	}
	if async {
		statistics.batch = newUsageBatcher(statistics)
	}
	return statistics
}

// Record ingests a new usage record and updates the aggregates.
func (s *RequestStatistics) Record(ctx context.Context, record coreusage.Record) {
	if s == nil {
		return
	}
	if !statisticsEnabled.Load() {
		return
	}
	timestamp := record.RequestedAt
	if timestamp.IsZero() {
		timestamp = time.Now()
	}
	detail := normaliseDetail(record.Detail)
	statsKey := record.APIKey
	if statsKey == "" {
		statsKey = resolveAPIIdentifier(ctx, record)
	}
	statsKey = strings.TrimSpace(statsKey)
	if statsKey == "" {
		statsKey = "unknown"
	}
	failed := record.Failed
	if !failed {
		failed = !resolveSuccess(ctx)
	}
	modelName := strings.TrimSpace(record.Model)
	if modelName == "" {
		modelName = "unknown"
	}
	requestDetail := RequestDetail{
		Timestamp:           timestamp,
		LatencyMs:           normaliseLatency(record.Latency),
		Source:              record.Source,
		ClientIP:            resolveClientIP(ctx),
		AuthIndex:           record.AuthIndex,
		RequestServiceTier:  strings.TrimSpace(record.RequestServiceTier),
		ResponseServiceTier: strings.TrimSpace(record.ResponseServiceTier),
		Tokens:              detail,
		Failed:              failed,
		Auxiliary:           record.Auxiliary,
	}
	if requestDetail.ResponseServiceTier == "" {
		requestDetail.ResponseServiceTier = strings.TrimSpace(record.Detail.ResponseServiceTier)
	}
	prepared := preparedUsageRecord{
		apiName:   statsKey,
		modelName: modelName,
		detail:    requestDetail,
	}
	if s.batch != nil {
		s.batch.enqueue(prepared)
		return
	}
	s.recordPreparedBatch([]preparedUsageRecord{prepared})
}

func (s *RequestStatistics) recordPreparedBatch(records []preparedUsageRecord) {
	if s == nil || len(records) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	for index := range records {
		s.recordPreparedLocked(records[index], now)
	}
	s.pruneTimeBucketsLocked(now)
}

func (s *RequestStatistics) recordPreparedLocked(record preparedUsageRecord, now time.Time) {
	statsKey := s.internStringLocked(record.apiName)
	modelName := s.internStringLocked(record.modelName)
	requestDetail := record.detail
	requestDetail.Source = s.internStringLocked(requestDetail.Source)
	requestDetail.ClientIP = s.internStringLocked(requestDetail.ClientIP)
	requestDetail.AuthIndex = s.internStringLocked(requestDetail.AuthIndex)
	requestDetail.RequestServiceTier = s.internStringLocked(requestDetail.RequestServiceTier)
	requestDetail.ResponseServiceTier = s.internStringLocked(requestDetail.ResponseServiceTier)
	totalTokens := nonNegativeInt64(requestDetail.Tokens.TotalTokens)
	if !requestDetail.Auxiliary {
		s.totalRequests = saturatingAddInt64(s.totalRequests, 1)
		if requestDetail.Failed {
			s.failureCount = saturatingAddInt64(s.failureCount, 1)
		} else {
			s.successCount = saturatingAddInt64(s.successCount, 1)
		}
	}
	s.totalTokens = saturatingAddInt64(s.totalTokens, totalTokens)

	stats := s.apis[statsKey]
	if stats == nil {
		stats = &apiStats{Models: make(map[string]*modelStats)}
		s.apis[statsKey] = stats
	}
	detailOffset := s.updateAPIStats(stats, modelName, requestDetail)
	s.updateAuthStats(modelName, requestDetail)
	s.updateRealtimeAggregatesLocked(statsKey, modelName, requestDetail, now)
	s.indexAuthSeriesLocked(statsKey, modelName, requestDetail.AuthIndex)
	s.insertDetailRefLocked(usageDetailRef{
		Timestamp: requestDetail.Timestamp,
		API:       statsKey,
		Model:     modelName,
		Offset:    detailOffset,
	})
	dayKey := requestDetail.Timestamp.Format("2006-01-02")
	hourKey := requestDetail.Timestamp.Hour()
	if !requestDetail.Auxiliary {
		s.requestsByDay[dayKey] = saturatingAddInt64(s.requestsByDay[dayKey], 1)
		s.requestsByHour[hourKey] = saturatingAddInt64(s.requestsByHour[hourKey], 1)
	}
	s.tokensByDay[dayKey] = saturatingAddInt64(s.tokensByDay[dayKey], totalTokens)
	s.tokensByHour[hourKey] = saturatingAddInt64(s.tokensByHour[hourKey], totalTokens)
	s.tokenRowsByDay[dayKey] = saturatingAddInt64(s.tokenRowsByDay[dayKey], 1)
	s.tokenRowsByHour[hourKey] = saturatingAddInt64(s.tokenRowsByHour[hourKey], 1)
	s.updateLegacyHourBucketLocked(requestDetail)
	s.markChangedLocked()
}

func (s *RequestStatistics) internStringLocked(value string) string {
	if value == "" {
		return ""
	}
	if existing, ok := s.stringPool[value]; ok {
		return existing
	}
	s.stringPool[value] = value
	return value
}

func (s *RequestStatistics) updateAPIStats(stats *apiStats, model string, detail RequestDetail) int {
	modelStatsValue, ok := stats.Models[model]
	if !ok {
		modelStatsValue = &modelStats{}
		stats.Models[model] = modelStatsValue
	}
	detailOffset := len(modelStatsValue.Details)
	modelStatsValue.Details = append(modelStatsValue.Details, detail)
	updateAPIAggregates(stats, modelStatsValue, detail)
	return detailOffset
}

func updateAPIAggregates(stats *apiStats, modelStatsValue *modelStats, detail RequestDetail) {
	if stats == nil || modelStatsValue == nil {
		return
	}
	updateUsageAggregate(
		&stats.TotalRequests,
		&stats.SuccessCount,
		&stats.FailureCount,
		&stats.TotalTokens,
		&stats.Tokens,
		detail,
	)
	if stats.LastUsedAt.IsZero() || detail.Timestamp.After(stats.LastUsedAt) {
		stats.LastUsedAt = detail.Timestamp
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

func (s *RequestStatistics) updateAuthStats(model string, detail RequestDetail) {
	updateAuthStatsMap(s.auths, model, detail)
}

func updateUsageAggregate(totalRequests, successCount, failureCount, totalTokens *int64, tokens *TokenStats, detail RequestDetail) {
	if !detail.Auxiliary {
		if totalRequests != nil {
			*totalRequests = saturatingAddInt64(*totalRequests, 1)
		}
		if detail.Failed {
			if failureCount != nil {
				*failureCount = saturatingAddInt64(*failureCount, 1)
			}
		} else if successCount != nil {
			*successCount = saturatingAddInt64(*successCount, 1)
		}
	}
	normalizedTokens := normaliseTokenStats(detail.Tokens)
	if totalTokens != nil {
		*totalTokens = saturatingAddInt64(*totalTokens, normalizedTokens.TotalTokens)
	}
	if tokens != nil {
		addTokenStats(tokens, normalizedTokens)
	}
}

// Snapshot returns a copy of the aggregated metrics for external consumption.
func (s *RequestStatistics) Snapshot() StatisticsSnapshot {
	result, _, _ := s.SnapshotWithState()
	return result
}

// SnapshotWithState returns a copy of the aggregated metrics together with the
// current mutation and persisted counters.
func (s *RequestStatistics) SnapshotWithState() (StatisticsSnapshot, uint64, uint64) {
	result := StatisticsSnapshot{}
	if s == nil {
		return result, 0, 0
	}
	s.flushPending()

	s.mu.RLock()
	defer s.mu.RUnlock()

	result = s.snapshotLocked()
	return result, s.changeCount, s.persistedCount
}

func (s *RequestStatistics) snapshotLocked() StatisticsSnapshot {
	result := StatisticsSnapshot{}
	result.TotalRequests = s.totalRequests
	result.SuccessCount = s.successCount
	result.FailureCount = s.failureCount
	result.TotalTokens = s.totalTokens

	result.APIs = make(map[string]APISnapshot, len(s.apis))
	for apiName, stats := range s.apis {
		apiSnapshot := APISnapshot{
			TotalRequests: stats.TotalRequests,
			TotalTokens:   stats.TotalTokens,
			Models:        make(map[string]ModelSnapshot, len(stats.Models)),
		}
		for modelName, modelStatsValue := range stats.Models {
			requestDetails := make([]RequestDetail, len(modelStatsValue.Details))
			copy(requestDetails, modelStatsValue.Details)
			apiSnapshot.Models[modelName] = ModelSnapshot{
				TotalRequests: modelStatsValue.TotalRequests,
				TotalTokens:   modelStatsValue.TotalTokens,
				Details:       requestDetails,
			}
		}
		result.APIs[apiName] = apiSnapshot
	}

	result.RequestsByDay = make(map[string]int64, len(s.requestsByDay))
	for k, v := range s.requestsByDay {
		result.RequestsByDay[k] = v
	}

	result.RequestsByHour = make(map[string]int64, len(s.requestsByHour))
	for hour, v := range s.requestsByHour {
		key := formatHour(hour)
		result.RequestsByHour[key] = v
	}

	result.TokensByDay = make(map[string]int64, len(s.tokensByDay))
	for k, v := range s.tokensByDay {
		result.TokensByDay[k] = v
	}

	result.TokensByHour = make(map[string]int64, len(s.tokensByHour))
	for hour, v := range s.tokensByHour {
		key := formatHour(hour)
		result.TokensByHour[key] = v
	}

	return result
}

type MergeResult struct {
	Added   int64 `json:"added"`
	Skipped int64 `json:"skipped"`
}

// MergeSnapshot merges an exported statistics snapshot into the current store.
// Existing data is preserved and duplicate request details are skipped.
func (s *RequestStatistics) MergeSnapshot(snapshot StatisticsSnapshot) MergeResult {
	return s.mergeSnapshot(snapshot, false)
}

func (s *RequestStatistics) mergePersistedSnapshot(snapshot StatisticsSnapshot) MergeResult {
	return s.mergeSnapshot(snapshot, true)
}

func (s *RequestStatistics) mergeSnapshot(snapshot StatisticsSnapshot, markPersistedIfClean bool) MergeResult {
	result := MergeResult{}
	if s == nil {
		return result
	}
	s.flushPending()

	s.mu.Lock()
	defer s.mu.Unlock()
	cleanBeforeMerge := s.changeCount == s.persistedCount
	now := time.Now().UTC()

	seen := make(map[string]struct{})
	for apiName, stats := range s.apis {
		if stats == nil {
			continue
		}
		for modelName, modelStatsValue := range stats.Models {
			if modelStatsValue == nil {
				continue
			}
			for _, detail := range modelStatsValue.Details {
				seen[dedupKey(apiName, modelName, detail)] = struct{}{}
			}
		}
	}

	for apiName, apiSnapshot := range snapshot.APIs {
		apiName = strings.TrimSpace(apiName)
		if apiName == "" {
			continue
		}
		stats, ok := s.apis[apiName]
		if !ok || stats == nil {
			stats = &apiStats{Models: make(map[string]*modelStats)}
			s.apis[apiName] = stats
		} else if stats.Models == nil {
			stats.Models = make(map[string]*modelStats)
		}
		for modelName, modelSnapshot := range apiSnapshot.Models {
			modelName = strings.TrimSpace(modelName)
			if modelName == "" {
				modelName = "unknown"
			}
			for _, detail := range modelSnapshot.Details {
				detail.Tokens = normaliseTokenStats(detail.Tokens)
				if detail.LatencyMs < 0 {
					detail.LatencyMs = 0
				}
				if detail.Timestamp.IsZero() {
					detail.Timestamp = time.Now()
				}
				key := dedupKey(apiName, modelName, detail)
				if _, exists := seen[key]; exists {
					result.Skipped++
					continue
				}
				seen[key] = struct{}{}
				s.recordImported(apiName, modelName, stats, detail, now)
				result.Added++
			}
		}
	}
	if result.Added > 0 {
		s.rebuildDetailIndexLocked()
		s.pruneTimeBucketsLocked(now)
	}
	if markPersistedIfClean && cleanBeforeMerge {
		s.persistedCount = s.changeCount
	}

	return result
}

// HasPendingPersistence reports whether there are in-memory changes that have
// not been written to the snapshot file yet.
func (s *RequestStatistics) HasPendingPersistence() bool {
	if s == nil {
		return false
	}
	s.flushPending()
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.changeCount != s.persistedCount
}

// MarkPersisted advances the persisted counter to the provided snapshot
// version. Newer in-memory changes remain pending.
func (s *RequestStatistics) MarkPersisted(version uint64) {
	if s == nil {
		return
	}
	s.flushPending()
	s.mu.Lock()
	defer s.mu.Unlock()
	if version > s.changeCount {
		version = s.changeCount
	}
	if version > s.persistedCount {
		s.persistedCount = version
	}
}

// MarkAllPersisted marks the current in-memory state as already persisted.
func (s *RequestStatistics) MarkAllPersisted() {
	if s == nil {
		return
	}
	s.flushPending()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.persistedCount = s.changeCount
}

// Clear removes all in-memory request statistics and marks the store changed.
func (s *RequestStatistics) Clear() StatisticsSnapshot {
	previous, _, _ := s.clearWithState()
	return previous
}

func (s *RequestStatistics) clearWithState() (StatisticsSnapshot, StatisticsSnapshot, uint64) {
	if s == nil {
		return StatisticsSnapshot{}, StatisticsSnapshot{}, 0
	}
	s.flushPending()
	s.mu.Lock()
	defer s.mu.Unlock()

	previous := s.snapshotLocked()
	s.resetLocked()
	s.markChangedLocked()
	return previous, s.snapshotLocked(), s.changeCount
}

func (s *RequestStatistics) resetLocked() {
	s.totalRequests = 0
	s.successCount = 0
	s.failureCount = 0
	s.totalTokens = 0
	s.tokens = TokenStats{}
	s.apis = make(map[string]*apiStats)
	s.auths = make(map[string]*authStats)
	s.models = make(map[string]*aggregateStats)
	s.sources = make(map[string]*aggregateStats)
	s.oldestAt = time.Time{}
	s.newestAt = time.Time{}
	s.dayOnlyFutureOldest = time.Time{}
	s.dayOnlyFutureNewest = time.Time{}
	s.requestsByDay = make(map[string]int64)
	s.requestsByHour = make(map[int]int64)
	s.tokensByDay = make(map[string]int64)
	s.tokensByHour = make(map[int]int64)
	s.tokenRowsByDay = make(map[string]int64)
	s.tokenRowsByHour = make(map[int]int64)
	s.minuteBuckets = make(map[int64]*usageAggregateBucket)
	s.hourBuckets = make(map[int64]*usageAggregateBucket)
	s.dayBuckets = make(map[int64]*usageAggregateBucket)
	s.legacyHourBuckets = make(map[legacyHourKey]*legacyHourStats)
	s.detailIndex = nil
	s.detailIndexDirty = false
	s.authSeries = make(map[string]map[usageSeriesKey]struct{})
	s.stringPool = make(map[string]string)
	s.lastPrunedMinute = 0
}

// RemoveAuthIndexes removes request details belonging to the supplied auth
// indexes and updates only the affected series and aggregates.
func (s *RequestStatistics) RemoveAuthIndexes(indexes []string) int {
	if s == nil {
		return 0
	}
	indexSet := make(map[string]struct{}, len(indexes))
	for _, index := range indexes {
		index = strings.TrimSpace(index)
		if index != "" {
			indexSet[index] = struct{}{}
		}
	}
	if len(indexSet) == 0 {
		return 0
	}
	s.flushPending()

	s.mu.Lock()
	defer s.mu.Unlock()
	removed := s.removeAuthIndexesLocked(indexSet)
	if removed == 0 {
		return 0
	}
	s.markChangedLocked()
	return removed
}

// PruneAuthIndexes removes request details whose auth index is no longer
// present in the valid set.
func (s *RequestStatistics) PruneAuthIndexes(valid map[string]struct{}) int {
	if s == nil {
		return 0
	}
	s.flushPending()

	s.mu.Lock()
	defer s.mu.Unlock()
	stale := make(map[string]struct{})
	for authIndex := range s.authSeries {
		if _, ok := valid[authIndex]; !ok {
			stale[authIndex] = struct{}{}
		}
	}
	removed := s.removeAuthIndexesLocked(stale)
	if removed == 0 {
		return 0
	}
	s.markChangedLocked()
	return removed
}

func (s *RequestStatistics) removeAuthIndexesLocked(indexSet map[string]struct{}) int {
	affected := make(map[usageSeriesKey]struct{})
	for authIndex := range indexSet {
		for series := range s.authSeries[authIndex] {
			affected[series] = struct{}{}
		}
	}
	removed := 0
	for series := range affected {
		stats := s.apis[series.API]
		if stats == nil {
			continue
		}
		modelStatsValue := stats.Models[series.Model]
		if modelStatsValue == nil {
			continue
		}
		kept := modelStatsValue.Details[:0]
		for _, detail := range modelStatsValue.Details {
			if _, ok := indexSet[strings.TrimSpace(detail.AuthIndex)]; !ok {
				kept = append(kept, detail)
				continue
			}
			s.removeDetailAggregatesLocked(series.API, series.Model, stats, modelStatsValue, detail)
			removed++
		}
		clear(modelStatsValue.Details[len(kept):])
		modelStatsValue.Details = kept
		if len(kept) == 0 {
			delete(stats.Models, series.Model)
		}
		if len(stats.Models) == 0 {
			delete(s.apis, series.API)
		}
	}
	if removed == 0 {
		return 0
	}
	for authIndex := range indexSet {
		delete(s.authSeries, authIndex)
		delete(s.auths, authIndex)
	}
	s.rebuildAffectedDetailIndexLocked(affected)
	s.restoreAggregateTimesLocked()
	return removed
}

func (s *RequestStatistics) removeDetailAggregatesLocked(apiName, modelName string, stats *apiStats, modelStatsValue *modelStats, detail RequestDetail) {
	subtractUsageAggregate(&s.totalRequests, &s.successCount, &s.failureCount, &s.totalTokens, &s.tokens, detail)
	subtractUsageAggregate(&stats.TotalRequests, &stats.SuccessCount, &stats.FailureCount, &stats.TotalTokens, &stats.Tokens, detail)
	subtractUsageAggregate(&modelStatsValue.TotalRequests, &modelStatsValue.SuccessCount, &modelStatsValue.FailureCount, &modelStatsValue.TotalTokens, &modelStatsValue.Tokens, detail)
	if stats.LastUsedAt.Equal(detail.Timestamp) {
		stats.LastUsedAt = time.Time{}
	}
	if modelStatsValue.LastUsedAt.Equal(detail.Timestamp) {
		modelStatsValue.LastUsedAt = time.Time{}
	}
	if aggregate := s.models[modelName]; aggregate != nil {
		aggregate.removeDetail(detail)
		if aggregate.empty() {
			delete(s.models, modelName)
		}
	}
	source := strings.TrimSpace(detail.Source)
	if source == "" {
		source = "unknown"
	}
	if aggregate := s.sources[source]; aggregate != nil {
		aggregate.removeDetail(detail)
		if aggregate.empty() {
			delete(s.sources, source)
		}
	}
	s.removeFromTimeBucketLocked(s.dayBuckets, 24*time.Hour, apiName, modelName, detail)
	s.removeFromTimeBucketLocked(s.hourBuckets, time.Hour, apiName, modelName, detail)
	s.removeFromTimeBucketLocked(s.minuteBuckets, time.Minute, apiName, modelName, detail)
	dayKey := detail.Timestamp.Format("2006-01-02")
	hourKey := detail.Timestamp.Hour()
	if !detail.Auxiliary {
		subtractStringCounter(s.requestsByDay, dayKey, 1)
		subtractIntCounter(s.requestsByHour, hourKey, 1)
	}
	totalTokens := nonNegativeInt64(detail.Tokens.TotalTokens)
	subtractTokenStringCounter(s.tokensByDay, s.tokenRowsByDay, dayKey, totalTokens)
	subtractTokenIntCounter(s.tokensByHour, s.tokenRowsByHour, hourKey, totalTokens)
	s.removeLegacyHourEntryLocked(detail)
}

func subtractStringCounter(values map[string]int64, key string, decrement int64) {
	remaining := subtractNonNegativeInt64(values[key], decrement)
	if remaining == 0 {
		delete(values, key)
		return
	}
	values[key] = remaining
}

func subtractIntCounter(values map[int]int64, key int, decrement int64) {
	remaining := subtractNonNegativeInt64(values[key], decrement)
	if remaining == 0 {
		delete(values, key)
		return
	}
	values[key] = remaining
}

func subtractTokenStringCounter(values, rows map[string]int64, key string, decrement int64) {
	remainingRows := subtractNonNegativeInt64(rows[key], 1)
	if remainingRows == 0 {
		delete(rows, key)
		delete(values, key)
		return
	}
	rows[key] = remainingRows
	values[key] = subtractNonNegativeInt64(values[key], decrement)
}

func subtractTokenIntCounter(values, rows map[int]int64, key int, decrement int64) {
	remainingRows := subtractNonNegativeInt64(rows[key], 1)
	if remainingRows == 0 {
		delete(rows, key)
		delete(values, key)
		return
	}
	rows[key] = remainingRows
	values[key] = subtractNonNegativeInt64(values[key], decrement)
}

func (s *RequestStatistics) rebuildAffectedDetailIndexLocked(affected map[usageSeriesKey]struct{}) {
	if len(affected) == 0 {
		return
	}
	kept := s.detailIndex[:0]
	for _, ref := range s.detailIndex {
		if _, ok := affected[usageSeriesKey{API: ref.API, Model: ref.Model}]; !ok {
			kept = append(kept, ref)
		}
	}
	clear(s.detailIndex[len(kept):])
	s.detailIndex = kept
	for series := range affected {
		stats := s.apis[series.API]
		if stats == nil {
			continue
		}
		modelStatsValue := stats.Models[series.Model]
		if modelStatsValue == nil {
			continue
		}
		for offset, detail := range modelStatsValue.Details {
			s.detailIndex = append(s.detailIndex, usageDetailRef{
				Timestamp: detail.Timestamp,
				API:       series.API,
				Model:     series.Model,
				Offset:    offset,
			})
		}
	}
	s.detailIndexDirty = true
	s.sortDetailIndexLocked()
}

func (s *RequestStatistics) restoreAggregateTimesLocked() {
	s.oldestAt = time.Time{}
	s.newestAt = time.Time{}
	s.dayOnlyFutureOldest = time.Time{}
	s.dayOnlyFutureNewest = time.Time{}
	resetAggregateMapTimes(s.models)
	resetAggregateMapTimes(s.sources)
	resetAggregateBucketMapTimes(s.dayBuckets)
	resetAggregateBucketMapTimes(s.hourBuckets)
	resetAggregateBucketMapTimes(s.minuteBuckets)
	for _, stats := range s.auths {
		if stats == nil {
			continue
		}
		stats.LastUsedAt = time.Time{}
		for _, modelStatsValue := range stats.Models {
			if modelStatsValue != nil {
				modelStatsValue.LastUsedAt = time.Time{}
			}
		}
	}
	now := time.Now().UTC()
	for apiName, stats := range s.apis {
		if stats == nil {
			continue
		}
		stats.LastUsedAt = time.Time{}
		for modelName, modelStatsValue := range stats.Models {
			if modelStatsValue == nil {
				continue
			}
			modelStatsValue.LastUsedAt = time.Time{}
			for _, detail := range modelStatsValue.Details {
				touchTime(&stats.LastUsedAt, detail.Timestamp)
				touchTime(&modelStatsValue.LastUsedAt, detail.Timestamp)
				touchAggregateMapTime(s.models, modelName, detail.Timestamp)
				source := strings.TrimSpace(detail.Source)
				if source == "" {
					source = "unknown"
				}
				touchAggregateMapTime(s.sources, source, detail.Timestamp)
				touchAuthTime(s.auths, modelName, detail)
				touchBucketTime(s.dayBuckets, 24*time.Hour, apiName, modelName, detail)
				touchBucketTime(s.hourBuckets, time.Hour, apiName, modelName, detail)
				touchBucketTime(s.minuteBuckets, time.Minute, apiName, modelName, detail)
				if s.oldestAt.IsZero() || detail.Timestamp.Before(s.oldestAt) {
					s.oldestAt = detail.Timestamp
				}
				if s.newestAt.IsZero() || detail.Timestamp.After(s.newestAt) {
					s.newestAt = detail.Timestamp
				}
				if detail.Timestamp.After(now.Add(usageFutureTolerance)) {
					if s.dayOnlyFutureOldest.IsZero() || detail.Timestamp.Before(s.dayOnlyFutureOldest) {
						s.dayOnlyFutureOldest = detail.Timestamp
					}
					if s.dayOnlyFutureNewest.IsZero() || detail.Timestamp.After(s.dayOnlyFutureNewest) {
						s.dayOnlyFutureNewest = detail.Timestamp
					}
				}
			}
		}
	}
}

func resetAggregateMapTimes(values map[string]*aggregateStats) {
	for _, value := range values {
		if value != nil {
			value.LastUsedAt = time.Time{}
		}
	}
}

func resetNestedAggregateMapTimes(values map[string]map[string]*aggregateStats) {
	for _, nested := range values {
		resetAggregateMapTimes(nested)
	}
}

func resetAggregateBucketMapTimes(values map[int64]*usageAggregateBucket) {
	for _, bucket := range values {
		if bucket == nil {
			continue
		}
		bucket.Total.LastUsedAt = time.Time{}
		resetAggregateMapTimes(bucket.APIs)
		resetAggregateMapTimes(bucket.Models)
		resetNestedAggregateMapTimes(bucket.APIModels)
		resetAggregateMapTimes(bucket.Auths)
		resetNestedAggregateMapTimes(bucket.AuthModels)
		resetAggregateMapTimes(bucket.Sources)
		resetNestedAggregateMapTimes(bucket.AuthSources)
	}
}

func touchTime(target *time.Time, timestamp time.Time) {
	if target != nil && (target.IsZero() || timestamp.After(*target)) {
		*target = timestamp
	}
}

func touchAggregateMapTime(values map[string]*aggregateStats, key string, timestamp time.Time) {
	if value := values[key]; value != nil {
		touchTime(&value.LastUsedAt, timestamp)
	}
}

func touchNestedAggregateMapTime(values map[string]map[string]*aggregateStats, first, second string, timestamp time.Time) {
	if nested := values[first]; nested != nil {
		touchAggregateMapTime(nested, second, timestamp)
	}
}

func touchAuthTime(values map[string]*authStats, modelName string, detail RequestDetail) {
	authIndex := strings.TrimSpace(detail.AuthIndex)
	if authIndex == "" {
		return
	}
	stats := values[authIndex]
	if stats == nil {
		return
	}
	touchTime(&stats.LastUsedAt, detail.Timestamp)
	if modelStatsValue := stats.Models[modelName]; modelStatsValue != nil {
		touchTime(&modelStatsValue.LastUsedAt, detail.Timestamp)
	}
}

func touchBucketTime(buckets map[int64]*usageAggregateBucket, step time.Duration, apiName, modelName string, detail RequestDetail) {
	bucket := buckets[truncateAggregateTime(detail.Timestamp, step).Unix()]
	if bucket == nil {
		return
	}
	touchTime(&bucket.Total.LastUsedAt, detail.Timestamp)
	touchAggregateMapTime(bucket.APIs, apiName, detail.Timestamp)
	touchAggregateMapTime(bucket.Models, modelName, detail.Timestamp)
	touchNestedAggregateMapTime(bucket.APIModels, apiName, modelName, detail.Timestamp)
	authIndex := strings.TrimSpace(detail.AuthIndex)
	if authIndex == "" {
		authIndex = "unknown"
	}
	touchAggregateMapTime(bucket.Auths, authIndex, detail.Timestamp)
	touchNestedAggregateMapTime(bucket.AuthModels, authIndex, modelName, detail.Timestamp)
	source := strings.TrimSpace(detail.Source)
	if source == "" {
		source = "unknown"
	}
	touchAggregateMapTime(bucket.Sources, source, detail.Timestamp)
	touchNestedAggregateMapTime(bucket.AuthSources, authIndex, source, detail.Timestamp)
}

func (s *RequestStatistics) removeLegacyHourEntryLocked(detail RequestDetail) {
	timestamp := detail.Timestamp
	start := timestamp.Add(-time.Duration(timestamp.Minute()) * time.Minute).
		Add(-time.Duration(timestamp.Second()) * time.Second).
		Add(-time.Duration(timestamp.Nanosecond()))
	key := legacyHourKey{StartUnix: start.Unix(), Day: timestamp.Format("2006-01-02"), Hour: timestamp.Hour()}
	stats := s.legacyHourBuckets[key]
	if stats == nil {
		return
	}
	wantedAuth := strings.TrimSpace(detail.AuthIndex)
	wantedTokens := normaliseTokenStats(detail.Tokens).TotalTokens
	wantedRequests := int64(1)
	if detail.Auxiliary {
		wantedRequests = 0
	}
	for index, entry := range stats.Entries {
		if !entry.Timestamp.Equal(timestamp) || entry.Tokens != wantedTokens || entry.Requests != wantedRequests || strings.TrimSpace(entry.AuthIndex) != wantedAuth {
			continue
		}
		stats.Entries = append(stats.Entries[:index], stats.Entries[index+1:]...)
		stats.Requests = subtractNonNegativeInt64(stats.Requests, wantedRequests)
		stats.Tokens = subtractNonNegativeInt64(stats.Tokens, wantedTokens)
		if len(stats.Entries) == 0 {
			delete(s.legacyHourBuckets, key)
		}
		return
	}
}

func (s *RequestStatistics) removeFromTimeBucketLocked(buckets map[int64]*usageAggregateBucket, step time.Duration, apiName, modelName string, detail RequestDetail) {
	key := truncateAggregateTime(detail.Timestamp, step).Unix()
	bucket := buckets[key]
	if bucket == nil {
		return
	}
	bucket.remove(apiName, modelName, detail)
	if bucket.Total.empty() {
		delete(buckets, key)
	}
}

func subtractUsageAggregate(totalRequests, successCount, failureCount, totalTokens *int64, tokens *TokenStats, detail RequestDetail) {
	if !detail.Auxiliary {
		*totalRequests = subtractNonNegativeInt64(*totalRequests, 1)
		if detail.Failed {
			*failureCount = subtractNonNegativeInt64(*failureCount, 1)
		} else {
			*successCount = subtractNonNegativeInt64(*successCount, 1)
		}
	}
	*totalTokens = subtractNonNegativeInt64(*totalTokens, detail.Tokens.TotalTokens)
	subtractTokenStats(tokens, detail.Tokens)
}

func (s *RequestStatistics) recordImported(apiName, modelName string, stats *apiStats, detail RequestDetail, now time.Time) {
	apiName = s.internStringLocked(apiName)
	modelName = s.internStringLocked(modelName)
	detail.Source = s.internStringLocked(detail.Source)
	detail.ClientIP = s.internStringLocked(detail.ClientIP)
	detail.AuthIndex = s.internStringLocked(detail.AuthIndex)
	detail.RequestServiceTier = s.internStringLocked(detail.RequestServiceTier)
	detail.ResponseServiceTier = s.internStringLocked(detail.ResponseServiceTier)
	totalTokens := detail.Tokens.TotalTokens
	if totalTokens < 0 {
		totalTokens = 0
	}

	if !detail.Auxiliary {
		s.totalRequests = saturatingAddInt64(s.totalRequests, 1)
		if detail.Failed {
			s.failureCount = saturatingAddInt64(s.failureCount, 1)
		} else {
			s.successCount = saturatingAddInt64(s.successCount, 1)
		}
	}
	s.totalTokens = saturatingAddInt64(s.totalTokens, totalTokens)

	s.updateAPIStats(stats, modelName, detail)
	s.updateAuthStats(modelName, detail)
	s.updateRealtimeAggregatesLocked(apiName, modelName, detail, now)
	s.indexAuthSeriesLocked(apiName, modelName, detail.AuthIndex)

	dayKey := detail.Timestamp.Format("2006-01-02")
	hourKey := detail.Timestamp.Hour()

	if !detail.Auxiliary {
		s.requestsByDay[dayKey] = saturatingAddInt64(s.requestsByDay[dayKey], 1)
		s.requestsByHour[hourKey] = saturatingAddInt64(s.requestsByHour[hourKey], 1)
	}
	s.tokensByDay[dayKey] = saturatingAddInt64(s.tokensByDay[dayKey], totalTokens)
	s.tokensByHour[hourKey] = saturatingAddInt64(s.tokensByHour[hourKey], totalTokens)
	s.tokenRowsByDay[dayKey] = saturatingAddInt64(s.tokenRowsByDay[dayKey], 1)
	s.tokenRowsByHour[hourKey] = saturatingAddInt64(s.tokenRowsByHour[hourKey], 1)
	s.updateLegacyHourBucketLocked(detail)
	s.markChangedLocked()
}

func (s *RequestStatistics) rebuildLocked() {
	s.totalRequests = 0
	s.successCount = 0
	s.failureCount = 0
	s.totalTokens = 0
	s.tokens = TokenStats{}
	s.requestsByDay = make(map[string]int64)
	s.requestsByHour = make(map[int]int64)
	s.tokensByDay = make(map[string]int64)
	s.tokensByHour = make(map[int]int64)
	s.tokenRowsByDay = make(map[string]int64)
	s.tokenRowsByHour = make(map[int]int64)
	s.auths = make(map[string]*authStats)
	s.models = make(map[string]*aggregateStats)
	s.sources = make(map[string]*aggregateStats)
	s.oldestAt = time.Time{}
	s.newestAt = time.Time{}
	s.dayOnlyFutureOldest = time.Time{}
	s.dayOnlyFutureNewest = time.Time{}
	s.minuteBuckets = make(map[int64]*usageAggregateBucket)
	s.hourBuckets = make(map[int64]*usageAggregateBucket)
	s.dayBuckets = make(map[int64]*usageAggregateBucket)
	s.legacyHourBuckets = make(map[legacyHourKey]*legacyHourStats)
	s.detailIndex = nil
	s.detailIndexDirty = false
	s.authSeries = make(map[string]map[usageSeriesKey]struct{})
	s.stringPool = make(map[string]string)
	s.lastPrunedMinute = 0
	now := time.Now().UTC()

	for apiName, stats := range s.apis {
		if stats == nil {
			delete(s.apis, apiName)
			continue
		}
		stats.TotalRequests = 0
		stats.SuccessCount = 0
		stats.FailureCount = 0
		stats.TotalTokens = 0
		stats.Tokens = TokenStats{}
		stats.LastUsedAt = time.Time{}
		if stats.Models == nil {
			stats.Models = make(map[string]*modelStats)
		}
		for modelName, modelStatsValue := range stats.Models {
			if modelStatsValue == nil || len(modelStatsValue.Details) == 0 {
				delete(stats.Models, modelName)
				continue
			}
			modelStatsValue.TotalRequests = 0
			modelStatsValue.SuccessCount = 0
			modelStatsValue.FailureCount = 0
			modelStatsValue.TotalTokens = 0
			modelStatsValue.Tokens = TokenStats{}
			modelStatsValue.LastUsedAt = time.Time{}
			for idx, detail := range modelStatsValue.Details {
				detail.Source = s.internStringLocked(detail.Source)
				detail.ClientIP = s.internStringLocked(detail.ClientIP)
				detail.AuthIndex = s.internStringLocked(detail.AuthIndex)
				detail.RequestServiceTier = s.internStringLocked(detail.RequestServiceTier)
				detail.ResponseServiceTier = s.internStringLocked(detail.ResponseServiceTier)
				detail.Tokens = normaliseTokenStats(detail.Tokens)
				if detail.LatencyMs < 0 {
					detail.LatencyMs = 0
				}
				if detail.Timestamp.IsZero() {
					detail.Timestamp = time.Now()
				}
				modelStatsValue.Details[idx] = detail

				totalTokens := nonNegativeInt64(detail.Tokens.TotalTokens)
				if !detail.Auxiliary {
					s.totalRequests = saturatingAddInt64(s.totalRequests, 1)
					if detail.Failed {
						s.failureCount = saturatingAddInt64(s.failureCount, 1)
					} else {
						s.successCount = saturatingAddInt64(s.successCount, 1)
					}
				}
				s.totalTokens = saturatingAddInt64(s.totalTokens, totalTokens)

				updateAPIAggregates(stats, modelStatsValue, detail)
				s.updateAuthStats(modelName, detail)
				s.updateRealtimeAggregatesLocked(apiName, modelName, detail, now)
				s.indexAuthSeriesLocked(apiName, modelName, detail.AuthIndex)

				dayKey := detail.Timestamp.Format("2006-01-02")
				hourKey := detail.Timestamp.Hour()
				if !detail.Auxiliary {
					s.requestsByDay[dayKey] = saturatingAddInt64(s.requestsByDay[dayKey], 1)
					s.requestsByHour[hourKey] = saturatingAddInt64(s.requestsByHour[hourKey], 1)
				}
				s.tokensByDay[dayKey] = saturatingAddInt64(s.tokensByDay[dayKey], totalTokens)
				s.tokensByHour[hourKey] = saturatingAddInt64(s.tokensByHour[hourKey], totalTokens)
				s.tokenRowsByDay[dayKey] = saturatingAddInt64(s.tokenRowsByDay[dayKey], 1)
				s.tokenRowsByHour[hourKey] = saturatingAddInt64(s.tokenRowsByHour[hourKey], 1)
				s.updateLegacyHourBucketLocked(detail)
			}
		}
		if len(stats.Models) == 0 {
			delete(s.apis, apiName)
		}
	}
	s.rebuildDetailIndexLocked()
	s.pruneTimeBucketsLocked(now)
}

func (s *RequestStatistics) indexAuthSeriesLocked(apiName, modelName, authIndex string) {
	authIndex = strings.TrimSpace(authIndex)
	if authIndex == "" {
		return
	}
	series := s.authSeries[authIndex]
	if series == nil {
		series = make(map[usageSeriesKey]struct{})
		s.authSeries[authIndex] = series
	}
	series[usageSeriesKey{API: apiName, Model: modelName}] = struct{}{}
}

func (s *RequestStatistics) markChangedLocked() {
	s.changeCount++
}

func dedupKey(apiName, modelName string, detail RequestDetail) string {
	timestamp := detail.Timestamp.UTC().Format(time.RFC3339Nano)
	tokens := normaliseTokenStats(detail.Tokens)
	return fmt.Sprintf(
		"%s|%s|%s|%s|%s|%s|%s|%s|%t|%t|%d|%d|%d|%d|%d|%d",
		apiName,
		modelName,
		timestamp,
		detail.Source,
		strings.TrimSpace(detail.ClientIP),
		detail.AuthIndex,
		strings.TrimSpace(detail.RequestServiceTier),
		strings.TrimSpace(detail.ResponseServiceTier),
		detail.Failed,
		detail.Auxiliary,
		tokens.InputTokens,
		tokens.OutputTokens,
		tokens.ReasoningTokens,
		tokens.CachedTokens,
		tokens.CacheCreationTokens,
		tokens.TotalTokens,
	)
}

func resolveAPIIdentifier(ctx context.Context, record coreusage.Record) string {
	if ctx != nil {
		if ginCtx, ok := ctx.Value("gin").(*gin.Context); ok && ginCtx != nil {
			path := ginCtx.FullPath()
			if path == "" && ginCtx.Request != nil {
				path = ginCtx.Request.URL.Path
			}
			method := ""
			if ginCtx.Request != nil {
				method = ginCtx.Request.Method
			}
			if path != "" {
				if method != "" {
					return method + " " + path
				}
				return path
			}
		}
	}
	if record.Provider != "" {
		return record.Provider
	}
	return "unknown"
}

func resolveSuccess(ctx context.Context) bool {
	if ctx == nil {
		return true
	}
	ginCtx, ok := ctx.Value("gin").(*gin.Context)
	if !ok || ginCtx == nil {
		return true
	}
	status := ginCtx.Writer.Status()
	if status == 0 {
		return true
	}
	return status < httpStatusBadRequest
}

func resolveClientIP(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	ginCtx, ok := ctx.Value("gin").(*gin.Context)
	if !ok || ginCtx == nil {
		return ""
	}
	return logging.ResolveClientIP(ginCtx)
}

const httpStatusBadRequest = 400

func normaliseDetail(detail coreusage.Detail) TokenStats {
	tokens := TokenStats{
		InputTokens:         detail.InputTokens,
		OutputTokens:        detail.OutputTokens,
		ReasoningTokens:     detail.ReasoningTokens,
		CachedTokens:        detail.CachedTokens,
		CacheCreationTokens: detail.CacheCreationTokens,
		TotalTokens:         detail.TotalTokens,
	}
	return normaliseTokenStats(tokens)
}

func normaliseTokenStats(tokens TokenStats) TokenStats {
	tokens.InputTokens = nonNegativeInt64(tokens.InputTokens)
	tokens.OutputTokens = nonNegativeInt64(tokens.OutputTokens)
	tokens.ReasoningTokens = nonNegativeInt64(tokens.ReasoningTokens)
	tokens.CachedTokens = nonNegativeInt64(tokens.CachedTokens)
	tokens.CacheCreationTokens = nonNegativeInt64(tokens.CacheCreationTokens)
	tokens.TotalTokens = nonNegativeInt64(tokens.TotalTokens)
	if tokens.TotalTokens == 0 {
		input := maxUsageTokenCount(tokens.InputTokens, tokens.CachedTokens, tokens.CacheCreationTokens)
		output := maxUsageTokenCount(tokens.OutputTokens, tokens.ReasoningTokens)
		tokens.TotalTokens = saturatingAddInt64(input, output)
	}
	return tokens
}

func maxUsageTokenCount(values ...int64) int64 {
	var maximum int64
	for _, value := range values {
		if value > maximum {
			maximum = value
		}
	}
	return maximum
}

func normaliseLatency(latency time.Duration) int64 {
	if latency <= 0 {
		return 0
	}
	return latency.Milliseconds()
}

func formatHour(hour int) string {
	if hour < 0 {
		hour = 0
	}
	hour = hour % 24
	return fmt.Sprintf("%02d", hour)
}
