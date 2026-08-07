package management

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

type usageExportPayload struct {
	Version    int                      `json:"version"`
	ExportedAt time.Time                `json:"exported_at"`
	Usage      usage.StatisticsSnapshot `json:"usage"`
}

type usageImportPayload struct {
	Version int                      `json:"version"`
	Usage   usage.StatisticsSnapshot `json:"usage"`
}

type usageAuthInfo struct {
	AuthIndex   string
	ID          string
	Name        string
	Provider    string
	Label       string
	Status      string
	Disabled    bool
	AccountType string
	Account     string
	Email       string
}

type usageAuthCatalogCache struct {
	mu       sync.Mutex
	manager  *coreauth.Manager
	revision uint64
	infos    map[string]usageAuthInfo
}

const (
	maxUsageHealthFilterValues = 200
	maxUsageHealthFilterBytes  = 16 * 1024
)

// GetUsageStatistics returns the in-memory request statistics snapshot.
func (h *Handler) GetUsageStatistics(c *gin.Context) {
	var snapshot usage.StatisticsSnapshot
	if stats := h.usageStatisticsSnapshot(); stats != nil {
		snapshot = stats.Snapshot()
	}
	c.JSON(http.StatusOK, gin.H{
		"usage":           snapshot,
		"failed_requests": snapshot.FailureCount,
	})
}

// GetUsageMeta returns the smallest usage statistics view for frontend refresh checks.
func (h *Handler) GetUsageMeta(c *gin.Context) {
	var meta usage.MetaSnapshot
	if stats := h.usageStatisticsSnapshot(); stats != nil {
		meta = stats.Meta()
	}
	c.JSON(http.StatusOK, gin.H{
		"usage":           meta,
		"failed_requests": meta.FailureCount,
	})
}

// ClearUsageStatistics removes all request usage statistics and persists the empty state.
func (h *Handler) ClearUsageStatistics(c *gin.Context) {
	stats := h.usageStatisticsSnapshot()
	if stats == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "usage statistics unavailable"})
		return
	}
	if err := coreusage.DefaultManager().Barrier(c.Request.Context()); err != nil {
		status, message := usageBarrierErrorResponse(err)
		c.JSON(status, gin.H{"error": message})
		return
	}
	path := h.usageStatisticsFilePath()
	previous, err := usage.ClearAndPersistRequestStatistics(path, stats)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"cleared": true,
			"error":   "failed to persist cleared usage statistics",
		})
		return
	}
	meta := stats.Meta()
	c.JSON(http.StatusOK, gin.H{
		"cleared":                true,
		"version":                meta.Version,
		"total_requests_before":  previous.TotalRequests,
		"success_count_before":   previous.SuccessCount,
		"failure_count_before":   previous.FailureCount,
		"total_tokens_before":    previous.TotalTokens,
		"total_requests_after":   meta.TotalRequests,
		"success_count_after":    meta.SuccessCount,
		"failure_count_after":    meta.FailureCount,
		"total_tokens_after":     meta.TotalTokens,
		"failed_requests_before": previous.FailureCount,
		"failed_requests_after":  meta.FailureCount,
	})
}

func usageBarrierErrorResponse(err error) (int, string) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return http.StatusRequestTimeout, "usage statistics clear canceled"
	}
	return http.StatusServiceUnavailable, "usage statistics queue unavailable"
}

// GetUsageSummary returns aggregated usage statistics without request details.
func (h *Handler) GetUsageSummary(c *gin.Context) {
	timeRange, ok := parseUsageTimeRange(c)
	if !ok {
		return
	}
	includeSources := true
	if raw := strings.TrimSpace(c.Query("include_sources")); raw != "" {
		var err error
		includeSources, err = strconv.ParseBool(raw)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid include_sources"})
			return
		}
	}
	var summary usage.SummarySnapshot
	if stats := h.usageStatisticsSnapshot(); stats != nil {
		if timeRange.IsZero() {
			if includeSources {
				summary = stats.Summary()
			} else {
				summary = stats.SummaryWithoutSources()
			}
		} else {
			if includeSources {
				summary = stats.SummaryForRange(timeRange)
			} else {
				summary = stats.SummaryForRangeWithoutSources(timeRange)
			}
		}
	}
	c.JSON(http.StatusOK, gin.H{
		"usage":           summary,
		"failed_requests": summary.FailureCount,
	})
}

// GetUsageDetails returns filtered request usage details with offset pagination.
func (h *Handler) GetUsageDetails(c *gin.Context) {
	query, ok := parseUsageDetailQuery(c)
	if !ok {
		return
	}
	var page usage.DetailPage
	if stats := h.usageStatisticsSnapshot(); stats != nil {
		page = stats.Details(query)
	} else {
		page = usage.DetailPage{Items: []usage.DetailEntry{}, Details: []usage.DetailEntry{}, Limit: query.Limit}
	}
	c.JSON(http.StatusOK, page)
}

// GetUsageAuthSummaries returns per-auth usage summaries enriched with current auth metadata.
func (h *Handler) GetUsageAuthSummaries(c *gin.Context) {
	timeRange, ok := parseUsageTimeRange(c)
	if !ok {
		return
	}
	pageQuery, ok := parseUsageAuthPageQuery(c)
	if !ok {
		return
	}
	authIndexes := parseUsageAuthIndexList(c)
	if pageQuery.Enabled {
		rows := h.cachedUsageAuthRows(timeRange, authIndexes, pageQuery)
		total := len(rows)
		pageRows, page, totalPages := paginateUsageAuthRows(rows, pageQuery.Page, pageQuery.PageSize)
		auths := make([]gin.H, 0, len(pageRows))
		for _, row := range pageRows {
			auths = append(auths, buildUsageAuthResponse(row.Summary, row.Info, row.Stale))
		}
		c.Header("Cache-Control", "no-store")
		c.JSON(http.StatusOK, gin.H{
			"auths": auths,
			"total": total,
			"pagination": gin.H{
				"enabled":     true,
				"page":        page,
				"page_size":   pageQuery.PageSize,
				"total_pages": totalPages,
			},
		})
		return
	}

	infoByIndex := h.usageAuthInfoByIndex()
	summaries := map[string]usage.AuthUsageSnapshot{}
	if stats := h.usageStatisticsSnapshot(); stats != nil {
		query := usage.AuthUsageQuery{TimeRange: timeRange, AuthIndexes: authIndexes}
		for _, summary := range stats.AuthSummariesForQuery(query) {
			summaries[summary.AuthIndex] = summary
		}
	}
	rows := mergeUsageAuthRows(authIndexes, infoByIndex, summaries)
	auths := make([]gin.H, 0, len(rows))
	for _, row := range rows {
		auths = append(auths, buildUsageAuthResponse(row.Summary, row.Info, row.Stale))
	}
	c.JSON(http.StatusOK, gin.H{"auths": auths})
}

// GetUsageFacets returns bounded remote suggestions for high-cardinality usage filters.
func (h *Handler) GetUsageFacets(c *gin.Context) {
	timeRange, ok := parseUsageTimeRange(c)
	if !ok {
		return
	}
	kind := strings.TrimSpace(c.Query("kind"))
	if kind == "" {
		kind = "source"
	}
	if kind != "source" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid kind"})
		return
	}
	query := strings.TrimSpace(c.Query("q"))
	if len(query) > maxUsageAuthSearchBytes {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid q"})
		return
	}
	limit := 100
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 100 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid limit"})
			return
		}
		limit = value
	}
	items := []usage.SourceFacet{}
	total := 0
	if stats := h.usageStatisticsSnapshot(); stats != nil {
		items, total = stats.SourceFacets(timeRange, query, limit)
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, gin.H{"kind": kind, "items": items, "total": total})
}

// GetUsageSeries returns grouped time-series usage aggregates.
func (h *Handler) GetUsageSeries(c *gin.Context) {
	query, ok := parseUsageSeriesQuery(c)
	if !ok {
		return
	}
	var result usage.SeriesResult
	if stats := h.usageStatisticsSnapshot(); stats != nil {
		result = stats.Series(query)
	} else {
		result = usage.SeriesResult{
			Bucket:  query.Bucket,
			GroupBy: query.GroupBy,
			Items:   []usage.SeriesEntry{},
		}
	}
	c.JSON(http.StatusOK, result)
}

// GetUsageHealth returns continuous service-health buckets without request details.
func (h *Handler) GetUsageHealth(c *gin.Context) {
	query, ok := parseUsageHealthQuery(c)
	if !ok {
		return
	}
	var result usage.HealthResult
	if stats := h.usageStatisticsSnapshot(); stats != nil {
		result = stats.Health(query)
	} else {
		result = usage.HealthResult{
			AsOf:    time.Now().UTC(),
			From:    query.TimeRange.From,
			To:      query.TimeRange.To,
			Bucket:  query.Bucket,
			GroupBy: query.GroupBy,
			Items:   []usage.HealthEntry{},
		}
	}
	c.JSON(http.StatusOK, result)
}

// GetUsageRates returns trailing RPM/TPM values and a minute sparkline.
func (h *Handler) GetUsageRates(c *gin.Context) {
	query, ok := parseUsageRatesQuery(c)
	if !ok {
		return
	}
	var result usage.RatesResult
	if stats := h.usageStatisticsSnapshot(); stats != nil {
		result = stats.Rates(query)
	} else {
		result = usage.RatesResult{
			AsOf:             time.Now().UTC(),
			WindowMinutes:    query.WindowMinutes,
			SparklineMinutes: query.SparklineMinutes,
			Items:            []usage.RateEntry{},
		}
	}
	c.JSON(http.StatusOK, result)
}

// GetUsageTokens returns token breakdowns grouped into time buckets.
func (h *Handler) GetUsageTokens(c *gin.Context) {
	query, ok := parseUsageTokenQuery(c)
	if !ok {
		return
	}
	var result usage.TokenResult
	if stats := h.usageStatisticsSnapshot(); stats != nil {
		result = stats.TokensForQuery(query)
	} else {
		result = usage.TokenResult{
			AsOf:    time.Now().UTC(),
			Bucket:  query.Bucket,
			GroupBy: query.GroupBy,
			Items:   []usage.TokenEntry{},
		}
	}
	c.JSON(http.StatusOK, result)
}

// GetUsageCosts returns server-side cost totals and breakdowns using shared prices.
func (h *Handler) GetUsageCosts(c *gin.Context) {
	query, ok := parseUsageCostQuery(c)
	if !ok {
		return
	}
	query.Prices = h.usageCostPrices()
	var result usage.CostResult
	if stats := h.usageStatisticsSnapshot(); stats != nil {
		result = stats.Costs(query)
	} else {
		result = usage.CostResult{
			AsOf:               time.Now().UTC(),
			Bucket:             query.Bucket,
			Total:              usage.Money{Currency: usage.CostCurrencyUSD},
			ByModel:            []usage.ModelCostEntry{},
			ByAPI:              []usage.APICostEntry{},
			Series:             []usage.CostSeriesEntry{},
			UnpricedModels:     []usage.UnpricedModelEntry{},
			UncalculatedModels: []usage.UncalculatedModelEntry{},
		}
	}
	c.JSON(http.StatusOK, result)
}

// GetUsageAuthSummary returns one auth usage summary by auth_index.
func (h *Handler) GetUsageAuthSummary(c *gin.Context) {
	authIndex := strings.TrimSpace(c.Param("auth_index"))
	if authIndex == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "auth_index is required"})
		return
	}
	info, current := h.usageAuthInfoByAuthIndex(authIndex)
	summary, hasUsage := usage.AuthUsageSnapshot{AuthIndex: authIndex}, false
	if stats := h.usageStatisticsSnapshot(); stats != nil {
		summary, hasUsage = stats.AuthSummary(authIndex)
	}
	if !current && !hasUsage {
		c.JSON(http.StatusNotFound, gin.H{"error": "auth usage not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"auth": buildUsageAuthResponse(summary, info, !current)})
}

// GetUsageAuthModelSummaries returns per-model usage summaries for one auth index.
func (h *Handler) GetUsageAuthModelSummaries(c *gin.Context) {
	authIndex := strings.TrimSpace(c.Param("auth_index"))
	if authIndex == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "auth_index is required"})
		return
	}
	info, current := h.usageAuthInfoByAuthIndex(authIndex)
	models := []usage.AuthModelUsageSnapshot{}
	hasUsage := false
	stats := h.usageStatisticsSnapshot()
	if stats != nil {
		models, hasUsage = stats.AuthModelSummaries(authIndex)
	}
	if !current && !hasUsage {
		c.JSON(http.StatusNotFound, gin.H{"error": "auth usage not found"})
		return
	}
	summary := usage.AuthUsageSnapshot{AuthIndex: authIndex}
	if stats != nil {
		if got, ok := stats.AuthSummary(authIndex); ok {
			summary = got
		}
	}
	c.JSON(http.StatusOK, gin.H{
		"auth":   buildUsageAuthResponse(summary, info, !current),
		"models": models,
	})
}

// ExportUsageStatistics returns a complete usage snapshot for backup/migration.
func (h *Handler) ExportUsageStatistics(c *gin.Context) {
	var snapshot usage.StatisticsSnapshot
	if stats := h.usageStatisticsSnapshot(); stats != nil {
		snapshot = stats.Snapshot()
	}
	c.JSON(http.StatusOK, usageExportPayload{
		Version:    1,
		ExportedAt: time.Now().UTC(),
		Usage:      snapshot,
	})
}

// ImportUsageStatistics merges a previously exported usage snapshot into memory.
func (h *Handler) ImportUsageStatistics(c *gin.Context) {
	stats := h.usageStatisticsSnapshot()
	if stats == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "usage statistics unavailable"})
		return
	}

	data, err := c.GetRawData()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read request body"})
		return
	}

	var payload usageImportPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
		return
	}
	if payload.Version != 0 && payload.Version != 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unsupported version"})
		return
	}

	result := stats.MergeSnapshot(payload.Usage)
	snapshot := stats.Snapshot()
	c.JSON(http.StatusOK, gin.H{
		"added":           result.Added,
		"skipped":         result.Skipped,
		"total_requests":  snapshot.TotalRequests,
		"failed_requests": snapshot.FailureCount,
	})
}

func parseUsageDetailQuery(c *gin.Context) (usage.DetailQuery, bool) {
	timeRange, ok := parseUsageTimeRange(c)
	if !ok {
		return usage.DetailQuery{}, false
	}
	sortBy, sortOrder, ok := parseUsageSort(c)
	if !ok {
		return usage.DetailQuery{}, false
	}
	query := usage.DetailQuery{
		API:       strings.TrimSpace(c.Query("api")),
		Model:     strings.TrimSpace(c.Query("model")),
		AuthIndex: strings.TrimSpace(c.Query("auth_index")),
		Source:    strings.TrimSpace(c.Query("source")),
		ClientIP:  strings.TrimSpace(c.Query("client_ip")),
		TimeRange: timeRange,
		SortBy:    sortBy,
		SortOrder: sortOrder,
		Limit:     usage.DefaultDetailsLimit,
	}
	if rawOffset := strings.TrimSpace(c.Query("offset")); rawOffset != "" {
		offset, err := strconv.Atoi(rawOffset)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid offset"})
			return usage.DetailQuery{}, false
		}
		query.Offset = offset
	}
	if rawLimit := strings.TrimSpace(c.Query("limit")); rawLimit != "" {
		limit, err := strconv.Atoi(rawLimit)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid limit"})
			return usage.DetailQuery{}, false
		}
		query.Limit = limit
	}
	if rawFailed := strings.TrimSpace(c.Query("failed")); rawFailed != "" {
		failed, err := strconv.ParseBool(rawFailed)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid failed"})
			return usage.DetailQuery{}, false
		}
		query.Failed = &failed
	}
	return query, true
}

func parseUsageTimeRange(c *gin.Context) (usage.TimeRange, bool) {
	var timeRange usage.TimeRange
	if rawFrom := strings.TrimSpace(c.Query("from")); rawFrom != "" {
		from, err := time.Parse(time.RFC3339, rawFrom)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid from"})
			return usage.TimeRange{}, false
		}
		timeRange.From = from
	}
	if rawTo := strings.TrimSpace(c.Query("to")); rawTo != "" {
		to, err := time.Parse(time.RFC3339, rawTo)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid to"})
			return usage.TimeRange{}, false
		}
		timeRange.To = to
	}
	if !timeRange.From.IsZero() && !timeRange.To.IsZero() && timeRange.From.After(timeRange.To) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid time range"})
		return usage.TimeRange{}, false
	}
	return timeRange, true
}

func parseUsageSort(c *gin.Context) (string, string, bool) {
	sortBy := strings.TrimSpace(c.Query("sort_by"))
	if sortBy == "" {
		sortBy = usage.SortByCreatedAt
	}
	if !isUsageSortBy(sortBy) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid sort_by"})
		return "", "", false
	}
	sortOrder := strings.TrimSpace(c.Query("sort_order"))
	if sortOrder == "" {
		sortOrder = usage.SortOrderDesc
	}
	if sortOrder != usage.SortOrderAsc && sortOrder != usage.SortOrderDesc {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid sort_order"})
		return "", "", false
	}
	return sortBy, sortOrder, true
}

func parseUsageAuthIndexList(c *gin.Context) []string {
	raw := strings.TrimSpace(c.Query("auth_index"))
	if raw == "" {
		return nil
	}
	seen := map[string]struct{}{}
	out := []string{}
	for _, part := range strings.Split(raw, ",") {
		authIndex := strings.TrimSpace(part)
		if authIndex == "" {
			continue
		}
		if _, ok := seen[authIndex]; ok {
			continue
		}
		seen[authIndex] = struct{}{}
		out = append(out, authIndex)
	}
	return out
}

func parseUsageSeriesQuery(c *gin.Context) (usage.SeriesQuery, bool) {
	timeRange, ok := parseUsageTimeRange(c)
	if !ok {
		return usage.SeriesQuery{}, false
	}
	if timeRange.IsZero() {
		now := time.Now().UTC()
		timeRange.From = now.Add(-24 * time.Hour)
		timeRange.To = now
	}
	bucket := strings.TrimSpace(c.Query("bucket"))
	if bucket == "" {
		bucket = usage.BucketHour
	}
	if !isUsageBucket(bucket) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid bucket"})
		return usage.SeriesQuery{}, false
	}
	groupBy := strings.TrimSpace(c.Query("group_by"))
	if groupBy == "" {
		groupBy = usage.GroupByModel
	}
	if !isUsageGroupBy(groupBy) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid group_by"})
		return usage.SeriesQuery{}, false
	}
	return usage.SeriesQuery{
		TimeRange: timeRange,
		Bucket:    bucket,
		GroupBy:   groupBy,
	}, true
}

func parseUsageHealthQuery(c *gin.Context) (usage.HealthQuery, bool) {
	timeRange, ok := parseUsageTimeRange(c)
	if !ok {
		return usage.HealthQuery{}, false
	}
	bucket := strings.TrimSpace(c.Query("bucket"))
	if bucket == "" {
		bucket = usage.Bucket15Min
	}
	if bucket != usage.Bucket15Min && bucket != usage.BucketHour && bucket != usage.BucketDay {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid bucket"})
		return usage.HealthQuery{}, false
	}
	groupBy := strings.TrimSpace(c.Query("group_by"))
	if groupBy == "" {
		groupBy = usage.GroupByNone
	}
	if groupBy != usage.GroupByNone && groupBy != usage.GroupByAuthIndex && groupBy != usage.GroupBySource {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid group_by"})
		return usage.HealthQuery{}, false
	}
	authIndexes, ok := parseUsageCSVList(c, "auth_index")
	if !ok {
		return usage.HealthQuery{}, false
	}
	sources, ok := parseUsageCSVList(c, "source")
	if !ok {
		return usage.HealthQuery{}, false
	}
	explicitGroups := 0
	if groupBy == usage.GroupByAuthIndex {
		explicitGroups = len(authIndexes)
	} else if groupBy == usage.GroupBySource {
		explicitGroups = len(sources)
	}
	if explicitGroups > 0 && usageHealthBucketCount(timeRange, bucket, time.Now().UTC())*explicitGroups > usage.MaxUsageAnalyticsItems {
		c.JSON(http.StatusBadRequest, gin.H{"error": "too many health groups for time range"})
		return usage.HealthQuery{}, false
	}
	return usage.HealthQuery{
		TimeRange:   timeRange,
		Bucket:      bucket,
		GroupBy:     groupBy,
		AuthIndexes: authIndexes,
		Sources:     sources,
	}, true
}

func parseUsageRatesQuery(c *gin.Context) (usage.RatesQuery, bool) {
	windowMinutes, ok := parseUsagePositiveMinutes(c, "window_minutes", usage.DefaultRatesWindowMinutes)
	if !ok {
		return usage.RatesQuery{}, false
	}
	sparklineMinutes, ok := parseUsagePositiveMinutes(c, "sparkline_minutes", usage.DefaultSparklineMinutes)
	if !ok {
		return usage.RatesQuery{}, false
	}
	return usage.RatesQuery{
		WindowMinutes:    windowMinutes,
		SparklineMinutes: sparklineMinutes,
	}, true
}

func parseUsageTokenQuery(c *gin.Context) (usage.TokenQuery, bool) {
	timeRange, ok := parseUsageTimeRange(c)
	if !ok {
		return usage.TokenQuery{}, false
	}
	if timeRange.IsZero() {
		now := time.Now().UTC()
		timeRange = usage.TimeRange{From: now.Add(-usage.DefaultTokenRange), To: now}
	}
	bucket := strings.TrimSpace(c.Query("bucket"))
	if bucket == "" {
		bucket = usage.BucketDay
	}
	if bucket != usage.BucketHour && bucket != usage.BucketDay {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid bucket"})
		return usage.TokenQuery{}, false
	}
	groupBy := strings.TrimSpace(c.Query("group_by"))
	if groupBy == "" {
		groupBy = usage.GroupByNone
	}
	if groupBy != usage.GroupByNone && groupBy != usage.GroupByModel && groupBy != usage.GroupByAPI {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid group_by"})
		return usage.TokenQuery{}, false
	}
	return usage.TokenQuery{TimeRange: timeRange, Bucket: bucket, GroupBy: groupBy}, true
}

func parseUsageCostQuery(c *gin.Context) (usage.CostQuery, bool) {
	timeRange, ok := parseUsageTimeRange(c)
	if !ok {
		return usage.CostQuery{}, false
	}
	if timeRange.IsZero() {
		now := time.Now().UTC()
		timeRange = usage.TimeRange{From: now.Add(-usage.DefaultTokenRange), To: now}
	}
	bucket := strings.TrimSpace(c.Query("bucket"))
	if bucket == "" {
		bucket = usage.BucketDay
	}
	if bucket != usage.BucketHour && bucket != usage.BucketDay {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid bucket"})
		return usage.CostQuery{}, false
	}
	return usage.CostQuery{TimeRange: timeRange, Bucket: bucket}, true
}

func parseUsagePositiveMinutes(c *gin.Context, name string, fallback int) (int, bool) {
	raw := strings.TrimSpace(c.Query(name))
	if raw == "" {
		return fallback, true
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 || value > usage.MaxUsageAnalyticsMinutes {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid " + name})
		return 0, false
	}
	return value, true
}

func parseUsageCSVList(c *gin.Context, name string) ([]string, bool) {
	raw := c.Query(name)
	if len(raw) > maxUsageHealthFilterBytes {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid " + name})
		return nil, false
	}
	seen := map[string]struct{}{}
	values := make([]string, 0)
	for _, value := range strings.Split(raw, ",") {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		values = append(values, value)
		if len(values) > maxUsageHealthFilterValues {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid " + name})
			return nil, false
		}
	}
	return values, true
}

func usageHealthBucketCount(timeRange usage.TimeRange, bucket string, now time.Time) int {
	step := 15 * time.Minute
	if bucket == usage.BucketHour {
		step = time.Hour
	} else if bucket == usage.BucketDay {
		step = 24 * time.Hour
	}
	if timeRange.IsZero() {
		end := truncateUsageManagementTime(now, step).Add(step)
		timeRange = usage.TimeRange{From: end.Add(-usage.DefaultHealthRange), To: end}
	} else {
		if timeRange.To.IsZero() {
			timeRange.To = now
		}
		if timeRange.From.IsZero() {
			timeRange.From = timeRange.To.Add(-usage.DefaultHealthRange)
		}
	}
	first := truncateUsageManagementTime(timeRange.From, step)
	if !first.Before(timeRange.To) {
		return 0
	}
	span := timeRange.To.Sub(first)
	buckets := int(span / step)
	if span%step != 0 {
		buckets++
	}
	if buckets > usage.MaxUsageAnalyticsBuckets {
		return usage.MaxUsageAnalyticsBuckets
	}
	return buckets
}

func truncateUsageManagementTime(value time.Time, step time.Duration) time.Time {
	value = value.UTC()
	if step == 24*time.Hour {
		return time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC)
	}
	return value.Truncate(step)
}

func isUsageSortBy(sortBy string) bool {
	switch sortBy {
	case usage.SortByCreatedAt, usage.SortByTokens, usage.SortByModel, usage.SortByAPI, usage.SortByAuthIndex:
		return true
	default:
		return false
	}
}

func isUsageBucket(bucket string) bool {
	switch bucket {
	case usage.BucketMinute, usage.BucketHour, usage.BucketDay:
		return true
	default:
		return false
	}
}

func isUsageGroupBy(groupBy string) bool {
	switch groupBy {
	case usage.GroupByAPI, usage.GroupByModel, usage.GroupByAuthIndex, usage.GroupBySource, usage.GroupByFailed:
		return true
	default:
		return false
	}
}

func (h *Handler) usageAuthInfoByIndex() map[string]usageAuthInfo {
	if h == nil || h.authManager == nil {
		return map[string]usageAuthInfo{}
	}
	manager := h.authManager
	revision := manager.ManagementAuthCatalogRevision()
	cache := &h.usageAuthCatalog
	cache.mu.Lock()
	if cache.manager == manager && cache.revision == revision && cache.infos != nil {
		infos := cache.infos
		cache.mu.Unlock()
		return infos
	}
	cache.mu.Unlock()

	revision, summaries := manager.UsageAuthCatalogSnapshot()
	infos := make(map[string]usageAuthInfo, len(summaries))
	for _, info := range summaries {
		if authIndex := strings.TrimSpace(info.AuthIndex); authIndex != "" {
			current, exists := infos[authIndex]
			if !exists || strings.TrimSpace(info.ID) < strings.TrimSpace(current.ID) {
				infos[authIndex] = usageAuthInfoFromCore(info)
			}
		}
	}

	cache.mu.Lock()
	if cache.manager != manager || cache.revision <= revision || cache.infos == nil {
		cache.manager = manager
		cache.revision = revision
		cache.infos = infos
	}
	infos = cache.infos
	cache.mu.Unlock()
	return infos
}

func (h *Handler) usageAuthInfoByAuthIndex(authIndex string) (usageAuthInfo, bool) {
	if h == nil || h.authManager == nil {
		return usageAuthInfo{}, false
	}
	info, ok := h.authManager.UsageAuthInfoByIndex(authIndex)
	if !ok {
		return usageAuthInfo{}, false
	}
	return usageAuthInfoFromCore(info), true
}

func buildUsageAuthResponse(summary usage.AuthUsageSnapshot, info usageAuthInfo, stale bool) gin.H {
	if summary.AuthIndex == "" {
		summary.AuthIndex = info.AuthIndex
	}
	item := gin.H{
		"auth_index":     summary.AuthIndex,
		"total_requests": summary.TotalRequests,
		"success_count":  summary.SuccessCount,
		"failure_count":  summary.FailureCount,
		"total_tokens":   summary.TotalTokens,
		"tokens":         summary.Tokens,
		"last_used_at":   summary.LastUsedAt,
		"model_count":    summary.ModelCount,
	}
	if stale {
		item["stale"] = true
	}
	if info.ID != "" {
		item["id"] = info.ID
	}
	if info.Name != "" {
		item["name"] = info.Name
	}
	if info.Provider != "" {
		item["provider"] = info.Provider
		item["type"] = info.Provider
	}
	if info.Label != "" {
		item["label"] = info.Label
	}
	if info.Status != "" {
		item["status"] = info.Status
	}
	item["disabled"] = info.Disabled
	if info.AccountType != "" {
		item["account_type"] = info.AccountType
	}
	if info.Account != "" {
		item["account"] = info.Account
	}
	if info.Email != "" {
		item["email"] = info.Email
	}
	return item
}
