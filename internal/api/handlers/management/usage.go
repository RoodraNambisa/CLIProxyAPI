package management

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
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

// GetUsageStatistics returns the in-memory request statistics snapshot.
func (h *Handler) GetUsageStatistics(c *gin.Context) {
	var snapshot usage.StatisticsSnapshot
	if h != nil && h.usageStats != nil {
		snapshot = h.usageStats.Snapshot()
	}
	c.JSON(http.StatusOK, gin.H{
		"usage":           snapshot,
		"failed_requests": snapshot.FailureCount,
	})
}

// GetUsageMeta returns the smallest usage statistics view for frontend refresh checks.
func (h *Handler) GetUsageMeta(c *gin.Context) {
	var meta usage.MetaSnapshot
	if h != nil && h.usageStats != nil {
		meta = h.usageStats.Meta()
	}
	c.JSON(http.StatusOK, gin.H{
		"usage":           meta,
		"failed_requests": meta.FailureCount,
	})
}

// GetUsageSummary returns aggregated usage statistics without request details.
func (h *Handler) GetUsageSummary(c *gin.Context) {
	timeRange, ok := parseUsageTimeRange(c)
	if !ok {
		return
	}
	var summary usage.SummarySnapshot
	if h != nil && h.usageStats != nil {
		if timeRange.IsZero() {
			summary = h.usageStats.Summary()
		} else {
			summary = h.usageStats.SummaryForRange(timeRange)
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
	if h != nil && h.usageStats != nil {
		page = h.usageStats.Details(query)
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
	authIndexes := parseUsageAuthIndexList(c)
	infoByIndex := h.usageAuthInfoByIndex()
	summaries := map[string]usage.AuthUsageSnapshot{}
	if h != nil && h.usageStats != nil {
		query := usage.AuthUsageQuery{
			TimeRange:   timeRange,
			AuthIndexes: authIndexes,
		}
		for _, summary := range h.usageStats.AuthSummariesForQuery(query) {
			summaries[summary.AuthIndex] = summary
		}
	}

	seen := make(map[string]struct{}, len(infoByIndex)+len(summaries))
	auths := make([]gin.H, 0, len(infoByIndex)+len(summaries))
	if len(authIndexes) > 0 {
		for _, authIndex := range authIndexes {
			summary, hasUsage := summaries[authIndex]
			info, current := infoByIndex[authIndex]
			if !hasUsage && !current {
				continue
			}
			if summary.AuthIndex == "" {
				summary.AuthIndex = authIndex
			}
			auths = append(auths, buildUsageAuthResponse(summary, info, !current))
		}
		c.JSON(http.StatusOK, gin.H{"auths": auths})
		return
	}
	for authIndex, info := range infoByIndex {
		summary := summaries[authIndex]
		if summary.AuthIndex == "" {
			summary.AuthIndex = authIndex
		}
		auths = append(auths, buildUsageAuthResponse(summary, info, false))
		seen[authIndex] = struct{}{}
	}
	for authIndex, summary := range summaries {
		if _, ok := seen[authIndex]; ok {
			continue
		}
		auths = append(auths, buildUsageAuthResponse(summary, usageAuthInfo{AuthIndex: authIndex}, true))
	}
	sortUsageAuthResponses(auths)
	c.JSON(http.StatusOK, gin.H{"auths": auths})
}

// GetUsageSeries returns grouped time-series usage aggregates.
func (h *Handler) GetUsageSeries(c *gin.Context) {
	query, ok := parseUsageSeriesQuery(c)
	if !ok {
		return
	}
	var result usage.SeriesResult
	if h != nil && h.usageStats != nil {
		result = h.usageStats.Series(query)
	} else {
		result = usage.SeriesResult{
			Bucket:  query.Bucket,
			GroupBy: query.GroupBy,
			Items:   []usage.SeriesEntry{},
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
	info, current := h.usageAuthInfoByIndex()[authIndex]
	summary, hasUsage := usage.AuthUsageSnapshot{AuthIndex: authIndex}, false
	if h != nil && h.usageStats != nil {
		summary, hasUsage = h.usageStats.AuthSummary(authIndex)
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
	info, current := h.usageAuthInfoByIndex()[authIndex]
	models := []usage.AuthModelUsageSnapshot{}
	hasUsage := false
	if h != nil && h.usageStats != nil {
		models, hasUsage = h.usageStats.AuthModelSummaries(authIndex)
	}
	if !current && !hasUsage {
		c.JSON(http.StatusNotFound, gin.H{"error": "auth usage not found"})
		return
	}
	summary := usage.AuthUsageSnapshot{AuthIndex: authIndex}
	if h != nil && h.usageStats != nil {
		if got, ok := h.usageStats.AuthSummary(authIndex); ok {
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
	if h != nil && h.usageStats != nil {
		snapshot = h.usageStats.Snapshot()
	}
	c.JSON(http.StatusOK, usageExportPayload{
		Version:    1,
		ExportedAt: time.Now().UTC(),
		Usage:      snapshot,
	})
}

// ImportUsageStatistics merges a previously exported usage snapshot into memory.
func (h *Handler) ImportUsageStatistics(c *gin.Context) {
	if h == nil || h.usageStats == nil {
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

	result := h.usageStats.MergeSnapshot(payload.Usage)
	snapshot := h.usageStats.Snapshot()
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
	out := map[string]usageAuthInfo{}
	if h == nil || h.authManager == nil {
		return out
	}
	for _, auth := range h.authManager.List() {
		if auth == nil {
			continue
		}
		authIndex := strings.TrimSpace(auth.Index)
		if authIndex == "" {
			authIndex = auth.EnsureIndex()
		}
		if authIndex == "" {
			continue
		}
		name := strings.TrimSpace(auth.FileName)
		if name == "" {
			name = auth.ID
		}
		accountType, account := auth.AccountInfo()
		out[authIndex] = usageAuthInfo{
			AuthIndex:   authIndex,
			ID:          strings.TrimSpace(auth.ID),
			Name:        name,
			Provider:    strings.TrimSpace(auth.Provider),
			Label:       strings.TrimSpace(auth.Label),
			Status:      string(auth.Status),
			Disabled:    auth.Disabled,
			AccountType: accountType,
			Account:     account,
			Email:       authEmail(auth),
		}
	}
	return out
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

func sortUsageAuthResponses(items []gin.H) {
	sort.Slice(items, func(i, j int) bool {
		left, _ := items[i]["auth_index"].(string)
		right, _ := items[j]["auth_index"].(string)
		return left < right
	})
}
