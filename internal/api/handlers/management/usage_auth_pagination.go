package management

import (
	"container/list"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

const (
	defaultUsageAuthPageSize = 50
	maxUsageAuthPageSize     = 100
	maxUsageAuthSearchBytes  = 256
	maxUsageAuthQueryCache   = 32
	maxUsageAuthCachedRows   = 100_000
)

type usageAuthRow struct {
	Summary usage.AuthUsageSnapshot
	Info    usageAuthInfo
	Stale   bool
}

type usageAuthPageQuery struct {
	Enabled   bool
	Page      int
	PageSize  int
	Search    string
	Providers map[string]struct{}
	Statuses  map[string]struct{}
	SortBy    string
	SortOrder string
}

type usageAuthPaginationCache struct {
	mu              sync.Mutex
	manager         *coreauth.Manager
	catalogRevision uint64
	stats           *usage.RequestStatistics
	usageVersion    uint64
	queries         map[string]*list.Element
	lru             *list.List
	rowCount        int
}

type usageAuthCachedQuery struct {
	key  string
	rows []usageAuthRow
}

func usageAuthQueryCacheKey(query usageAuthPageQuery, timeRange usage.TimeRange, authIndexes []string) string {
	return strings.Join([]string{
		strings.Join(authIndexes, ","),
		timeRange.From.UTC().Format(time.RFC3339Nano),
		timeRange.To.UTC().Format(time.RFC3339Nano),
		query.Search,
		strings.Join(sortedUsageAuthFilterValues(query.Providers), ","),
		strings.Join(sortedUsageAuthFilterValues(query.Statuses), ","),
		query.SortBy,
		query.SortOrder,
	}, "\x00")
}

func sortedUsageAuthFilterValues(values map[string]struct{}) []string {
	if len(values) == 0 {
		return nil
	}
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func (cache *usageAuthPaginationCache) resetLocked(manager *coreauth.Manager, catalogRevision uint64, stats *usage.RequestStatistics, usageVersion uint64) {
	cache.manager = manager
	cache.catalogRevision = catalogRevision
	cache.stats = stats
	cache.usageVersion = usageVersion
	cache.queries = make(map[string]*list.Element)
	cache.lru = list.New()
	cache.rowCount = 0
}

func (cache *usageAuthPaginationCache) lookupLocked(key string) ([]usageAuthRow, bool) {
	if element := cache.queries[key]; element != nil {
		cache.lru.MoveToFront(element)
		return element.Value.(usageAuthCachedQuery).rows, true
	}
	return nil, false
}

func (cache *usageAuthPaginationCache) storeLocked(key string, rows []usageAuthRow) {
	entry := usageAuthCachedQuery{key: key, rows: rows}
	element := cache.lru.PushFront(entry)
	cache.queries[key] = element
	cache.rowCount += len(rows)
	for cache.lru.Len() > maxUsageAuthQueryCache || (cache.rowCount > maxUsageAuthCachedRows && cache.lru.Len() > 1) {
		oldest := cache.lru.Back()
		cached := oldest.Value.(usageAuthCachedQuery)
		delete(cache.queries, cached.key)
		cache.rowCount -= len(cached.rows)
		cache.lru.Remove(oldest)
	}
}

func (h *Handler) cachedUsageAuthRows(timeRange usage.TimeRange, authIndexes []string, query usageAuthPageQuery) []usageAuthRow {
	if h == nil {
		return nil
	}
	manager := h.authManager
	stats := h.usageStatisticsSnapshot()
	cache := &h.usageAuthPagination
	key := usageAuthQueryCacheKey(query, timeRange, authIndexes)
	for attempt := 0; attempt < 3; attempt++ {
		catalogRevision := uint64(0)
		if manager != nil {
			catalogRevision = manager.ManagementAuthCatalogRevision()
		}
		usageVersion := uint64(0)
		if stats != nil {
			usageVersion = stats.Meta().Version
		}

		cache.mu.Lock()
		if cache.manager == manager && cache.catalogRevision == catalogRevision && cache.stats == stats && cache.usageVersion == usageVersion {
			if rows, ok := cache.lookupLocked(key); ok {
				cache.mu.Unlock()
				return rows
			}
		}
		cache.mu.Unlock()

		infos := h.usageAuthInfoByIndex()
		summaries := make(map[string]usage.AuthUsageSnapshot)
		if stats != nil {
			usageQuery := usage.AuthUsageQuery{TimeRange: timeRange, AuthIndexes: authIndexes}
			for _, summary := range stats.AuthSummariesForQuery(usageQuery) {
				summaries[summary.AuthIndex] = summary
			}
		}
		rows := mergeUsageAuthRows(authIndexes, infos, summaries)
		rows = filterUsageAuthRows(rows, query)
		sortUsageAuthRows(rows, query)

		latestCatalogRevision := uint64(0)
		if manager != nil {
			latestCatalogRevision = manager.ManagementAuthCatalogRevision()
		}
		latestStats := h.usageStatisticsSnapshot()
		latestUsageVersion := uint64(0)
		if latestStats != nil {
			latestUsageVersion = latestStats.Meta().Version
		}
		if latestCatalogRevision != catalogRevision || latestStats != stats || latestUsageVersion != usageVersion {
			stats = latestStats
			if attempt < 2 {
				continue
			}
			return rows
		}

		cache.mu.Lock()
		if cache.manager == manager && cache.stats == stats &&
			(cache.catalogRevision > catalogRevision || cache.usageVersion > usageVersion) {
			if cached, ok := cache.lookupLocked(key); ok {
				cache.mu.Unlock()
				return cached
			}
			cache.mu.Unlock()
			return rows
		}
		if cache.manager != manager || cache.catalogRevision != catalogRevision || cache.stats != stats || cache.usageVersion != usageVersion {
			cache.resetLocked(manager, catalogRevision, stats, usageVersion)
		}
		if cached, ok := cache.lookupLocked(key); ok {
			cache.mu.Unlock()
			return cached
		}
		cache.storeLocked(key, rows)
		cache.mu.Unlock()
		return rows
	}
	return nil
}

func parseUsageAuthPageQuery(c *gin.Context) (usageAuthPageQuery, bool) {
	query := usageAuthPageQuery{Page: 1, PageSize: defaultUsageAuthPageSize, SortBy: "auth_index", SortOrder: "asc"}
	if raw := strings.TrimSpace(c.Query("paged")); raw != "" {
		enabled, err := strconv.ParseBool(raw)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid paged"})
			return usageAuthPageQuery{}, false
		}
		query.Enabled = enabled
	}
	if !query.Enabled {
		return query, true
	}
	if raw := strings.TrimSpace(c.Query("page")); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid page"})
			return usageAuthPageQuery{}, false
		}
		query.Page = value
	}
	if raw := strings.TrimSpace(c.Query("page_size")); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > maxUsageAuthPageSize {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid page_size"})
			return usageAuthPageQuery{}, false
		}
		query.PageSize = value
	}
	query.Search = strings.TrimSpace(c.Query("q"))
	if len(query.Search) > maxUsageAuthSearchBytes {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid q"})
		return usageAuthPageQuery{}, false
	}
	query.Providers = usageAuthFilterSet(c.Query("provider"))
	query.Statuses = usageAuthFilterSet(c.Query("status"))
	if raw := strings.TrimSpace(c.Query("sort_by")); raw != "" {
		query.SortBy = raw
	}
	switch query.SortBy {
	case "auth_index", "name", "provider", "status", "total_requests", "total_tokens", "last_used_at":
	default:
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid sort_by"})
		return usageAuthPageQuery{}, false
	}
	if raw := strings.TrimSpace(c.Query("sort_order")); raw != "" {
		query.SortOrder = raw
	}
	if query.SortOrder != "asc" && query.SortOrder != "desc" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid sort_order"})
		return usageAuthPageQuery{}, false
	}
	return query, true
}

func usageAuthFilterSet(raw string) map[string]struct{} {
	values := make(map[string]struct{})
	for _, part := range strings.Split(raw, ",") {
		if value := strings.ToLower(strings.TrimSpace(part)); value != "" {
			values[value] = struct{}{}
		}
	}
	return values
}

func mergeUsageAuthRows(authIndexes []string, infos map[string]usageAuthInfo, summaries map[string]usage.AuthUsageSnapshot) []usageAuthRow {
	if len(authIndexes) > 0 {
		rows := make([]usageAuthRow, 0, len(authIndexes))
		for _, authIndex := range authIndexes {
			summary, hasUsage := summaries[authIndex]
			info, current := infos[authIndex]
			if !hasUsage && !current {
				continue
			}
			if summary.AuthIndex == "" {
				summary.AuthIndex = authIndex
			}
			rows = append(rows, usageAuthRow{Summary: summary, Info: info, Stale: !current})
		}
		return rows
	}

	rows := make([]usageAuthRow, 0, len(infos)+len(summaries))
	seen := make(map[string]struct{}, len(infos))
	for authIndex, info := range infos {
		summary := summaries[authIndex]
		if summary.AuthIndex == "" {
			summary.AuthIndex = authIndex
		}
		rows = append(rows, usageAuthRow{Summary: summary, Info: info})
		seen[authIndex] = struct{}{}
	}
	for authIndex, summary := range summaries {
		if _, ok := seen[authIndex]; ok {
			continue
		}
		if summary.AuthIndex == "" {
			summary.AuthIndex = authIndex
		}
		rows = append(rows, usageAuthRow{Summary: summary, Info: usageAuthInfo{AuthIndex: authIndex}, Stale: true})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Summary.AuthIndex < rows[j].Summary.AuthIndex })
	return rows
}

func filterUsageAuthRows(rows []usageAuthRow, query usageAuthPageQuery) []usageAuthRow {
	search := strings.ToLower(query.Search)
	filtered := make([]usageAuthRow, 0, len(rows))
	for _, row := range rows {
		if len(query.Providers) > 0 {
			if _, ok := query.Providers[strings.ToLower(row.Info.Provider)]; !ok {
				continue
			}
		}
		if len(query.Statuses) > 0 && !usageAuthStatusMatches(row, query.Statuses) {
			continue
		}
		if search != "" && !usageAuthSearchMatches(row, search) {
			continue
		}
		filtered = append(filtered, row)
	}
	return filtered
}

func usageAuthStatusMatches(row usageAuthRow, statuses map[string]struct{}) bool {
	if row.Stale {
		_, ok := statuses["stale"]
		return ok
	}
	if row.Info.Disabled {
		_, ok := statuses["disabled"]
		return ok
	}
	if _, ok := statuses[strings.ToLower(row.Info.Status)]; ok {
		return true
	}
	_, enabled := statuses["enabled"]
	return enabled && !row.Info.Disabled
}

func usageAuthSearchMatches(row usageAuthRow, search string) bool {
	values := [...]string{
		row.Summary.AuthIndex,
		row.Info.ID,
		row.Info.Name,
		row.Info.Provider,
		row.Info.Label,
		row.Info.Status,
		row.Info.AccountType,
		row.Info.Account,
		row.Info.Email,
	}
	for _, value := range values {
		if strings.Contains(strings.ToLower(value), search) {
			return true
		}
	}
	return false
}

func sortUsageAuthRows(rows []usageAuthRow, query usageAuthPageQuery) {
	sort.Slice(rows, func(i, j int) bool {
		comparison := compareUsageAuthRows(rows[i], rows[j], query.SortBy)
		if comparison == 0 {
			return rows[i].Summary.AuthIndex < rows[j].Summary.AuthIndex
		}
		if query.SortOrder == "desc" {
			return comparison > 0
		}
		return comparison < 0
	})
}

func compareUsageAuthRows(left, right usageAuthRow, sortBy string) int {
	switch sortBy {
	case "name":
		return strings.Compare(strings.ToLower(left.Info.Name), strings.ToLower(right.Info.Name))
	case "provider":
		return strings.Compare(strings.ToLower(left.Info.Provider), strings.ToLower(right.Info.Provider))
	case "status":
		return strings.Compare(usageAuthStatusValue(left), usageAuthStatusValue(right))
	case "total_requests":
		return compareInt64(left.Summary.TotalRequests, right.Summary.TotalRequests)
	case "total_tokens":
		return compareInt64(left.Summary.TotalTokens, right.Summary.TotalTokens)
	case "last_used_at":
		return compareTimePointers(left.Summary.LastUsedAt, right.Summary.LastUsedAt)
	default:
		return strings.Compare(left.Summary.AuthIndex, right.Summary.AuthIndex)
	}
}

func usageAuthStatusValue(row usageAuthRow) string {
	if row.Stale {
		return "stale"
	}
	if row.Info.Disabled {
		return "disabled"
	}
	return strings.ToLower(row.Info.Status)
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

func compareTimePointers(left, right *time.Time) int {
	switch {
	case left == nil && right == nil:
		return 0
	case left == nil:
		return -1
	case right == nil:
		return 1
	case left.Before(*right):
		return -1
	case left.After(*right):
		return 1
	default:
		return 0
	}
}

func paginateUsageAuthRows(rows []usageAuthRow, page, pageSize int) ([]usageAuthRow, int, int) {
	totalPages := 0
	if len(rows) > 0 {
		totalPages = (len(rows) + pageSize - 1) / pageSize
		if page > totalPages {
			page = totalPages
		}
	}
	if totalPages == 0 {
		return []usageAuthRow{}, 1, 0
	}
	start := (page - 1) * pageSize
	end := min(start+pageSize, len(rows))
	return rows[start:end], page, totalPages
}

func usageAuthInfoFromCore(info coreauth.UsageAuthInfo) usageAuthInfo {
	return usageAuthInfo{
		AuthIndex:   info.AuthIndex,
		ID:          info.ID,
		Name:        info.Name,
		Provider:    info.Provider,
		Label:       info.Label,
		Status:      info.Status,
		Disabled:    info.Disabled,
		AccountType: info.AccountType,
		Account:     info.Account,
		Email:       info.Email,
	}
}
