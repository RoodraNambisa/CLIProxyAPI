package management

import (
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

const (
	defaultUsageAuthPageSize = 50
	maxUsageAuthPageSize     = 100
	maxUsageAuthSearchBytes  = 256
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
