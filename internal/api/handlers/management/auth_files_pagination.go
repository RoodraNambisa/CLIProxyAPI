package management

import (
	"container/list"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

const (
	defaultAuthFilesPageSize = 20
	maxAuthFilesPageSize     = 100
	maxAuthFilesFilterLength = 512
	maxAuthFilesQueryCache   = 32
)

type authFilesPaginationCache struct {
	mu              sync.Mutex
	manager         *coreauth.Manager
	revision        uint64
	authDir         string
	retiredRevision uint64
	records         []*authFileListRecord
	retiredRecords  []*authFileListRecord
	managedNames    map[string]struct{}
	queries         map[string]*list.Element
	lru             *list.List
}

type authFilesCachedQuery struct {
	key     string
	matched []*authFileListRecord
	facets  authFilesFacetsResponse
}

type authFilesListQuery struct {
	page         int
	pageSize     int
	provider     string
	plan         string
	priority     string
	problemOnly  bool
	enabledOnly  bool
	disabledOnly bool
	search       string
	searchRE     *regexp.Regexp
	sort         string
}

type authFileListRecord struct {
	auth         *coreauth.Auth
	retiredEntry gin.H
	name         string
	id           string
	provider     string
	plan         string
	priority     int
	prioritySet  bool
	disabled     bool
	problem      bool
	runtimeOnly  bool
	retired      bool
	searchValues []string
}

type authFilesPaginationResponse struct {
	Enabled    bool `json:"enabled"`
	Page       int  `json:"page,omitempty"`
	PageSize   int  `json:"page_size,omitempty"`
	TotalPages int  `json:"total_pages,omitempty"`
}

type authFilesFacetValue struct {
	Value string `json:"value"`
	Count int    `json:"count"`
}

type authFilesFacetsResponse struct {
	Providers  []authFilesFacetValue `json:"providers"`
	Priorities []authFilesFacetValue `json:"priorities"`
	Plans      []authFilesFacetValue `json:"plans"`
}

func authFilesQueryCacheKey(query authFilesListQuery) string {
	return strings.Join([]string{
		query.provider,
		query.plan,
		query.priority,
		strconv.FormatBool(query.problemOnly),
		strconv.FormatBool(query.enabledOnly),
		strconv.FormatBool(query.disabledOnly),
		query.search,
		query.sort,
	}, "\x00")
}

func (cache *authFilesPaginationCache) resetLocked(manager *coreauth.Manager, revision uint64, authDir string, retiredRevision uint64, records, retiredRecords []*authFileListRecord, managedNames map[string]struct{}) {
	cache.manager = manager
	cache.revision = revision
	cache.authDir = authDir
	cache.retiredRevision = retiredRevision
	cache.records = records
	cache.retiredRecords = retiredRecords
	cache.managedNames = managedNames
	cache.queries = make(map[string]*list.Element)
	cache.lru = list.New()
}

func (cache *authFilesPaginationCache) queryLocked(query authFilesListQuery) authFilesCachedQuery {
	key := authFilesQueryCacheKey(query)
	if element := cache.queries[key]; element != nil {
		cache.lru.MoveToFront(element)
		return element.Value.(authFilesCachedQuery)
	}
	result := buildAuthFilesCachedQuery(cache.records, query)
	element := cache.lru.PushFront(result)
	cache.queries[key] = element
	if cache.lru.Len() > maxAuthFilesQueryCache {
		oldest := cache.lru.Back()
		delete(cache.queries, oldest.Value.(authFilesCachedQuery).key)
		cache.lru.Remove(oldest)
	}
	return result
}

func buildAuthFilesCachedQuery(records []*authFileListRecord, query authFilesListQuery) authFilesCachedQuery {
	statusRecords := filterAuthFileStatusRecords(records, query)
	result := authFilesCachedQuery{
		key:    authFilesQueryCacheKey(query),
		facets: buildAuthFilesFacets(statusRecords, query.priority),
	}
	result.matched = filterAuthFileRecords(statusRecords, query)
	sortAuthFileRecords(result.matched, query.sort)
	return result
}

func (h *Handler) cachedAuthFileRecords(manager *coreauth.Manager, query authFilesListQuery) authFilesCachedQuery {
	if h == nil || manager == nil {
		return authFilesCachedQuery{}
	}
	cache := &h.authFilesPagination
	for attempt := 0; attempt < 3; attempt++ {
		cfg := h.currentConfig()
		authDir := ""
		if cfg != nil {
			authDir = strings.TrimSpace(cfg.AuthDir)
		}
		retiredRevision := authfileguard.RetiredRevision()
		revision := manager.ManagementAuthCatalogRevision()

		cache.mu.Lock()
		if cache.manager == manager && cache.revision == revision && cache.authDir == authDir && cache.retiredRevision == retiredRevision && cache.retiredRecords != nil {
			result := cache.queryLocked(query)
			cache.mu.Unlock()
			return result
		}
		cache.mu.Unlock()

		revision, summaries := manager.ManagementAuthCatalogSnapshot()
		records, managedNames := h.authFileListRecordsFromSummaries(summaries)
		var retiredRecords []*authFileListRecord
		cache.mu.Lock()
		if cache.authDir == authDir && cache.retiredRevision == retiredRevision && authFilesNameSetsEqual(cache.managedNames, managedNames) && cache.retiredRecords != nil {
			retiredRecords = cache.retiredRecords
		}
		cache.mu.Unlock()
		if retiredRecords == nil {
			retiredRecords = h.retiredAuthFileListRecords(managedNames)
		}
		records = append(records, retiredRecords...)
		latestCfg := h.currentConfig()
		latestAuthDir := ""
		if latestCfg != nil {
			latestAuthDir = strings.TrimSpace(latestCfg.AuthDir)
		}
		if latestAuthDir != authDir || manager.ManagementAuthCatalogRevision() != revision || authfileguard.RetiredRevision() != retiredRevision {
			if attempt < 2 {
				continue
			}
			return buildAuthFilesCachedQuery(records, query)
		}

		cache.mu.Lock()
		if cache.manager == manager && cache.revision > revision && cache.authDir == authDir && cache.retiredRevision == retiredRevision && cache.retiredRecords != nil {
			result := cache.queryLocked(query)
			cache.mu.Unlock()
			return result
		}
		if cache.manager != manager || cache.revision <= revision || cache.authDir != authDir || cache.retiredRevision != retiredRevision {
			cache.resetLocked(manager, revision, authDir, retiredRevision, records, retiredRecords, managedNames)
		}
		result := cache.queryLocked(query)
		cache.mu.Unlock()
		return result
	}
	return authFilesCachedQuery{}
}

func parseAuthFilesPagedFlag(c *gin.Context) (bool, error) {
	if c == nil || c.Request == nil || c.Request.URL == nil {
		return false, nil
	}
	values, present := c.Request.URL.Query()["paged"]
	if !present {
		return false, nil
	}
	if len(values) != 1 {
		return false, fmt.Errorf("paged must be true or false")
	}
	value := strings.TrimSpace(values[0])
	if value != "true" && value != "false" {
		return false, fmt.Errorf("paged must be true or false")
	}
	return value == "true", nil
}

func authFilesPagedRequested(c *gin.Context) bool {
	paged, err := parseAuthFilesPagedFlag(c)
	return err == nil && paged
}

func writeLegacyAuthFilesResponse(c *gin.Context, files []gin.H, paged bool) {
	response := gin.H{"files": files}
	if paged {
		response["total"] = len(files)
		response["pagination"] = authFilesPaginationResponse{Enabled: false}
	}
	c.JSON(http.StatusOK, response)
}

func parseAuthFilesListQuery(c *gin.Context, pagination bool) (authFilesListQuery, error) {
	query := authFilesListQuery{
		page:     1,
		pageSize: defaultAuthFilesPageSize,
		provider: "all",
		plan:     "all",
		priority: "all",
		sort:     "default",
	}
	if c == nil || c.Request == nil || c.Request.URL == nil {
		return query, nil
	}
	values := c.Request.URL.Query()
	var err error
	pageValue, errPageValue := authFilesSingleQueryValue(values, "page")
	if errPageValue != nil {
		return query, errPageValue
	}
	pageSizeValue, errPageSizeValue := authFilesSingleQueryValue(values, "page_size")
	if errPageSizeValue != nil {
		return query, errPageSizeValue
	}
	if pagination {
		if query.page, err = parsePositiveAuthFilesInt(pageValue, 1, 0, "page"); err != nil {
			return query, err
		}
		if query.pageSize, err = parsePositiveAuthFilesInt(pageSizeValue, defaultAuthFilesPageSize, maxAuthFilesPageSize, "page_size"); err != nil {
			return query, err
		}
	}
	if query.problemOnly, err = parseAuthFilesBool(values, "problem_only"); err != nil {
		return query, err
	}
	if query.enabledOnly, err = parseAuthFilesBool(values, "enabled_only"); err != nil {
		return query, err
	}
	if query.disabledOnly, err = parseAuthFilesBool(values, "disabled_only"); err != nil {
		return query, err
	}
	if query.enabledOnly && query.disabledOnly {
		return query, fmt.Errorf("enabled_only and disabled_only cannot both be true")
	}
	providerValue, errProvider := authFilesSingleQueryValue(values, "provider")
	if errProvider != nil {
		return query, errProvider
	}
	planValue, errPlan := authFilesSingleQueryValue(values, "plan")
	if errPlan != nil {
		return query, errPlan
	}
	priorityValue, errPriorityValue := authFilesSingleQueryValue(values, "priority")
	if errPriorityValue != nil {
		return query, errPriorityValue
	}
	sortValue, errSort := authFilesSingleQueryValue(values, "sort")
	if errSort != nil {
		return query, errSort
	}
	searchValue, errSearch := authFilesSingleQueryValue(values, "search")
	if errSearch != nil {
		return query, errSearch
	}
	query.provider = normalizeAuthFilesProvider(authFilesFilterValue(providerValue, "all"))
	query.plan = strings.ToLower(authFilesFilterValue(planValue, "all"))
	query.priority = authFilesFilterValue(priorityValue, "all")
	if query.priority != "all" && query.priority != "__unset__" {
		if _, errPriority := strconv.Atoi(query.priority); errPriority != nil {
			return query, fmt.Errorf("priority must be all, __unset__, or an integer")
		}
	}
	query.sort = authFilesFilterValue(sortValue, "default")
	switch query.sort {
	case "default", "az", "priority":
	default:
		return query, fmt.Errorf("sort must be default, az, or priority")
	}
	query.search = strings.TrimSpace(searchValue)
	for name, value := range map[string]string{
		"provider": query.provider,
		"plan":     query.plan,
		"priority": query.priority,
		"search":   query.search,
	} {
		if len(value) > maxAuthFilesFilterLength {
			return query, fmt.Errorf("%s is too long", name)
		}
	}
	if strings.Contains(query.search, "*") {
		parts := strings.Split(query.search, "*")
		for index := range parts {
			parts[index] = regexp.QuoteMeta(parts[index])
		}
		compiled, errCompile := regexp.Compile("(?i)" + strings.Join(parts, ".*"))
		if errCompile != nil {
			return query, fmt.Errorf("search is invalid")
		}
		query.searchRE = compiled
	}
	return query, nil
}

func authFilesSingleQueryValue(values map[string][]string, name string) (string, error) {
	rawValues, present := values[name]
	if !present {
		return "", nil
	}
	if len(rawValues) != 1 {
		return "", fmt.Errorf("%s must be specified once", name)
	}
	return rawValues[0], nil
}

func parsePositiveAuthFilesInt(raw string, fallback, maximum int, name string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 || (maximum > 0 && value > maximum) {
		if maximum > 0 {
			return 0, fmt.Errorf("%s must be between 1 and %d", name, maximum)
		}
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return value, nil
}

func parseAuthFilesBool(values map[string][]string, name string) (bool, error) {
	rawValues, present := values[name]
	if !present {
		return false, nil
	}
	if len(rawValues) != 1 {
		return false, fmt.Errorf("%s must be true or false", name)
	}
	value := strings.TrimSpace(rawValues[0])
	if value != "true" && value != "false" {
		return false, fmt.Errorf("%s must be true or false", name)
	}
	return value == "true", nil
}

func authFilesFilterValue(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}

func (h *Handler) listAuthFilesPaged(c *gin.Context, manager *coreauth.Manager) {
	query, errQuery := parseAuthFilesListQuery(c, true)
	if errQuery != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errQuery.Error()})
		return
	}
	cached := h.cachedAuthFileRecords(manager, query)
	matched := cached.matched

	total := len(matched)
	totalPages := max(1, (total+query.pageSize-1)/query.pageSize)
	if query.page > totalPages {
		query.page = totalPages
	}
	start := (query.page - 1) * query.pageSize
	if start > total {
		start = total
	}
	end := min(total, start+query.pageSize)
	pageRecords := matched[start:end]
	pageIDs := make([]string, 0, len(pageRecords))
	for _, record := range pageRecords {
		if record.auth != nil {
			pageIDs = append(pageIDs, record.id)
		}
	}
	pageAuths := manager.GetByIDs(pageIDs)
	graph, _ := manager.ChatGPTWebDependencyGraphForAuths(pageAuths)
	pageAuthByID := make(map[string]*coreauth.Auth, len(pageAuths))
	for _, auth := range pageAuths {
		pageAuthByID[auth.ID] = auth
	}
	runtimeSummaries := h.authFileRuntimeSummariesForAuths(manager, pageAuths)
	now := time.Now()
	files := make([]gin.H, 0, len(pageRecords))
	for _, record := range pageRecords {
		auth := pageAuthByID[record.id]
		if auth == nil {
			if record.retiredEntry != nil {
				files = append(files, record.retiredEntry)
			}
			continue
		}
		runtimeSummary := authFileRuntimeSummaryForAuth(auth, graph, runtimeSummaries)
		entry := h.buildAuthFileEntryAtWithRuntime(auth, now, runtimeSummary)
		if entry == nil {
			continue
		}
		applyChatGPTWebDependencySummary(entry, auth, graph)
		files = append(files, entry)
	}
	c.JSON(http.StatusOK, gin.H{
		"files": files,
		"total": total,
		"pagination": authFilesPaginationResponse{
			Enabled:    true,
			Page:       query.page,
			PageSize:   query.pageSize,
			TotalPages: totalPages,
		},
		"facets": cached.facets,
	})
}

// ListAuthFileSelection returns safe descriptors for all matching batch-operation targets.
func (h *Handler) ListAuthFileSelection(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	if h == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "handler not initialized"})
		return
	}
	query, errQuery := parseAuthFilesListQuery(c, false)
	if errQuery != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errQuery.Error()})
		return
	}
	h.mu.Lock()
	manager := h.authManager
	h.mu.Unlock()
	if manager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "auth manager is unavailable"})
		return
	}
	cached := h.cachedAuthFileRecords(manager, query)
	matched := cached.matched
	files := make([]gin.H, 0, len(matched))
	now := time.Now()
	ids := make([]string, 0, len(matched))
	for _, record := range matched {
		if record.auth != nil && !record.runtimeOnly && !record.retired {
			ids = append(ids, record.id)
		}
	}
	selectedAuths := manager.GetByIDs(ids)
	graph, _ := manager.ChatGPTWebDependencyGraphForAuths(selectedAuths)
	authByID := make(map[string]*coreauth.Auth, len(ids))
	for _, auth := range selectedAuths {
		authByID[auth.ID] = auth
	}
	for _, record := range matched {
		current := authByID[record.id]
		if current == nil || record.runtimeOnly || record.retired {
			continue
		}
		currentRecord := *record
		currentRecord.auth = current
		files = append(files, buildAuthFileSelectionEntry(&currentRecord, graph, now))
	}
	c.JSON(http.StatusOK, gin.H{"files": files, "total": len(files)})
}

func (h *Handler) authFileListRecordsFromSummaries(auths []*coreauth.Auth) ([]*authFileListRecord, map[string]struct{}) {
	records := make([]*authFileListRecord, 0, len(auths))
	managedNames := make(map[string]struct{}, len(auths))

	root, lexicalAuthDir, authDir, errRoot := h.openManagedAuthRootSnapshot()
	if errRoot == nil {
		defer closeManagedAuthRoot(root)
	}
	for _, auth := range auths {
		if auth == nil {
			continue
		}
		runtimeOnly := isRuntimeOnlyAuth(auth)
		if runtimeOnly && (auth.Disabled || auth.Status == coreauth.StatusDisabled) {
			continue
		}
		path := strings.TrimSpace(authAttribute(auth, "path"))
		if path == "" && !runtimeOnly {
			continue
		}
		managedName := ""
		managed := false
		if fileName := filepath.ToSlash(strings.TrimSpace(auth.FileName)); isTopLevelManagedAuthName(fileName) {
			managedName = fileName
			managed = true
		} else if errRoot == nil {
			managedName, managed = managedAuthBackingFileNameAtRoot(root, lexicalAuthDir, authDir, auth)
		}
		if managed && !isTopLevelManagedAuthName(managedName) {
			continue
		}
		name := strings.TrimSpace(auth.FileName)
		if managed {
			name = managedName
		}
		if name == "" {
			name = auth.ID
		}
		record := authFileRecordForAuth(auth, name, runtimeOnly)
		records = append(records, record)
		if managed && managedName != "" {
			managedNames[managedAuthNameKey(managedName)] = struct{}{}
		}
	}
	return records, managedNames
}

func (h *Handler) retiredAuthFileListRecords(managedNames map[string]struct{}) []*authFileListRecord {
	entries, errRetired := h.listRetiredGeminiCLIAuthFilesExcluding(managedNames)
	if errRetired != nil {
		return nil
	}
	records := make([]*authFileListRecord, 0, len(entries))
	for _, entry := range entries {
		records = append(records, authFileRecordForRetiredEntry(entry))
	}
	return records
}

func authFilesNameSetsEqual(left, right map[string]struct{}) bool {
	if len(left) != len(right) {
		return false
	}
	for name := range left {
		if _, ok := right[name]; !ok {
			return false
		}
	}
	return true
}

func authFileRecordForAuth(auth *coreauth.Auth, name string, runtimeOnly bool) *authFileListRecord {
	provider := normalizeAuthFilesProvider(auth.Provider)
	statusMessage := strings.TrimSpace(auth.StatusMessage)
	plan := effectiveCodexPlanType(auth)
	if strings.EqualFold(provider, chatgptwebauth.Provider) {
		statusMessage = chatgptwebauth.SafeLifecycleReason(stringValue(auth.Metadata, "lifecycle_reason"))
		plan = strings.TrimSpace(stringValue(auth.Metadata, "plan_type"))
	} else if plan == "" {
		plan = strings.TrimSpace(stringValue(auth.Metadata, "plan_type"))
	}
	plan = strings.ToLower(plan)
	priority, prioritySet := authFilePriority(auth)
	note := authFileNote(auth)
	searchValues := []string{name, provider, plan, string(auth.Status), note, statusMessage, authEmail(auth)}
	if auth.LastError != nil {
		searchValues = append(searchValues, auth.LastError.Code, auth.LastError.Message)
	}
	return &authFileListRecord{
		auth:         auth,
		name:         name,
		id:           auth.ID,
		provider:     provider,
		plan:         plan,
		priority:     priority,
		prioritySet:  prioritySet,
		disabled:     auth.Disabled,
		problem:      strings.TrimSpace(statusMessage) != "",
		runtimeOnly:  runtimeOnly,
		searchValues: searchValues,
	}
}

func authFileRecordForRetiredEntry(entry gin.H) *authFileListRecord {
	name, _ := entry["name"].(string)
	provider, _ := entry["provider"].(string)
	provider = normalizeAuthFilesProvider(provider)
	statusMessage, _ := entry["status_message"].(string)
	disabled, _ := entry["disabled"].(bool)
	return &authFileListRecord{
		retiredEntry: entry,
		name:         name,
		provider:     provider,
		disabled:     disabled,
		problem:      strings.TrimSpace(statusMessage) != "",
		retired:      true,
		searchValues: []string{name, provider, "unsupported", statusMessage},
	}
}

func normalizeAuthFilesProvider(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, "_", "-")
	if value == "" {
		return "unknown"
	}
	if value == "grok" || value == "x-ai" {
		return "xai"
	}
	return value
}

func authFilePriority(auth *coreauth.Auth) (int, bool) {
	if auth == nil {
		return 0, false
	}
	if raw := strings.TrimSpace(authAttribute(auth, "priority")); raw != "" {
		value, err := strconv.Atoi(raw)
		return value, err == nil
	}
	if auth.Metadata == nil {
		return 0, false
	}
	switch value := auth.Metadata["priority"].(type) {
	case int:
		return value, true
	case int64:
		return int(value), true
	case float64:
		return int(value), true
	case json.Number:
		parsed, err := strconv.Atoi(string(value))
		return parsed, err == nil
	case string:
		parsed, err := strconv.Atoi(strings.TrimSpace(value))
		return parsed, err == nil
	default:
		return 0, false
	}
}

func authFileNote(auth *coreauth.Auth) string {
	if note := strings.TrimSpace(authAttribute(auth, "note")); note != "" {
		return note
	}
	if auth != nil && auth.Metadata != nil {
		return strings.TrimSpace(stringValue(auth.Metadata, "note"))
	}
	return ""
}

func filterAuthFileStatusRecords(records []*authFileListRecord, query authFilesListQuery) []*authFileListRecord {
	filtered := make([]*authFileListRecord, 0, len(records))
	for _, record := range records {
		if record == nil || (query.problemOnly && !record.problem) || (query.enabledOnly && record.disabled) || (query.disabledOnly && !record.disabled) {
			continue
		}
		filtered = append(filtered, record)
	}
	return filtered
}

func filterAuthFileRecords(records []*authFileListRecord, query authFilesListQuery) []*authFileListRecord {
	filtered := make([]*authFileListRecord, 0, len(records))
	for _, record := range records {
		if record == nil {
			continue
		}
		if query.provider != "all" && !strings.EqualFold(record.provider, query.provider) {
			continue
		}
		if query.plan != "all" && record.plan != query.plan {
			continue
		}
		switch query.priority {
		case "all":
		case "__unset__":
			if record.prioritySet {
				continue
			}
		default:
			if !record.prioritySet || strconv.Itoa(record.priority) != query.priority {
				continue
			}
		}
		if !authFileRecordMatchesSearch(record, query) {
			continue
		}
		filtered = append(filtered, record)
	}
	return filtered
}

func authFileRecordMatchesSearch(record *authFileListRecord, query authFilesListQuery) bool {
	if query.search == "" {
		return true
	}
	needle := strings.ToLower(query.search)
	for _, value := range record.searchValues {
		if query.searchRE != nil {
			if query.searchRE.MatchString(value) {
				return true
			}
		} else if strings.Contains(strings.ToLower(value), needle) {
			return true
		}
	}
	return false
}

func sortAuthFileRecords(records []*authFileListRecord, mode string) {
	sort.SliceStable(records, func(i, j int) bool {
		a, b := records[i], records[j]
		if mode == "priority" && a.priority != b.priority {
			return a.priority > b.priority
		}
		if mode == "default" {
			providerCompare := strings.Compare(strings.ToLower(a.provider), strings.ToLower(b.provider))
			if providerCompare != 0 {
				return providerCompare < 0
			}
		}
		nameCompare := strings.Compare(strings.ToLower(a.name), strings.ToLower(b.name))
		if nameCompare != 0 {
			return nameCompare < 0
		}
		if a.name != b.name {
			return a.name < b.name
		}
		return a.id < b.id
	})
}

func buildAuthFilesFacets(records []*authFileListRecord, priorityFilter string) authFilesFacetsResponse {
	providerCounts := make(map[string]int)
	priorityCounts := make(map[string]int)
	planCounts := make(map[string]int)
	for _, record := range records {
		if record.provider != "" {
			providerCounts[record.provider]++
		}
		priorityKey := "__unset__"
		if record.prioritySet {
			priorityKey = strconv.Itoa(record.priority)
		}
		priorityCounts[priorityKey]++
		priorityMatches := priorityFilter == "all" || priorityFilter == priorityKey
		if priorityMatches && record.plan != "" {
			planCounts[record.plan]++
		}
	}
	providers := facetValues(providerCounts, false)
	priorities := facetValues(priorityCounts, true)
	plans := facetValues(planCounts, false)
	return authFilesFacetsResponse{Providers: providers, Priorities: priorities, Plans: plans}
}

func facetValues(counts map[string]int, priority bool) []authFilesFacetValue {
	values := make([]authFilesFacetValue, 0, len(counts))
	for value, count := range counts {
		values = append(values, authFilesFacetValue{Value: value, Count: count})
	}
	sort.Slice(values, func(i, j int) bool {
		if priority {
			if values[i].Value == "__unset__" {
				return false
			}
			if values[j].Value == "__unset__" {
				return true
			}
			left, _ := strconv.Atoi(values[i].Value)
			right, _ := strconv.Atoi(values[j].Value)
			return left > right
		}
		return values[i].Value < values[j].Value
	})
	return values
}

func buildAuthFileSelectionEntry(record *authFileListRecord, graph *coreauth.ChatGPTWebDependencyGraph, now time.Time) gin.H {
	auth := record.auth
	entry := gin.H{
		"name":           record.name,
		"type":           record.provider,
		"provider":       record.provider,
		"status":         auth.Status,
		"status_message": "",
		"disabled":       auth.Disabled,
		"runtime_only":   false,
	}
	if record.problem {
		entry["status_message"] = "problem"
	}
	if strings.EqualFold(record.provider, "chatgpt-web") {
		entry["account_info_refreshable"] = !auth.Disabled && auth.Status != coreauth.StatusDisabled && auth.LifecycleRefreshable()
	}
	if strings.EqualFold(record.provider, "codex") {
		entry["retained_for_dependents"] = coreauth.ChatGPTWebAuthRetainedForDependents(auth)
		count, names, _ := retainedDependencySummary(auth, graph)
		entry["dependent_count"] = count
		entry["dependent_names"] = names
		if !codexAuthUsesAPIKeyCredential(auth) {
			applyCodexAuthModeSummary(entry, auth.Metadata, now)
		}
	}
	return entry
}
