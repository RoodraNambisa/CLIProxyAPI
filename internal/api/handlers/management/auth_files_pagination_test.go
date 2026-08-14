package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestAuthFilesPaginationNegotiationFilteringAndSelection(t *testing.T) {
	h, manager, authDir := newAuthFilesPaginationTestHandler(t, false)
	registerAuthFilesPaginationTestAuth(t, manager, authDir, "alpha.json", "codex", false, "", 2, true, "plus")
	registerAuthFilesPaginationTestAuth(t, manager, authDir, "bravo.json", "chatgpt-web", false, "", 1, true, "free")
	registerAuthFilesPaginationTestAuth(t, manager, authDir, "charlie.json", "claude", true, "", 0, false, "")
	registerAuthFilesPaginationTestAuth(t, manager, authDir, "delta.json", "codex", false, "temporary_failure", 2, true, "pro")
	registerAuthFilesPaginationTestAuth(t, manager, authDir, "echo.json", "xai", false, "", 0, false, "")
	bravo, ok := manager.GetByID("bravo.json")
	if !ok {
		t.Fatal("bravo auth not found")
	}
	bravo.Metadata["email"] = "refill-curio-76@icloud.com"
	if _, errUpdate := manager.Update(coreauth.WithSkipPersist(t.Context()), bravo); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	retiredPath := filepath.Join(authDir, "retired.json")
	if errWrite := os.WriteFile(retiredPath, []byte(`{"type":"gemini","email":"legacy@example.com"}`), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}

	router := gin.New()
	router.GET("/auth-files", h.ListAuthFiles)
	router.GET("/auth-files/selection", h.ListAuthFileSelection)

	legacy := performAuthFilesPaginationRequest(router, "/auth-files")
	if legacy.Code != http.StatusOK || legacy.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("legacy response = %d, cache=%q", legacy.Code, legacy.Header().Get("Cache-Control"))
	}
	var legacyBody map[string]any
	decodeAuthFilesPaginationResponse(t, legacy, &legacyBody)
	if _, exists := legacyBody["pagination"]; exists {
		t.Fatalf("legacy response unexpectedly contains pagination: %#v", legacyBody)
	}
	if len(legacyBody["files"].([]any)) != 6 {
		t.Fatalf("legacy files = %#v", legacyBody["files"])
	}

	disabled := performAuthFilesPaginationRequest(router, "/auth-files?paged=true&page=1&page_size=2")
	var disabledBody struct {
		Files      []map[string]any            `json:"files"`
		Total      int                         `json:"total"`
		Pagination authFilesPaginationResponse `json:"pagination"`
	}
	decodeAuthFilesPaginationResponse(t, disabled, &disabledBody)
	if disabledBody.Pagination.Enabled || disabledBody.Total != 6 || len(disabledBody.Files) != 6 {
		t.Fatalf("disabled pagination response = %#v", disabledBody)
	}

	h.SetConfig(&config.Config{
		AuthDir: authDir,
		RemoteManagement: config.RemoteManagement{
			AuthFilesPagination: config.AuthFilesPaginationConfig{Enabled: true},
		},
	})
	paged := performAuthFilesPaginationRequest(router, "/auth-files?paged=true&page=2&page_size=2&sort=az")
	var pagedBody struct {
		Files      []map[string]any            `json:"files"`
		Total      int                         `json:"total"`
		Pagination authFilesPaginationResponse `json:"pagination"`
		Facets     authFilesFacetsResponse     `json:"facets"`
	}
	decodeAuthFilesPaginationResponse(t, paged, &pagedBody)
	if !pagedBody.Pagination.Enabled || pagedBody.Pagination.Page != 2 || pagedBody.Pagination.TotalPages != 3 || pagedBody.Total != 6 {
		t.Fatalf("pagination = %#v, total=%d", pagedBody.Pagination, pagedBody.Total)
	}
	if got := authFilesPaginationNames(pagedBody.Files); strings.Join(got, ",") != "charlie.json,delta.json" {
		t.Fatalf("page names = %v", got)
	}
	if len(pagedBody.Facets.Providers) != 5 || len(pagedBody.Facets.Priorities) != 3 {
		t.Fatalf("facets = %#v", pagedBody.Facets)
	}

	emailSearch := performAuthFilesPaginationRequest(router, "/auth-files?paged=true&page=1&page_size=10&search=refill-curio-76%40icloud.com")
	decodeAuthFilesPaginationResponse(t, emailSearch, &pagedBody)
	if pagedBody.Total != 1 || strings.Join(authFilesPaginationNames(pagedBody.Files), ",") != "bravo.json" ||
		pagedBody.Files[0]["email"] != "refill-curio-76@icloud.com" {
		t.Fatalf("email search response = %#v", pagedBody)
	}

	filtered := performAuthFilesPaginationRequest(router, "/auth-files?paged=true&page=99&page_size=1&provider=codex&priority=2&sort=priority")
	var filteredBody struct {
		Files      []map[string]any            `json:"files"`
		Total      int                         `json:"total"`
		Pagination authFilesPaginationResponse `json:"pagination"`
	}
	decodeAuthFilesPaginationResponse(t, filtered, &filteredBody)
	if filteredBody.Total != 2 || filteredBody.Pagination.Page != 2 || authFilesPaginationNames(filteredBody.Files)[0] != "delta.json" {
		t.Fatalf("filtered response = %#v", filteredBody)
	}

	selection := performAuthFilesPaginationRequest(router, "/auth-files/selection?provider=codex&priority=2")
	if selection.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("selection cache header = %q", selection.Header().Get("Cache-Control"))
	}
	if strings.Contains(selection.Body.String(), "access-token-secret") ||
		strings.Contains(selection.Body.String(), `"metadata"`) ||
		strings.Contains(selection.Body.String(), `"path"`) ||
		strings.Contains(selection.Body.String(), `"email"`) ||
		strings.Contains(selection.Body.String(), `"plan_type"`) ||
		strings.Contains(selection.Body.String(), `"priority"`) {
		t.Fatalf("selection leaked unsafe fields: %s", selection.Body.String())
	}
	var selectionBody struct {
		Files []map[string]any `json:"files"`
		Total int              `json:"total"`
	}
	decodeAuthFilesPaginationResponse(t, selection, &selectionBody)
	if selectionBody.Total != 2 || strings.Join(authFilesPaginationNames(selectionBody.Files), ",") != "alpha.json,delta.json" {
		t.Fatalf("selection = %#v", selectionBody)
	}
	allSelection := performAuthFilesPaginationRequest(router, "/auth-files/selection")
	decodeAuthFilesPaginationResponse(t, allSelection, &selectionBody)
	if selectionBody.Total != 5 || strings.Contains(allSelection.Body.String(), "retired.json") {
		t.Fatalf("all selection included a retired file: %#v", selectionBody)
	}

	invalid := performAuthFilesPaginationRequest(router, "/auth-files?paged=true&page_size=101")
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid status = %d, body=%s", invalid.Code, invalid.Body.String())
	}
}

func TestAuthFilesPaginationRejectsInvalidQueryValues(t *testing.T) {
	h, _, _ := newAuthFilesPaginationTestHandler(t, true)
	router := gin.New()
	router.GET("/auth-files", h.ListAuthFiles)
	for _, path := range []string{
		"/auth-files?paged=yes",
		"/auth-files?paged=true&page=0",
		"/auth-files?paged=true&page_size=0",
		"/auth-files?paged=true&enabled_only=true&disabled_only=true",
		"/auth-files?paged=true&priority=high",
		"/auth-files?paged=true&sort=random",
		"/auth-files?paged=true&page=1&page=2",
		"/auth-files?paged=true&provider=codex&provider=claude",
	} {
		response := performAuthFilesPaginationRequest(router, path)
		if response.Code != http.StatusBadRequest {
			t.Errorf("%s status = %d, body=%s", path, response.Code, response.Body.String())
		}
	}
}

func TestAuthFilesPaginationKeepsDependencySummariesAcrossSelection(t *testing.T) {
	h, manager, authDir := newAuthFilesPaginationTestHandler(t, true)
	source := managementDependencyCodexAuth("codex-source.json", "source-uid", false)
	dependent := managementDependencyWebAuth("web-copy.json", source.ID, "source-uid")
	for _, auth := range []*coreauth.Auth{source, dependent} {
		path := filepath.Join(authDir, auth.FileName)
		auth.Attributes = map[string]string{"path": path, "source": path}
		payload, errMarshal := json.Marshal(auth.Metadata)
		if errMarshal != nil {
			t.Fatal(errMarshal)
		}
		if errWrite := os.WriteFile(path, payload, 0o600); errWrite != nil {
			t.Fatal(errWrite)
		}
		if _, errRegister := manager.Register(t.Context(), auth); errRegister != nil {
			t.Fatal(errRegister)
		}
	}

	router := gin.New()
	router.GET("/auth-files", h.ListAuthFiles)
	router.GET("/auth-files/selection", h.ListAuthFileSelection)
	paged := performAuthFilesPaginationRequest(router, "/auth-files?paged=true&page=1&page_size=1&provider=codex")
	if paged.Code != http.StatusOK || !strings.Contains(paged.Body.String(), `"dependent_count":1`) {
		t.Fatalf("paged dependency summary: status=%d body=%s", paged.Code, paged.Body.String())
	}
	selection := performAuthFilesPaginationRequest(router, "/auth-files/selection?provider=codex")
	if selection.Code != http.StatusOK ||
		!strings.Contains(selection.Body.String(), `"dependent_count":1`) ||
		!strings.Contains(selection.Body.String(), `"web-copy.json"`) ||
		strings.Contains(selection.Body.String(), "codex-access") {
		t.Fatalf("selection dependency summary: status=%d body=%s", selection.Code, selection.Body.String())
	}
}

func TestAuthFilesPaginationReusesQueriesAndIgnoresOpaqueCredentialRotation(t *testing.T) {
	h, manager, authDir := newAuthFilesPaginationTestHandler(t, true)
	registerAuthFilesPaginationTestAuth(t, manager, authDir, "alpha.json", "codex", false, "", 1, true, "plus")
	registerAuthFilesPaginationTestAuth(t, manager, authDir, "bravo.json", "codex", false, "", 1, true, "plus")
	retiredPath := filepath.Join(authDir, "retired.json")
	if errWrite := os.WriteFile(retiredPath, []byte(`{"type":"gemini","email":"legacy@example.com"}`), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	router := gin.New()
	router.GET("/auth-files", h.ListAuthFiles)

	first := performAuthFilesPaginationRequest(router, "/auth-files?paged=true&page=1&page_size=1&sort=az")
	if first.Code != http.StatusOK {
		t.Fatalf("first page status=%d body=%s", first.Code, first.Body.String())
	}
	h.authFilesPagination.mu.Lock()
	if h.authFilesPagination.lru == nil {
		h.authFilesPagination.mu.Unlock()
		t.Fatal("pagination query cache was not initialized")
	}
	queryCount := h.authFilesPagination.lru.Len()
	recordCount := len(h.authFilesPagination.records)
	records := h.authFilesPagination.records
	retiredRecords := h.authFilesPagination.retiredRecords
	revision := h.authFilesPagination.revision
	h.authFilesPagination.mu.Unlock()
	if queryCount != 1 || recordCount != 3 || len(retiredRecords) != 1 {
		t.Fatalf("initial cache queries=%d records=%d", queryCount, recordCount)
	}

	second := performAuthFilesPaginationRequest(router, "/auth-files?paged=true&page=2&page_size=1&sort=az")
	if second.Code != http.StatusOK {
		t.Fatalf("second page status=%d body=%s", second.Code, second.Body.String())
	}
	h.authFilesPagination.mu.Lock()
	if h.authFilesPagination.lru.Len() != 1 || &h.authFilesPagination.records[0] != &records[0] {
		t.Fatalf("page change rebuilt cached query: queries=%d", h.authFilesPagination.lru.Len())
	}
	h.authFilesPagination.mu.Unlock()

	alpha, ok := manager.GetByID("alpha.json")
	if !ok {
		t.Fatal("alpha auth not found")
	}
	alpha.Metadata["access_token"] = "rotated-access-token"
	alpha.Metadata["session_cookie"] = "rotated-session-cookie"
	rotatedPath := filepath.Join(authDir, ".alpha-rotated.json")
	if errWrite := os.WriteFile(rotatedPath, []byte(`{"type":"codex","access_token":"rotated-access-token"}`), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	if errRename := os.Rename(rotatedPath, filepath.Join(authDir, "alpha.json")); errRename != nil {
		t.Fatal(errRename)
	}
	if _, errUpdate := manager.Update(coreauth.WithSkipPersist(t.Context()), alpha); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	if got := manager.ManagementAuthCatalogRevision(); got != revision {
		t.Fatalf("opaque rotation changed catalog revision from %d to %d", revision, got)
	}
	rotated := performAuthFilesPaginationRequest(router, "/auth-files?paged=true&page=1&page_size=1&sort=az")
	if rotated.Code != http.StatusOK {
		t.Fatalf("rotated page status=%d body=%s", rotated.Code, rotated.Body.String())
	}
	h.authFilesPagination.mu.Lock()
	if &h.authFilesPagination.records[0] != &records[0] {
		t.Fatal("opaque credential rotation rebuilt the pagination directory")
	}
	h.authFilesPagination.mu.Unlock()

	if errWrite := os.WriteFile(retiredPath, []byte(`{"type":"gemini","email":"updated-legacy@example.com"}`), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	authfileguard.NotifyRetiredFileChanged(retiredPath)
	retiredUpdated := performAuthFilesPaginationRequest(router, "/auth-files?paged=true&page=1&page_size=10&search=retired")
	if retiredUpdated.Code != http.StatusOK || !strings.Contains(retiredUpdated.Body.String(), "updated-legacy@example.com") {
		t.Fatalf("retired update was not reflected: status=%d body=%s", retiredUpdated.Code, retiredUpdated.Body.String())
	}
	h.authFilesPagination.mu.Lock()
	if len(h.authFilesPagination.retiredRecords) != 1 || h.authFilesPagination.retiredRecords[0] == retiredRecords[0] {
		h.authFilesPagination.mu.Unlock()
		t.Fatal("retired credential rewrite did not rebuild the retired catalog")
	}
	retiredRecords = h.authFilesPagination.retiredRecords
	h.authFilesPagination.mu.Unlock()

	alpha, _ = manager.GetByID("alpha.json")
	alpha.StatusMessage = "temporary failure"
	if _, errUpdate := manager.Update(coreauth.WithSkipPersist(t.Context()), alpha); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	problem := performAuthFilesPaginationRequest(router, "/auth-files?paged=true&page=1&page_size=10&problem_only=true")
	if problem.Code != http.StatusOK || !strings.Contains(problem.Body.String(), "alpha.json") {
		t.Fatalf("visible update was not reflected: status=%d body=%s", problem.Code, problem.Body.String())
	}
	h.authFilesPagination.mu.Lock()
	if h.authFilesPagination.revision != manager.ManagementAuthCatalogRevision() || h.authFilesPagination.revision == revision {
		t.Fatalf("visible update did not invalidate cache: cache=%d manager=%d", h.authFilesPagination.revision, manager.ManagementAuthCatalogRevision())
	}
	if len(h.authFilesPagination.retiredRecords) != 1 || h.authFilesPagination.retiredRecords[0] != retiredRecords[0] {
		t.Fatal("visible runtime update rescanned the retired credential directory")
	}
	h.authFilesPagination.mu.Unlock()

	for index := 0; index < maxAuthFilesQueryCache+8; index++ {
		response := performAuthFilesPaginationRequest(router, "/auth-files?paged=true&page=1&page_size=1&search=query-"+strconv.Itoa(index))
		if response.Code != http.StatusOK {
			t.Fatalf("query %d status=%d body=%s", index, response.Code, response.Body.String())
		}
	}
	h.authFilesPagination.mu.Lock()
	if got := h.authFilesPagination.lru.Len(); got != maxAuthFilesQueryCache {
		t.Fatalf("query LRU size=%d want=%d", got, maxAuthFilesQueryCache)
	}
	h.authFilesPagination.mu.Unlock()
}

func BenchmarkAuthFilesPaginationFilterAndSort10000(b *testing.B) {
	records := make([]*authFileListRecord, 10_000)
	for index := range records {
		records[index] = &authFileListRecord{
			name:         "auth-" + strconv.Itoa(index) + ".json",
			id:           strconv.Itoa(index),
			provider:     []string{"codex", "chatgpt-web", "claude", "xai"}[index%4],
			priority:     index % 10,
			prioritySet:  true,
			searchValues: []string{"auth", strconv.Itoa(index)},
		}
	}
	query := authFilesListQuery{provider: "all", plan: "all", priority: "all", sort: "priority", search: "auth"}
	b.ResetTimer()
	for range b.N {
		filtered := filterAuthFileRecords(records, query)
		sortAuthFileRecords(filtered, query.sort)
		if len(filtered) != len(records) {
			b.Fatal("unexpected filtered count")
		}
	}
}

func BenchmarkAuthFilesPaginationList10000(b *testing.B) {
	authDir := b.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	for index := 0; index < 10_000; index++ {
		name := "auth-" + strconv.Itoa(index) + ".json"
		path := filepath.Join(authDir, name)
		if errWrite := os.WriteFile(path, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
			b.Fatal(errWrite)
		}
		auth := &coreauth.Auth{
			ID:         name,
			FileName:   name,
			Provider:   []string{"codex", "chatgpt-web", "claude", "xai"}[index%4],
			Status:     coreauth.StatusActive,
			Attributes: map[string]string{"path": path, "priority": strconv.Itoa(index % 10)},
			Metadata:   map[string]any{"priority": index % 10},
		}
		if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
			b.Fatal(errRegister)
		}
	}
	h := NewHandler(&config.Config{
		AuthDir: authDir,
		RemoteManagement: config.RemoteManagement{
			AuthFilesPagination: config.AuthFilesPaginationConfig{Enabled: true},
		},
	}, "", manager)
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if errShutdown := h.Shutdown(ctx); errShutdown != nil {
			b.Errorf("shutdown: %v", errShutdown)
		}
	})
	router := gin.New()
	router.GET("/auth-files", h.ListAuthFiles)

	b.ResetTimer()
	for range b.N {
		response := performAuthFilesPaginationRequest(router, "/auth-files?paged=true&page=500&page_size=20&sort=priority")
		if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"total":10000`) {
			b.Fatalf("unexpected response: status=%d body=%s", response.Code, response.Body.String())
		}
	}
}

func newAuthFilesPaginationTestHandler(t *testing.T, enabled bool) (*Handler, *coreauth.Manager, string) {
	t.Helper()
	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandler(&config.Config{
		AuthDir: authDir,
		RemoteManagement: config.RemoteManagement{
			AuthFilesPagination: config.AuthFilesPaginationConfig{Enabled: enabled},
		},
	}, "", manager)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if errShutdown := h.Shutdown(ctx); errShutdown != nil {
			t.Errorf("shutdown: %v", errShutdown)
		}
	})
	return h, manager, authDir
}

func registerAuthFilesPaginationTestAuth(t *testing.T, manager *coreauth.Manager, authDir, name, provider string, disabled bool, statusMessage string, priority int, prioritySet bool, plan string) {
	t.Helper()
	metadata := map[string]any{"type": provider, "access_token": "access-token-secret", "disabled": disabled}
	if prioritySet {
		metadata["priority"] = priority
	}
	if plan != "" {
		metadata["plan_type"] = plan
	}
	payload, errMarshal := json.Marshal(metadata)
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	path := filepath.Join(authDir, name)
	if errWrite := os.WriteFile(path, payload, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	auth := &coreauth.Auth{
		ID:            name,
		FileName:      name,
		Provider:      provider,
		Status:        coreauth.StatusActive,
		StatusMessage: statusMessage,
		Disabled:      disabled,
		Attributes:    map[string]string{"path": path, "source": path},
		Metadata:      metadata,
	}
	if prioritySet {
		auth.Attributes["priority"] = strconv.Itoa(priority)
	}
	if _, errRegister := manager.Register(t.Context(), auth); errRegister != nil {
		t.Fatal(errRegister)
	}
}

func performAuthFilesPaginationRequest(router http.Handler, path string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
	return recorder
}

func decodeAuthFilesPaginationResponse(t *testing.T, recorder *httptest.ResponseRecorder, target any) {
	t.Helper()
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), target); errDecode != nil {
		t.Fatal(errDecode)
	}
}

func authFilesPaginationNames(files []map[string]any) []string {
	names := make([]string, 0, len(files))
	for _, file := range files {
		name, _ := file["name"].(string)
		names = append(names, name)
	}
	return names
}
