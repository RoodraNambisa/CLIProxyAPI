package management

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	xaiauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/xai"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	sdkAuth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestPatchAuthFileFields_MergeHeadersAndDeleteEmptyValues(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	store := &memoryAuthStore{}
	manager := coreauth.NewManager(store, nil, nil)
	record := &coreauth.Auth{
		ID:       "test.json",
		FileName: "test.json",
		Provider: "claude",
		Attributes: map[string]string{
			"path":            "/tmp/test.json",
			"header:X-Old":    "old",
			"header:X-Remove": "gone",
		},
		Metadata: map[string]any{
			"type": "claude",
			"headers": map[string]any{
				"X-Old":    "old",
				"X-Remove": "gone",
			},
		},
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("failed to register auth record: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)

	body := `{"name":"test.json","prefix":"p1","proxy_url":"http://proxy.local","headers":{"X-Old":"new","X-New":"v","X-Remove":"  ","X-Nope":""}}`
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/fields", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req
	h.PatchAuthFileFields(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d with body %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	updated, ok := manager.GetByID("test.json")
	if !ok || updated == nil {
		t.Fatalf("expected auth record to exist after patch")
	}

	if updated.Prefix != "p1" {
		t.Fatalf("prefix = %q, want %q", updated.Prefix, "p1")
	}
	if updated.ProxyURL != "http://proxy.local" {
		t.Fatalf("proxy_url = %q, want %q", updated.ProxyURL, "http://proxy.local")
	}

	if updated.Metadata == nil {
		t.Fatalf("expected metadata to be non-nil")
	}
	if got, _ := updated.Metadata["prefix"].(string); got != "p1" {
		t.Fatalf("metadata.prefix = %q, want %q", got, "p1")
	}
	if got, _ := updated.Metadata["proxy_url"].(string); got != "http://proxy.local" {
		t.Fatalf("metadata.proxy_url = %q, want %q", got, "http://proxy.local")
	}

	headersMeta, ok := updated.Metadata["headers"].(map[string]any)
	if !ok {
		raw, _ := json.Marshal(updated.Metadata["headers"])
		t.Fatalf("metadata.headers = %T (%s), want map[string]any", updated.Metadata["headers"], string(raw))
	}
	if got := headersMeta["X-Old"]; got != "new" {
		t.Fatalf("metadata.headers.X-Old = %#v, want %q", got, "new")
	}
	if got := headersMeta["X-New"]; got != "v" {
		t.Fatalf("metadata.headers.X-New = %#v, want %q", got, "v")
	}
	if _, ok := headersMeta["X-Remove"]; ok {
		t.Fatalf("expected metadata.headers.X-Remove to be deleted")
	}
	if _, ok := headersMeta["X-Nope"]; ok {
		t.Fatalf("expected metadata.headers.X-Nope to be absent")
	}

	if got := updated.Attributes["header:X-Old"]; got != "new" {
		t.Fatalf("attrs header:X-Old = %q, want %q", got, "new")
	}
	if got := updated.Attributes["header:X-New"]; got != "v" {
		t.Fatalf("attrs header:X-New = %q, want %q", got, "v")
	}
	if _, ok := updated.Attributes["header:X-Remove"]; ok {
		t.Fatalf("expected attrs header:X-Remove to be deleted")
	}
	if _, ok := updated.Attributes["header:X-Nope"]; ok {
		t.Fatalf("expected attrs header:X-Nope to be absent")
	}
}

func TestPatchAuthFileFields_HeadersEmptyMapIsNoop(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	store := &memoryAuthStore{}
	manager := coreauth.NewManager(store, nil, nil)
	record := &coreauth.Auth{
		ID:       "noop.json",
		FileName: "noop.json",
		Provider: "claude",
		Attributes: map[string]string{
			"path":         "/tmp/noop.json",
			"header:X-Kee": "1",
		},
		Metadata: map[string]any{
			"type": "claude",
			"headers": map[string]any{
				"X-Kee": "1",
			},
		},
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("failed to register auth record: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)

	body := `{"name":"noop.json","note":"hello","headers":{}}`
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/fields", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req
	h.PatchAuthFileFields(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d with body %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	updated, ok := manager.GetByID("noop.json")
	if !ok || updated == nil {
		t.Fatalf("expected auth record to exist after patch")
	}
	if got := updated.Attributes["header:X-Kee"]; got != "1" {
		t.Fatalf("attrs header:X-Kee = %q, want %q", got, "1")
	}
	headersMeta, ok := updated.Metadata["headers"].(map[string]any)
	if !ok {
		t.Fatalf("expected metadata.headers to remain a map, got %T", updated.Metadata["headers"])
	}
	if got := headersMeta["X-Kee"]; got != "1" {
		t.Fatalf("metadata.headers.X-Kee = %#v, want %q", got, "1")
	}
}

func TestPatchAuthFileFields_LegacyUnchangedHeadersReturnNoFields(t *testing.T) {
	manager := coreauth.NewManager(&memoryAuthStore{}, nil, nil)
	record := &coreauth.Auth{
		ID:         "unchanged.json",
		FileName:   "unchanged.json",
		Provider:   "claude",
		Attributes: map[string]string{"path": "/tmp/unchanged.json", "header:X-Keep": "1"},
		Metadata:   map[string]any{"type": "claude", "headers": map[string]any{"X-Keep": "1"}},
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/fields", strings.NewReader(`{"name":"unchanged.json","headers":{"X-Keep":"1"}}`))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req
	h.PatchAuthFileFields(ctx)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestPatchAuthFileFieldsFallsBackForStoreWithoutConditionalSave(t *testing.T) {
	store := &memoryAuthStore{}
	manager := coreauth.NewManager(store, nil, nil)
	record := &coreauth.Auth{
		ID:         "legacy-store.json",
		FileName:   "legacy-store.json",
		Provider:   "codex",
		Attributes: map[string]string{"path": "/tmp/legacy-store.json", coreauth.SourceHashAttributeKey: "source-hash"},
		Metadata:   map[string]any{"type": "codex"},
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatal(errRegister)
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/fields", strings.NewReader(`{"name":"legacy-store.json","note":"updated"}`))
	request.Header.Set("Content-Type", "application/json")
	ctx.Request = request
	h.PatchAuthFileFields(ctx)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	updated, ok := manager.GetByID(record.ID)
	if !ok || updated.Attributes["note"] != "updated" {
		t.Fatalf("updated auth = %#v", updated)
	}
}

func TestPatchAuthFileFields_XAIBooleanFields(t *testing.T) {
	store := &memoryAuthStore{}
	manager := coreauth.NewManager(store, nil, nil)
	record := &coreauth.Auth{
		ID:         "xai-user.json",
		FileName:   "xai-user.json",
		Provider:   "xai",
		Attributes: map[string]string{"path": "/tmp/xai-user.json"},
		Metadata:   map[string]any{"type": "xai"},
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("register xai auth: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/fields", strings.NewReader(`{"name":"xai-user.json","using_api":true,"websockets":true}`))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req
	h.PatchAuthFileFields(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	updated, ok := manager.GetByID("xai-user.json")
	if !ok || updated == nil {
		t.Fatal("updated xai auth not found")
	}
	if got, okBool := updated.Metadata["using_api"].(bool); !okBool || !got {
		t.Fatalf("metadata.using_api = %#v, want true", updated.Metadata["using_api"])
	}
	if got, okBool := updated.Metadata["websockets"].(bool); !okBool || !got {
		t.Fatalf("metadata.websockets = %#v, want true", updated.Metadata["websockets"])
	}
	if updated.Attributes["using_api"] != "true" || updated.Attributes["websockets"] != "true" {
		t.Fatalf("unexpected attributes: %#v", updated.Attributes)
	}
}

func TestPatchAuthFileFields_PersistsXAIBooleanFields(t *testing.T) {
	authDir := t.TempDir()
	tokenStore := sdkAuth.NewFileTokenStore()
	tokenStore.SetBaseDir(authDir)
	manager := coreauth.NewManager(tokenStore, nil, nil)
	record := &coreauth.Auth{
		ID:       "xai-persist.json",
		FileName: "xai-persist.json",
		Provider: "xai",
		Storage: &xaiauth.TokenStorage{
			AccessToken: "secret",
			BaseURL:     xaiauth.DefaultAPIBaseURL,
		},
		Metadata: map[string]any{
			"type":       "xai",
			"using_api":  false,
			"websockets": false,
		},
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("register xai auth: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/fields", strings.NewReader(`{"name":"xai-persist.json","using_api":true,"websockets":true}`))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req
	h.PatchAuthFileFields(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	raw, errRead := os.ReadFile(filepath.Join(authDir, "xai-persist.json"))
	if errRead != nil {
		t.Fatalf("read persisted auth: %v", errRead)
	}
	var persisted map[string]any
	if errUnmarshal := json.Unmarshal(raw, &persisted); errUnmarshal != nil {
		t.Fatalf("decode persisted auth: %v", errUnmarshal)
	}
	if usingAPI, ok := persisted["using_api"].(bool); !ok || !usingAPI {
		t.Fatalf("persisted using_api = %#v, want true", persisted["using_api"])
	}
	if websockets, ok := persisted["websockets"].(bool); !ok || !websockets {
		t.Fatalf("persisted websockets = %#v, want true", persisted["websockets"])
	}
}

func TestPatchAuthFileFields_RejectsXAIFieldsForOtherProviders(t *testing.T) {
	store := &memoryAuthStore{}
	manager := coreauth.NewManager(store, nil, nil)
	record := &coreauth.Auth{
		ID:         "claude.json",
		FileName:   "claude.json",
		Provider:   "claude",
		Prefix:     "old",
		Attributes: map[string]string{"path": "/tmp/claude.json"},
		Metadata:   map[string]any{"type": "claude", "prefix": "old"},
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("register claude auth: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/fields", strings.NewReader(`{"name":"claude.json","prefix":"new","using_api":true}`))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req
	h.PatchAuthFileFields(ctx)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	updated, _ := manager.GetByID("claude.json")
	if updated.Prefix != "old" {
		t.Fatalf("prefix changed on rejected request: %q", updated.Prefix)
	}
}

func TestPatchAuthFileFields_BatchReplacementAndExplicitPriority(t *testing.T) {
	store := &countingAuthStore{}
	manager := coreauth.NewManager(store, nil, nil)
	record := &coreauth.Auth{
		ID:       "xai-batch.json",
		FileName: "xai-batch.json",
		Provider: "xai",
		Attributes: map[string]string{
			"path":            "/tmp/xai-batch.json",
			"priority":        "9",
			"header:X-Old":    "old",
			"excluded_models": "old-model",
			"disable_cooling": "false",
			"websockets":      "false",
		},
		Metadata: map[string]any{
			"type":            "xai",
			"priority":        9,
			"headers":         map[string]any{"X-Old": "old"},
			"excluded_models": []string{"old-model"},
			"disable_cooling": false,
			"websockets":      false,
		},
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("register xai auth: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	hookCalls := 0
	h.SetAuthStatusHook(func(_ context.Context, auth *coreauth.Auth) {
		if auth != nil && auth.ID == record.ID {
			hookCalls++
		}
	})

	body := `{"names":["xai-batch.json"," xai-batch.json "],"fields":{"priority":0,"headers":{"X-New":" value "},"excluded_models":[" GPT-5 ","gpt-5"],"disable_cooling":true,"websockets":true}}`
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/fields", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req
	h.PatchAuthFileFields(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var response struct {
		Matched int                         `json:"matched"`
		Updated int                         `json:"updated"`
		Files   []string                    `json:"files"`
		Failed  []authFileFieldPatchFailure `json:"failed"`
	}
	if errDecode := json.Unmarshal(rec.Body.Bytes(), &response); errDecode != nil {
		t.Fatalf("decode response: %v", errDecode)
	}
	if response.Matched != 1 || response.Updated != 1 || len(response.Files) != 1 || len(response.Failed) != 0 {
		t.Fatalf("unexpected batch response: %#v", response)
	}
	if store.saveCalls != 2 {
		t.Fatalf("save calls = %d, want 2 (register plus one deduplicated update)", store.saveCalls)
	}
	if hookCalls != 1 {
		t.Fatalf("hook calls = %d, want 1", hookCalls)
	}

	updated, ok := manager.GetByID(record.ID)
	if !ok || updated == nil {
		t.Fatal("updated auth not found")
	}
	if got := updated.Attributes["priority"]; got != "0" {
		t.Fatalf("priority attribute = %q, want 0", got)
	}
	if got, okPriority := updated.Metadata["priority"].(int); !okPriority || got != 0 {
		t.Fatalf("priority metadata = %#v, want int(0)", updated.Metadata["priority"])
	}
	if _, exists := updated.Attributes["header:X-Old"]; exists {
		t.Fatal("old header was not replaced")
	}
	if got := updated.Attributes["header:X-New"]; got != "value" {
		t.Fatalf("new header = %q, want value", got)
	}
	if got := updated.Attributes["excluded_models"]; got != "gpt-5" {
		t.Fatalf("excluded_models attribute = %q, want gpt-5", got)
	}
	if got, okBool := updated.Metadata["disable_cooling"].(bool); !okBool || !got {
		t.Fatalf("disable_cooling metadata = %#v, want true", updated.Metadata["disable_cooling"])
	}
	if got, okBool := updated.Metadata["websockets"].(bool); !okBool || !got {
		t.Fatalf("websockets metadata = %#v, want true", updated.Metadata["websockets"])
	}

	clearBody := `{"names":["xai-batch.json"],"fields":{"priority":null,"headers":{},"excluded_models":[]}}`
	clearRec := httptest.NewRecorder()
	clearCtx, _ := gin.CreateTestContext(clearRec)
	clearReq := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/fields", strings.NewReader(clearBody))
	clearReq.Header.Set("Content-Type", "application/json")
	clearCtx.Request = clearReq
	h.PatchAuthFileFields(clearCtx)
	if clearRec.Code != http.StatusOK {
		t.Fatalf("clear status = %d, want %d; body=%s", clearRec.Code, http.StatusOK, clearRec.Body.String())
	}

	cleared, _ := manager.GetByID(record.ID)
	for _, key := range []string{"priority", "headers", "excluded_models"} {
		if _, exists := cleared.Metadata[key]; exists {
			t.Fatalf("metadata %q was not cleared: %#v", key, cleared.Metadata[key])
		}
	}
	for _, key := range []string{"priority", "header:X-New", "excluded_models"} {
		if _, exists := cleared.Attributes[key]; exists {
			t.Fatalf("attribute %q was not cleared: %#v", key, cleared.Attributes[key])
		}
	}
}

func TestPatchAuthFileFields_BatchReturnsPerTargetFailures(t *testing.T) {
	manager := coreauth.NewManager(&memoryAuthStore{}, nil, nil)
	for _, record := range []*coreauth.Auth{
		{ID: "codex.json", FileName: "codex.json", Provider: "codex", Attributes: map[string]string{"path": "/tmp/codex.json"}, Metadata: map[string]any{"type": "codex"}},
		{ID: "claude.json", FileName: "claude.json", Provider: "claude", Attributes: map[string]string{"path": "/tmp/claude.json"}, Metadata: map[string]any{"type": "claude"}},
	} {
		if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
			t.Fatalf("register %s: %v", record.ID, errRegister)
		}
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	hooked := make([]string, 0, 1)
	h.SetAuthStatusHook(func(_ context.Context, auth *coreauth.Auth) {
		if auth != nil {
			hooked = append(hooked, auth.ID)
		}
	})

	body := `{"names":["codex.json","claude.json","missing.json"],"fields":{"websockets":true}}`
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/fields", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req
	h.PatchAuthFileFields(ctx)

	if rec.Code != http.StatusMultiStatus {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusMultiStatus, rec.Body.String())
	}
	var response struct {
		Matched int                         `json:"matched"`
		Updated int                         `json:"updated"`
		Files   []string                    `json:"files"`
		Failed  []authFileFieldPatchFailure `json:"failed"`
	}
	if errDecode := json.Unmarshal(rec.Body.Bytes(), &response); errDecode != nil {
		t.Fatalf("decode response: %v", errDecode)
	}
	if response.Matched != 2 || response.Updated != 1 || len(response.Files) != 1 || response.Files[0] != "codex.json" {
		t.Fatalf("unexpected batch response: %#v", response)
	}
	if len(response.Failed) != 2 || response.Failed[0].Status != http.StatusBadRequest || response.Failed[1].Status != http.StatusNotFound {
		t.Fatalf("unexpected failures: %#v", response.Failed)
	}
	if len(hooked) != 1 || hooked[0] != "codex.json" {
		t.Fatalf("hooked auths = %#v, want [codex.json]", hooked)
	}
	updated, _ := manager.GetByID("codex.json")
	if got, ok := updated.Metadata["websockets"].(bool); !ok || !got {
		t.Fatalf("codex websockets = %#v, want true", updated.Metadata["websockets"])
	}
	unchanged, _ := manager.GetByID("claude.json")
	if _, exists := unchanged.Metadata["websockets"]; exists {
		t.Fatalf("unsupported target was mutated: %#v", unchanged.Metadata)
	}
}

func TestPatchAuthFileFieldsRejectsExternalReplacement(t *testing.T) {
	h, manager, authDir, store := newChatGPTWebDependencyRaceManagementHandler(t)
	record := registerChatGPTWebDependencyManagementAuth(t, manager, &coreauth.Auth{
		ID: "fields-race.json", FileName: "fields-race.json", Provider: "xai",
		Attributes: map[string]string{"note": "original"},
		Metadata:   map[string]any{"type": "xai", "note": "original"},
	})
	path := filepath.Join(authDir, record.FileName)
	store.setBeforeConditionalSave(func() {
		replaceManagementDependencyAuthFile(t, path, "external fields replacement", "")
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/fields", strings.NewReader(
		`{"names":["fields-race.json"],"fields":{"note":"batch update"}}`,
	))
	request.Header.Set("Content-Type", "application/json")
	ctx.Request = request
	h.PatchAuthFileFields(ctx)

	if recorder.Code != http.StatusMultiStatus {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusMultiStatus, recorder.Body.String())
	}
	var response struct {
		Updated int                         `json:"updated"`
		Failed  []authFileFieldPatchFailure `json:"failed"`
	}
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &response); errDecode != nil {
		t.Fatal(errDecode)
	}
	if response.Updated != 0 || len(response.Failed) != 1 || response.Failed[0].Status != http.StatusConflict {
		t.Fatalf("response = %#v", response)
	}
	assertManagementDependencyAuthNote(t, path, "external fields replacement")
	current, _ := manager.GetByID(record.ID)
	if got := current.Attributes["note"]; got != "original" {
		t.Fatalf("runtime note = %q, want original", got)
	}
}

func TestPatchAuthFileFields_BatchMergesGlobalOAuthExclusions(t *testing.T) {
	manager := coreauth.NewManager(&memoryAuthStore{}, nil, nil)
	record := &coreauth.Auth{
		ID:         "codex-oauth.json",
		FileName:   "codex-oauth.json",
		Provider:   "codex",
		Attributes: map[string]string{"path": "/tmp/codex-oauth.json", "auth_kind": "oauth"},
		Metadata:   map[string]any{"type": "codex"},
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("register codex auth: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{
		AuthDir:             t.TempDir(),
		OAuthExcludedModels: map[string][]string{"codex": {"global-model"}},
	}, manager)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/fields", strings.NewReader(`{"names":["codex-oauth.json"],"fields":{"excluded_models":["local-model"]}}`))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req
	h.PatchAuthFileFields(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	updated, _ := manager.GetByID(record.ID)
	if got := updated.Attributes["excluded_models"]; got != "global-model,local-model" {
		t.Fatalf("effective excluded_models = %q, want global-model,local-model", got)
	}
	if got := updated.Attributes["excluded_models_hash"]; got == "" {
		t.Fatal("excluded_models_hash was not refreshed")
	}
	if got := updated.Metadata["excluded_models"]; fmt.Sprint(got) != "[local-model]" {
		t.Fatalf("persisted per-auth excluded_models = %#v, want [local-model]", got)
	}
}

func TestPatchAuthFileFields_BatchRejectsCaseInsensitiveDuplicateHeaders(t *testing.T) {
	manager := coreauth.NewManager(&memoryAuthStore{}, nil, nil)
	record := &coreauth.Auth{
		ID:         "headers.json",
		FileName:   "headers.json",
		Provider:   "claude",
		Attributes: map[string]string{"path": "/tmp/headers.json"},
		Metadata:   map[string]any{"type": "claude"},
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/fields", strings.NewReader(`{"names":["headers.json"],"fields":{"headers":{"X-Test":"a","x-test":"b"}}}`))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req
	h.PatchAuthFileFields(ctx)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	updated, _ := manager.GetByID(record.ID)
	if _, exists := updated.Metadata["headers"]; exists {
		t.Fatalf("duplicate headers request mutated auth: %#v", updated.Metadata)
	}
}

type countingAuthStore struct {
	memoryAuthStore
	saveCalls int
}

func (s *countingAuthStore) Save(ctx context.Context, auth *coreauth.Auth) (string, error) {
	s.saveCalls++
	return s.memoryAuthStore.Save(ctx, auth)
}

func TestBuildAuthFileEntry_XAIEffectiveBooleanDefaults(t *testing.T) {
	tests := []struct {
		name       string
		attributes map[string]string
		metadata   map[string]any
		wantUsing  bool
	}{
		{name: "oauth", metadata: map[string]any{"type": "xai", "auth_kind": "oauth"}, wantUsing: false},
		{name: "api key", attributes: map[string]string{"auth_kind": "apikey"}, metadata: map[string]any{"type": "xai"}, wantUsing: true},
		{name: "missing auth kind", metadata: map[string]any{"type": "xai"}, wantUsing: true},
		{name: "explicit false wins", attributes: map[string]string{"auth_kind": "apikey", "using_api": "false"}, metadata: map[string]any{"type": "xai"}, wantUsing: false},
		{name: "explicit true wins", attributes: map[string]string{"auth_kind": "oauth", "using_api": "true"}, metadata: map[string]any{"type": "xai"}, wantUsing: true},
	}

	h := &Handler{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attributes := map[string]string{"path": "/tmp/xai.json"}
			for key, value := range tt.attributes {
				attributes[key] = value
			}
			entry := h.buildAuthFileEntry(&coreauth.Auth{
				ID:         "xai.json",
				FileName:   "xai.json",
				Provider:   "xai",
				Attributes: attributes,
				Metadata:   tt.metadata,
			})
			if entry == nil {
				t.Fatal("buildAuthFileEntry() returned nil")
			}
			if usingAPI, ok := entry["using_api"].(bool); !ok || usingAPI != tt.wantUsing {
				t.Fatalf("using_api = %#v, want %t", entry["using_api"], tt.wantUsing)
			}
			if websockets, ok := entry["websockets"].(bool); !ok || websockets {
				t.Fatalf("websockets = %#v, want false", entry["websockets"])
			}
		})
	}
}

func TestListAuthFilesFromDisk_XAIEffectiveBooleanDefaults(t *testing.T) {
	authDir := t.TempDir()
	files := map[string]string{
		"xai-api.json":   `{"type":"xai","email":"api@example.com"}`,
		"xai-oauth.json": `{"type":"xai","auth_kind":"oauth","email":"oauth@example.com"}`,
	}
	for name, body := range files {
		if errWrite := os.WriteFile(filepath.Join(authDir, name), []byte(body), 0o600); errWrite != nil {
			t.Fatalf("write %s: %v", name, errWrite)
		}
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, nil)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files", nil)
	h.ListAuthFiles(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var response struct {
		Files []map[string]any `json:"files"`
	}
	if errDecode := json.Unmarshal(rec.Body.Bytes(), &response); errDecode != nil {
		t.Fatalf("decode list response: %v", errDecode)
	}
	if len(response.Files) != 2 {
		t.Fatalf("files count = %d, want 2", len(response.Files))
	}
	byName := make(map[string]map[string]any, len(response.Files))
	for _, file := range response.Files {
		name, _ := file["name"].(string)
		byName[name] = file
	}
	if usingAPI, ok := byName["xai-api.json"]["using_api"].(bool); !ok || !usingAPI {
		t.Fatalf("API credential using_api = %#v, want true", byName["xai-api.json"]["using_api"])
	}
	if usingAPI, ok := byName["xai-oauth.json"]["using_api"].(bool); !ok || usingAPI {
		t.Fatalf("OAuth credential using_api = %#v, want false", byName["xai-oauth.json"]["using_api"])
	}
	for name, file := range byName {
		if websockets, ok := file["websockets"].(bool); !ok || websockets {
			t.Fatalf("%s websockets = %#v, want false", name, file["websockets"])
		}
	}
}
