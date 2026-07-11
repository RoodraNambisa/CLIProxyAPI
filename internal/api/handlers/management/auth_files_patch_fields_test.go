package management

import (
	"context"
	"encoding/json"
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
