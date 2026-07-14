package management

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestPutInteractionsKeysNormalizesEntries(t *testing.T) {
	t.Parallel()

	h := &Handler{cfg: &config.Config{}, configFilePath: writeTestConfigFile(t)}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPut, "/v0/management/interactions-api-key", strings.NewReader(`[
		{"api-key":" key ","prefix":"/native/","base-url":" https://example.com ","excluded-models":["GEMINI-2.5-PRO"]},
		{"api-key":"key","base-url":"https://example.com"}
	]`))
	c.Request.Header.Set("Content-Type", "application/json")

	h.PutInteractionsKeys(c)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := len(h.cfg.InteractionsKey); got != 1 {
		t.Fatalf("interactions keys len = %d, want 1", got)
	}
	entry := h.cfg.InteractionsKey[0]
	if entry.APIKey != "key" || entry.Prefix != "native" || entry.BaseURL != "https://example.com" {
		t.Fatalf("normalized entry = %#v", entry)
	}
	if got := entry.ExcludedModels; len(got) != 1 || got[0] != "gemini-2.5-pro" {
		t.Fatalf("excluded-models = %#v", got)
	}
}

func TestPatchInteractionsKeyByMatch(t *testing.T) {
	t.Parallel()

	h := &Handler{
		cfg:            &config.Config{InteractionsKey: []config.GeminiKey{{APIKey: "key", BaseURL: "https://old.example.com"}}},
		configFilePath: writeTestConfigFile(t),
	}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPatch, "/v0/management/interactions-api-key", strings.NewReader(`{
		"match":"key",
		"value":{"priority":3,"base-url":" https://new.example.com ","proxy-url":" direct ","models":[{"name":"gemini-3-pro","alias":"pro"}],"headers":{"x-test":"value"}}
	}`))
	c.Request.Header.Set("Content-Type", "application/json")

	h.PatchInteractionsKey(c)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	entry := h.cfg.InteractionsKey[0]
	if entry.Priority != 3 || entry.BaseURL != "https://new.example.com" || entry.ProxyURL != "direct" || entry.Headers["x-test"] != "value" {
		t.Fatalf("patched entry = %#v", entry)
	}
	if got := entry.Models; len(got) != 1 || got[0].Name != "gemini-3-pro" || got[0].Alias != "pro" {
		t.Fatalf("patched models = %#v", got)
	}
}

func TestPutInteractionsKeysAcceptsEmptyItems(t *testing.T) {
	t.Parallel()

	h := &Handler{
		cfg:            &config.Config{InteractionsKey: []config.GeminiKey{{APIKey: "key"}}},
		configFilePath: writeTestConfigFile(t),
	}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPut, "/v0/management/interactions-api-key", strings.NewReader(`{"items":[]}`))
	c.Request.Header.Set("Content-Type", "application/json")

	h.PutInteractionsKeys(c)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if len(h.cfg.InteractionsKey) != 0 {
		t.Fatalf("interactions keys = %#v, want empty", h.cfg.InteractionsKey)
	}
}

func TestPatchInteractionsKeyRejectsAmbiguousMatch(t *testing.T) {
	t.Parallel()

	h := &Handler{
		cfg: &config.Config{InteractionsKey: []config.GeminiKey{
			{APIKey: "shared-key", BaseURL: "https://a.example.com"},
			{APIKey: "shared-key", BaseURL: "https://b.example.com"},
		}},
		configFilePath: writeTestConfigFile(t),
	}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPatch, "/v0/management/interactions-api-key", strings.NewReader(`{
		"match":"shared-key",
		"value":{"priority":3}
	}`))
	c.Request.Header.Set("Content-Type", "application/json")

	h.PatchInteractionsKey(c)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	for i := range h.cfg.InteractionsKey {
		if h.cfg.InteractionsKey[i].Priority != 0 {
			t.Fatalf("entry %d was modified: %#v", i, h.cfg.InteractionsKey[i])
		}
	}
}

func TestDeleteInteractionsKeyRequiresBaseURLWhenAPIKeyDuplicated(t *testing.T) {
	t.Parallel()

	h := &Handler{
		cfg: &config.Config{InteractionsKey: []config.GeminiKey{
			{APIKey: "shared-key", BaseURL: "https://a.example.com"},
			{APIKey: "shared-key", BaseURL: "https://b.example.com"},
		}},
		configFilePath: writeTestConfigFile(t),
	}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/interactions-api-key?api-key=shared-key", nil)

	h.DeleteInteractionsKey(c)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := len(h.cfg.InteractionsKey); got != 2 {
		t.Fatalf("interactions keys len = %d, want 2", got)
	}
}
