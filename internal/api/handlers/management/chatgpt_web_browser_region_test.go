package management

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type browserRegionTestExecutor struct {
	coreauth.ProviderExecutor
	refreshes int
	code      string
	updates   int
}

func (e *browserRegionTestExecutor) Identifier() string          { return chatgptwebauth.Provider }
func (e *browserRegionTestExecutor) UpdateConfig(*config.Config) { e.updates++ }
func (e *browserRegionTestExecutor) BrowserRegionPreview(_ *coreauth.Auth, refresh bool) (chatgptwebauth.BrowserRegionSnapshot, string) {
	if refresh {
		e.refreshes++
	}
	return chatgptwebauth.BrowserRegionSnapshot{Timezone: "UTC", Language: "en-US", CacheState: "missing", RefreshState: "queued"}, e.code
}

func TestBrowserRegionManagementPersistenceAndValidation(t *testing.T) {
	h, path := newPersistedChatGPTWebLoginProxyHandler(t, config.ChatGPTWebLoginProxyConfig{Enabled: false})
	manager := coreauth.NewManager(nil, nil, nil)
	e := &browserRegionTestExecutor{}
	manager.RegisterExecutor(e)
	h.authManager = manager
	ctx, w := newChatGPTWebLoginProxyRequest(http.MethodPatch, `{"auto-timezone":true,"language":"ja-JP","timezone":"Asia/Tokyo","timezone-offset-minutes":null}`)
	h.PatchChatGPTWebBrowserRegion(ctx)
	if w.Code != 200 || e.updates != 1 {
		t.Fatalf("PATCH failed: %d %s", w.Code, w.Body.String())
	}
	loaded, err := config.LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.ChatGPTWeb.BrowserRegion.AutoTimezone || loaded.ChatGPTWeb.Timezone != "Asia/Tokyo" || loaded.ChatGPTWeb.BrowserRegion.Resolved().RouteCacheSize != 100000 {
		t.Fatal("configuration did not roundtrip")
	}
	for _, body := range []string{`null`, `[]`, `{"unknown":1}`, `{"auto-timezone":null}`, `{"language":null}`, `{"timezone":"Invalid/Zone"}`, `{} {}`} {
		ctx, w = newChatGPTWebLoginProxyRequest(http.MethodPatch, body)
		h.PatchChatGPTWebBrowserRegion(ctx)
		if w.Code != 400 {
			t.Fatalf("invalid body accepted: %s %d", body, w.Code)
		}
	}
	for _, body := range []string{`{"lookup-url":"https://example.test"}`, `{"cache-ttl-seconds":42}`, `{"route-cache-size":1}`} {
		ctx, w = newChatGPTWebLoginProxyRequest(http.MethodPatch, body)
		h.PatchChatGPTWebBrowserRegion(ctx)
		if w.Code != 409 {
			t.Fatalf("legacy common settings were writable: %d", w.Code)
		}
	}
	ctx, w = newChatGPTWebLoginProxyRequest(http.MethodGet, "")
	h.GetChatGPTWebBrowserRegion(ctx)
	var result chatGPTWebBrowserRegionResponse
	if json.Unmarshal(w.Body.Bytes(), &result) != nil || w.Header().Get("Cache-Control") != "no-store" || result.Timezone != "Asia/Tokyo" {
		t.Fatal("GET lost settings")
	}
}
func TestBrowserRegionManagementRollback(t *testing.T) {
	h := &Handler{cfg: &config.Config{}, configFilePath: filepath.Join(t.TempDir(), "missing", "config.yaml")}
	ctx, w := newChatGPTWebLoginProxyRequest(http.MethodPatch, `{"auto-timezone":true}`)
	h.PatchChatGPTWebBrowserRegion(ctx)
	if w.Code != 500 || h.cfg.ChatGPTWeb.BrowserRegion.AutoTimezone {
		t.Fatal("failed persistence changed runtime settings")
	}
}
func TestBrowserRegionManagementPreviewAndRefresh(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	e := &browserRegionTestExecutor{}
	manager.RegisterExecutor(e)
	auth, err := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{ID: "fixture", Provider: chatgptwebauth.Provider})
	if err != nil {
		t.Fatal(err)
	}
	h := &Handler{cfg: &config.Config{}, authManager: manager}
	ctx, w := newChatGPTWebLoginProxyRequest(http.MethodGet, "")
	ctx.Request.URL.RawQuery = "auth_index=" + auth.EnsureIndex()
	h.GetChatGPTWebBrowserRegionPreview(ctx)
	if w.Code != 200 || e.refreshes != 0 {
		t.Fatal("preview created work")
	}
	for _, tc := range []struct {
		code   string
		status int
	}{{"", 202}, {"queue_full", 429}, {"refresh_cooldown", 429}, {"route_unresolved", 409}, {"closed", 503}} {
		e.code = tc.code
		ctx, w = newChatGPTWebLoginProxyRequest(http.MethodPost, `{"auth_index":"`+auth.EnsureIndex()+`"}`)
		h.RefreshChatGPTWebBrowserRegion(ctx)
		if w.Code != tc.status {
			t.Fatalf("code %s mapped to %d", tc.code, w.Code)
		}
	}
}
