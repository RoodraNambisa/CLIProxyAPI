package management

import (
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
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	sdkAuth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestXAICredentialBaseURLPersistsAndCanReturnToGlobal(t *testing.T) {
	dir := t.TempDir()
	store := sdkAuth.NewFileTokenStore()
	store.SetBaseDir(dir)
	manager := coreauth.NewManager(store, nil, nil)
	_, err := manager.Register(t.Context(), &coreauth.Auth{
		ID: "endpoint.json", FileName: "endpoint.json", Provider: "xai",
		Storage:    &xaiauth.TokenStorage{AccessToken: "fixture-token", RefreshToken: "fixture-refresh", BaseURL: "https://old.example/v1"},
		Attributes: map[string]string{"base_url": "https://old.example/v1", "auth_kind": "oauth"},
		Metadata:   map[string]any{"type": "xai", "auth_kind": "oauth", "base_url": "https://old.example/v1", helps.XAIIdentitySeedKey: "fixture-seed"},
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{AuthDir: dir, XAI: config.XAIConfig{DefaultBaseURLMode: "us-west-2"}}
	h := NewHandlerWithoutConfigFilePath(cfg, manager)
	for _, tc := range []struct{ patch, wantURL, source string }{
		{`{"base_url":"https://api.x.ai/v1","using_api":false}`, "https://api.x.ai/v1", "credential"},
		{`{"base_url":"https://cli-chat-proxy.grok.com/v1","using_api":true}`, "https://cli-chat-proxy.grok.com/v1", "credential"},
		{`{"base_url":"https://eu-west-1.api.x.ai/v1/"}`, "https://eu-west-1.api.x.ai/v1", "credential"},
		{`{"base_url":""}`, "https://us-west-2.api.x.ai/v1", "global"},
	} {
		w := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(w)
		ctx.Request = httptest.NewRequest(http.MethodPatch, "/auth-files/fields", strings.NewReader(`{"names":["endpoint.json"],"fields":`+tc.patch+`}`))
		ctx.Request.Header.Set("Content-Type", "application/json")
		h.PatchAuthFileFields(ctx)
		if w.Code != http.StatusOK {
			t.Fatalf("patch %s: %d %s", tc.patch, w.Code, w.Body.String())
		}
		auth, _ := manager.GetByID("endpoint.json")
		resolved := helps.ResolveXAIUpstream(auth, cfg)
		if resolved.BaseURL != tc.wantURL || resolved.Source != tc.source {
			t.Fatalf("patch %s resolved to %#v", tc.patch, resolved)
		}
		raw, errRead := os.ReadFile(filepath.Join(dir, "endpoint.json"))
		if errRead != nil {
			t.Fatal(errRead)
		}
		var metadata map[string]any
		if errDecode := json.Unmarshal(raw, &metadata); errDecode != nil {
			t.Fatal(errDecode)
		}
		if metadata["access_token"] != "fixture-token" || metadata["refresh_token"] != "fixture-refresh" || metadata[helps.XAIIdentitySeedKey] != "fixture-seed" {
			t.Fatal("endpoint edit lost token or persistent identity metadata")
		}
		loaded := &coreauth.Auth{Provider: "xai", Metadata: metadata}
		if got := helps.ResolveXAIUpstream(loaded, cfg); got != resolved {
			t.Fatalf("persisted endpoint differs after reload: %#v vs %#v", got, resolved)
		}
		entry := gin.H{}
		h.applyXAIUpstreamInfo(entry, auth)
		if entry["upstream_source"] != tc.source || entry["upstream_base_url"] != tc.wantURL {
			t.Fatalf("management routing state = %#v", entry)
		}
	}
}

func TestXAICredentialBaseURLValidationIsAtomic(t *testing.T) {
	for _, tc := range []struct{ provider, url string }{
		{"xai", "https://user:secret@api.x.ai/v1"},
		{"xai", "https://api.x.ai/v1?token=secret"},
		{"xai", "file:///tmp/socket"},
		{"claude", "https://api.x.ai/v1"},
	} {
		dir := t.TempDir()
		store := sdkAuth.NewFileTokenStore()
		store.SetBaseDir(dir)
		manager := coreauth.NewManager(store, nil, nil)
		_, err := manager.Register(t.Context(), &coreauth.Auth{ID: "test.json", FileName: "test.json", Provider: tc.provider, Prefix: "before", Metadata: map[string]any{"type": tc.provider}})
		if err != nil {
			t.Fatal(err)
		}
		h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: dir}, manager)
		body, _ := json.Marshal(map[string]any{"name": "test.json", "prefix": "after", "base_url": tc.url})
		w := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(w)
		ctx.Request = httptest.NewRequest(http.MethodPatch, "/auth-files/fields", strings.NewReader(string(body)))
		ctx.Request.Header.Set("Content-Type", "application/json")
		h.PatchAuthFileFields(ctx)
		current, _ := manager.GetByID("test.json")
		if w.Code != http.StatusBadRequest || current.Prefix != "before" {
			t.Fatalf("invalid endpoint changed credential: status=%d prefix=%s", w.Code, current.Prefix)
		}
	}
}
