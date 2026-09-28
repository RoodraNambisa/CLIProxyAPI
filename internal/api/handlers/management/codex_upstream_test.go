package management

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	codexauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/watcher/synthesizer"
	sdkAuth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestCodexBaseURLCredentialPatchPersistsAndClears(t *testing.T) {
	dir := t.TempDir()
	store := sdkAuth.NewFileTokenStore()
	store.SetBaseDir(dir)
	manager := auth.NewManager(store, nil, nil)
	_, err := manager.Register(t.Context(), &auth.Auth{ID: "codex.json", FileName: "codex.json", Provider: "codex", Storage: &codexauth.CodexTokenStorage{AccessToken: "fixture-token", RefreshToken: "fixture-refresh"}, Metadata: map[string]any{"type": "codex", "note": "keep"}})
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{AuthDir: dir, Codex: config.CodexConfig{BaseURL: "https://global.test/codex"}}
	h := NewHandlerWithoutConfigFilePath(cfg, manager)
	for _, batch := range []bool{false, true} {
		for _, value := range []string{"https://override.test/proxy/codex/", ""} {
			body := map[string]any{"name": "codex.json", "base_url": value}
			if batch {
				body = map[string]any{"names": []string{"codex.json"}, "fields": map[string]any{"base_url": value}}
			}
			raw, _ := json.Marshal(body)
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest("PATCH", "/auth-files/fields", strings.NewReader(string(raw)))
			c.Request.Header.Set("Content-Type", "application/json")
			h.PatchAuthFileFields(c)
			if w.Code != 200 {
				t.Fatalf("patch %d %s", w.Code, w.Body.String())
			}
			current, _ := manager.GetByID("codex.json")
			want := strings.TrimSuffix(value, "/")
			source := "credential"
			if want == "" {
				want = cfg.Codex.BaseURL
				source = "global"
			}
			if got := helps.ResolveCodexUpstream(current, cfg); got.BaseURL != want || got.Source != source {
				t.Fatalf("runtime: %#v", got)
			}
			data, errRead := os.ReadFile(filepath.Join(dir, "codex.json"))
			if errRead != nil {
				t.Fatal(errRead)
			}
			var metadata map[string]any
			if errDecode := json.Unmarshal(data, &metadata); errDecode != nil {
				t.Fatal(errDecode)
			}
			if metadata["access_token"] != "fixture-token" || metadata["refresh_token"] != "fixture-refresh" || metadata["note"] != "keep" || metadata["using_api"] != nil {
				t.Fatal("endpoint edit changed credentials or Grok flags")
			}
			loaded, errLoad := store.List(t.Context())
			if errLoad != nil || len(loaded) != 1 {
				t.Fatalf("store reload: %v", errLoad)
			}
			synth := synthesizer.SynthesizeAuthFile(&synthesizer.SynthesisContext{Config: cfg, AuthDir: dir, IDGenerator: synthesizer.NewStableIDGenerator()}, filepath.Join(dir, "codex.json"), data)
			if len(synth) != 1 {
				t.Fatal("watcher did not load credential")
			}
			for _, a := range []*auth.Auth{loaded[0], synth[0]} {
				if got := helps.ResolveCodexUpstream(a, cfg); got.BaseURL != want {
					t.Fatalf("reload route: %#v", got)
				}
			}
			entry := gin.H{}
			h.applyCodexUpstreamInfo(entry, current)
			if entry["upstream_source"] != source || entry["upstream_base_url"] != want {
				t.Fatal("incorrect management routing state")
			}
		}
	}
}

func TestCodexBaseURLRejectsInvalidImport(t *testing.T) {
	h := NewHandlerWithoutConfigFilePath(&config.Config{}, nil)
	for _, raw := range []string{`"file:///tmp/socket"`, `42`, `"https://user:secret@example.test"`, `"https://example.test?token=secret"`} {
		if _, err := h.buildAuthFromFileData(filepath.Join(t.TempDir(), "codex.json"), []byte(`{"type":"codex","base_url":`+raw+`}`)); err == nil {
			t.Fatal("invalid import accepted")
		}
	}
}
