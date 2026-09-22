package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexstate"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestCodexCookieManagementActionsAndCompatibility(t *testing.T) {
	cfg := &config.Config{Codex: config.CodexConfig{StateOverride: config.CodexStateOverrideConfig{Enabled: true, Strategy: "cookie-only", Lengths: []int{}, Acquisition: "manual"}}}
	manager := auth.NewManager(nil, nil, nil)
	a, err := manager.Register(auth.WithSkipPersist(t.Context()), &auth.Auth{ID: "cookie.json", FileName: "cookie.json", Provider: "codex"})
	if err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "alias", UpstreamID: "model"}})
	t.Cleanup(func() {
		codexstate.Default.Sync(config.CodexStateOverrideConfig{}, nil)
		codexstate.Default.Wait()
		registry.GetGlobalRegistry().UnregisterClient(a.ID)
	})
	codexstate.Default.Sync(cfg.Codex.StateOverride, helps.ManagedStateModels(cfg, a))
	h := &Handler{cfg: cfg, authManager: manager}
	action := func(op string) {
		body, _ := json.Marshal(map[string]any{"name": a.FileName, "model": "alias", "action": op, "strategy": "cookie-only"})
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("POST", "/auth-files/codex/state", strings.NewReader(string(body)))
		h.CodexStateAction(c)
		if w.Code != 200 {
			t.Fatalf("%s: %d %s", op, w.Code, w.Body.String())
		}
	}
	action("acquire")
	codexstate.Default.Tick(t.Context(), time.Now(), func(context.Context, codexstate.Credential, config.CodexStateOverrideConfig) (codexstate.Result, error) {
		return codexstate.Result{Completed: true, Model: "model", Cookies: codexstate.CaptureCookies("https://chatgpt.com/backend-api/codex/responses", http.Header{"Set-Cookie": {"__oailb=never-expose-this; Path=/; Secure"}}, time.Now())}, nil
	})
	codexstate.Default.Wait()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/auth-files/codex/state?name=cookie.json", nil)
	h.GetCodexState(c)
	var response struct {
		Models []codexstate.Snapshot      `json:"models"`
		Cookie *codexstate.CookieSnapshot `json:"cookie"`
	}
	if err = json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Models == nil || len(response.Models) != 0 || response.Cookie == nil || response.Cookie.Main == nil {
		t.Fatalf("incompatible payload: %s", w.Body.String())
	}
	if strings.Contains(w.Body.String(), "never-expose-this") {
		t.Fatal("cookie value leaked")
	}
	action("pause")
	if codexstate.Default.CookieSnapshot(a.ID, time.Now()).Status != "paused" {
		t.Fatal("not paused")
	}
	action("resume")
	action("clear")
	if codexstate.Default.CookieSnapshot(a.ID, time.Now()).Main != nil {
		t.Fatal("not cleared")
	}
}

func TestCodexCookieProbeModeConflict(t *testing.T) {
	for _, mode := range []string{"managed", "candidate"} {
		for _, state := range []string{"custom", "managed", "acquired", "configured"} {
			if _, err := validateModelProbeCookie(&modelProbeCookieInput{Mode: mode}, "codex", state); err == nil {
				t.Fatal("conflicting State allowed")
			}
		}
		for _, state := range []string{"auto", "none"} {
			if _, err := validateModelProbeCookie(&modelProbeCookieInput{Mode: mode}, "codex", state); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := validateModelProbeCookie(&modelProbeCookieInput{Mode: "managed"}, "xai", "none"); err == nil {
		t.Fatal("non-Codex cookie mode allowed")
	}
}
