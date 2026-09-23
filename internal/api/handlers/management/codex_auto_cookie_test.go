package management

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexstate"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestAutoCookieDiagnosticClearsPreviousAttempt(t *testing.T) {
	trace := &modelProbeTrace{codexCookie: &modelProbeCookieResult{Mode: "configured"}}
	trace.cookieApplied("automatic", codexstate.CookieSelection{Header: "__oailb=private"})
	if !trace.codexCookie.Sent || trace.codexCookie.Digest == "" {
		t.Fatal("missing initial diagnostic")
	}
	trace.cookieApplied("empty", codexstate.CookieSelection{})
	if trace.codexCookie.Sent || trace.codexCookie.Digest != "" || len(trace.codexCookie.Names) != 0 {
		t.Fatal("stale cookie diagnostic retained")
	}
}

func TestAutoCookieManagementCapabilityAndConflicts(t *testing.T) {
	cfg := &config.Config{Codex: config.CodexConfig{AutoCookie: true}}
	manager := auth.NewManager(nil, nil, nil)
	a, err := manager.Register(auth.WithSkipPersist(t.Context()), &auth.Auth{ID: "auto-cookie.json", FileName: "auto-cookie.json", Provider: "codex"})
	if err != nil {
		t.Fatal(err)
	}
	h := &Handler{cfg: cfg, authManager: manager}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/auth-files/codex/state/options", nil)
	h.GetCodexStateOptions(c)
	var options map[string]any
	if err = json.Unmarshal(w.Body.Bytes(), &options); err != nil {
		t.Fatal(err)
	}
	if options["auto_cookie_enabled"] != true || options["features"].(map[string]any)["auto_cookie"] != true {
		t.Fatal(options)
	}
	for _, diagnostic := range []bool{true, false} {
		body, _ := json.Marshal(map[string]any{"name": a.FileName, "strategy": "cookie-only", "diagnostic": diagnostic, "action": "acquire", "model": "gpt-5.5"})
		w = httptest.NewRecorder()
		c, _ = gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("POST", "/auth-files/codex/state", strings.NewReader(string(body)))
		h.CodexStateAction(c)
		if w.Code != 409 || !strings.Contains(w.Body.String(), "auto-cookie") {
			t.Fatal(w.Code, w.Body.String())
		}
	}
}
