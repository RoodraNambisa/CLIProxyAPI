package management

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestOAuthRequestScopedErrorCRUDAndRollback(t *testing.T) {
	h := &Handler{cfg: &config.Config{}, configFilePath: writeTestConfigFile(t)}
	r := gin.New()
	r.GET("/rules", h.GetOAuthRequestScopedErrors)
	r.PUT("/rules", h.PutOAuthRequestScopedErrors)
	r.PATCH("/rules", h.PatchOAuthRequestScopedErrors)
	r.DELETE("/rules", h.DeleteOAuthRequestScopedErrors)
	call := func(method, url, body string, want int) {
		t.Helper()
		w := httptest.NewRecorder()
		req := httptest.NewRequest(method, url, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
		if w.Code != want {
			t.Fatalf("%s returned %d, want %d", method, w.Code, want)
		}
	}
	call(http.MethodGet, "/rules", "", 200)
	call(http.MethodPut, "/rules", `{" CoDeX ":[{"status":400,"match":[" x "],"action":"stop"}],"claude":[{"status":500,"match":["temporary"],"action":"continue"}]}`, 200)
	call(http.MethodPatch, "/rules", `{"provider":"codex","rules":[{"status":400,"match":["changed"],"action":"stop-and-cooldown"}]}`, 200)
	if len(h.cfg.OAuthRequestScopedErrors) != 2 || h.cfg.OAuthRequestScopedErrors[" CoDeX "][0].Action != "stop-and-cooldown" {
		t.Fatal("patch lost provider spelling or unrelated channel")
	}
	before, _ := os.ReadFile(h.configFilePath)
	for _, body := range []string{`{"channel":"codex"}`, `{"channel":"codex","rules":[{"status":400,"match-regexr":["["],"action":"stop"}]}`} {
		call(http.MethodPatch, "/rules", body, 400)
		after, _ := os.ReadFile(h.configFilePath)
		if !bytes.Equal(before, after) {
			t.Fatal("invalid patch changed disk")
		}
	}
	call(http.MethodPut, "/rules", `{"codex":[]," CODEX ":[]}`, 400)
	fail := true
	h.runtimeConfigApplier = func(context.Context, *config.Config) (config.RuntimeApplyResult, error) {
		if fail {
			return config.RuntimeApplyResult{}, errors.New("fixture apply rejected")
		}
		return config.RuntimeApplyResult{Applied: true}, nil
	}
	call(http.MethodDelete, "/rules?channel=CODEX", "", 500)
	after, _ := os.ReadFile(h.configFilePath)
	if !bytes.Equal(before, after) || len(h.cfg.OAuthRequestScopedErrors) != 2 {
		t.Fatal("failed apply did not restore rules")
	}
	fail = false
	call(http.MethodDelete, "/rules?channel=CODEX", "", 200)
	call(http.MethodDelete, "/rules?provider=codex", "", 404)
	call(http.MethodPatch, "/rules", `{"channel":"claude","rules":null}`, 200)
	loaded, err := config.LoadConfig(h.configFilePath)
	if err != nil || len(loaded.OAuthRequestScopedErrors) != 0 {
		t.Fatal("deleting final provider did not persist")
	}
	call(http.MethodPut, "/rules", `{"items":{"codex":[{"status":400,"match":["fixture"],"action":"stop"}]}}`, 200)
	call(http.MethodPut, "/rules", `{"items":null}`, 200)
	if len(h.cfg.OAuthRequestScopedErrors) != 0 {
		t.Fatal("null wrapper did not clear rules")
	}
	call(http.MethodPut, "/rules", "null", 200)
}
