package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

func TestResponseGuardPreviewWithoutManagedResources(t *testing.T) {
	cfg := &config.Config{}
	cfg.ResponseModelRewrite = config.ResponseModelRewriteConfig{Enabled: true, Rules: []config.ResponseModelRewriteRule{{Providers: []string{"codex"}}}}
	m := auth.NewManager(nil, nil, nil)
	m.SetConfig(cfg)
	a, err := m.Register(auth.WithSkipPersist(t.Context()), &auth.Auth{ID: "guard-preview", Provider: "codex", FileName: "preview.json", Attributes: map[string]string{"api_key": "fixture", "priority": "0"}})
	if err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "sol"}})
	defer registry.GetGlobalRegistry().UnregisterClient(a.ID)
	h := &Handler{cfg: cfg, authManager: m}
	for _, length := range []int{292, 312} {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		body, _ := json.Marshal(map[string]any{"name": a.Index, "model": "sol", "returned_model": "6-sol", "state_present": true, "state_length": length, "config": map[string]any{"enabled": true, "mode": "enforce", "allowed-returned-models": []string{"6-sol"}, "length-mode": "allow", "lengths": []int{292}}})
		c.Request = httptest.NewRequest(http.MethodPost, "/preview", strings.NewReader(string(body)))
		h.PreviewCodexResponseGuard(c)
		if w.Code != 200 {
			t.Fatalf("preview=%d %s", w.Code, w.Body.String())
		}
		want := "allowed"
		if length == 312 {
			want = "blocked"
		}
		if gjson.GetBytes(w.Body.Bytes(), "outcome").String() != want || gjson.GetBytes(w.Body.Bytes(), "managed").Bool() {
			t.Fatalf("independence failed: %s", w.Body.String())
		}
		public := gjson.GetBytes(w.Body.Bytes(), "response_model").String()
		if length == 292 && public != "sol" || length == 312 && public != "" {
			t.Fatalf("preview naming: %q", public)
		}
	}
	if stats := m.AuthResponseModelRewriteSummary(a, true); stats.Total != 0 || stats.Blocked != 0 || len(stats.Recent) > 0 {
		t.Fatal("preview changed statistics")
	}
	if cfg.Codex.ResponseGuard.Enabled || cfg.Codex.StateOverride.Enabled {
		t.Fatal("preview saved draft or enabled resources")
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/options", nil)
	h.GetCodexResponseGuardOptions(c)
	if w.Code != 200 || !gjson.GetBytes(w.Body.Bytes(), "features.response_guard").Bool() || len(gjson.GetBytes(w.Body.Bytes(), "credentials").Array()) != 1 {
		t.Fatalf("API key credential missing from options: %s", w.Body.String())
	}
}
