package management

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

func TestAuthFileResponseModelRewriteDetails(t *testing.T) {
	cfg := &config.Config{}
	cfg.ResponseModelRewrite = config.ResponseModelRewriteConfig{Enabled: true, Rules: []config.ResponseModelRewriteRule{{Providers: []string{"codex"}, AuthPriorities: []int{0}, RequestModels: []string{"gpt-*"}}}}
	m := coreauth.NewManager(nil, nil, nil)
	m.SetConfig(cfg)
	_, err := m.Register(t.Context(), &coreauth.Auth{ID: "fixture.json", FileName: "fixture.json", Provider: "codex", Metadata: map[string]any{"access_token": "secret-fixture"}})
	if err != nil {
		t.Fatal(err)
	}
	h := &Handler{cfg: cfg, authManager: m}
	for _, tc := range []struct {
		name   string
		status int
	}{{"fixture.json", 200}, {"missing.json", 404}, {"", 400}} {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodGet, "/auth-files/response-model-rewrite?name="+tc.name, nil)
		h.GetAuthFileResponseModelRewrite(c)
		if w.Code != tc.status || strings.Contains(w.Body.String(), "secret-fixture") {
			t.Fatalf("invalid response: %d %s", w.Code, w.Body.String())
		}
		if tc.status == 200 && (!gjson.GetBytes(w.Body.Bytes(), "enabled").Bool() || !gjson.GetBytes(w.Body.Bytes(), "conditional").Bool() || w.Header().Get("Cache-Control") != "no-store") {
			t.Fatal("missing policy details")
		}
	}
}
