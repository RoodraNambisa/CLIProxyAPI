package management

import (
	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCodexIdentityPreviewIsReadOnly(t *testing.T) {
	cfg := &config.Config{Codex: config.CodexConfig{IdentityConfuse: true}}
	m := auth.NewManager(nil, nil, nil)
	m.SetConfig(cfg)
	a, err := m.Register(auth.WithSkipPersist(t.Context()), &auth.Auth{ID: "identity-preview", Provider: "codex", Attributes: map[string]string{"priority": "5"}})
	if err != nil {
		t.Fatal(err)
	}
	h := &Handler{cfg: cfg, authManager: m}
	for _, draft := range []string{"", `,"codex":{"identity-confuse-require-session-affinity":false,"identity-confuse-restore-response":false}`} {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("POST", "/preview", strings.NewReader(`{"auth_index":"`+a.Index+`"`+draft+`}`))
		h.PreviewCodexIdentityPolicy(c)
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		if gjson.GetBytes(w.Body.Bytes(), "policy.identity-confuse-effective").Bool() != (draft != "") {
			t.Fatal(w.Body.String())
		}
		if gjson.GetBytes(w.Body.Bytes(), "priority").Int() != 5 {
			t.Fatal("wrong priority")
		}
	}
	after, _ := m.GetByAuthIndex(a.Index)
	if after.Metadata["codex_session_identity_pool"] != nil || len(cfg.Routing.PriorityOverrides) != 0 {
		t.Fatal("preview prepared or persisted identity state")
	}
}
