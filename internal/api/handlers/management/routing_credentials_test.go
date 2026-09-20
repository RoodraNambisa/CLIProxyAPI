package management

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

func TestRoutingCredentialOptionsAreProviderNeutralAndSecretFree(t *testing.T) {
	m := auth.NewManager(nil, nil, nil)
	for _, provider := range []string{"codex", "xai", "claude", "chatgpt-web", "custom-compat"} {
		_, err := m.Register(auth.WithSkipPersist(t.Context()), &auth.Auth{ID: provider, FileName: provider + ".json", Provider: provider, Attributes: map[string]string{"priority": "3", "api_key": "NEVER-EXPOSE"}, Metadata: map[string]any{"access_token": "NEVER-EXPOSE", "refresh_token": "NEVER-EXPOSE"}})
		if err != nil {
			t.Fatal(err)
		}
	}
	h := &Handler{authManager: m, cfg: &config.Config{}}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/routing/credential-options", nil)
	h.GetRoutingCredentialOptions(c)
	if w.Code != 200 || len(gjson.Get(w.Body.String(), "credentials").Array()) != 5 || strings.Contains(w.Body.String(), "NEVER-EXPOSE") {
		t.Fatalf("bad options: %s", w.Body.String())
	}
	for _, item := range gjson.Get(w.Body.String(), "credentials").Array() {
		a, _ := m.GetByID(item.Get("provider").String())
		if item.Get("id").String() != a.Index || item.Get("priority").Int() != 3 {
			t.Fatal("picker did not use stable identity or effective priority")
		}
	}
	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	h.GetRoutingPriorityOverrides(c)
	if !gjson.Get(w.Body.String(), "features.credential_request_limits").Bool() {
		t.Fatal("save capability missing")
	}
}
