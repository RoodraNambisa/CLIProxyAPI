package management

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

func TestAuthFileErrorHistoryDetails(t *testing.T) {
	m := coreauth.NewManager(nil, nil, nil)
	a, err := m.Register(t.Context(), &coreauth.Auth{ID: "fixture.json", FileName: "fixture.json", Provider: "codex"})
	if err != nil {
		t.Fatal(err)
	}
	m.MarkResult(t.Context(), coreauth.Result{AuthID: a.ID, Model: "gpt-a", Error: &coreauth.Error{HTTPStatus: 503, Message: "overloaded"}})
	h := &Handler{cfg: &config.Config{}, authManager: m}
	for _, tc := range []struct {
		name   string
		status int
	}{{"fixture.json", 200}, {"missing.json", 404}, {"", 400}} {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodGet, "/auth-files/errors?name="+tc.name, nil)
		h.GetAuthFileErrorHistory(c)
		if w.Code != tc.status || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("unexpected response: %d %s", w.Code, w.Body.String())
		}
		if tc.status == 200 && (gjson.GetBytes(w.Body.Bytes(), "total").Int() != 1 || gjson.GetBytes(w.Body.Bytes(), "recent.0.models.0.model").String() != "gpt-a") {
			t.Fatal("missing error details")
		}
	}
}
