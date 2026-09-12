package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	logtest "github.com/sirupsen/logrus/hooks/test"
)

func TestExecutorParentContextPreservesAccessLogCredential(t *testing.T) {
	gin.SetMode(gin.TestMode)
	hook := logtest.NewGlobal()
	defer hook.Reset()
	h := &BaseAPIHandler{Cfg: &config.SDKConfig{}}
	engine := gin.New()
	engine.Use(logging.GinLogrusLogger())
	engine.POST("/v1/chat/completions", func(c *gin.Context) {
		ctx, cancel := h.GetContextWithCancel(nil, c, context.Background())
		defer cancel()
		logging.SetRequestCredential(ctx, logging.CredentialIdentity{Provider: "codex", Index: "fixture-index", Name: "fixture.json"})
		c.JSON(500, gin.H{"error": gin.H{"message": "fixture overload"}})
	})
	engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))
	entries := hook.AllEntries()
	if len(entries) != 1 || entries[0].Data["auth_name"] != "fixture.json" || entries[0].Data["request_id"] == "" {
		t.Fatal("executor context did not reach the access log")
	}
}
