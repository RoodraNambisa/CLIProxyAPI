package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestCompactionDoesNotUseImageDeadline(t *testing.T) {
	cfg := &config.SDKConfig{}
	cfg.Images.CodexRequestTimeoutSeconds = 1
	handler := NewBaseAPIHandlers(cfg, nil)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	body := []byte(`{"input":[{"type":"compaction_trigger"}],"tools":[{"type":"image_generation"}]}`)
	ctx, cancel := handler.GetImageContextWithCancel(nil, c, t.Context(), body)
	defer cancel()
	if core.ImageRequestBudgetFromContext(ctx) != nil {
		t.Fatal("context compaction acquired image generation budget")
	}
}
