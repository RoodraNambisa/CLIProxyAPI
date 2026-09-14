package handlers

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestImageBootstrapHandlerSnapshotsAndScope(t *testing.T) {
	cfg := &sdkconfig.SDKConfig{}
	h := NewBaseAPIHandlers(cfg, nil)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/images/generations", nil)
	finish := h.BeginImageRequestBudget(c)
	defer finish()
	cfg.Images.ChatGPTWeb.BootstrapTimeoutSeconds = 30
	cfg.Images.ChatGPTWeb.BootstrapRetries = 1
	for range 2 {
		ctx, cancel := h.GetContextWithCancel(nil, c, context.Background())
		policy, pinned := core.ImageBootstrapPolicyFromContext(ctx)
		cancel()
		if !pinned || policy.Enabled() {
			t.Fatalf("zero snapshot changed across n: %+v %v", policy, pinned)
		}
	}
	ws, _ := gin.CreateTestContext(httptest.NewRecorder())
	ws.Request = httptest.NewRequest("GET", "/v1/responses", nil)
	payload := []byte(`{"tools":[{"type":"image_generation"}]}`)
	ctx, cancel := h.GetImageContextWithCancel(nil, ws, t.Context(), payload)
	defer cancel()
	policy, pinned := core.ImageBootstrapPolicyFromContext(ctx)
	if !pinned || policy.Timeout != 30*time.Second || policy.Retries != 1 {
		t.Fatalf("logical request policy: %+v", policy)
	}
	cfg.Images.ChatGPTWeb.BootstrapTimeoutSeconds = 10
	next, nextCancel := h.GetImageContextWithCancel(nil, ws, t.Context(), payload)
	defer nextCancel()
	if newer, _ := core.ImageBootstrapPolicyFromContext(next); newer.Timeout != 10*time.Second {
		t.Fatal("new WebSocket request retained previous policy")
	}
	if existing, _ := core.ImageBootstrapPolicyFromContext(ctx); existing != policy {
		t.Fatal("in-flight policy changed")
	}
	text, textCancel := h.GetImageContextWithCancel(nil, ws, t.Context(), []byte(`{"tools":[{"type":"image_generation"}],"tool_choice":"none"}`))
	defer textCancel()
	if core.ImageRequestBudgetFromContext(text) != nil {
		t.Fatal("non-image request started a deadline")
	}
}

func TestImageBootstrapRewriteAndSanitization(t *testing.T) {
	for _, rewrite := range []bool{false, true} {
		cfg := &sdkconfig.SDKConfig{Images: sdkconfig.ImagesConfig{ChatGPTWeb: sdkconfig.ChatGPTWebImageConfig{SanitizeErrorResponses: true}}}
		wantStatus := 504
		if rewrite {
			body := map[string]any{"error": map[string]any{"message": "Please retry later.", "type": "rate_limit_error", "code": "rate_limit_exceeded"}}
			cfg.ErrorResponseRewrites = []sdkconfig.ErrorResponseRewriteRule{{StatusCode: 504, MessageContains: "chatgpt web", ResponseStatusCode: 429, ResponseBody: &body}}
			wantStatus = 429
		}
		h := NewBaseAPIHandlers(cfg, nil)
		original := &core.ImageBootstrapError{Cause: context.DeadlineExceeded, Timeout: true}
		projected := h.RewriteExecutionErrorResponse(ExecutionErrorMessage(original))
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		h.BeginChatGPTWebImageErrorSanitization(c, true)
		h.WriteErrorResponse(c, projected)
		if recorder.Code != wantStatus {
			t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
		}
		assertSanitizedImageBody(t, recorder.Body.String())
		if strings.Contains(recorder.Body.String(), "bootstrap") || strings.Contains(recorder.Body.String(), "homepage") {
			t.Fatalf("internal phase exposed: %s", recorder.Body.String())
		}
		if !original.RetryOtherAuth() || !original.SkipAuthResult() {
			t.Fatal("public rewrite changed internal behavior")
		}
	}
}
