package handlers

import (
	"context"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func (h *BaseAPIHandler) newImageRequestBudget(ctx context.Context) *coreexecutor.ImageRequestBudget {
	cfg := h.ConfigSnapshot()
	if cfg == nil {
		return nil
	}
	images := cfg.Images
	return coreexecutor.NewImageRequestBudget(ctx, time.Now(),
		time.Duration(images.CodexRequestTimeoutSeconds)*time.Second,
		time.Duration(images.ChatGPTWeb.RequestTimeoutSeconds)*time.Second)
}

// BeginImageRequestBudget owns the whole Images API request, not one n iteration.
// Provider discovery and preflight select the applicable budget before upstream work.
func (h *BaseAPIHandler) BeginImageRequestBudget(c *gin.Context) func() {
	if c == nil || c.Request == nil {
		return func() {}
	}
	budget := h.newImageRequestBudget(c.Request.Context())
	if budget == nil {
		return func() {}
	}
	c.Set(coreexecutor.ImageRequestBudgetMetadataKey, budget)
	return budget.Close
}

func imageRequestBudgetForGin(c *gin.Context) *coreexecutor.ImageRequestBudget {
	if c == nil {
		return nil
	}
	value, _ := c.Get(coreexecutor.ImageRequestBudgetMetadataKey)
	budget, _ := value.(*coreexecutor.ImageRequestBudget)
	return budget
}

// GetImageContextWithCancel gives a selectable image tool its own logical request
// budget. In WebSocket mode this is called once per response.create, not per socket.
func (h *BaseAPIHandler) GetImageContextWithCancel(handler interfaces.APIHandler, c *gin.Context, parent context.Context, payload []byte) (context.Context, APIHandlerCancelFunc) {
	ctx, cancel := h.GetContextWithCancel(handler, c, parent)
	if coreexecutor.ImageRequestBudgetFromContext(ctx) != nil || !coreauth.PayloadMaySelectImageGenerationTool(payload) {
		return ctx, cancel
	}
	budget := h.newImageRequestBudget(ctx)
	if budget == nil {
		return ctx, cancel
	}
	bound, release := budget.Bind(ctx)
	return bound, func(args ...interface{}) { release(); budget.Close(); cancel(args...) }
}

// ImageRequestTimeoutResponse retains the original source for rewrite matching.
func (h *BaseAPIHandler) ImageRequestTimeoutResponse(ctx context.Context) *interfaces.ErrorMessage {
	err := coreexecutor.ImageRequestContextError(ctx, nil)
	if !coreexecutor.IsImageRequestTimeout(err) {
		return nil
	}
	return h.RewriteExecutionErrorResponseForContext(ctx, ExecutionErrorMessage(err))
}

func (h *BaseAPIHandler) ImageRequestTimeoutResponseForGin(c *gin.Context) *interfaces.ErrorMessage {
	if err := imageRequestBudgetForGin(c).Err(); err != nil {
		ctx := context.WithValue(context.Background(), "gin", c)
		ctx, _ = ensureErrorResponseSourceTracker(ctx, c)
		return h.RewriteExecutionErrorResponseForContext(ctx, ExecutionErrorMessage(err))
	}
	return nil
}
