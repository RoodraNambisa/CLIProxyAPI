package gemini

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
)

// GeminiCLIAPIHandler is retained for v6 source compatibility and never proxies requests.
type GeminiCLIAPIHandler struct {
	*handlers.BaseAPIHandler
}

// NewGeminiCLIAPIHandler creates a compatibility handler for the retired endpoint.
func NewGeminiCLIAPIHandler(apiHandlers *handlers.BaseAPIHandler) *GeminiCLIAPIHandler {
	return &GeminiCLIAPIHandler{BaseAPIHandler: apiHandlers}
}

// HandlerType returns the retired protocol identifier.
func (h *GeminiCLIAPIHandler) HandlerType() string {
	return "gemini-cli"
}

// Models returns no models because Gemini CLI execution has been removed.
func (h *GeminiCLIAPIHandler) Models() []map[string]any {
	return []map[string]any{}
}

// CLIHandler always reports that the endpoint has been removed.
func (h *GeminiCLIAPIHandler) CLIHandler(c *gin.Context) {
	c.JSON(http.StatusGone, handlers.ErrorResponse{Error: handlers.ErrorDetail{
		Message: "Gemini CLI endpoint is no longer supported",
		Type:    "unsupported_provider",
	}})
}
