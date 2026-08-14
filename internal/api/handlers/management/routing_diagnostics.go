package management

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type routingDiagnosticsResponse struct {
	Routing                 coreauth.RoutingDiagnosticsSnapshot              `json:"routing"`
	RequestExecutionMetrics cliproxyexecutor.RequestExecutionMetricsSnapshot `json:"request_execution_metrics"`
}

// GetRoutingDiagnostics returns low-overhead routing availability and request
// boundary counters without exposing credential material.
func (h *Handler) GetRoutingDiagnostics(c *gin.Context) {
	provider := strings.TrimSpace(c.Query("provider"))
	model := strings.TrimSpace(c.Query("model"))
	if provider == "" || model == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "provider and model are required"})
		return
	}
	if h == nil || h.authManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "auth manager unavailable"})
		return
	}
	c.JSON(http.StatusOK, routingDiagnosticsResponse{
		Routing:                 h.authManager.RoutingDiagnostics(provider, model, time.Now()),
		RequestExecutionMetrics: h.authManager.RequestExecutionMetrics(),
	})
}
