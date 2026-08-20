package management

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
)

// GetProxyHealthCheck returns the normalized global proxy-probe configuration.
func (h *Handler) GetProxyHealthCheck(c *gin.Context) {
	current := config.ProxyHealthCheckConfig{}
	if cfg := h.currentConfig(); cfg != nil {
		current = cfg.ProxyHealthCheck
	}
	normalized, errNormalize := config.NormalizeProxyHealthCheckConfiguration(current)
	if errNormalize != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "invalid proxy health configuration"})
		return
	}
	response := gin.H{"proxy-health-check": normalized}
	if manager := h.proxyPoolRuntimeManager(); manager != nil {
		response["runtime"] = manager.CheckAdmissionSnapshot()
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, response)
}

// PatchProxyHealthCheck updates and hot-applies global proxy-probe settings.
func (h *Handler) PatchProxyHealthCheck(c *gin.Context) {
	var body struct {
		Concurrency            *int                                     `json:"concurrency"`
		EndpointTimeoutSeconds *int                                     `json:"endpoint-timeout-seconds"`
		FailureThreshold       *int                                     `json:"failure-threshold"`
		Endpoints              *[]config.ProxyHealthCheckEndpointConfig `json:"endpoints"`
	}
	if errBind := c.ShouldBindJSON(&body); errBind != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	if body.Concurrency != nil && *body.Concurrency < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "concurrency must be at least 1"})
		return
	}
	if body.EndpointTimeoutSeconds != nil && *body.EndpointTimeoutSeconds < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "endpoint-timeout-seconds must be at least 1"})
		return
	}
	if body.FailureThreshold != nil && *body.FailureThreshold < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failure-threshold must be at least 1"})
		return
	}
	if body.Endpoints != nil && len(*body.Endpoints) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "at least one endpoint is required"})
		return
	}

	h.mu.Lock()
	previous := h.cfg.ProxyHealthCheck
	candidate := previous
	if body.Concurrency != nil {
		candidate.Concurrency = *body.Concurrency
	}
	if body.EndpointTimeoutSeconds != nil {
		candidate.EndpointTimeoutSeconds = *body.EndpointTimeoutSeconds
	}
	if body.FailureThreshold != nil {
		candidate.FailureThreshold = *body.FailureThreshold
	}
	if body.Endpoints != nil {
		candidate.Endpoints = append([]config.ProxyHealthCheckEndpointConfig(nil), (*body.Endpoints)...)
	}
	normalized, errNormalize := config.NormalizeProxyHealthCheckConfiguration(candidate)
	if errNormalize != nil {
		h.mu.Unlock()
		c.JSON(http.StatusBadRequest, gin.H{"error": errNormalize.Error()})
		return
	}
	h.cfg.ProxyHealthCheck = normalized
	if !h.persistLocked(c) {
		h.cfg.ProxyHealthCheck = previous
	}
	h.mu.Unlock()
}

// PostProxyHealthCheckTest verifies configured endpoints without exposing response bodies.
func (h *Handler) PostProxyHealthCheckTest(c *gin.Context) {
	current := config.ProxyHealthCheckConfig{}
	if cfg := h.currentConfig(); cfg != nil {
		current = cfg.ProxyHealthCheck
	}
	normalized, errNormalize := config.NormalizeProxyHealthCheckConfiguration(current)
	if errNormalize != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "invalid proxy health configuration"})
		return
	}
	type endpointResult struct {
		Name      string `json:"name"`
		Mode      string `json:"mode"`
		OK        bool   `json:"ok"`
		ElapsedMS int64  `json:"elapsed_ms"`
		Error     string `json:"error,omitempty"`
	}
	results := make([]endpointResult, 0, len(normalized.Endpoints))
	for _, endpoint := range normalized.Endpoints {
		result := proxyutil.CheckTrace(c.Request.Context(), "direct", proxyutil.TraceOptions{
			Endpoint: endpoint.URL,
			Timeout:  time.Duration(normalized.EndpointTimeoutSeconds) * time.Second,
			Mode:     endpoint.Mode,
		})
		results = append(results, endpointResult{
			Name:      endpoint.Name,
			Mode:      endpoint.Mode,
			OK:        result.OK,
			ElapsedMS: result.Elapsed.Milliseconds(),
			Error:     result.Error,
		})
	}
	c.JSON(http.StatusOK, gin.H{"results": results})
}
