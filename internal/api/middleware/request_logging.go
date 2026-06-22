// Package middleware provides HTTP middleware components for the CLI Proxy API server.
// This file contains the request logging middleware that captures comprehensive
// request and response data when enabled through configuration.
package middleware

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

const maxErrorOnlyCapturedRequestBodyBytes int64 = 1 << 20 // 1 MiB

// RequestBodyReleaseConfigProvider returns the current request body release configuration.
type RequestBodyReleaseConfigProvider func() config.RequestBodyReleaseConfig

// RequestLoggingMiddleware creates a Gin middleware that logs HTTP requests and responses.
// It captures detailed information about the request and response, including headers and body,
// and uses the provided RequestLogger to record this data. When full request logging is disabled,
// body capture is limited to small known-size payloads to avoid large per-request memory spikes.
func RequestLoggingMiddleware(logger logging.RequestLogger, releaseProviders ...RequestBodyReleaseConfigProvider) gin.HandlerFunc {
	return func(c *gin.Context) {
		if logger == nil {
			c.Next()
			return
		}

		if shouldSkipMethodForRequestLogging(c.Request) {
			c.Next()
			return
		}

		path := c.Request.URL.Path
		if !shouldLogRequest(path) {
			c.Next()
			return
		}
		ensureRequestLogContextMutex(c)

		loggerEnabled := logger.IsEnabled()
		releaseCfg := config.RequestBodyReleaseConfig{}
		if len(releaseProviders) > 0 && releaseProviders[0] != nil {
			releaseCfg = config.NormalizeRequestBodyRelease(releaseProviders[0]())
		}

		// Capture request information
		requestInfo, err := captureRequestInfo(c, shouldCaptureRequestBody(loggerEnabled, c.Request), releaseCfg)
		if err != nil {
			// Log error but continue processing
			// In a real implementation, you might want to use a proper logger here
			c.Next()
			return
		}

		// Create response writer wrapper
		wrapper := NewResponseWriterWrapper(c.Writer, logger, requestInfo)
		if !loggerEnabled {
			wrapper.logOnErrorOnly = true
		}
		c.Writer = wrapper

		// Process the request
		c.Next()

		// Finalize logging after request processing
		if err = wrapper.Finalize(c); err != nil {
			// Log error but don't interrupt the response
			// In a real implementation, you might want to use a proper logger here
		}
	}
}

func shouldSkipMethodForRequestLogging(req *http.Request) bool {
	if req == nil {
		return true
	}
	if req.Method != http.MethodGet {
		return false
	}
	return !isResponsesWebsocketUpgrade(req)
}

func isResponsesWebsocketUpgrade(req *http.Request) bool {
	if req == nil || req.URL == nil {
		return false
	}
	if req.URL.Path != "/v1/responses" {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(req.Header.Get("Upgrade")), "websocket")
}

func shouldCaptureRequestBody(loggerEnabled bool, req *http.Request) bool {
	if loggerEnabled {
		return true
	}
	if req == nil || req.Body == nil {
		return false
	}
	contentType := strings.ToLower(strings.TrimSpace(req.Header.Get("Content-Type")))
	if strings.HasPrefix(contentType, "multipart/form-data") {
		return false
	}
	if req.ContentLength <= 0 {
		return false
	}
	return req.ContentLength <= maxErrorOnlyCapturedRequestBodyBytes
}

// captureRequestInfo extracts relevant information from the incoming HTTP request.
// It captures the URL, method, headers, and body. The request body is read and then
// restored so that it can be processed by subsequent handlers.
func captureRequestInfo(c *gin.Context, captureBody bool, releaseCfg config.RequestBodyReleaseConfig) (*RequestInfo, error) {
	// Capture URL with sensitive query parameters masked
	maskedQuery := util.MaskSensitiveQuery(c.Request.URL.RawQuery)
	url := c.Request.URL.Path
	if maskedQuery != "" {
		url += "?" + maskedQuery
	}

	// Capture method
	method := c.Request.Method

	// Capture headers
	headers := make(map[string][]string)
	for key, values := range c.Request.Header {
		headers[key] = values
	}

	// Capture request body
	var body []byte
	if captureBody && c.Request.Body != nil {
		// Read the body
		bodyBytes, err := io.ReadAll(c.Request.Body)
		if err != nil {
			return nil, err
		}

		// Restore the body for the actual request processing, then drop this
		// restored copy as soon as the handler consumes it.
		c.Request.Body = cliproxyexecutor.NewReleasableReadCloser(bodyBytes, nil)
		body = bodyBytes
	}

	requestInfo := &RequestInfo{
		URL:       url,
		Method:    method,
		Headers:   headers,
		Body:      body,
		RequestID: logging.GetGinRequestID(c),
		Timestamp: time.Now(),
		StreamHint: bytes.Contains(body, []byte(`"stream": true`)) ||
			bytes.Contains(body, []byte(`"stream":true`)),
	}
	if ctrl := ensureRequestBodyReleaseController(c, releaseCfg, int64(len(body))); ctrl != nil && len(body) > 0 {
		ctrl.RegisterReleaseCallback(func(placeholder []byte) {
			requestInfo.SetBody(placeholder)
		})
	}
	return requestInfo, nil
}

func ensureRequestBodyReleaseController(c *gin.Context, cfg config.RequestBodyReleaseConfig, bodySize int64) *cliproxyexecutor.RequestBodyReleaseController {
	if c == nil || c.Request == nil {
		return nil
	}
	cfg = config.NormalizeRequestBodyRelease(cfg)
	if !cfg.Enable {
		return nil
	}
	if bodySize <= 0 && c.Request.ContentLength > 0 {
		bodySize = c.Request.ContentLength
	}
	if bodySize <= 0 {
		return nil
	}
	if cfg.MinBodyBytes > 0 && bodySize < cfg.MinBodyBytes {
		return nil
	}
	if raw, exists := c.Get(cliproxyexecutor.BodyReleaseControllerMetadataKey); exists {
		if ctrl, ok := raw.(*cliproxyexecutor.RequestBodyReleaseController); ok && ctrl != nil {
			if cfg.AfterSeconds > 0 {
				ctrl.StartTimer(time.Duration(cfg.AfterSeconds)*time.Second, c.Request.Context().Done())
			}
			return ctrl
		}
	}
	placeholder := cliproxyexecutor.RequestBodyReleaseTimerPlaceholder(bodySize, cfg.AfterSeconds, cfg.LogOnly)
	ctrl := cliproxyexecutor.NewRequestBodyReleaseControllerWithMode(bodySize, placeholder, cfg.LogOnly)
	c.Set(cliproxyexecutor.BodyReleaseControllerMetadataKey, ctrl)
	if cfg.AfterSeconds > 0 {
		ctrl.StartTimer(time.Duration(cfg.AfterSeconds)*time.Second, c.Request.Context().Done())
	}
	return ctrl
}

// shouldLogRequest determines whether the request should be logged.
// It skips management endpoints to avoid leaking secrets but allows
// all other routes, including module-provided ones, to honor request-log.
func shouldLogRequest(path string) bool {
	if isManagementAPIPath(path) || strings.HasPrefix(path, "/management") {
		return false
	}

	if strings.HasPrefix(path, "/api") {
		return strings.HasPrefix(path, "/api/provider")
	}

	return true
}

func isManagementAPIPath(path string) bool {
	const marker = "/v0/management"
	for {
		idx := strings.Index(path, marker)
		if idx < 0 {
			return false
		}
		end := idx + len(marker)
		if end == len(path) || path[end] == '/' || path[end] == '?' {
			return true
		}
		path = path[idx+1:]
	}
}
