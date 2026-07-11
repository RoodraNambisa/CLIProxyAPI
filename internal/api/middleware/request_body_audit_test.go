package middleware

import (
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestRequestBodyAuditMiddlewareBlocksKeywordCaseInsensitive(t *testing.T) {
	router := gin.New()
	router.Use(RequestBodyAuditMiddleware(func() config.RequestBodyAuditConfig {
		return config.NormalizeRequestBodyAudit(config.RequestBodyAuditConfig{
			Enable:   true,
			Keywords: []string{"BLOCKED"},
			Error: config.RequestBodyAuditErrorConfig{
				StatusCode: http.StatusForbidden,
				Message:    "blocked by test",
				Type:       "policy_error",
				Code:       "blocked_body",
			},
		})
	}))
	router.POST("/v1/chat/completions", func(c *gin.Context) {
		c.String(http.StatusOK, "handler reached")
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"input":"blocked"}`))
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusForbidden, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "blocked by test") || !strings.Contains(rec.Body.String(), "blocked_body") {
		t.Fatalf("body = %s, want custom audit error", rec.Body.String())
	}
}

func TestRequestBodyAuditMiddlewareAllowsWhenCaseSensitiveDoesNotMatch(t *testing.T) {
	router := gin.New()
	router.Use(RequestBodyAuditMiddleware(func() config.RequestBodyAuditConfig {
		return config.NormalizeRequestBodyAudit(config.RequestBodyAuditConfig{
			Enable:        true,
			Keywords:      []string{"BLOCKED"},
			CaseSensitive: true,
		})
	}))
	router.POST("/v1/chat/completions", func(c *gin.Context) {
		data, _ := io.ReadAll(c.Request.Body)
		c.String(http.StatusOK, string(data))
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"input":"blocked"}`))
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := rec.Body.String(); got != `{"input":"blocked"}` {
		t.Fatalf("restored body = %q, want original", got)
	}
}

func TestRequestBodyAuditMiddlewareBlocksBase64Keyword(t *testing.T) {
	router := gin.New()
	router.Use(RequestBodyAuditMiddleware(func() config.RequestBodyAuditConfig {
		return config.NormalizeRequestBodyAudit(config.RequestBodyAuditConfig{
			Enable:         true,
			KeywordsBase64: []string{base64.StdEncoding.EncodeToString([]byte{0x00, 0x01, 'x'})},
		})
	}))
	router.POST("/v1/responses", func(c *gin.Context) {
		c.String(http.StatusOK, "handler reached")
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader("\x00\x01x suffix"))
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestRequestBodyAuditMiddlewareSkipsNonAPIPaths(t *testing.T) {
	router := gin.New()
	router.Use(RequestBodyAuditMiddleware(func() config.RequestBodyAuditConfig {
		return config.NormalizeRequestBodyAudit(config.RequestBodyAuditConfig{
			Enable:   true,
			Keywords: []string{"blocked"},
		})
	}))
	router.POST("/v0/management/config", func(c *gin.Context) {
		data, _ := io.ReadAll(c.Request.Body)
		c.String(http.StatusOK, string(data))
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v0/management/config", strings.NewReader("blocked"))
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if rec.Body.String() != "blocked" {
		t.Fatalf("body = %q, want passthrough", rec.Body.String())
	}
}

func TestRequestBodyAuditMiddlewareRejectsOversize(t *testing.T) {
	router := gin.New()
	router.Use(RequestBodyAuditMiddleware(func() config.RequestBodyAuditConfig {
		return config.NormalizeRequestBodyAudit(config.RequestBodyAuditConfig{
			Enable:         true,
			Keywords:       []string{"blocked"},
			MaxBodyBytes:   3,
			RejectOversize: true,
		})
	}))
	router.POST("/v1/completions", func(c *gin.Context) {
		c.String(http.StatusOK, "handler reached")
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader("abcd"))
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestShouldAuditRequestBodySkipsRetiredAmpRoutes(t *testing.T) {
	for _, path := range []string{
		"/api/provider/openai/v1/messages",
		"/api/auth/token",
		"/auth/token",
	} {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader("blocked"))
		if shouldAuditRequestBody(req) {
			t.Fatalf("shouldAuditRequestBody(%q) = true, want false", path)
		}
	}
	apiReq := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader("blocked"))
	if !shouldAuditRequestBody(apiReq) {
		t.Fatal("shouldAuditRequestBody(/v1/responses) = false, want true")
	}
}
