package management

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestGetPprofProfile_Disabled(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := NewHandlerWithoutConfigFilePath(&config.Config{}, nil)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/pprof/profile/goroutine", nil)
	ctx.Params = gin.Params{{Key: "profile", Value: "goroutine"}}

	handler.GetPprofProfile(ctx)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
	if !strings.Contains(rec.Body.String(), "pprof is disabled") {
		t.Fatalf("body = %q, want disabled error", rec.Body.String())
	}
}

func TestGetPprofProfile_GoroutineText(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := NewHandlerWithoutConfigFilePath(&config.Config{Pprof: config.PprofConfig{Enable: true}}, nil)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/pprof/profile/goroutine?format=text", nil)
	ctx.Params = gin.Params{{Key: "profile", Value: "goroutine"}}

	handler.GetPprofProfile(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%q", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "goroutine") {
		t.Fatalf("body = %q, want goroutine dump", rec.Body.String())
	}
}

func TestGetPprofConfigReportsCapabilities(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := NewHandlerWithoutConfigFilePath(&config.Config{Pprof: config.PprofConfig{Enable: true, Addr: "127.0.0.1:8316"}}, nil)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/pprof/config", nil)

	handler.GetPprofConfig(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	for _, want := range []string{"\"enable\":true", "\"addr\":\"127.0.0.1:8316\"", "go_tool_available", "graphviz_available"} {
		if !strings.Contains(body, want) {
			t.Fatalf("body = %q, want %q", body, want)
		}
	}
}
