package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestCaptureRequestInfoOwnsHeaderValues(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Request.Header = http.Header{"X-Fixture": {"first", "second"}}
	info, err := captureRequestInfo(c, false, config.RequestBodyReleaseConfig{})
	if err != nil {
		t.Fatal(err)
	}
	c.Request.Header["X-Fixture"][0] = "mutated by later middleware"
	delete(c.Request.Header, "X-Fixture")
	if values := info.Headers["X-Fixture"]; len(values) != 2 || values[0] != "first" || values[1] != "second" {
		t.Fatal("request logging snapshot retained mutable caller header values")
	}
	c.Request.Header = nil
	info, err = captureRequestInfo(c, false, config.RequestBodyReleaseConfig{})
	if err != nil || info.Headers == nil || len(info.Headers) != 0 {
		t.Fatal("absent request headers did not produce the existing empty map")
	}
}
