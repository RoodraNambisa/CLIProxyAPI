package management

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestPutRequestRetry_ClampsNegativeValues(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)

	h := &Handler{
		cfg:            &config.Config{RequestRetry: 3},
		configFilePath: writeTestConfigFile(t),
	}

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPut, "/v0/management/request-retry", bytes.NewBufferString(`{"value":-7}`))
	c.Request.Header.Set("Content-Type", "application/json")

	h.PutRequestRetry(c)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if h.cfg.RequestRetry != 0 {
		t.Fatalf("request-retry = %d, want 0", h.cfg.RequestRetry)
	}
}

func TestPutMaxRetryInterval_ClampsNegativeValues(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)

	h := &Handler{
		cfg:            &config.Config{MaxRetryInterval: 60},
		configFilePath: writeTestConfigFile(t),
	}

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPut, "/v0/management/max-retry-interval", bytes.NewBufferString(`{"value":-15}`))
	c.Request.Header.Set("Content-Type", "application/json")

	h.PutMaxRetryInterval(c)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if h.cfg.MaxRetryInterval != 0 {
		t.Fatalf("max-retry-interval = %d, want 0", h.cfg.MaxRetryInterval)
	}
}
