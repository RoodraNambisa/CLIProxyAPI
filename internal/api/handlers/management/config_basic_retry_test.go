package management

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func init() {
	gin.SetMode(gin.TestMode)
}

func TestPutRequestRetry_ClampsNegativeValues(t *testing.T) {
	t.Parallel()

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

func TestPutMaxRetryCredentials_ClampsNegativeValues(t *testing.T) {
	t.Parallel()

	h := &Handler{
		cfg:            &config.Config{MaxRetryCredentials: 3},
		configFilePath: writeTestConfigFile(t),
	}

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPut, "/v0/management/max-retry-credentials", bytes.NewBufferString(`{"value":-4}`))
	c.Request.Header.Set("Content-Type", "application/json")

	h.PutMaxRetryCredentials(c)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if h.cfg.MaxRetryCredentials != 0 {
		t.Fatalf("max-retry-credentials = %d, want 0", h.cfg.MaxRetryCredentials)
	}
}
