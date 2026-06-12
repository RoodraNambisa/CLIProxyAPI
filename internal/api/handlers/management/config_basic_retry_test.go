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

func TestPutRoutingPriorityOverrides_NormalizesValues(t *testing.T) {
	t.Parallel()

	h := &Handler{
		cfg:            &config.Config{},
		configFilePath: writeTestConfigFile(t),
	}

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPut, "/v0/management/routing/priority-overrides", bytes.NewBufferString(`{"value":[{"priority":0,"strategy":"ff","max-retry-credentials":2},{"priority":-1,"max-retry-credentials":-3}]}`))
	c.Request.Header.Set("Content-Type", "application/json")

	h.PutRoutingPriorityOverrides(c)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if len(h.cfg.Routing.PriorityOverrides) != 2 {
		t.Fatalf("PriorityOverrides length = %d, want 2", len(h.cfg.Routing.PriorityOverrides))
	}
	first := h.cfg.Routing.PriorityOverrides[0]
	if first.Strategy != "fill-first" || first.MaxRetryCredentials == nil || *first.MaxRetryCredentials != 2 {
		t.Fatalf("first override = %+v, want fill-first limit 2", first)
	}
	second := h.cfg.Routing.PriorityOverrides[1]
	if second.MaxRetryCredentials == nil || *second.MaxRetryCredentials != 0 {
		t.Fatalf("second override = %+v, want limit 0", second)
	}
}

func TestPutRoutingPriorityOverrides_RejectsDuplicatePriority(t *testing.T) {
	t.Parallel()

	h := &Handler{
		cfg:            &config.Config{},
		configFilePath: writeTestConfigFile(t),
	}

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPut, "/v0/management/routing/priority-overrides", bytes.NewBufferString(`{"value":[{"priority":0},{"priority":0}]}`))
	c.Request.Header.Set("Content-Type", "application/json")

	h.PutRoutingPriorityOverrides(c)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if len(h.cfg.Routing.PriorityOverrides) != 0 {
		t.Fatalf("PriorityOverrides = %+v, want unchanged empty", h.cfg.Routing.PriorityOverrides)
	}
}

func TestPutRequestBodyAudit_NormalizesAndPersists(t *testing.T) {
	t.Parallel()

	h := &Handler{
		cfg:            &config.Config{},
		configFilePath: writeTestConfigFile(t),
	}

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPut, "/v0/management/request-body-audit", bytes.NewBufferString(`{"value":{"enable":true,"keywords":[" blocked ",""],"keywords-base64":["@@bad@@"],"error":{"status-code":451,"message":"blocked","type":"policy_error","code":"policy_blocked"}}}`))
	c.Request.Header.Set("Content-Type", "application/json")

	h.PutRequestBodyAudit(c)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !h.cfg.RequestBodyAudit.Enable {
		t.Fatal("request-body-audit.enable = false, want true")
	}
	if len(h.cfg.RequestBodyAudit.Keywords) != 1 || h.cfg.RequestBodyAudit.Keywords[0] != "blocked" {
		t.Fatalf("keywords = %#v, want normalized keyword", h.cfg.RequestBodyAudit.Keywords)
	}
	if len(h.cfg.RequestBodyAudit.KeywordsBase64) != 0 {
		t.Fatalf("keywords-base64 = %#v, want invalid entries dropped", h.cfg.RequestBodyAudit.KeywordsBase64)
	}
	if h.cfg.RequestBodyAudit.Error.StatusCode != http.StatusUnavailableForLegalReasons {
		t.Fatalf("status-code = %d, want %d", h.cfg.RequestBodyAudit.Error.StatusCode, http.StatusUnavailableForLegalReasons)
	}
}
