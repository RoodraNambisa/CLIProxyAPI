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

func TestPutRequestBodyRelease_NormalizesValues(t *testing.T) {
	t.Parallel()

	h := &Handler{
		cfg:            &config.Config{},
		configFilePath: writeTestConfigFile(t),
	}

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPatch, "/v0/management/request-body-release", bytes.NewBufferString(`{"value":{"enable":true,"log-only":true,"after-seconds":-7,"min-body-bytes":-1024}}`))
	c.Request.Header.Set("Content-Type", "application/json")

	h.PutRequestBodyRelease(c)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !h.cfg.RequestBodyRelease.Enable {
		t.Fatal("request-body-release.enable = false, want true")
	}
	if !h.cfg.RequestBodyRelease.LogOnly {
		t.Fatal("request-body-release.log-only = false, want true")
	}
	if h.cfg.RequestBodyRelease.AfterSeconds != 0 {
		t.Fatalf("after-seconds = %d, want 0", h.cfg.RequestBodyRelease.AfterSeconds)
	}
	if h.cfg.RequestBodyRelease.MinBodyBytes != 0 {
		t.Fatalf("min-body-bytes = %d, want 0", h.cfg.RequestBodyRelease.MinBodyBytes)
	}
}

func TestPutNonRetryableErrors_NormalizesValues(t *testing.T) {
	t.Parallel()

	h := &Handler{
		cfg:            &config.Config{},
		configFilePath: writeTestConfigFile(t),
	}

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPut, "/v0/management/non-retryable-errors", bytes.NewBufferString(`{"value":[{"status-code":400,"type":" Image_Generation_User_Error ","code":" INVALID_VALUE "},{"message-contains":" Safety System "},{"status-code":99,"type":"bad"}]}`))
	c.Request.Header.Set("Content-Type", "application/json")

	h.PutNonRetryableErrors(c)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if len(h.cfg.NonRetryableErrors) != 2 {
		t.Fatalf("non-retryable-errors = %+v, want 2 rules", h.cfg.NonRetryableErrors)
	}
	first := h.cfg.NonRetryableErrors[0]
	if first.StatusCode != http.StatusBadRequest || first.Type != "image_generation_user_error" || first.Code != "invalid_value" {
		t.Fatalf("first rule = %+v, want normalized image rule", first)
	}
	second := h.cfg.NonRetryableErrors[1]
	if second.MessageContains != "safety system" {
		t.Fatalf("second message-contains = %q, want safety system", second.MessageContains)
	}
}

func TestPutAuthModelExclusions_NormalizesValues(t *testing.T) {
	t.Parallel()

	h := &Handler{
		cfg:            &config.Config{},
		configFilePath: writeTestConfigFile(t),
	}

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPatch, "/v0/management/auth-model-exclusions", bytes.NewBufferString(`{"value":[{"models":[" gpt-image-2 ","GPT-IMAGE-2"],"priorities":[-1,-1]},{"models":["gpt-image-1.5"],"keyword-contains":[" Free ","free"],"providers":[" CoDeX "]},{"disable-image-generation":true,"priorities":[0,0],"keyword-contains":[" Trial "]},{"models":["gpt-5.5"]}]}`))
	c.Request.Header.Set("Content-Type", "application/json")

	h.PutAuthModelExclusions(c)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if len(h.cfg.AuthModelExclusions) != 3 {
		t.Fatalf("auth-model-exclusions = %+v, want 3 rules", h.cfg.AuthModelExclusions)
	}
	first := h.cfg.AuthModelExclusions[0]
	if len(first.Models) != 1 || first.Models[0] != "gpt-image-2" {
		t.Fatalf("first models = %#v, want gpt-image-2", first.Models)
	}
	if len(first.Priorities) != 1 || first.Priorities[0] != -1 {
		t.Fatalf("first priorities = %#v, want [-1]", first.Priorities)
	}
	second := h.cfg.AuthModelExclusions[1]
	if len(second.Providers) != 1 || second.Providers[0] != "codex" {
		t.Fatalf("second providers = %#v, want [codex]", second.Providers)
	}
	if len(second.KeywordContains) != 1 || second.KeywordContains[0] != "free" {
		t.Fatalf("second keywords = %#v, want [free]", second.KeywordContains)
	}
	third := h.cfg.AuthModelExclusions[2]
	if !third.DisableImageGeneration {
		t.Fatalf("third disable-image-generation = false, want true")
	}
	if len(third.Models) != 0 {
		t.Fatalf("third models = %#v, want empty", third.Models)
	}
	if len(third.Priorities) != 1 || third.Priorities[0] != 0 {
		t.Fatalf("third priorities = %#v, want [0]", third.Priorities)
	}
	if len(third.KeywordContains) != 1 || third.KeywordContains[0] != "trial" {
		t.Fatalf("third keywords = %#v, want [trial]", third.KeywordContains)
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
