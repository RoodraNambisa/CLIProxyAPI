package management

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
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

func TestPutRequestRetryPreservesIgnoredAmpConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	original := `request-retry: 1
ampcode:
  upstream-url: https://nested.example
  upstream-api-key: nested-sentinel
amp-upstream-api-key: flat-sentinel
`
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	h := &Handler{cfg: &config.Config{RequestRetry: 1}, configFilePath: path}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPut, "/v0/management/request-retry", bytes.NewBufferString(`{"value":2}`))
	c.Request.Header.Set("Content-Type", "application/json")
	h.PutRequestRetry(c)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	saved := string(data)
	for _, value := range []string{"nested-sentinel", "flat-sentinel", "request-retry: 2"} {
		if !strings.Contains(saved, value) {
			t.Fatalf("saved config missing %q:\n%s", value, saved)
		}
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
	c.Request = httptest.NewRequest(http.MethodPut, "/v0/management/routing/priority-overrides", bytes.NewBufferString(`{"value":[{"priority":0,"strategy":"ff","max-retry-credentials":2,"fill-first-range":5,"fill-first-per-auth-rpm":0,"per-auth-request-limit":120,"per-auth-request-window-minutes":5,"subscription-overrides":[{"providers":["Codex"],"plan-types":["Pro"],"per-auth-request-limit":180}]},{"priority":-1,"max-retry-credentials":-3,"fill-first-range":0,"fill-first-per-auth-rpm":-3,"per-auth-request-limit":-3,"per-auth-request-window-minutes":0}]}`))
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
	if first.FillFirstRange == nil || *first.FillFirstRange != 5 {
		t.Fatalf("first FillFirstRange = %v, want 5", first.FillFirstRange)
	}
	if first.FillFirstPerAuthRPM == nil || *first.FillFirstPerAuthRPM != 0 {
		t.Fatalf("first FillFirstPerAuthRPM = %v, want 0", first.FillFirstPerAuthRPM)
	}
	if first.PerAuthRequestLimit == nil || *first.PerAuthRequestLimit != 120 {
		t.Fatalf("first PerAuthRequestLimit = %v, want 120", first.PerAuthRequestLimit)
	}
	if first.PerAuthRequestWindowMinutes == nil || *first.PerAuthRequestWindowMinutes != 5 {
		t.Fatalf("first PerAuthRequestWindowMinutes = %v, want 5", first.PerAuthRequestWindowMinutes)
	}
	if len(first.SubscriptionOverrides) != 1 {
		t.Fatalf("first SubscriptionOverrides = %+v, want one rule", first.SubscriptionOverrides)
	}
	subscription := first.SubscriptionOverrides[0]
	if len(subscription.Providers) != 1 || subscription.Providers[0] != "codex" ||
		len(subscription.PlanTypes) != 1 || subscription.PlanTypes[0] != "pro" ||
		subscription.PerAuthRequestLimit == nil || *subscription.PerAuthRequestLimit != 180 {
		t.Fatalf("normalized subscription override = %+v", subscription)
	}
	second := h.cfg.Routing.PriorityOverrides[1]
	if second.MaxRetryCredentials == nil || *second.MaxRetryCredentials != 0 {
		t.Fatalf("second override = %+v, want limit 0", second)
	}
	if second.FillFirstRange == nil || *second.FillFirstRange != 1 {
		t.Fatalf("second FillFirstRange = %v, want 1", second.FillFirstRange)
	}
	if second.FillFirstPerAuthRPM == nil || *second.FillFirstPerAuthRPM != 0 {
		t.Fatalf("second FillFirstPerAuthRPM = %v, want 0", second.FillFirstPerAuthRPM)
	}
	if second.PerAuthRequestLimit == nil || *second.PerAuthRequestLimit != 0 {
		t.Fatalf("second PerAuthRequestLimit = %v, want 0", second.PerAuthRequestLimit)
	}
	if second.PerAuthRequestWindowMinutes == nil || *second.PerAuthRequestWindowMinutes != 1 {
		t.Fatalf("second PerAuthRequestWindowMinutes = %v, want 1", second.PerAuthRequestWindowMinutes)
	}
}

func TestPutRoutingPriorityOverrides_RejectsOverlappingSubscriptionRules(t *testing.T) {
	t.Parallel()

	h := &Handler{
		cfg:            &config.Config{},
		configFilePath: writeTestConfigFile(t),
	}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPut, "/v0/management/routing/priority-overrides", bytes.NewBufferString(`{"value":[{"priority":0,"subscription-overrides":[{"plan-types":["pro"],"per-auth-request-limit":10},{"providers":["codex"],"plan-types":["pro"],"per-auth-request-limit":20}]}]}`))
	c.Request.Header.Set("Content-Type", "application/json")

	h.PutRoutingPriorityOverrides(c)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestRoutingPerAuthRequestLimitHandlers_NormalizeValues(t *testing.T) {
	t.Parallel()

	h := &Handler{
		cfg:            &config.Config{},
		configFilePath: writeTestConfigFile(t),
	}

	limitRec := httptest.NewRecorder()
	limitCtx, _ := gin.CreateTestContext(limitRec)
	limitCtx.Request = httptest.NewRequest(http.MethodPatch, "/v0/management/routing/per-auth-request-limit", bytes.NewBufferString(`{"value":-1}`))
	limitCtx.Request.Header.Set("Content-Type", "application/json")
	h.PutRoutingPerAuthRequestLimit(limitCtx)
	if limitRec.Code != http.StatusOK || h.cfg.Routing.PerAuthRequestLimit != 0 {
		t.Fatalf("limit update status=%d value=%d body=%s", limitRec.Code, h.cfg.Routing.PerAuthRequestLimit, limitRec.Body.String())
	}

	windowRec := httptest.NewRecorder()
	windowCtx, _ := gin.CreateTestContext(windowRec)
	windowCtx.Request = httptest.NewRequest(http.MethodPatch, "/v0/management/routing/per-auth-request-window-minutes", bytes.NewBufferString(`{"value":0}`))
	windowCtx.Request.Header.Set("Content-Type", "application/json")
	h.PutRoutingPerAuthRequestWindowMinutes(windowCtx)
	if windowRec.Code != http.StatusOK || h.cfg.Routing.PerAuthRequestWindowMinutes != 1 {
		t.Fatalf("window update status=%d value=%d body=%s", windowRec.Code, h.cfg.Routing.PerAuthRequestWindowMinutes, windowRec.Body.String())
	}

	getRec := httptest.NewRecorder()
	getCtx, _ := gin.CreateTestContext(getRec)
	h.GetRoutingPerAuthRequestWindowMinutes(getCtx)
	var body map[string]int
	if errDecode := json.Unmarshal(getRec.Body.Bytes(), &body); errDecode != nil {
		t.Fatalf("decode response: %v", errDecode)
	}
	if body["per-auth-request-window-minutes"] != 1 {
		t.Fatalf("window response = %d, want 1", body["per-auth-request-window-minutes"])
	}

	maxInt := int(^uint(0) >> 1)
	maxWindowRec := httptest.NewRecorder()
	maxWindowCtx, _ := gin.CreateTestContext(maxWindowRec)
	maxWindowCtx.Request = httptest.NewRequest(http.MethodPut, "/v0/management/routing/per-auth-request-window-minutes", bytes.NewBufferString(`{"value":`+strconv.Itoa(maxInt)+`}`))
	maxWindowCtx.Request.Header.Set("Content-Type", "application/json")
	h.PutRoutingPerAuthRequestWindowMinutes(maxWindowCtx)
	if maxWindowRec.Code != http.StatusOK || h.cfg.Routing.PerAuthRequestWindowMinutes != config.NormalizePerAuthRequestWindowMinutes(maxInt) {
		t.Fatalf("maximum window update status=%d value=%d body=%s", maxWindowRec.Code, h.cfg.Routing.PerAuthRequestWindowMinutes, maxWindowRec.Body.String())
	}
}

func TestRoutingPerAuthRequestLimitHandlers_PersistExplicitZero(t *testing.T) {
	t.Parallel()

	configPath := writeTestConfigFile(t)
	if errWrite := os.WriteFile(configPath, []byte("routing:\n  per-auth-request-limit: 60\n"), 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}
	h := &Handler{
		cfg: &config.Config{Routing: config.RoutingConfig{
			PerAuthRequestLimit:         60,
			PerAuthRequestWindowMinutes: 1,
		}},
		configFilePath: configPath,
	}

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPatch, "/v0/management/routing/per-auth-request-limit", bytes.NewBufferString(`{"value":0}`))
	c.Request.Header.Set("Content-Type", "application/json")
	h.PutRoutingPerAuthRequestLimit(c)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	loaded, errLoad := config.LoadConfig(configPath)
	if errLoad != nil {
		t.Fatalf("load persisted config: %v", errLoad)
	}
	if loaded.Routing.PerAuthRequestLimit != 0 {
		t.Fatalf("persisted limit = %d, want 0", loaded.Routing.PerAuthRequestLimit)
	}
}

func TestPutRoutingPriorityOverrides_PersistsExplicitZeroLimit(t *testing.T) {
	t.Parallel()

	configPath := writeTestConfigFile(t)
	if errWrite := os.WriteFile(configPath, []byte("routing:\n  per-auth-request-limit: 60\n"), 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}
	h := &Handler{
		cfg: &config.Config{Routing: config.RoutingConfig{
			PerAuthRequestLimit:         60,
			PerAuthRequestWindowMinutes: 1,
		}},
		configFilePath: configPath,
	}

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPatch, "/v0/management/routing/priority-overrides", bytes.NewBufferString(`{"value":[{"priority":0,"per-auth-request-limit":0}]}`))
	c.Request.Header.Set("Content-Type", "application/json")
	h.PutRoutingPriorityOverrides(c)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	loaded, errLoad := config.LoadConfig(configPath)
	if errLoad != nil {
		t.Fatalf("load persisted config: %v", errLoad)
	}
	if len(loaded.Routing.PriorityOverrides) != 1 || loaded.Routing.PriorityOverrides[0].PerAuthRequestLimit == nil || *loaded.Routing.PriorityOverrides[0].PerAuthRequestLimit != 0 {
		t.Fatalf("persisted overrides = %+v, want explicit zero limit", loaded.Routing.PriorityOverrides)
	}
}

func TestRoutingManagementUpdatesRequestLimiterImmediately(t *testing.T) {
	tests := []struct {
		name          string
		prepareUpdate func(*config.Config)
		request       func() *http.Request
		update        func(*Handler, *gin.Context)
	}{
		{
			name: "priority override endpoint",
			request: func() *http.Request {
				request := httptest.NewRequest(http.MethodPatch, "/v0/management/routing/priority-overrides", strings.NewReader(`{"value":[{"priority":4,"per-auth-request-limit":100,"per-auth-request-window-minutes":1}]}`))
				request.Header.Set("Content-Type", "application/json")
				return request
			},
			update: func(handler *Handler, requestContext *gin.Context) {
				handler.PutRoutingPriorityOverrides(requestContext)
			},
		},
		{
			name: "full YAML endpoint",
			request: func() *http.Request {
				request := httptest.NewRequest(http.MethodPut, "/v0/management/config.yaml", strings.NewReader("routing:\n  per-auth-request-limit: 0\n  per-auth-request-window-minutes: 1\n"))
				request.Header.Set("Content-Type", "application/yaml")
				return request
			},
			update: func(handler *Handler, requestContext *gin.Context) {
				handler.PutConfigYAML(requestContext)
			},
		},
		{
			name: "unrelated field endpoint",
			prepareUpdate: func(cfg *config.Config) {
				limit := 100
				window := 1
				cfg.Routing.PriorityOverrides = []config.RoutingPriorityOverride{{
					Priority:                    4,
					PerAuthRequestLimit:         &limit,
					PerAuthRequestWindowMinutes: &window,
				}}
			},
			request: func() *http.Request {
				request := httptest.NewRequest(http.MethodPut, "/v0/management/debug", strings.NewReader(`{"value":true}`))
				request.Header.Set("Content-Type", "application/json")
				return request
			},
			update: func(handler *Handler, requestContext *gin.Context) {
				handler.PutDebug(requestContext)
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			authID := "routing-runtime-sync-" + strings.NewReplacer("/", "-", " ", "-").Replace(t.Name())
			model := authID + "-model"
			cfg := &config.Config{Routing: config.RoutingConfig{
				PerAuthRequestLimit:         1,
				PerAuthRequestWindowMinutes: 1,
			}}
			manager := coreauth.NewManager(nil, nil, nil)
			manager.RegisterExecutor(&chatGPTWebManagementTestExecutor{})
			manager.SetConfig(cfg)
			registry.GetGlobalRegistry().RegisterClient(authID, "chatgpt-web", []*registry.ModelInfo{{ID: model}})
			t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })
			if _, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{
				ID:         authID,
				Provider:   "chatgpt-web",
				Status:     coreauth.StatusActive,
				Metadata:   map[string]any{"access_token": "test-token"},
				Attributes: map[string]string{"priority": "4"},
			}); errRegister != nil {
				t.Fatalf("register auth: %v", errRegister)
			}

			request := cliproxyexecutor.Request{Model: model}
			options := cliproxyexecutor.Options{}
			if _, errExecute := manager.Execute(t.Context(), []string{"chatgpt-web"}, request, options); errExecute != nil {
				t.Fatalf("first execution: %v", errExecute)
			}
			if _, errExecute := manager.Execute(t.Context(), []string{"chatgpt-web"}, request, options); errExecute == nil || !strings.Contains(errExecute.Error(), "auth_request_limited") {
				t.Fatalf("second execution error = %v, want auth_request_limited", errExecute)
			}
			if testCase.prepareUpdate != nil {
				testCase.prepareUpdate(cfg)
			}

			configPath := writeTestConfigFile(t)
			handler := &Handler{cfg: cfg, configFilePath: configPath, authManager: manager}
			recorder := httptest.NewRecorder()
			requestContext, _ := gin.CreateTestContext(recorder)
			requestContext.Request = testCase.request()
			testCase.update(handler, requestContext)
			if recorder.Code != http.StatusOK {
				t.Fatalf("update status = %d, body = %s", recorder.Code, recorder.Body.String())
			}

			if _, errExecute := manager.Execute(t.Context(), []string{"chatgpt-web"}, request, options); errExecute != nil {
				t.Fatalf("execution after management update = %v, want immediate success", errExecute)
			}
		})
	}
}

func TestManagementRuntimeApplyFailureRollsBackFileAndRuntimeOutsideHandlerLock(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if errWrite := os.WriteFile(configPath, []byte("request-retry: 1\n"), 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}
	handler := NewHandler(&config.Config{RequestRetry: 1}, configPath, nil)
	var applied []int
	handler.SetRuntimeConfigApplier(func(_ context.Context, candidate *config.Config) (config.RuntimeApplyResult, error) {
		applied = append(applied, candidate.RequestRetry)
		if candidate.RequestRetry == 9 {
			return config.RuntimeApplyResult{}, errors.New("synthetic runtime failure")
		}
		if errSet := handler.SetConfig(candidate); errSet != nil {
			return config.RuntimeApplyResult{}, errSet
		}
		return config.RuntimeApplyResult{Applied: true}, nil
	})

	recorder := httptest.NewRecorder()
	requestContext, _ := gin.CreateTestContext(recorder)
	requestContext.Request = httptest.NewRequest(http.MethodPut, "/v0/management/request-retry", strings.NewReader(`{"value":9}`))
	requestContext.Request.Header.Set("Content-Type", "application/json")
	handler.PutRequestRetry(requestContext)
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body=%s", recorder.Code, recorder.Body.String())
	}
	if len(applied) != 2 || strings.Join([]string{strconv.Itoa(applied[0]), strconv.Itoa(applied[1])}, ",") != "9,1" {
		t.Fatalf("runtime apply sequence = %#v, want [9 1]", applied)
	}
	loaded, errLoad := config.LoadConfig(configPath)
	if errLoad != nil {
		t.Fatalf("load rolled back config: %v", errLoad)
	}
	if loaded.RequestRetry != 1 || handler.cfg.RequestRetry != 1 {
		t.Fatalf("rollback state: file=%d handler=%d", loaded.RequestRetry, handler.cfg.RequestRetry)
	}
}

func TestConfigMutationMiddlewareSerializesRuntimeTransactions(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if errWrite := os.WriteFile(configPath, []byte("request-retry: 1\n"), 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}
	handler := NewHandler(&config.Config{RequestRetry: 1}, configPath, nil)
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	applyValues := make(chan int, 2)
	handler.SetRuntimeConfigApplier(func(_ context.Context, candidate *config.Config) (config.RuntimeApplyResult, error) {
		applyValues <- candidate.RequestRetry
		if candidate.RequestRetry == 2 {
			close(firstEntered)
			<-releaseFirst
		}
		if errSet := handler.SetConfig(candidate); errSet != nil {
			return config.RuntimeApplyResult{}, errSet
		}
		return config.RuntimeApplyResult{Applied: true}, nil
	})

	router := gin.New()
	router.Use(handler.ConfigMutationMiddleware())
	router.PUT("/v0/management/request-retry", handler.PutRequestRetry)
	type response struct {
		code int
		body string
	}
	request := func(value int, result chan<- response) {
		recorder := httptest.NewRecorder()
		body := strings.NewReader(fmt.Sprintf(`{"value":%d}`, value))
		httpRequest := httptest.NewRequest(http.MethodPut, "/v0/management/request-retry", body)
		httpRequest.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(recorder, httpRequest)
		result <- response{code: recorder.Code, body: recorder.Body.String()}
	}
	results := make(chan response, 2)
	go request(2, results)
	select {
	case <-firstEntered:
	case <-time.After(time.Second):
		t.Fatal("first runtime transaction did not start")
	}
	if firstValue := <-applyValues; firstValue != 2 {
		t.Fatalf("first runtime apply = %d, want 2", firstValue)
	}
	go request(3, results)
	select {
	case value := <-applyValues:
		t.Fatalf("second runtime transaction started before first completed: %d", value)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseFirst)
	for range 2 {
		result := <-results
		if result.code != http.StatusOK {
			t.Fatalf("config update status = %d, body=%s", result.code, result.body)
		}
	}
	if secondValue := <-applyValues; secondValue != 3 {
		t.Fatalf("second runtime apply = %d, want 3", secondValue)
	}
	loaded, errLoad := config.LoadConfig(configPath)
	if errLoad != nil {
		t.Fatalf("load config: %v", errLoad)
	}
	if loaded.RequestRetry != 3 || handler.cfg.RequestRetry != 3 {
		t.Fatalf("final config: file=%d handler=%d, want 3", loaded.RequestRetry, handler.cfg.RequestRetry)
	}
}

func TestPutRoutingPriorityOverrides_PersistsSubscriptionExplicitZeroLimit(t *testing.T) {
	t.Parallel()

	configPath := writeTestConfigFile(t)
	if errWrite := os.WriteFile(configPath, []byte("routing:\n  per-auth-request-limit: 60\n"), 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}
	h := &Handler{
		cfg: &config.Config{Routing: config.RoutingConfig{
			PerAuthRequestLimit:         60,
			PerAuthRequestWindowMinutes: 1,
		}},
		configFilePath: configPath,
	}

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPatch, "/v0/management/routing/priority-overrides", bytes.NewBufferString(`{"value":[{"priority":0,"subscription-overrides":[{"providers":["codex"],"plan-types":["plus"],"per-auth-request-limit":0}]}]}`))
	c.Request.Header.Set("Content-Type", "application/json")
	h.PutRoutingPriorityOverrides(c)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	loaded, errLoad := config.LoadConfig(configPath)
	if errLoad != nil {
		t.Fatalf("load persisted config: %v", errLoad)
	}
	if len(loaded.Routing.PriorityOverrides) != 1 ||
		len(loaded.Routing.PriorityOverrides[0].SubscriptionOverrides) != 1 ||
		loaded.Routing.PriorityOverrides[0].SubscriptionOverrides[0].PerAuthRequestLimit == nil ||
		*loaded.Routing.PriorityOverrides[0].SubscriptionOverrides[0].PerAuthRequestLimit != 0 {
		t.Fatalf("persisted overrides = %+v, want nested explicit zero limit", loaded.Routing.PriorityOverrides)
	}
}

func TestRoutingFillFirstRangeHandlers_NormalizeValues(t *testing.T) {
	t.Parallel()

	h := &Handler{
		cfg:            &config.Config{},
		configFilePath: writeTestConfigFile(t),
	}

	putRec := httptest.NewRecorder()
	putCtx, _ := gin.CreateTestContext(putRec)
	putCtx.Request = httptest.NewRequest(http.MethodPatch, "/v0/management/routing/fill-first-range", bytes.NewBufferString(`{"value":0}`))
	putCtx.Request.Header.Set("Content-Type", "application/json")

	h.PutRoutingFillFirstRange(putCtx)

	if putRec.Code != http.StatusOK {
		t.Fatalf("PUT status = %d, want %d; body=%s", putRec.Code, http.StatusOK, putRec.Body.String())
	}
	if h.cfg.Routing.FillFirstRange != 1 {
		t.Fatalf("FillFirstRange = %d, want 1", h.cfg.Routing.FillFirstRange)
	}

	getRec := httptest.NewRecorder()
	getCtx, _ := gin.CreateTestContext(getRec)
	getCtx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/routing/fill-first-range", nil)

	h.GetRoutingFillFirstRange(getCtx)

	if getRec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want %d; body=%s", getRec.Code, http.StatusOK, getRec.Body.String())
	}
	var body map[string]int
	if errDecode := json.Unmarshal(getRec.Body.Bytes(), &body); errDecode != nil {
		t.Fatalf("decode response: %v", errDecode)
	}
	if body["fill-first-range"] != 1 {
		t.Fatalf("fill-first-range response = %d, want 1", body["fill-first-range"])
	}
}

func TestRoutingFillFirstPerAuthRPMHandlers_NormalizeValues(t *testing.T) {
	t.Parallel()

	h := &Handler{
		cfg:            &config.Config{},
		configFilePath: writeTestConfigFile(t),
	}

	putRec := httptest.NewRecorder()
	putCtx, _ := gin.CreateTestContext(putRec)
	putCtx.Request = httptest.NewRequest(http.MethodPatch, "/v0/management/routing/fill-first-per-auth-rpm", bytes.NewBufferString(`{"value":-1}`))
	putCtx.Request.Header.Set("Content-Type", "application/json")

	h.PutRoutingFillFirstPerAuthRPM(putCtx)

	if putRec.Code != http.StatusOK {
		t.Fatalf("PUT status = %d, want %d; body=%s", putRec.Code, http.StatusOK, putRec.Body.String())
	}
	if h.cfg.Routing.FillFirstPerAuthRPM != 0 {
		t.Fatalf("FillFirstPerAuthRPM = %d, want 0", h.cfg.Routing.FillFirstPerAuthRPM)
	}

	getRec := httptest.NewRecorder()
	getCtx, _ := gin.CreateTestContext(getRec)
	getCtx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/routing/fill-first-per-auth-rpm", nil)

	h.GetRoutingFillFirstPerAuthRPM(getCtx)

	if getRec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want %d; body=%s", getRec.Code, http.StatusOK, getRec.Body.String())
	}
	var body map[string]int
	if errDecode := json.Unmarshal(getRec.Body.Bytes(), &body); errDecode != nil {
		t.Fatalf("decode response: %v", errDecode)
	}
	if body["fill-first-per-auth-rpm"] != 0 {
		t.Fatalf("fill-first-per-auth-rpm response = %d, want 0", body["fill-first-per-auth-rpm"])
	}
}

func TestRoutingFillFirstPerAuthRPMHandlers_RejectConflict(t *testing.T) {
	t.Parallel()

	h := &Handler{
		cfg: &config.Config{
			Routing: config.RoutingConfig{
				Strategy:       "fill-first",
				FillFirstRange: 2,
			},
		},
		configFilePath: writeTestConfigFile(t),
	}

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPatch, "/v0/management/routing/fill-first-per-auth-rpm", bytes.NewBufferString(`{"value":60}`))
	c.Request.Header.Set("Content-Type", "application/json")

	h.PutRoutingFillFirstPerAuthRPM(c)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if h.cfg.Routing.FillFirstPerAuthRPM != 0 {
		t.Fatalf("FillFirstPerAuthRPM = %d, want unchanged 0", h.cfg.Routing.FillFirstPerAuthRPM)
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
