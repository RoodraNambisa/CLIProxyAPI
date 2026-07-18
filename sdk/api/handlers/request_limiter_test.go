package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

type requestLimitHandlerExecutor struct{}

func (requestLimitHandlerExecutor) Identifier() string { return "request-limit-test" }

func (requestLimitHandlerExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{Payload: []byte(`{"ok":true}`)}, nil
}

func (requestLimitHandlerExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	return nil, nil
}

func (requestLimitHandlerExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (requestLimitHandlerExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, nil
}

func (requestLimitHandlerExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

func TestBaseAPIHandlerWritesPerAuthRequestLimitContract(t *testing.T) {
	const (
		provider = "request-limit-test"
		model    = "request-limit-model"
		authID   = "request-limit-auth"
	)
	manager := coreauth.NewManager(nil, &coreauth.FillFirstSelector{}, nil)
	manager.RegisterExecutor(requestLimitHandlerExecutor{})
	manager.SetConfig(&config.Config{Routing: config.RoutingConfig{
		PerAuthRequestLimit:         1,
		PerAuthRequestWindowMinutes: 5,
	}})
	registry.GetGlobalRegistry().RegisterClient(authID, provider, []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })
	if _, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{ID: authID, Provider: provider}); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	base := NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	if _, _, errFirst := base.ExecuteWithProviders(t.Context(), []string{provider}, "openai-response", model, []byte(`{"model":"request-limit-model"}`), ""); errFirst != nil {
		t.Fatalf("first request error = %v", errFirst)
	}
	before, ok := manager.GetByID(authID)
	if !ok || before == nil {
		t.Fatal("auth missing before request limit")
	}
	_, _, errSecond := base.ExecuteWithProviders(t.Context(), []string{provider}, "openai-response", model, []byte(`{"model":"request-limit-model"}`), "")
	if errSecond == nil {
		t.Fatal("second request error = nil, want request limit")
	}

	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	base.WriteErrorResponse(ctx, errSecond)
	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", recorder.Code)
	}
	if contentType := recorder.Header().Get("Content-Type"); contentType != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", contentType)
	}
	retryAfter, errRetryAfter := strconv.ParseInt(recorder.Header().Get("Retry-After"), 10, 64)
	if errRetryAfter != nil || retryAfter < 1 || retryAfter > 5*60 {
		t.Fatalf("Retry-After = %q (%v), want 1..300 seconds", recorder.Header().Get("Retry-After"), errRetryAfter)
	}
	var payload struct {
		Error struct {
			Code          string `json:"code"`
			Message       string `json:"message"`
			Limit         int    `json:"limit"`
			WindowMinutes int    `json:"window_minutes"`
			ResetSeconds  int64  `json:"reset_seconds"`
		} `json:"error"`
	}
	if errJSON := json.Unmarshal(recorder.Body.Bytes(), &payload); errJSON != nil {
		t.Fatalf("decode response: %v", errJSON)
	}
	if payload.Error.Code != "auth_request_limited" ||
		payload.Error.Message != "All available credentials reached their request limit" ||
		payload.Error.Limit != 1 || payload.Error.WindowMinutes != 5 ||
		payload.Error.ResetSeconds != retryAfter {
		t.Fatalf("response error = %#v", payload.Error)
	}
	current, ok := manager.GetByID(authID)
	if !ok || current == nil {
		t.Fatal("auth missing after request limit")
	}
	if current.Status != before.Status || current.StatusMessage != before.StatusMessage ||
		current.Unavailable != before.Unavailable || current.NextRetryAfter != before.NextRetryAfter ||
		current.CooldownScope != before.CooldownScope || !reflect.DeepEqual(current.LastError, before.LastError) ||
		!reflect.DeepEqual(current.Quota, before.Quota) || !reflect.DeepEqual(current.ModelStates, before.ModelStates) {
		t.Fatalf("auth cooldown state changed: before=%+v after=%+v", before, current)
	}
}
