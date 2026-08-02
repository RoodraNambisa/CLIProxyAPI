package executor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	fhttp "github.com/bogdanfinn/fhttp"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	openaihandlers "github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers/openai"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

type chatGPTWebHandlerRateLimitExecutor struct {
	err error
}

func (*chatGPTWebHandlerRateLimitExecutor) Identifier() string {
	return "chatgpt-web"
}

func (e *chatGPTWebHandlerRateLimitExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, e.err
}

func (e *chatGPTWebHandlerRateLimitExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	chunks := make(chan coreexecutor.StreamChunk, 2)
	chunks <- coreexecutor.BootstrapCommitStreamChunk()
	chunks <- coreexecutor.StreamChunk{Err: e.err}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func (*chatGPTWebHandlerRateLimitExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (*chatGPTWebHandlerRateLimitExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (*chatGPTWebHandlerRateLimitExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func newChatGPTWebImageHandlerManager(t *testing.T, err error, models ...string) (*coreauth.Manager, string) {
	t.Helper()
	manager := coreauth.NewManager(nil, &coreauth.FillFirstSelector{}, nil)
	manager.SetRetryConfig(0, 0, 0)
	manager.RegisterExecutor(&chatGPTWebHandlerRateLimitExecutor{err: err})
	authID := "chatgpt-web-image-handler-" + uuid.NewString()
	if _, errRegister := manager.Register(t.Context(), &coreauth.Auth{
		ID:       authID,
		Provider: "chatgpt-web",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"access_token":    "test-token",
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	}); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	modelInfos := make([]*registry.ModelInfo, 0, len(models))
	for _, model := range models {
		modelInfos = append(modelInfos, &registry.ModelInfo{ID: model})
	}
	registry.GetGlobalRegistry().RegisterClient(authID, "chatgpt-web", modelInfos)
	manager.RefreshSchedulerEntry(authID)
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(authID)
	})
	return manager, authID
}

func newCommittedChatGPTWebImageHandlerError(body string) error {
	upstream := newChatGPTWebStatusError(
		http.StatusTooManyRequests,
		"/backend-api/f/conversation",
		[]byte(body),
		fhttp.Header{"Retry-After": {"17"}},
	)
	return chatGPTWebCommittedRequestError(context.Background(), chatGPTWebImageRequestError(upstream))
}

func assertChatGPTWebImageHandlerRateLimit(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429; body=%s", response.Code, response.Body.String())
	}
	if retryAfter := response.Header().Get("Retry-After"); retryAfter != "17" {
		t.Fatalf("Retry-After = %q, want 17; body=%s", retryAfter, response.Body.String())
	}
}

func assertChatGPTWebImageHandlerOpenAIRateLimit(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	assertChatGPTWebImageHandlerRateLimit(t, response)
	var payload struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    string `json:"code"`
		} `json:"error"`
	}
	if errDecode := json.Unmarshal(response.Body.Bytes(), &payload); errDecode != nil {
		t.Fatalf("decode error response: %v; body=%s", errDecode, response.Body.String())
	}
	if payload.Error.Message != "Rate limit reached for requests. Please try again later." ||
		payload.Error.Type != "rate_limit_error" ||
		payload.Error.Code != "rate_limit_exceeded" {
		t.Fatalf("error response = %+v; body=%s", payload.Error, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "You've hit your limit") {
		t.Fatalf("error response exposed upstream text: %s", response.Body.String())
	}
}

func TestChatGPTWebImageErrorsReachHTTPHandlersAsRateLimits(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("direct image", func(t *testing.T) {
		errImage := newCommittedChatGPTWebImageHandlerError(`{"error":{"code":"image_generation_limit_reached"}}`)
		manager, _ := newChatGPTWebImageHandlerManager(t, errImage, "gpt-image-2", "gpt-5.4-mini")
		base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{
			Images: sdkconfig.ImagesConfig{CodexModel: "gpt-5.4-mini"},
		}, manager)
		handler := openaihandlers.NewOpenAIImagesAPIHandler(base)
		router := gin.New()
		router.POST("/v1/images/generations", handler.Generations)

		request := httptest.NewRequest(
			http.MethodPost,
			"/v1/images/generations",
			strings.NewReader(`{"model":"gpt-image-2","prompt":"draw"}`),
		)
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		assertChatGPTWebImageHandlerOpenAIRateLimit(t, response)
	})

	t.Run("generic upstream image rate limit uses OpenAI error format", func(t *testing.T) {
		errImage := newCommittedChatGPTWebImageHandlerError(`{"error":{"code":"rate_limit_exceeded","message":"Too many requests"}}`)
		manager, _ := newChatGPTWebImageHandlerManager(t, errImage, "gpt-image-2", "gpt-5.4-mini")
		base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{
			Images: sdkconfig.ImagesConfig{CodexModel: "gpt-5.4-mini"},
		}, manager)
		handler := openaihandlers.NewOpenAIImagesAPIHandler(base)
		router := gin.New()
		router.POST("/v1/images/generations", handler.Generations)

		request := httptest.NewRequest(
			http.MethodPost,
			"/v1/images/generations",
			strings.NewReader(`{"model":"gpt-image-2","prompt":"draw"}`),
		)
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		assertChatGPTWebImageHandlerOpenAIRateLimit(t, response)
	})

	t.Run("image quota message uses OpenAI error format", func(t *testing.T) {
		errImage := newCommittedChatGPTWebImageHandlerError(`{"error":{"message":"You've hit your limit. Please try again later."}}`)
		manager, authID := newChatGPTWebImageHandlerManager(t, errImage, "gpt-image-2", "gpt-5.4-mini")
		base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{
			Images: sdkconfig.ImagesConfig{CodexModel: "gpt-5.4-mini"},
		}, manager)
		handler := openaihandlers.NewOpenAIImagesAPIHandler(base)
		router := gin.New()
		router.POST("/v1/images/generations", handler.Generations)

		request := httptest.NewRequest(
			http.MethodPost,
			"/v1/images/generations",
			strings.NewReader(`{"model":"gpt-image-2","prompt":"draw"}`),
		)
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		assertChatGPTWebImageHandlerOpenAIRateLimit(t, response)
		current, ok := manager.GetByID(authID)
		if !ok || current == nil || current.Metadata["quota_state"] != "exhausted" {
			t.Fatalf("explicit image quota did not immediately exhaust credential: %+v", current)
		}
		state := current.ModelStates["gpt-image-2"]
		if state == nil || !state.Unavailable || !state.Quota.Exceeded || state.Quota.Reason != "chatgpt_web_image_quota" {
			t.Fatalf("explicit image quota did not immediately suspend image model: %+v", state)
		}
	})

	t.Run("image tool", func(t *testing.T) {
		const model = "chatgpt-web-image-tool-handler"
		errImage := newCommittedChatGPTWebImageHandlerError(`{"error":{"message":"You've hit your limit. Please try again later."}}`)
		manager, _ := newChatGPTWebImageHandlerManager(t, errImage, model, "gpt-image-2")
		handler := openaihandlers.NewOpenAIResponsesAPIHandler(
			handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager),
		)
		router := gin.New()
		router.POST("/v1/responses", handler.Responses)

		request := httptest.NewRequest(
			http.MethodPost,
			"/v1/responses",
			strings.NewReader(`{"model":"`+model+`","input":"draw","tools":[{"type":"image_generation"}]}`),
		)
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		assertChatGPTWebImageHandlerOpenAIRateLimit(t, response)
	})

	t.Run("stream bootstrap", func(t *testing.T) {
		const model = "chatgpt-web-image-tool-stream-handler"
		errImage := newCommittedChatGPTWebImageHandlerError(`{"error":{"message":"You've hit your limit. Please try again later."}}`)
		manager, _ := newChatGPTWebImageHandlerManager(t, errImage, model, "gpt-image-2")
		handler := openaihandlers.NewOpenAIResponsesAPIHandler(
			handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager),
		)
		router := gin.New()
		router.POST("/v1/responses", handler.Responses)

		request := httptest.NewRequest(
			http.MethodPost,
			"/v1/responses",
			strings.NewReader(`{"model":"`+model+`","stream":true,"input":"draw","tools":[{"type":"image_generation"}]}`),
		)
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		assertChatGPTWebImageHandlerOpenAIRateLimit(t, response)
	})
}
