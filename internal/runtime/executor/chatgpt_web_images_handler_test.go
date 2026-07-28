package executor

import (
	"context"
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

func newChatGPTWebImageHandlerManager(t *testing.T, err error, models ...string) *coreauth.Manager {
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
	return manager
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

func TestChatGPTWebImageErrorsReachHTTPHandlersAsRateLimits(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("direct image", func(t *testing.T) {
		errImage := newCommittedChatGPTWebImageHandlerError(`{"error":{"code":"image_generation_limit_reached"}}`)
		manager := newChatGPTWebImageHandlerManager(t, errImage, "gpt-image-2", "gpt-5.4-mini")
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
		assertChatGPTWebImageHandlerRateLimit(t, response)
	})

	t.Run("image tool", func(t *testing.T) {
		const model = "chatgpt-web-image-tool-handler"
		errImage := newCommittedChatGPTWebImageHandlerError(`{"error":{"code":"rate_limit_exceeded","message":"Too many requests"}}`)
		manager := newChatGPTWebImageHandlerManager(t, errImage, model, "gpt-image-2")
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
		assertChatGPTWebImageHandlerRateLimit(t, response)
	})

	t.Run("stream bootstrap", func(t *testing.T) {
		const model = "chatgpt-web-image-tool-stream-handler"
		errImage := newCommittedChatGPTWebImageHandlerError(`{"error":{"code":"rate_limit_exceeded","message":"Too many requests"}}`)
		manager := newChatGPTWebImageHandlerManager(t, errImage, model, "gpt-image-2")
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
		assertChatGPTWebImageHandlerRateLimit(t, response)
	})
}
