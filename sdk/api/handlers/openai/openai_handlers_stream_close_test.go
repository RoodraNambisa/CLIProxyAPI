package openai

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

type committedContextError struct{}

func (committedContextError) Error() string {
	return `{"error":{"message":"maximum context length exceeded","type":"invalid_request_error","code":"context_too_large"}}`
}

func (committedContextError) StatusCode() int      { return http.StatusTooManyRequests }
func (committedContextError) SkipAuthResult() bool { return true }

type committedContextErrorExecutor struct{}

func (*committedContextErrorExecutor) Identifier() string { return "codex" }

func (*committedContextErrorExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (*committedContextErrorExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	chunks := make(chan coreexecutor.StreamChunk, 2)
	chunks <- coreexecutor.BootstrapCommitStreamChunk()
	chunks <- coreexecutor.StreamChunk{Err: committedContextError{}}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func (*committedContextErrorExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (*committedContextErrorExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (*committedContextErrorExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func TestOpenAIStreamingDoesNotLoseCommittedErrorWhenDataCloses(t *testing.T) {
	gin.SetMode(gin.TestMode)

	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(&committedContextErrorExecutor{})
	model := "openai-committed-context-error"
	auth := &coreauth.Auth{ID: model + "-auth", Provider: "codex", Status: coreauth.StatusActive}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })

	handler := NewOpenAIAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager))
	testCases := []struct {
		name    string
		path    string
		payload string
		handle  gin.HandlerFunc
	}{
		{
			name:    "chat_completions",
			path:    "/v1/chat/completions",
			payload: fmt.Sprintf(`{"model":%q,"stream":true,"messages":[{"role":"user","content":"hello"}]}`, model),
			handle:  handler.ChatCompletions,
		},
		{
			name:    "completions",
			path:    "/v1/completions",
			payload: fmt.Sprintf(`{"model":%q,"stream":true,"prompt":"hello"}`, model),
			handle:  handler.Completions,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			router := gin.New()
			router.POST(testCase.path, testCase.handle)
			for attempt := 0; attempt < 64; attempt++ {
				request := httptest.NewRequest(http.MethodPost, testCase.path, strings.NewReader(testCase.payload))
				request.Header.Set("Content-Type", "application/json")
				response := httptest.NewRecorder()
				router.ServeHTTP(response, request)

				if response.Code != http.StatusTooManyRequests {
					t.Fatalf("attempt %d status = %d, want 429; body=%s", attempt, response.Code, response.Body.String())
				}
				if !strings.Contains(response.Body.String(), "context_too_large") {
					t.Fatalf("attempt %d body = %q, want context error", attempt, response.Body.String())
				}
				if strings.Contains(response.Body.String(), "[DONE]") {
					t.Fatalf("attempt %d body = %q, must not contain DONE", attempt, response.Body.String())
				}
			}
		})
	}
}

func TestTakePendingStreamError(t *testing.T) {
	want := &interfaces.ErrorMessage{StatusCode: http.StatusTooManyRequests, Error: committedContextError{}}
	errs := make(chan *interfaces.ErrorMessage, 1)
	errs <- want
	close(errs)
	if got := takePendingStreamError(errs); got != want {
		t.Fatalf("pending error = %#v, want %#v", got, want)
	}

	empty := make(chan *interfaces.ErrorMessage)
	close(empty)
	if got := takePendingStreamError(empty); got != nil {
		t.Fatalf("closed empty error channel returned %#v", got)
	}

	open := make(chan *interfaces.ErrorMessage)
	if got := takePendingStreamError(open); got != nil {
		t.Fatalf("open empty error channel returned %#v", got)
	}
}
