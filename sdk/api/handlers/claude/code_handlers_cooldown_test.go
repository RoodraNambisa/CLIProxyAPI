package claude

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/tidwall/gjson"
)

type cooldownOnlyExecutor struct{ calls atomic.Int64 }

func (*cooldownOnlyExecutor) Identifier() string { return "claude" }
func (e *cooldownOnlyExecutor) unexpected() error {
	e.calls.Add(1)
	return errors.New("cooling credential reached executor")
}
func (e *cooldownOnlyExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, e.unexpected()
}
func (e *cooldownOnlyExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	return nil, e.unexpected()
}
func (e *cooldownOnlyExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, e.unexpected()
}
func (e *cooldownOnlyExecutor) Refresh(context.Context, *coreauth.Auth) (*coreauth.Auth, error) {
	return nil, e.unexpected()
}
func (e *cooldownOnlyExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, e.unexpected()
}

func TestClaudeCoolingResponsesPreserveRetryAfterWithoutExecuting(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, passthrough := range []bool{false, true} {
		for _, mode := range []string{"messages", "stream", "count"} {
			t.Run(fmt.Sprintf("%s/passthrough=%t", mode, passthrough), func(t *testing.T) {
				manager := coreauth.NewManager(nil, nil, nil)
				executor := &cooldownOnlyExecutor{}
				manager.RegisterExecutor(executor)
				id := strings.ReplaceAll(t.Name(), "/", "-")
				model := "claude-cooling-test"
				credential := &coreauth.Auth{ID: id, Provider: "claude", Status: coreauth.StatusActive}
				registry.GetGlobalRegistry().RegisterClient(id, "claude", []*registry.ModelInfo{{ID: model}})
				t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
				if _, err := manager.Register(coreauth.WithSkipPersist(t.Context()), credential); err != nil {
					t.Fatal(err)
				}
				delay := time.Minute
				manager.MarkResult(coreauth.WithSkipPersist(t.Context()), coreauth.Result{
					AuthID: id, Provider: "claude", Model: model,
					RetryAfter: &delay, Error: &coreauth.Error{HTTPStatus: 429, Message: "test-private-previous-response", Diagnostic: &coreauth.ErrorDiagnostic{ResponseBody: "test-private-diagnostic"}},
				})
				installed, _ := manager.GetByID(id)
				if state := installed.ModelStates[model]; state == nil || !state.NextRetryAfter.After(time.Now()) || !installed.LifecycleSelectable() {
					t.Fatalf("invalid cooldown fixture: lifecycle=%t state=%+v", installed.LifecycleSelectable(), state)
				}
				handler := NewClaudeCodeAPIHandler(handlers.NewBaseAPIHandlers(&config.SDKConfig{PassthroughHeaders: passthrough}, manager))
				engine := gin.New()
				engine.POST("/v1/messages", handler.ClaudeMessages)
				engine.POST("/v1/messages/count_tokens", handler.ClaudeCountTokens)
				path := "/v1/messages"
				if mode == "count" {
					path += "/count_tokens"
				}
				body := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"hello"}],"stream":%t}`, model, mode == "stream")
				request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
				request.Header.Set("Content-Type", "application/json")
				recorder := httptest.NewRecorder()
				engine.ServeHTTP(recorder, request)
				wait, err := strconv.Atoi(recorder.Header().Get("Retry-After"))
				if recorder.Code != http.StatusServiceUnavailable || err != nil || wait < 1 || wait > 60 {
					t.Fatalf("cooldown response: status=%d retry-after=%q calls=%d code=%s message=%s", recorder.Code, recorder.Header().Get("Retry-After"), executor.calls.Load(), gjson.GetBytes(recorder.Body.Bytes(), "error.code").String(), gjson.GetBytes(recorder.Body.Bytes(), "error.message").String())
				}
				if len(recorder.Header().Values("Retry-After")) != 1 || !strings.Contains(gjson.GetBytes(recorder.Body.Bytes(), "error.message").String(), "auth_unavailable") {
					t.Fatal("cooldown response lost its code or duplicated the header")
				}
				if executor.calls.Load() != 0 {
					t.Fatal("an unavailable pool invoked or refreshed a credential")
				}
				if !strings.Contains(recorder.Body.String(), "previous credential failure: HTTP 429") || strings.Contains(recorder.Body.String(), "test-private") {
					t.Fatal("stored failure summary was missing or exposed private details")
				}
			})
		}
	}
}
