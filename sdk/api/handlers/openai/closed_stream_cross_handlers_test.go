package openai

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers/claude"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers/gemini"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

type queuedStreamErrorContext struct{ context.Context }

func (ctx queuedStreamErrorContext) Done() <-chan struct{} {
	// Let the stream producer queue its terminal error before the handler's
	// first select, exercising both ready channels without adding real sleeps.
	for range 16 {
		runtime.Gosched()
	}
	return ctx.Context.Done()
}

func TestOtherClientStreamingRetainsCodexErrorWhenDataCloses(t *testing.T) {
	gin.SetMode(gin.TestMode)
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(&committedContextErrorExecutor{})
	model := "cross-handler-committed-error"
	auth := &coreauth.Auth{ID: model + "-auth", Provider: "codex", Status: coreauth.StatusActive}
	if _, err := manager.Register(t.Context(), auth); err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager)
	claudeHandler := claude.NewClaudeCodeAPIHandler(base)
	geminiHandler := gemini.NewGeminiAPIHandler(base)
	for _, fixture := range []struct {
		name, route, path, body string
		handle                  gin.HandlerFunc
	}{
		{"claude", "/v1/messages", "/v1/messages", fmt.Sprintf(`{"model":%q,"stream":true,"messages":[{"role":"user","content":"fixture"}]}`, model), claudeHandler.ClaudeMessages},
		{"gemini SSE", "/v1beta/models/*action", "/v1beta/models/" + model + ":streamGenerateContent?alt=sse", `{"contents":[{"role":"user","parts":[{"text":"fixture"}]}]}`, geminiHandler.GeminiHandler},
		{"gemini JSON", "/v1beta/models/*action", "/v1beta/models/" + model + ":streamGenerateContent?alt=json", `{"contents":[{"role":"user","parts":[{"text":"fixture"}]}]}`, geminiHandler.GeminiHandler},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			router := gin.New()
			router.POST(fixture.route, fixture.handle)
			for attempt := range 64 {
				r := httptest.NewRequest(http.MethodPost, fixture.path, strings.NewReader(fixture.body))
				r = r.WithContext(queuedStreamErrorContext{t.Context()})
				r.Header.Set("Content-Type", "application/json")
				w := httptest.NewRecorder()
				router.ServeHTTP(w, r)
				if w.Code != 429 || !strings.Contains(w.Body.String(), "context_too_large") || strings.Contains(w.Body.String(), "[DONE]") {
					t.Fatalf("attempt=%d status=%d missing original error or unexpected success terminator", attempt, w.Code)
				}
			}
		})
	}
}
