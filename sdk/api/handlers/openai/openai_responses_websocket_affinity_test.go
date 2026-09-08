package openai

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

type websocketAffinityExecutor struct {
	websocketCompactionCaptureExecutor
	calls  chan string
	closed chan struct{}
}

func (e *websocketAffinityExecutor) ExecuteStream(_ context.Context, auth *coreauth.Auth, _ core.Request, _ core.Options) (*core.StreamResult, error) {
	e.calls <- auth.ID
	chunks := make(chan core.StreamChunk, 1)
	chunks <- core.StreamChunk{Payload: []byte(`{"type":"response.completed","response":{"id":"affinity-response","status":"completed","output":[]}}`)}
	close(chunks)
	return &core.StreamResult{Chunks: chunks}, nil
}

func (e *websocketAffinityExecutor) CloseExecutionSession(string) { e.closed <- struct{}{} }

func TestResponsesWebsocketAffinityUsesCallerHistoryAndExplicitIdentity(t *testing.T) {
	const model = "websocket-affinity-model"
	const history = `{"input":[{"role":"user","content":"root prompt"},{"role":"assistant","content":"root answer"},{"role":"user","content":"branch prompt"}]}`
	for _, mode := range []string{"disabled", "history", "other-caller", "other-scope", "explicit-header", "explicit-frame", "subagent", "fork", "frame-without-parent"} {
		t.Run(mode, func(t *testing.T) {
			selector := coreauth.NewSessionAffinitySelectorWithConfig(coreauth.SessionAffinityConfig{
				Fallback: &coreauth.FillFirstSelector{}, LCP: mode != "disabled", Subagents: true,
			})
			t.Cleanup(selector.Stop)
			manager := coreauth.NewManager(nil, selector, nil)
			manager.SetConfig(&config.Config{Routing: config.RoutingConfig{SessionAffinity: true}})
			executor := &websocketAffinityExecutor{calls: make(chan string, 8), closed: make(chan struct{}, 8)}
			manager.RegisterExecutor(executor)
			for _, id := range []string{"a", "b"} {
				auth := &coreauth.Auth{ID: "ws-affinity-" + id, Provider: executor.Identifier(), Status: coreauth.StatusActive}
				if _, err := manager.Register(coreauth.WithSkipPersist(t.Context()), auth); err != nil {
					t.Fatal(err)
				}
				registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: model}})
				t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
			}
			identify := func(c *gin.Context, caller, scope string) {
				c.Set("apiKey", caller)
				c.Set("accessProvider", "fixture-auth")
				c.Set("accessMetadata", map[string]string{"allowed_providers": scope})
			}
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			identify(c, "caller-a", executor.Identifier())
			ctx := context.WithValue(t.Context(), "gin", c)
			opts := core.Options{Headers: make(http.Header), SourceFormat: sdktranslator.FormatOpenAIResponse, OriginalRequest: []byte(history)}
			selector.BindSession(ctx, executor.Identifier(), model, opts, "ws-affinity-b")
			opts.Headers.Set("Session-Id", "parent")
			selector.BindSession(ctx, executor.Identifier(), model, opts, "ws-affinity-b")
			handler := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager))
			router := gin.New()
			router.GET("/v1/responses", func(c *gin.Context) {
				caller, scope := "caller-a", executor.Identifier()
				if mode == "other-caller" {
					caller = "caller-b"
				}
				if mode == "other-scope" {
					scope += ",other"
				}
				identify(c, caller, scope)
				handler.ResponsesWebsocket(c)
			})
			server := httptest.NewServer(router)
			defer server.Close()
			headers := make(http.Header)
			metadata := ""
			want := "ws-affinity-a"
			switch mode {
			case "history":
				want = "ws-affinity-b"
			case "explicit-header":
				headers.Set("Session-Id", "separate")
			case "explicit-frame":
				metadata = `,"client_metadata":{"session_id":"separate"}`
			case "subagent":
				metadata = `,"client_metadata":{"session_id":"child","x-codex-parent-thread-id":"parent","x-openai-subagent":"true"}`
				want = "ws-affinity-b"
			case "fork":
				metadata = `,"client_metadata":{"session_id":"child"},"forked_from_thread_id":"parent"`
				want = "ws-affinity-b"
			case "frame-without-parent":
				headers.Set("Session-Id", "old-child")
				headers.Set("X-Codex-Parent-Thread-Id", "parent")
				metadata = `,"client_metadata":{"session_id":"separate"}`
			}
			for range 2 {
				conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", headers)
				if err != nil {
					t.Fatal(err)
				}
				request := fmt.Sprintf(`{"type":"response.create","model":%q,"input":[{"role":"user","content":"root prompt"},{"role":"assistant","content":"root answer"},{"role":"user","content":"branch prompt"},{"role":"assistant","content":"branch answer"},{"role":"user","content":"followup"}]%s}`, model, metadata)
				if err = conn.WriteMessage(websocket.TextMessage, []byte(request)); err != nil {
					_ = conn.Close()
					t.Fatal(err)
				}
				for {
					_, payload, errRead := conn.ReadMessage()
					if errRead != nil {
						_ = conn.Close()
						t.Fatal(errRead)
					}
					if responsesWebsocketTerminalEvent(gjson.GetBytes(payload, "type").String()) {
						if gjson.GetBytes(payload, "type").String() != "response.completed" {
							t.Errorf("unexpected terminal: %s", payload)
						}
						break
					}
				}
				_ = conn.Close()
				<-executor.closed
				if got := <-executor.calls; got != want {
					t.Fatalf("selected %s, want %s", got, want)
				}
				if len(executor.calls) != 0 {
					t.Fatal("request added an unexpected retry")
				}
			}
		})
	}
}
