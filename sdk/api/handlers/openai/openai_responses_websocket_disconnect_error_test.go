package openai

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/tidwall/gjson"
)

type requestFaultDisconnectExecutor struct {
	websocketUpstreamDisconnectExecutor
	started chan string
	release chan struct{}
	cause   error
	calls   atomic.Int32
}

func (e *requestFaultDisconnectExecutor) ExecuteStream(ctx context.Context, _ *coreauth.Auth, _ coreexecutor.Request, opts coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.calls.Add(1)
	sessionID, _ := opts.Metadata[coreexecutor.ExecutionSessionMetadataKey].(string)
	chunks := make(chan coreexecutor.StreamChunk)
	go func() {
		defer close(chunks)
		e.started <- sessionID
		select {
		case <-ctx.Done():
			return
		case <-e.release:
		}
		select {
		case <-ctx.Done():
		case chunks <- coreexecutor.StreamChunk{Err: e.cause}:
		}
	}()
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func TestResponsesWebsocketDisconnectSharesRequestProjection(t *testing.T) {
	for _, notifyFirst := range []bool{false, true} {
		for _, rewrite := range []bool{false, true} {
			t.Run(fmt.Sprintf("notifyFirst=%t/rewrite=%t", notifyFirst, rewrite), func(t *testing.T) {
				gin.SetMode(gin.TestMode)
				cause := websocketPinnedFailoverStatusError{status: 403, msg: `{"error":{"code":"misalignment_policy_violation","message":"test policy refusal"}}`}
				exec := &requestFaultDisconnectExecutor{started: make(chan string, 1), release: make(chan struct{}), cause: cause}
				manager := coreauth.NewManager(nil, nil, nil)
				manager.RegisterExecutor(exec)
				for _, id := range []string{"disconnect-policy-a", "disconnect-policy-b"} {
					auth := &coreauth.Auth{ID: id, Provider: "codex", Status: coreauth.StatusActive, Attributes: map[string]string{"priority": "3"}}
					if _, err := manager.Register(t.Context(), auth); err != nil {
						t.Fatal(err)
					}
					registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: "disconnect-policy-model"}})
					t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
				}
				cfg := &sdkconfig.SDKConfig{}
				wantCode, wantStatus := "misalignment_policy_violation", int64(403)
				if rewrite {
					body := map[string]any{"error": map[string]any{"code": "public_policy_refusal", "message": "public test message"}}
					cfg.ErrorResponseRewrites = []sdkconfig.ErrorResponseRewriteRule{{Sources: []string{"codex"}, AuthPriorities: []int{3}, StatusCode: 403, ResponseStatusCode: 422, ResponseBody: &body}}
					wantCode, wantStatus = "public_policy_refusal", 422
				}
				base := handlers.NewBaseAPIHandlers(cfg, manager)
				h := NewOpenAIResponsesAPIHandler(base)
				router := gin.New()
				handlerDone := make(chan struct{})
				timeline := make(chan string, 1)
				router.GET("/v1/responses", func(c *gin.Context) {
					defer close(handlerDone)
					h.ResponsesWebsocket(c)
					value, _ := c.Get(wsTimelineBodyKey)
					body, _ := value.([]byte)
					timeline <- string(body)
				})
				server := httptest.NewServer(router)
				defer server.Close()
				conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = conn.Close() }()
				if err = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"disconnect-policy-model","input":[{"role":"user","content":"test"}]}`)); err != nil {
					t.Fatal(err)
				}
				var sessionID string
				select {
				case sessionID = <-exec.started:
				case <-time.After(5 * time.Second):
					t.Fatal("execution not started")
				}
				// Hot reload cannot change a turn already waiting on upstream output.
				base.UpdateClients(&sdkconfig.SDKConfig{ErrorResponseRewrites: []sdkconfig.ErrorResponseRewriteRule{{StatusCode: 403, ResponseStatusCode: 418}}})
				if notifyFirst {
					exec.TriggerDisconnect(sessionID, cause)
				}
				close(exec.release)
				_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
				_, payload, err := conn.ReadMessage()
				if err != nil {
					t.Fatalf("policy rejection lost: %v", err)
				}
				if gjson.GetBytes(payload, "error.code").String() != wantCode || gjson.GetBytes(payload, "status").Int() != wantStatus {
					t.Fatalf("projection changed: %s", payload)
				}
				if !notifyFirst {
					exec.TriggerDisconnect(sessionID, cause)
				}
				if _, duplicate, err := conn.ReadMessage(); err == nil {
					t.Fatalf("duplicate terminal: %s", duplicate)
				}
				select {
				case <-handlerDone:
				case <-time.After(5 * time.Second):
					t.Fatal("handler or observer did not exit")
				}
				if exec.calls.Load() != 1 {
					t.Fatalf("request rejection rotated credentials: %d calls", exec.calls.Load())
				}
				for _, id := range []string{"disconnect-policy-a", "disconnect-policy-b"} {
					auth, _ := manager.GetByID(id)
					if auth == nil || auth.Unavailable || !auth.NextRetryAfter.IsZero() {
						t.Fatal("request rejection cooled credential")
					}
					if state := auth.ModelStates["disconnect-policy-model"]; state != nil && (state.Unavailable || !state.NextRetryAfter.IsZero()) {
						t.Fatal("request rejection cooled model")
					}
				}
				if log := <-timeline; !strings.Contains(log, "misalignment_policy_violation") {
					t.Fatal("original disconnect reason absent from timeline")
				}
			})
		}
	}
}

func TestResponsesWebsocketDisconnectPreservesRequestError(t *testing.T) {
	for _, code := range []string{"misalignment_policy_violation", "cyber_policy", "context_length_exceeded", "unstored_history"} {
		t.Run(code, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			exec := &websocketUpstreamDisconnectExecutor{subscribed: make(chan string, 1)}
			manager := coreauth.NewManager(nil, nil, nil)
			manager.RegisterExecutor(exec)
			h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager))
			router := gin.New()
			router.GET("/v1/responses", h.ResponsesWebsocket)
			server := httptest.NewServer(router)
			defer server.Close()
			conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = conn.Close() }()
			var sessionID string
			select {
			case sessionID = <-exec.subscribed:
			case <-time.After(5 * time.Second):
				t.Fatal("subscription missing")
			}
			message := fmt.Sprintf(`{"error":{"code":%q,"message":"request rejected"}}`, code)
			wantCode := code
			if code == "unstored_history" {
				message = "Item with id 'fixture' not found. Items are not persisted when `store` is set to false."
				wantCode = "internal_server_error"
			}
			exec.TriggerDisconnect(sessionID, websocketPinnedFailoverStatusError{status: 502, msg: message})
			_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			_, payload, err := conn.ReadMessage()
			if err != nil {
				t.Fatalf("request rejection lost on disconnect: %v", err)
			}
			if gjson.GetBytes(payload, "error.code").String() != wantCode || gjson.GetBytes(payload, "status").Int() != 502 {
				t.Fatalf("error changed: %s", payload)
			}
			if _, _, err = conn.ReadMessage(); err == nil {
				t.Fatal("connection retained after upstream disconnect")
			}
		})
	}
}
