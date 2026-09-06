package executor

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestCodexPolicyRefusalsSurviveExecutorClassification(t *testing.T) {
	for _, transport := range []string{"http", "sse", "incomplete", "websocket"} {
		for _, field := range []string{"code", "type"} {
			t.Run(fmt.Sprintf("%s/%s", transport, field), func(t *testing.T) {
				var calls atomic.Int64
				errorObject := fmt.Sprintf(`{%q:"misalignment_policy_violation","message":"request rejected"}`, field)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					switch transport {
					case "websocket":
						upgrader := websocket.Upgrader{}
						conn, err := upgrader.Upgrade(w, r, nil)
						if err != nil {
							t.Error(err)
							return
						}
						defer func() { _ = conn.Close() }()
						_, _, _ = conn.ReadMessage()
						_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","status":401,"error":`+errorObject+`}`))
						_, _, _ = conn.ReadMessage()
					case "sse", "incomplete":
						w.Header().Set("Content-Type", "text/event-stream")
						event := "response.failed"
						if transport == "incomplete" {
							event = "response.incomplete"
						}
						_, _ = fmt.Fprintf(w, "data: {\"type\":%q,\"response\":{\"error\":%s}}\n\n", event, errorObject)
					default:
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(401)
						_, _ = fmt.Fprint(w, `{"error":`+errorObject+`}`)
					}
				}))
				defer server.Close()
				cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}}
				manager := auth.NewManager(nil, &auth.FillFirstSelector{}, nil)
				manager.SetRetryConfig(2, 0, 0)
				var exec auth.ProviderExecutor = NewCodexExecutor(cfg)
				if transport == "websocket" {
					exec = NewCodexWebsocketsExecutor(cfg)
				}
				manager.RegisterExecutor(exec)
				for range 2 {
					id := uuid.NewString()
					if _, err := manager.Register(t.Context(), &auth.Auth{ID: id, Provider: "codex", Attributes: map[string]string{"api_key": "test", "base_url": server.URL}}); err != nil {
						t.Fatal(err)
					}
					registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: "gpt-5.4"}})
					t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
				}
				req := core.Request{Model: "gpt-5.4", Payload: []byte(`{"input":[]}`)}
				opts := core.Options{SourceFormat: translator.FromString("codex")}
				stream, err := manager.ExecuteStream(t.Context(), []string{"codex"}, req, opts)
				if stream != nil {
					for chunk := range stream.Chunks {
						if chunk.Err != nil {
							err = chunk.Err
						}
					}
				}
				if calls.Load() != 1 || err == nil || !strings.Contains(err.Error(), "misalignment_policy_violation") {
					t.Errorf("policy refusal lost its code or retried: calls=%d", calls.Load())
				}
				for _, credential := range manager.List() {
					if credential.Unavailable || len(credential.ModelStates) > 0 {
						t.Error("policy refusal cooled credential")
					}
				}
			})
		}
	}
}
