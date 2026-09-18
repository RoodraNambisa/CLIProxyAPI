package executor

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	sdkauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestCodexQuotaAutoDisableHTTPAndWebsocketFinishResponse(t *testing.T) {
	for _, transport := range []string{"http", "websocket"} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%t", transport, stream), func(t *testing.T) {
				threshold := 10.0
				cfg := &config.Config{SDKConfig: config.SDKConfig{ProxyURL: "direct"}, Codex: config.CodexConfig{
					ObserveQuota:     true,
					QuotaAutoDisable: config.CodexQuotaAutoDisableConfig{Enabled: true, Rules: []config.CodexQuotaAutoDisableRule{{FiveHourRemainingPercent: &threshold}}},
				}}
				store := sdkauth.NewFileTokenStore()
				store.SetBaseDir(t.TempDir())
				manager := coreauth.NewManager(store, nil, nil)
				manager.SetConfig(cfg)
				if transport == "websocket" {
					ws := NewCodexWebsocketsExecutor(cfg)
					manager.RegisterExecutor(ws)
					t.Cleanup(func() { ws.CloseExecutionSession(t.Name()) })
				} else {
					manager.RegisterExecutor(NewCodexExecutor(cfg))
				}
				var calls atomic.Int32
				completed := `{"type":"response.completed","response":{"id":"resp_quota","model":"gpt-5.4","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"OK"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					if transport == "websocket" {
						upgrader := websocket.Upgrader{}
						conn, err := upgrader.Upgrade(w, r, nil)
						if err != nil {
							t.Error(err)
							return
						}
						defer func() { _ = conn.Close() }()
						if _, _, err = conn.ReadMessage(); err != nil {
							t.Error(err)
							return
						}
						for _, frame := range []string{`{"type":"codex.rate_limits","rate_limits":{"limit_id":"codex","primary":{"used_percent":95,"window_minutes":300,"reset_after_seconds":18000},"secondary":{"used_percent":10,"window_minutes":10080,"reset_after_seconds":604800}}}`, completed} {
							if err = conn.WriteMessage(websocket.TextMessage, []byte(frame)); err != nil {
								t.Error(err)
								return
							}
						}
						return
					}
					w.Header().Set("X-Codex-Active-Limit", "premium")
					w.Header().Set("X-Codex-Primary-Window-Minutes", "300")
					w.Header().Set("X-Codex-Primary-Used-Percent", "95")
					w.Header().Set("X-Codex-Secondary-Window-Minutes", "10080")
					w.Header().Set("X-Codex-Secondary-Used-Percent", "10")
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = fmt.Fprint(w, "data: "+completed+"\n\n")
				}))
				t.Cleanup(server.Close)
				id := "quota.json"
				auth, err := manager.Register(t.Context(), &coreauth.Auth{ID: id, Provider: "codex", FileName: id,
					Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL, "websockets": "true"},
					Metadata:   map[string]any{"type": "codex", "access_token": "fixture"},
				})
				if err != nil {
					t.Fatal(err)
				}
				registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: "gpt-5.4"}})
				t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
				req := core.Request{Model: "gpt-5.4", Payload: []byte(`{"model":"gpt-5.4","input":[]}`)}
				opts := core.Options{SourceFormat: translator.FormatCodex, Metadata: map[string]any{core.ExecutionSessionMetadataKey: t.Name()}}
				var response []byte
				if stream {
					result, errStream := manager.ExecuteStream(t.Context(), []string{"codex"}, req, opts)
					if errStream != nil {
						t.Fatal(errStream)
					}
					for chunk := range result.Chunks {
						if chunk.Err != nil {
							t.Fatal(chunk.Err)
						}
						response = append(response, chunk.Payload...)
					}
				} else {
					result, errExecute := manager.Execute(t.Context(), []string{"codex"}, req, opts)
					if errExecute != nil {
						t.Fatal(errExecute)
					}
					response = result.Payload
				}
				if !bytes.Contains(response, []byte(`"text":"OK"`)) {
					t.Fatalf("answer was interrupted: %s", response)
				}
				current, _ := manager.GetByID(id)
				if !current.Disabled || current.Status != coreauth.StatusDisabled || current.StatusMessage == "" || current.RuntimeInstanceID() != auth.RuntimeInstanceID() {
					t.Fatalf("incorrect runtime disable: %+v", current.Status)
				}
				if _, err = manager.Execute(t.Context(), []string{"codex"}, req, opts); err == nil {
					t.Fatal("disabled credential accepted another request")
				}
				if calls.Load() != 1 {
					t.Fatal("auto-disable retried or sent extra requests")
				}
				stored, errList := store.List(t.Context())
				if errList != nil || len(stored) != 1 || !stored[0].Disabled || coreauth.CodexQuotaAutoDisableReason(stored[0]) == "" {
					t.Fatalf("disable was not persisted: %v", errList)
				}
			})
		}
	}
}
