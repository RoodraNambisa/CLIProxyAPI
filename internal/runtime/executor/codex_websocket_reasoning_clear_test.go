package executor

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexWebsocketSignatureFailureClearsMatchedReplay(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, eventType := range []string{"error", "response.failed"} {
			t.Run(fmt.Sprintf("stream=%t/%s", stream, eventType), func(t *testing.T) {
				var calls atomic.Int32
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					upgrader := websocket.Upgrader{}
					conn, err := upgrader.Upgrade(w, r, nil)
					if err != nil {
						t.Error(err)
						return
					}
					defer func() { _ = conn.Close() }()
					if _, _, err := conn.ReadMessage(); err != nil {
						t.Error(err)
						return
					}
					calls.Add(1)
					failure := `{"type":"error","status":400,"error":{"code":"invalid_encrypted_content","message":"fixture signature failure"}}`
					if eventType == "response.failed" {
						failure = `{"type":"response.failed","response":{"status":"failed","error":{"code":"invalid_encrypted_content","message":"fixture signature failure"}}}`
					}
					_ = conn.WriteMessage(websocket.TextMessage, []byte(failure))
				}))
				defer upstream.Close()
				credential := &coreauth.Auth{ID: uuid.NewString(), Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"api_key": "fixture", "base_url": upstream.URL}}
				opts := core.Options{SourceFormat: translator.FormatClaude, Metadata: map[string]any{core.ExecutionSessionMetadataKey: t.Name()}}
				namespace := helps.ReasoningReplayNamespace(t.Context(), "codex", credential.ID)
				_, scope := helps.ApplyCodexReasoningReplay(t.Context(), "claude", namespace, "gpt-5.4-mini", nil, []byte(`{"input":[]}`), nil, opts.Metadata, nil)
				signature := make([]byte, 73)
				signature[0] = 0x80
				completed := fmt.Appendf(nil, `{"type":"response.completed","response":{"output":[{"type":"reasoning","encrypted_content":%q},{"type":"function_call","call_id":"pair","name":"lookup","arguments":"{}"}]}}`, base64.RawURLEncoding.EncodeToString(signature))
				if !helps.CacheCodexReasoningReplayFromCompleted(scope, completed) {
					t.Fatal("failed to seed replay history")
				}
				query := []byte(`{"input":[{"type":"function_call_output","call_id":"pair","output":"result"}]}`)
				checkReplay := func(want bool) {
					out, _ := helps.ApplyCodexReasoningReplay(t.Context(), "claude", namespace, "gpt-5.4-mini", nil, query, nil, opts.Metadata, nil)
					if gjson.GetBytes(out, `input.#(type=="reasoning")`).Exists() != want {
						t.Fatalf("replay state does not match expected presence %t", want)
					}
				}
				checkReplay(true)
				req := core.Request{Model: "gpt-5.4-mini", Payload: []byte(`{"messages":[{"role":"user","content":"request"}]}`)}
				executor := NewCodexWebsocketsExecutor(&config.Config{})
				executor.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
				defer executor.CloseExecutionSession(t.Name())
				var executionErr error
				if stream {
					opts.OriginalRequest = req.Payload
					req.Payload = translator.TranslateRequest(translator.FormatClaude, translator.FormatCodex, req.Model, req.Payload, true)
					result, err := executor.ExecuteStream(t.Context(), credential, req, opts)
					executionErr = err
					if err == nil {
						for chunk := range result.Chunks {
							if chunk.Err != nil {
								executionErr = chunk.Err
							}
						}
					}
				} else {
					_, executionErr = executor.Execute(t.Context(), credential, req, opts)
				}
				if executionErr == nil || calls.Load() != 1 {
					t.Fatal("signature failure lost the error or introduced retries")
				}
				checkReplay(false)
			})
		}
	}
}
