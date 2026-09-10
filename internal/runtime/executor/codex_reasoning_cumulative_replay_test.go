package executor

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexTransportsRestoreCumulativeReasoningOnlyFromCompletedTurns(t *testing.T) {
	for _, transport := range []string{"http", "websocket", "http-fallback"} {
		for _, stream := range []bool{false, true} {
			for _, release := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/stream=%t/release=%t", transport, stream, release), func(t *testing.T) {
					type attempt struct {
						cancel context.CancelFunc
						body   *core.RequestBodyReleaseController
						done   chan struct{}
					}
					attempts := make(chan attempt, 1)
					var calls atomic.Int32
					signature := func(tag byte) string {
						payload := make([]byte, 73)
						payload[0], payload[72] = 0x80, tag
						return base64.RawURLEncoding.EncodeToString(payload)
					}
					respond := func(body []byte, write func(string) error) {
						current := <-attempts
						defer close(current.done)
						call := int(calls.Add(1))
						items := gjson.GetBytes(body, `input.#(type=="reasoning")#`).Array()
						if len(items) != min(call-1, 2) {
							t.Errorf("call %d restored %d turns", call, len(items))
						}
						for index, item := range items {
							if item.Get("encrypted_content").String() != signature(byte(index+1)) {
								t.Errorf("call %d restored an unsuccessful or misplaced turn", call)
							}
						}
						if release {
							current.body.Release()
						}
						if call == 5 {
							current.cancel()
						}
						status := "completed"
						if call == 3 {
							status = "incomplete"
						} else if call == 4 {
							status = "failed"
						}
						turn := min(call, 3)
						_ = write(fmt.Sprintf(`{"type":"response.output_item.done","output_index":0,"item":{"type":"reasoning","encrypted_content":%q}}`, signature(byte(turn))))
						_ = write(fmt.Sprintf(`{"type":"response.output_item.done","output_index":1,"item":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer%d"}]}}`, turn))
						errorJSON := "null"
						if call == 4 {
							errorJSON = `{"code":"misalignment_policy_violation","message":"fixture rejection"}`
						}
						_ = write(fmt.Sprintf(`{"type":"response.%s","response":{"status":%q,"error":%s,"incomplete_details":{"reason":"max_tokens"},"output":[]}}`, status, status, errorJSON))
					}
					upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if websocket.IsWebSocketUpgrade(r) {
							if transport == "http-fallback" {
								w.WriteHeader(http.StatusUpgradeRequired)
								return
							}
							upgrader := websocket.Upgrader{}
							conn, err := upgrader.Upgrade(w, r, nil)
							if err != nil {
								t.Error(err)
								return
							}
							defer func() { _ = conn.Close() }()
							for {
								_, body, errRead := conn.ReadMessage()
								if errRead != nil {
									return
								}
								respond(body, func(event string) error { return conn.WriteMessage(websocket.TextMessage, []byte(event)) })
							}
						}
						body, errRead := io.ReadAll(r.Body)
						if errRead != nil {
							t.Error(errRead)
							return
						}
						w.Header().Set("Content-Type", "text/event-stream")
						respond(body, func(event string) error { _, err := fmt.Fprintf(w, "data: %s\n\n", event); return err })
					}))
					defer upstream.Close()
					var executor coreauth.ProviderExecutor = NewCodexExecutor(&config.Config{})
					if transport != "http" {
						ws := NewCodexWebsocketsExecutor(&config.Config{})
						ws.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
						defer ws.CloseExecutionSession(t.Name())
						executor = ws
					}
					credential := &coreauth.Auth{ID: uuid.NewString(), Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"api_key": "fixture", "base_url": upstream.URL}}
					for call := 1; call <= 6; call++ {
						historyTurns := min(call-1, 2)
						if call == 6 {
							historyTurns = 3
						}
						messages := []map[string]string{}
						for turn := 1; turn <= historyTurns+1; turn++ {
							messages = append(messages, map[string]string{"role": "user", "content": fmt.Sprintf("question%d", turn)})
							if turn <= historyTurns {
								messages = append(messages, map[string]string{"role": "assistant", "content": fmt.Sprintf("answer%d", turn)})
							}
						}
						payload, errMarshal := json.Marshal(map[string]any{"messages": messages})
						if errMarshal != nil {
							t.Fatal(errMarshal)
						}
						ctx, cancel := context.WithCancel(t.Context())
						ctrl := core.NewRequestBodyReleaseController(int64(len(payload)), []byte("<released>"))
						done := make(chan struct{})
						attempts <- attempt{cancel, ctrl, done}
						opts := core.Options{SourceFormat: translator.FormatClaude, Stream: stream, Metadata: map[string]any{core.ExecutionSessionMetadataKey: t.Name(), core.BodyReleaseControllerMetadataKey: ctrl}}
						req := core.Request{Model: "gpt-5.4-mini", Payload: payload}
						if transport != "http" && stream {
							opts.OriginalRequest = payload
							req.Payload = translator.TranslateRequest(translator.FormatClaude, translator.FormatCodex, req.Model, payload, true)
						}
						var executionErr error
						if stream {
							result, errExecute := executor.ExecuteStream(ctx, credential, req, opts)
							executionErr = errExecute
							if errExecute == nil {
								for chunk := range result.Chunks {
									if chunk.Err != nil {
										executionErr = chunk.Err
									}
								}
							}
						} else {
							_, executionErr = executor.Execute(ctx, credential, req, opts)
						}
						<-done
						if call == 5 && ctx.Err() == nil {
							t.Error("request cancellation was not observed")
						}
						if call == 5 && executionErr == nil {
							executionErr = ctx.Err()
						}
						cancel()
						if (executionErr != nil) != (call == 4 || call == 5) {
							t.Fatalf("call %d returned unexpected error: %v", call, executionErr)
						}
					}
					if calls.Load() != 6 {
						t.Fatalf("upstream calls = %d, want 6", calls.Load())
					}
				})
			}
		}
	}
}
