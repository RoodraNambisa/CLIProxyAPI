package executor

import (
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexPartialResponsePreservesCommittedReasoningReplay(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			ctx := t.Context()
			auth := &coreauth.Auth{ID: t.Name(), Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"api_key": "fixture"}}
			namespace := helps.ReasoningReplayNamespace(ctx, "codex", auth.ID)
			metadata := map[string]any{core.ExecutionSessionMetadataKey: t.Name()}
			query := []byte(`{"input":[{"type":"function_call_output","call_id":"pair","output":"result"}]}`)
			_, scope := helps.ApplyCodexReasoningReplay(ctx, "claude", namespace, "gpt-5.4-mini", nil, []byte(`{"input":[]}`), nil, metadata, nil)
			signature := make([]byte, 73)
			signature[0] = 0x80
			oldSignature := base64.RawURLEncoding.EncodeToString(signature)
			signature[72] = 1
			newSignature := base64.RawURLEncoding.EncodeToString(signature)
			old := []byte(`{"response":{"output":[{"type":"reasoning","encrypted_content":"` + oldSignature + `"},{"type":"function_call","call_id":"pair","name":"old","arguments":"{}"}]}}`)
			if !helps.CacheCodexReasoningReplayFromCompleted(scope, old) {
				t.Fatal("failed to seed committed reasoning")
			}
			defer helps.ClearCodexReasoningReplayOnInvalidSignature(scope, 400, []byte(`{"error":{"code":"invalid_encrypted_content"}}`))
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = fmt.Fprintf(w, "data: %s\n\n", `{"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"max_tokens"},"output":[{"type":"reasoning","encrypted_content":"`+newSignature+`"},{"type":"function_call","call_id":"pair","name":"new","arguments":"{}"}]}}`)
			}))
			defer server.Close()
			auth.Attributes["base_url"] = server.URL
			executor := NewCodexExecutor(&config.Config{})
			req := core.Request{Model: "gpt-5.4-mini", Payload: []byte(`{"model":"gpt-5.4-mini","messages":[{"role":"user","content":"continue"}]}`), Metadata: metadata}
			opts := core.Options{SourceFormat: translator.FormatClaude, Stream: stream, Metadata: metadata}
			if stream {
				result, err := executor.ExecuteStream(ctx, auth, req, opts)
				if err != nil {
					t.Fatal(err)
				}
				for chunk := range result.Chunks {
					if chunk.Err != nil {
						t.Fatal(chunk.Err)
					}
				}
			} else {
				if _, err := executor.Execute(ctx, auth, req, opts); err != nil {
					t.Fatal(err)
				}
			}
			replayed, _ := helps.ApplyCodexReasoningReplay(ctx, "claude", namespace, "gpt-5.4-mini", nil, query, nil, metadata, nil)
			if gjson.GetBytes(replayed, "input.0.encrypted_content").String() != oldSignature || gjson.GetBytes(replayed, "input.1.name").String() != "old" {
				t.Fatal("partial result replaced or cleared the committed replay")
			}
		})
	}
}

func TestCodexPartialWebsocketKeepsCommittedMultiAgentPolicy(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			var calls, connections atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				connections.Add(1)
				upgrader := websocket.Upgrader{}
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					t.Error(err)
					return
				}
				defer func() { _ = conn.Close() }()
				for {
					if _, _, err := conn.ReadMessage(); err != nil {
						return
					}
					turn := calls.Add(1)
					status := "completed"
					if turn == 2 {
						status = "incomplete"
					}
					response := fmt.Sprintf(`{"type":"response.%s","response":{"id":"resp_%d","status":%q,"incomplete_details":{"reason":"max_tokens"},"output":[{"type":"function_call","namespace":"collaboration-optimize","name":"spawn_agent","call_id":"pair","arguments":"{}"}]}}`, status, turn, status)
					if err := conn.WriteMessage(websocket.TextMessage, []byte(response)); err != nil {
						return
					}
				}
			}))
			defer server.Close()
			executor := NewCodexWebsocketsExecutor(&config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: true}})
			executor.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
			defer executor.CloseExecutionSession(t.Name())
			auth := &coreauth.Auth{ID: t.Name(), Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
			opts := core.Options{SourceFormat: translator.FormatOpenAIResponse, Headers: http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}}, Metadata: map[string]any{core.ExecutionSessionMetadataKey: t.Name()}}
			for index, body := range []string{
				`{"model":"gpt-5.4","input":"hello","tools":[{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"spawn_agent"}]}]}`,
				`{"model":"gpt-5.4","input":"truncated","tools":[{"type":"namespace","name":"collaboration-optimize","tools":[{"type":"function","name":"spawn_agent"}]}]}`,
				`{"model":"gpt-5.4","previous_response_id":"resp_1","input":[]}`,
			} {
				result, err := runCodexMultiAgentWebsocketRequest(t.Context(), executor, auth, core.Request{Model: "gpt-5.4", Payload: []byte(body)}, opts, stream)
				if err != nil {
					t.Fatal(err)
				}
				if index != 1 && (gjson.GetBytes(result.Payload, "output.0.namespace").String() != "collaboration" || !gjson.GetBytes(result.Payload, "output.0.encrypted_function_args").Exists()) {
					t.Fatal("incomplete turn replaced committed tool provenance")
				}
			}
			if calls.Load() != 3 || connections.Load() != 1 {
				t.Fatal("partial result changed connection reuse or call count")
			}
		})
	}
}
