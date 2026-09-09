package executor

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexWebsocketOutputIdentityStaysWithinAttempt(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, trusted := range []bool{false, true} {
			for _, release := range []bool{false, true} {
				t.Run(fmt.Sprintf("stream=%t/trusted=%t/release=%t", stream, trusted, release), func(t *testing.T) {
					var turns, connections atomic.Int32
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
							turn := turns.Add(1)
							if turn == 1 || turn == 3 || turn == 6 {
								item := fmt.Sprintf(`{"type":"response.output_item.done","output_index":0,"item":{"type":"message","id":"msg_%d","content":[{"type":"output_text","text":"answer"}]}}`, turn)
								if err := conn.WriteMessage(websocket.TextMessage, []byte(item)); err != nil {
									return
								}
							}
							if turn == 3 {
								if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.failed","response":{"status":"failed","error":{"code":"misalignment_policy_violation","message":"fixture refusal"}}}`)); err != nil {
									return
								}
								continue
							}
							id := ""
							if turn == 6 {
								id = `,"id":"saved"`
							}
							if err := conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.done","response":{"id":"resp_%d","status":"completed","output":[{"type":"message"%s,"content":[{"type":"output_text","text":"answer"}]}]}}`, turn, id))); err != nil {
								return
							}
						}
					}))
					defer server.Close()
					executor := NewCodexWebsocketsExecutor(&config.Config{})
					executor.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
					sessionID := t.Name()
					defer executor.CloseExecutionSession(sessionID)
					auth := &coreauth.Auth{ID: "first", Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL}}
					for turn := 1; turn <= 6; turn++ {
						if turn == 5 {
							auth = &coreauth.Auth{ID: "second", Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL}}
						}
						controller := core.NewRequestBodyReleaseController(1, []byte("<released>"))
						opts := core.Options{SourceFormat: translator.FormatOpenAIResponse, Stream: stream, Metadata: map[string]any{core.ExecutionSessionMetadataKey: sessionID, core.TrustUpstreamSSEMetadataKey: trusted}}
						if release {
							opts.Metadata[core.BodyReleaseControllerMetadataKey] = controller
						}
						response, err := runCodexMultiAgentWebsocketRequest(t.Context(), executor, auth, core.Request{Model: "gpt-5.4-mini", Payload: []byte(`{"model":"gpt-5.4-mini","input":"question"}`)}, opts, stream)
						if turn == 3 {
							if !coreauth.IsPolicyRefusalError(err) || controller.Released() != release {
								t.Fatal("terminal refusal was lost")
							}
							continue
						}
						if err != nil {
							t.Fatal(err)
						}
						want := ""
						if turn == 1 && (!stream || !trusted) {
							want = "msg_1"
						}
						if turn == 6 {
							want = "saved"
						}
						if gjson.GetBytes(response.Payload, "output.0.id").String() != want || controller.Released() != release {
							t.Fatalf("turn %d: identity leaked, was overwritten, or was not repaired", turn)
						}
					}
					if turns.Load() != 6 || connections.Load() != 2 {
						t.Fatal("output repair changed attempt counts or credential isolation")
					}
				})
			}
		}
	}
}
