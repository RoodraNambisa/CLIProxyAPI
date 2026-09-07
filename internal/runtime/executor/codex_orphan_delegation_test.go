package executor

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexOrphanDelegationWireUsesSnapshotAcrossTransports(t *testing.T) {
	for _, transport := range []string{"http", "compact", "websocket"} {
		for _, stream := range []bool{false, true} {
			if transport == "compact" && stream {
				continue
			}
			for _, enabled := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/stream=%t/enabled=%t", transport, stream, enabled), func(t *testing.T) {
					captured := make(chan []byte, 1)
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if transport == "websocket" {
							upgrader := websocket.Upgrader{}
							conn, err := upgrader.Upgrade(w, r, nil)
							if err != nil {
								t.Error(err)
								return
							}
							defer func() { _ = conn.Close() }()
							_, payload, err := conn.ReadMessage()
							if err != nil {
								t.Error(err)
								return
							}
							captured <- payload
							_ = conn.WriteMessage(websocket.TextMessage, []byte(codexTestBootstrapCompleted))
							return
						}
						payload, err := io.ReadAll(r.Body)
						if err != nil {
							t.Error(err)
							return
						}
						captured <- payload
						if transport == "compact" {
							_, _ = fmt.Fprint(w, `{"object":"response.compaction","output":[]}`)
							return
						}
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = fmt.Fprintf(w, "data: %s\n\n", codexTestBootstrapCompleted)
					}))
					defer server.Close()
					req := core.Request{Model: "gpt-5.4", Payload: []byte(`{"input":[{"type":"function_call_output","namespace":"codex_app","name":"create_thread","call_id":"orphan","output":"keep"}],"prompt_cache_key":"cache"}`)}
					opts := core.Options{SourceFormat: translator.FromString("codex"), Headers: http.Header{"X-Openai-Subagent": {"collab_spawn"}}, Stream: stream}
					if transport == "compact" {
						opts.Alt = "responses/compact"
					}
					preparer := NewCodexExecutor(&config.Config{Codex: config.CodexConfig{OrphanDelegationCompatibility: enabled}})
					var err error
					opts, err = preparer.ensureCodexPreparedSessionIdentity(t.Context(), req, opts, core.RequestOperationExecute)
					if err != nil {
						t.Fatal(err)
					}
					cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}, Codex: config.CodexConfig{OrphanDelegationCompatibility: !enabled}}
					var exec auth.ProviderExecutor = NewCodexExecutor(cfg)
					if transport == "websocket" {
						exec = NewCodexWebsocketsExecutor(cfg)
					}
					credential := &auth.Auth{ID: "orphan-wire", Provider: "codex", Attributes: map[string]string{"api_key": "test", "base_url": server.URL}}
					if stream {
						var result *core.StreamResult
						result, err = exec.ExecuteStream(t.Context(), credential, req, opts)
						if result != nil {
							for chunk := range result.Chunks {
								if chunk.Err != nil {
									err = chunk.Err
								}
							}
						}
					} else {
						_, err = exec.Execute(t.Context(), credential, req, opts)
					}
					if err != nil {
						t.Fatal(err)
					}
					got := <-captured
					if (gjson.GetBytes(got, "input.0.type").String() == "message") != enabled {
						t.Fatal("orphan policy changed across preparation or transport")
					}
					if enabled && gjson.GetBytes(got, "input.0.content.0.text").String() != "Tool output from codex_app__create_thread:\nkeep" {
						t.Fatal("wire lost delegation output")
					}
				})
			}
		}
	}
}
