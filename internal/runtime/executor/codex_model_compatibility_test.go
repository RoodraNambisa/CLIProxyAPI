package executor

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexModelCompatibilityUsesSelectedPolicyAcrossTransports(t *testing.T) {
	for _, transport := range []string{"http", "sse", "compact", "websocket", "websocket-stream", "count"} {
		for _, optimize := range []bool{false, true} {
			for _, compat := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/optimize=%t/compat=%t", transport, optimize, compat), func(t *testing.T) {
					type fields struct{ kind, role, text, cipherKind, cipher, cache, id, reasoningSignature string }
					captured := make(chan fields, 4)
					var calls atomic.Int32
					upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						calls.Add(1)
						var body []byte
						var conn *websocket.Conn
						var err error
						if strings.HasPrefix(transport, "websocket") {
							upgrader := websocket.Upgrader{}
							conn, err = upgrader.Upgrade(w, r, nil)
							if err != nil {
								t.Error(err)
								return
							}
							defer func() { _ = conn.Close() }()
							_, body, err = conn.ReadMessage()
						} else {
							body, err = io.ReadAll(r.Body)
						}
						if err != nil {
							t.Error(err)
							return
						}
						captured <- fields{
							gjson.GetBytes(body, "input.0.type").String(), gjson.GetBytes(body, "input.0.role").String(),
							gjson.GetBytes(body, "input.0.content.0.text").String(), gjson.GetBytes(body, "input.1.type").String(),
							gjson.GetBytes(body, "input.1.content.0.encrypted_content").String(), gjson.GetBytes(body, "prompt_cache_key").String(),
							gjson.GetBytes(body, "input.0.id").String(),
							gjson.GetBytes(body, "input.2.encrypted_content").Raw,
						}
						terminal := []byte(`{"type":"response.completed","response":{"id":"resp_fixture","object":"response","status":"completed","output":[]}}`)
						if conn != nil {
							_ = conn.WriteMessage(websocket.TextMessage, terminal)
						} else if transport == "compact" {
							w.Header().Set("Content-Type", "application/json")
							_, _ = w.Write([]byte(`{"id":"resp_fixture","object":"response.compaction","output":[]}`))
						} else {
							w.Header().Set("Content-Type", "text/event-stream")
							_, _ = fmt.Fprintf(w, "data: %s\n\n", terminal)
						}
					}))
					t.Cleanup(upstream.Close)
					cfg := &config.Config{
						SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"},
						Codex:     config.CodexConfig{OptimizeMultiAgentV2: optimize, PassthroughPromptCacheKey: true},
						CodexKey:  []config.CodexKey{{APIKey: "fixture", BaseURL: upstream.URL, Models: []config.CodexModel{{Name: "upstream", Alias: "alias", IsCompat: compat}}}},
					}
					manager := coreauth.NewManager(nil, nil, nil)
					manager.SetConfig(cfg)
					if strings.HasPrefix(transport, "websocket") {
						executor := NewCodexWebsocketsExecutor(cfg)
						manager.RegisterExecutor(executor)
						t.Cleanup(func() { executor.CloseExecutionSession(t.Name()) })
					} else {
						manager.RegisterExecutor(NewCodexExecutor(cfg))
					}
					_, err := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{
						ID: t.Name(), Provider: "codex", Attributes: map[string]string{"api_key": "fixture", "base_url": upstream.URL, "websockets": "true"},
					})
					if err != nil {
						t.Fatal(err)
					}
					reg := registry.GetGlobalRegistry()
					reg.RegisterClient(t.Name(), "codex", []*registry.ModelInfo{{ID: "alias", IsCompat: !compat}})
					t.Cleanup(func() { reg.UnregisterClient(t.Name()) })
					payload := []byte(`{"model":"alias","prompt_cache_key":"cache-fixture","input":[{"type":"agent_message","id":"plain","content":[{"type":"input_text","text":"plaintext"}]},{"type":"agent_message","content":[{"type":"encrypted_content","encrypted_content":"opaque"}]},{"type":"reasoning","encrypted_content":""}]}`)
					req := core.Request{Model: "alias", Payload: payload}
					opts := core.Options{SourceFormat: translator.FormatOpenAIResponse, OriginalRequest: payload,
						Headers: http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}}, Metadata: map[string]any{core.ExecutionSessionMetadataKey: t.Name()}}
					if transport == "compact" {
						opts.Alt = "responses/compact"
					}
					if transport == "count" {
						opts.SourceFormat = translator.FormatCodex
						result, err := manager.ExecuteCount(t.Context(), []string{"codex"}, req, opts)
						if err != nil {
							t.Fatal(err)
						}
						tokens := gjson.GetBytes(result.Payload, "response.usage.input_tokens")
						if !tokens.Exists() || (tokens.Int() > 0) != (optimize && compat) || calls.Load() != 0 {
							t.Fatal("local token count omitted compatible plaintext or connected to upstream")
						}
						return
					}
					if transport == "sse" || transport == "websocket-stream" {
						result, err := manager.ExecuteStream(t.Context(), []string{"codex"}, req, opts)
						if err != nil {
							t.Fatal(err)
						}
						for chunk := range result.Chunks {
							if chunk.Err != nil {
								t.Fatal(chunk.Err)
							}
						}
					} else if _, err := manager.Execute(t.Context(), []string{"codex"}, req, opts); err != nil {
						t.Fatal(err)
					}
					select {
					case got := <-captured:
						wantType, wantRole := "agent_message", ""
						if optimize && compat {
							wantType, wantRole = "message", "user"
						}
						if got.kind != wantType || got.role != wantRole || got.text != "plaintext" ||
							got.cipherKind != "agent_message" || got.cipher != "opaque" || got.cache != "cache-fixture" {
							t.Fatal("outbound model policy or opaque/cache preservation failed")
						}
						wantID := "plain"
						if optimize && compat {
							wantID = "msg_plain"
						}
						if got.id != wantID {
							t.Fatal("outbound message ID did not match its final item type")
						}
						wantSignature := ""
						if compat {
							wantSignature = `""`
						}
						if got.reasoningSignature != wantSignature {
							t.Fatal("unsigned reasoning did not follow model compatibility independently of Multi-agent v2")
						}
					default:
						t.Fatal("upstream request was not observed")
					}
					if calls.Load() != 1 {
						t.Fatal("compatibility changed the upstream attempt count")
					}
				})
			}
		}
	}
}
