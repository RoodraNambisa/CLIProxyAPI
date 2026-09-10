package executor

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexClaudeInputEstimateSnapshotScope(t *testing.T) {
	payload := []byte(`{"messages":[{"role":"user","content":"estimate this text"}]}`)
	for _, enabled := range []bool{false, true} {
		for _, source := range []translator.Format{translator.FormatClaude, translator.FormatCodex, translator.FormatOpenAI} {
			for _, operation := range []core.RequestOperation{core.RequestOperationExecute, core.RequestOperationStream} {
				cfg := &config.Config{Codex: config.CodexConfig{EstimateClaudeInputTokens: enabled}}
				exec := NewCodexExecutor(cfg)
				req := core.Request{Model: "gpt-5.4", Payload: []byte(`{"input":"translated"}`)}
				opts := core.Options{SourceFormat: source, OriginalRequest: payload}
				prepared, err := exec.PrepareProviderRequest(t.Context(), req, opts, operation)
				if err != nil {
					t.Fatal(err)
				}
				got := prepared.(codexPreparedSessionIdentity).ClaudeInputTokensEstimate
				want := int64(0)
				if enabled && source == translator.FormatClaude && operation == core.RequestOperationStream {
					want = helps.EstimateClaudeInputTokens(t.Context(), payload)
				}
				if got != want {
					t.Fatalf("enabled=%t source=%v op=%v count=%d want=%d", enabled, source, operation, got, want)
				}
				cfg.Codex.EstimateClaudeInputTokens = !enabled
				opts = core.WithProviderPreparedRequest(opts, exec.Identifier(), prepared)
				opts.OriginalRequest = nil
				if actual := exec.codexPreparedSessionIdentity(t.Context(), req, opts).ClaudeInputTokensEstimate; actual != want {
					t.Fatal("hot update or body release changed snapshot")
				}
			}
		}
	}
	exec := NewCodexExecutor(&config.Config{Codex: config.CodexConfig{EstimateClaudeInputTokens: true}})
	for _, canceled := range []bool{false, true} {
		ctx, cancel := context.WithCancel(t.Context())
		opts := core.Options{SourceFormat: translator.FormatClaude, OriginalRequest: payload}
		if canceled {
			cancel()
		} else {
			opts.ResponseFormat = translator.FormatOpenAIResponse
		}
		prepared, err := exec.PrepareProviderRequest(ctx, core.Request{}, opts, core.RequestOperationStream)
		cancel()
		if err != nil || prepared.(codexPreparedSessionIdentity).ClaudeInputTokensEstimate != 0 {
			t.Fatal("excluded/canceled request estimated")
		}
	}
}

func TestCodexClaudeInputEstimateAcrossTransports(t *testing.T) {
	for _, transport := range []string{"http", "websocket", "http-fallback"} {
		for _, enabled := range []bool{false, true} {
			for _, release := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/enabled=%t/release=%t", transport, enabled, release), func(t *testing.T) {
					controller := core.NewRequestBodyReleaseController(1, []byte("released"))
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						var conn *websocket.Conn
						if websocket.IsWebSocketUpgrade(r) {
							if transport == "http-fallback" {
								w.WriteHeader(http.StatusUpgradeRequired)
								return
							}
							upgrader := websocket.Upgrader{}
							var err error
							conn, err = upgrader.Upgrade(w, r, nil)
							if err != nil {
								t.Error(err)
								return
							}
							defer func() { _ = conn.Close() }()
							if _, _, err = conn.ReadMessage(); err != nil {
								t.Error(err)
								return
							}
						} else {
							_, _ = io.Copy(io.Discard, r.Body)
							w.Header().Set("Content-Type", "text/event-stream")
						}
						if release {
							controller.Release()
						}
						for _, event := range []string{
							`{"type":"response.created","response":{"id":"test","model":"gpt-5.4"}}`,
							`{"type":"response.output_text.delta","delta":"Answer"}`,
							`{"type":"response.completed","response":{"id":"test","status":"completed","output":[],"usage":{"input_tokens":11,"output_tokens":2,"input_tokens_details":{"cached_tokens":2}}}}`,
						} {
							if conn != nil {
								_ = conn.WriteMessage(websocket.TextMessage, []byte(event))
							} else {
								_, _ = fmt.Fprintf(w, "data: %s\n\n", event)
							}
						}
					}))
					defer server.Close()
					cfg := &config.Config{Codex: config.CodexConfig{EstimateClaudeInputTokens: enabled}}
					base := NewCodexExecutor(cfg)
					var exec auth.ProviderExecutor = base
					payload := []byte(`{"messages":[{"role":"user","content":"Please estimate this request"}]}`)
					opts := core.Options{SourceFormat: translator.FormatClaude, OriginalRequest: payload}
					prepared, err := base.PrepareProviderRequest(t.Context(), core.Request{Model: "gpt-5.4", Payload: payload}, opts, core.RequestOperationStream)
					if err != nil {
						t.Fatal(err)
					}
					opts = core.WithProviderPreparedRequest(opts, "codex", prepared)
					// A subsequent executor configuration must not alter this logical request.
					nextCfg := &config.Config{Codex: config.CodexConfig{EstimateClaudeInputTokens: !enabled}}
					exec = NewCodexExecutor(nextCfg)
					if release {
						opts.Metadata[core.BodyReleaseControllerMetadataKey] = controller
					}
					if transport != "http" {
						exec = NewCodexWebsocketsExecutor(nextCfg)
						payload = translator.TranslateRequest(translator.FormatClaude, translator.FormatCodex, "gpt-5.4", payload, true)
					}
					credential := &auth.Auth{Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL}}
					result, err := exec.ExecuteStream(t.Context(), credential, core.Request{Model: "gpt-5.4", Payload: payload}, opts)
					if err != nil {
						t.Fatal(err)
					}
					starts, finals := 0, 0
					for chunk := range result.Chunks {
						if chunk.Err != nil {
							t.Fatal(chunk.Err)
						}
						for _, line := range strings.Split(string(chunk.Payload), "\n") {
							if !strings.HasPrefix(line, "data: ") {
								continue
							}
							event := gjson.Parse(strings.TrimPrefix(line, "data: "))
							switch event.Get("type").String() {
							case "message_start":
								starts++
								if got := event.Get("message.usage.input_tokens").Int(); got != prepared.(codexPreparedSessionIdentity).ClaudeInputTokensEstimate {
									t.Fatalf("start=%d differs from snapshot", got)
								}
							case "message_delta":
								finals++
								if event.Get("usage.input_tokens").Int() != 9 || event.Get("usage.output_tokens").Int() != 2 || event.Get("usage.cache_read_input_tokens").Int() != 2 {
									t.Fatal("real usage changed")
								}
							}
						}
					}
					if starts != 1 || finals != 1 || controller.Released() != release {
						t.Fatal("missing event or request-body release")
					}
				})
			}
		}
	}
}
