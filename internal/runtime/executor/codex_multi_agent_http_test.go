package executor

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	_ "github.com/router-for-me/CLIProxyAPI/v6/internal/translator"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexHTTPMultiAgentPreparationAndResponseAcrossConfigChange(t *testing.T) {
	for _, compact := range []bool{false, true} {
		for _, enabled := range []bool{false, true} {
			for _, official := range []bool{false, true} {
				t.Run(fmt.Sprintf("compact=%t/enabled=%t/official=%t", compact, enabled, official), func(t *testing.T) {
					captured := make(chan []byte, 2)
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						body, err := io.ReadAll(r.Body)
						if err != nil {
							t.Error(err)
							return
						}
						captured <- body
						namespace := gjson.GetBytes(body, "tools.0.name").String()
						response := fmt.Sprintf(`{"id":"resp_1","object":"response","status":"completed","output":[{"type":"function_call","id":"fc_item","call_id":"pair","namespace":%q,"name":"spawn_agent","arguments":"{\"message\":\"collaboration-optimize business\"}"}]}`, namespace)
						if strings.HasSuffix(r.URL.Path, "/compact") {
							w.Header().Set("Content-Type", "application/json")
							_, _ = io.WriteString(w, response)
						} else {
							w.Header().Set("Content-Type", "text/event-stream")
							_, _ = fmt.Fprintf(w, "data: {\"type\":\"response.completed\",\"response\":%s}\n\n", response)
						}
					}))
					defer server.Close()
					cfg := &config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: enabled, PassthroughPromptCacheKey: true, IdentityConfuse: true}}
					executor := NewCodexExecutor(cfg)
					auth := &cliproxyauth.Auth{ID: "multi-agent-fixture", Provider: "codex", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL}}
					raw := []byte(`{"model":"gpt-5.4","prompt_cache_key":"cache-original","tools":[{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"spawn_agent","description":"Create a worker.","parameters":{"type":"object","properties":{"message":{"type":"string","encrypted":true}}}}]}],"tool_choice":{"type":"function","namespace":"collaboration","name":"spawn_agent"},"input":[{"type":"agent_message","content":[{"type":"encrypted_content","encrypted_content":"opaque-history"}]}]}`)
					userAgent := "curl/8.0"
					if official {
						userAgent = "codex_cli_rs/0.153.4"
					}
					opts := core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, OriginalRequest: raw, Headers: http.Header{"User-Agent": {userAgent}}}
					if compact {
						opts.Alt = "responses/compact"
					}
					req := core.Request{Model: "gpt-5.4", Payload: raw}
					prepared, err := executor.PrepareProviderRequest(t.Context(), req, opts, core.RequestOperationExecute)
					if err != nil {
						t.Fatal(err)
					}
					firstOpts := core.WithProviderPreparedRequest(opts, "codex", prepared)
					cfg.Codex.OptimizeMultiAgentV2 = !enabled
					for index, requestOpts := range []core.Options{firstOpts, opts} {
						wantActive := enabled && official
						if index == 1 {
							wantActive = !enabled && official
						}
						response, err := executor.Execute(t.Context(), auth, req, requestOpts)
						if err != nil {
							t.Fatalf("execute fixture: %v", err)
						}
						body := <-captured
						wantNamespace := "collaboration"
						if wantActive {
							wantNamespace = "collaboration-optimize"
						}
						if gjson.GetBytes(body, "tools.0.name").String() != wantNamespace || gjson.GetBytes(body, "tool_choice.namespace").String() != wantNamespace {
							t.Fatal("outbound tool declaration or explicit choice did not honor the request policy")
						}
						if gjson.GetBytes(body, "tools.0.tools.0.parameters.properties.message.encrypted").Exists() == wantActive {
							t.Fatal("outbound plaintext schema did not honor the request policy")
						}
						if gjson.GetBytes(body, "prompt_cache_key").String() != "cache-original" || gjson.GetBytes(body, "input.0.content.0.encrypted_content").String() != "opaque-history" {
							t.Fatal("cache identity or opaque history changed")
						}
						if gjson.GetBytes(response.Payload, "output.0.namespace").String() != "collaboration" || gjson.GetBytes(response.Payload, "output.0.call_id").String() != "pair" {
							t.Fatal("client tool identity or pairing changed")
						}
						marker := gjson.GetBytes(response.Payload, "output.0.encrypted_function_args")
						if wantActive && marker.Raw != "[]" || !wantActive && marker.Exists() {
							t.Fatal("client plaintext marker did not match the prepared schema")
						}
						if !bytes.Contains(response.Payload, []byte("collaboration-optimize business")) {
							t.Fatal("business argument content was rewritten")
						}
					}
				})
			}
		}
	}
}
