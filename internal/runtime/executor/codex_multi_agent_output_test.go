package executor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestOpenAICompatMultiAgentPlaintextToolMarkers(t *testing.T) {
	runMultiAgentPlaintextToolMarkers(t, "openai-compatibility")
}

func TestClaudeAndKimiMultiAgentPlaintextToolMarkers(t *testing.T) {
	for _, provider := range []string{"claude", "kimi"} {
		t.Run(provider, func(t *testing.T) { runMultiAgentPlaintextToolMarkers(t, provider) })
	}
}

func runMultiAgentPlaintextToolMarkers(t *testing.T, provider string) {
	t.Helper()
	for _, stream := range []bool{false, true} {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("stream=%t/enabled=%t", stream, enabled), func(t *testing.T) {
				cfg := &config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: enabled}}
				controller := core.NewRequestBodyReleaseController(1, []byte("<released>"))
				handle := func(w http.ResponseWriter, r *http.Request) {
					body, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
					}
					namePath, encryptionPath := "tools.0.function.name", "tools.0.function.parameters.properties.message.encrypted"
					if provider == "claude" {
						namePath, encryptionPath = "tools.0.name", "tools.0.input_schema.properties.message.encrypted"
					}
					name := gjson.GetBytes(body, namePath).String()
					if name != "collaboration__spawn_agent" {
						t.Errorf("collaboration declaration name = %q", name)
					}
					if gjson.GetBytes(body, encryptionPath).Exists() == enabled {
						t.Error("direct SDK tool schema did not follow the optimization policy")
					}
					cfg.Codex.OptimizeMultiAgentV2 = !enabled
					controller.Release()
					if provider == "claude" {
						w.Header().Set("Content-Type", "text/event-stream")
						for _, event := range []string{
							`{"type":"message_start","message":{"id":"result","type":"message","role":"assistant","model":"claude-sonnet-4-6","content":[],"usage":{"input_tokens":1,"output_tokens":0}}}`,
							`{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"pair","name":"collaboration__spawn_agent","input":{}}}`,
							`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"message\":\"work\"}"}}`,
							`{"type":"content_block_stop","index":0}`,
							`{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":1}}`,
							`{"type":"message_stop"}`,
						} {
							_, _ = io.WriteString(w, "data: "+event+"\n\n")
						}
					} else if stream {
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = io.WriteString(w, "data: {\"id\":\"result\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"pair\",\"type\":\"function\",\"function\":{\"name\":\"collaboration__spawn_agent\",\"arguments\":\"{\\\"message\\\":\\\"work\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n")
					} else {
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, `{"id":"result","choices":[{"index":0,"message":{"role":"assistant","tool_calls":[{"id":"pair","type":"function","function":{"name":"collaboration__spawn_agent","arguments":"{\"message\":\"work\"}"}}]},"finish_reason":"tool_calls"}]}`)
					}
				}
				server := httptest.NewServer(http.HandlerFunc(handle))
				defer server.Close()
				var executor multiAgentTranslationExecutor = NewOpenAICompatExecutor("openai-compatibility", cfg)
				ctx := t.Context()
				if provider == "claude" {
					executor = NewClaudeExecutor(cfg)
				} else if provider == "kimi" {
					executor = NewKimiExecutor(cfg)
					ctx = context.WithValue(ctx, "cliproxy.roundtripper", streamTerminalRoundTripFunc(func(r *http.Request) (*http.Response, error) {
						recorder := httptest.NewRecorder()
						handle(recorder, r)
						return recorder.Result(), nil
					}))
				}
				auth := &cliproxyauth.Auth{Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
				req := core.Request{Model: "gpt-5.4", Payload: []byte(`{"model":"gpt-5.4","input":[],"tools":[{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"spawn_agent","parameters":{"type":"object","properties":{"message":{"type":"string","encrypted":true}}}}]}]}`)}
				if provider == "claude" {
					req.Model = "claude-sonnet-4-6"
				}
				opts := core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Headers: http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}}, Metadata: map[string]any{core.BodyReleaseControllerMetadataKey: controller}}
				var item gjson.Result
				if stream {
					result, err := executor.ExecuteStream(ctx, auth, req, opts)
					if err != nil {
						t.Fatal(err)
					}
					for chunk := range result.Chunks {
						if chunk.Err != nil {
							t.Fatal(chunk.Err)
						}
						for _, line := range bytes.Split(chunk.Payload, []byte("\n")) {
							if !bytes.HasPrefix(line, []byte("data:")) {
								continue
							}
							event := gjson.ParseBytes(helps.JSONPayload(line))
							if event.Get("type").String() == "response.completed" {
								for _, output := range event.Get("response.output").Array() {
									if output.Get("type").String() == "function_call" {
										item = output
									}
								}
							}
						}
					}
				} else {
					result, err := executor.Execute(ctx, auth, req, opts)
					if err != nil {
						t.Fatal(err)
					}
					for _, output := range gjson.GetBytes(result.Payload, "output").Array() {
						if output.Get("type").String() == "function_call" {
							item = output
						}
					}
				}
				if item.Get("type").String() != "function_call" || item.Get("call_id").String() != "pair" || item.Get("arguments").String() != `{"message":"work"}` {
					t.Fatalf("translated fixture tool: type=%q call_id=%q arguments=%q", item.Get("type").String(), item.Get("call_id").String(), item.Get("arguments").String())
				}
				if item.Get("namespace").String() != "collaboration" || item.Get("name").String() != "spawn_agent" {
					t.Fatal("tool output did not restore the caller's namespace and short name")
				}
				if item.Get("encrypted_function_args").Exists() != enabled || enabled && item.Get("encrypted_function_args").Raw != "[]" {
					t.Fatal("plaintext marker lost its request snapshot after release and config change")
				}
				if !controller.Released() {
					t.Fatal("release fixture was not exercised")
				}
			})
		}
	}
}

func TestOpenAICompatMultiAgentPreservesExistingArgumentMarkers(t *testing.T) {
	cfg := &config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: true}}
	policy := helps.CodexPlaintextResponsePolicy(t.Context(), http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}}, cfg, sdktranslator.FormatOpenAIResponse)
	raw := []byte(`{"output":[{"type":"function_call","namespace":"collaboration","name":"spawn_agent","encrypted_function_args":["message"],"arguments":"opaque"},{"type":"function_call","namespace":"other","name":"spawn_agent","arguments":"business"}]}`)
	got := policy.TranslateNonStream(t.Context(), sdktranslator.FormatOpenAIResponse, sdktranslator.FormatOpenAIResponse, "", nil, nil, raw, nil)
	if string(got) != string(raw) {
		t.Fatal("existing markers or foreign namespaces changed")
	}
	if strings.Contains(string(got), `"encrypted_function_args":[]`) {
		t.Fatal("opaque arguments were relabeled")
	}
}
