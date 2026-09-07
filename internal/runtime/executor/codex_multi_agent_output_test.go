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
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestOpenAICompatMultiAgentPlaintextToolMarkers(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("stream=%t/enabled=%t", stream, enabled), func(t *testing.T) {
				cfg := &config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: enabled}}
				controller := core.NewRequestBodyReleaseController(1, []byte("<released>"))
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					body, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
					}
					name := gjson.GetBytes(body, "tools.0.function.name").String()
					if name != "collaboration__spawn_agent" {
						t.Error("collaboration declaration was not translated")
					}
					if gjson.GetBytes(body, "tools.0.function.parameters.properties.message.encrypted").Exists() == enabled {
						t.Error("direct SDK tool schema did not follow the optimization policy")
					}
					cfg.Codex.OptimizeMultiAgentV2 = !enabled
					controller.Release()
					if stream {
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = io.WriteString(w, "data: {\"id\":\"result\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"pair\",\"type\":\"function\",\"function\":{\"name\":\"collaboration__spawn_agent\",\"arguments\":\"{\\\"message\\\":\\\"work\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n")
					} else {
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, `{"id":"result","choices":[{"index":0,"message":{"role":"assistant","tool_calls":[{"id":"pair","type":"function","function":{"name":"collaboration__spawn_agent","arguments":"{\"message\":\"work\"}"}}]},"finish_reason":"tool_calls"}]}`)
					}
				}))
				defer server.Close()
				executor := NewOpenAICompatExecutor("openai-compatibility", cfg)
				auth := &cliproxyauth.Auth{Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
				req := core.Request{Model: "gpt-5.4", Payload: []byte(`{"model":"gpt-5.4","input":[],"tools":[{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"spawn_agent","parameters":{"type":"object","properties":{"message":{"type":"string","encrypted":true}}}}]}]}`)}
				opts := core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Headers: http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}}, Metadata: map[string]any{core.BodyReleaseControllerMetadataKey: controller}}
				var item gjson.Result
				if stream {
					result, err := executor.ExecuteStream(t.Context(), auth, req, opts)
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
								item = event.Get("response.output.0")
							}
						}
					}
				} else {
					result, err := executor.Execute(t.Context(), auth, req, opts)
					if err != nil {
						t.Fatal(err)
					}
					item = gjson.GetBytes(result.Payload, "output.0")
				}
				if item.Get("type").String() != "function_call" || item.Get("call_id").String() != "pair" || item.Get("arguments").String() != `{"message":"work"}` {
					t.Fatalf("translated fixture tool: type=%q call_id=%q arguments=%q", item.Get("type").String(), item.Get("call_id").String(), item.Get("arguments").String())
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
