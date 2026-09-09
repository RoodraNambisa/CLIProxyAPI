package executor

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func claudeCompatibilityManager(t *testing.T, baseURL string, compat bool) *coreauth.Manager {
	t.Helper()
	cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}, ClaudeKey: []config.ClaudeKey{{APIKey: "fixture", BaseURL: baseURL, Models: []config.ClaudeModel{{Name: "claude-compat-private", Alias: "bound-claude", IsCompat: compat}}}}}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetConfig(cfg)
	manager.RegisterExecutor(NewClaudeExecutor(cfg))
	if _, err := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{ID: t.Name(), Provider: "claude", Attributes: map[string]string{"api_key": "fixture", "base_url": baseURL}}); err != nil {
		t.Fatal(err)
	}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(t.Name(), "claude", []*registry.ModelInfo{{ID: "bound-claude", IsCompat: !compat}})
	t.Cleanup(func() { reg.UnregisterClient(t.Name()) })
	return manager
}

func writeClaudeCompatibilityFixture(w http.ResponseWriter, operation string) {
	if operation == "count" {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"input_tokens":3}`)
	} else if operation == "stream" {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_fixture\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-compat-private\",\"content\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
	} else {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"msg_fixture","type":"message","role":"assistant","model":"claude-compat-private","content":[],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`)
	}
}

func runClaudeCompatibilityRequest(t *testing.T, manager *coreauth.Manager, operation string, raw []byte, source translator.Format) {
	t.Helper()
	req := core.Request{Model: "bound-claude", Payload: raw}
	opts := core.Options{SourceFormat: source, OriginalRequest: raw}
	switch operation {
	case "count":
		if _, err := manager.ExecuteCount(t.Context(), []string{"claude"}, req, opts); err != nil {
			t.Fatal(err)
		}
	case "stream":
		result, err := manager.ExecuteStream(t.Context(), []string{"claude"}, req, opts)
		if err != nil {
			t.Fatal(err)
		}
		for chunk := range result.Chunks {
			if chunk.Err != nil {
				t.Fatal(chunk.Err)
			}
		}
	default:
		if _, err := manager.Execute(t.Context(), []string{"claude"}, req, opts); err != nil {
			t.Fatal(err)
		}
	}
}

func TestClaudeModelCompatibilityPreservesChatThinkingAcrossOperations(t *testing.T) {
	for _, operation := range []string{"execute", "stream", "count"} {
		for _, compat := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/compat=%t", operation, compat), func(t *testing.T) {
				captured := make(chan bool, 2)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					body, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
						return
					}
					var thought, tool bool
					thoughtCount := 0
					var parts []gjson.Result
					for _, message := range gjson.GetBytes(body, "messages").Array() {
						if message.Get("role").String() == "assistant" {
							parts = message.Get("content").Array()
							for _, part := range parts {
								if part.Get("type").String() == "thinking" {
									thoughtCount++
									thought = part.Get("thinking").String() == "kept" && part.Get("signature").Raw == `""`
								}
								if part.Get("type").String() == "tool_use" {
									tool = part.Get("id").String() == "paired" && part.Get("name").String() == "lookup"
								}
							}
						}
					}
					wantThoughtCount := 0
					if compat {
						wantThoughtCount = 1
					}
					valid := thought == compat && thoughtCount == wantThoughtCount && tool && !bytes.Contains(body, []byte("untrusted"))
					if compat {
						valid = valid && len(parts) == 3 && parts[0].Get("type").String() == "thinking" &&
							parts[1].Get("text").String() == "answer" && parts[2].Get("type").String() == "tool_use"
					}
					captured <- valid
					writeClaudeCompatibilityFixture(w, operation)
				}))
				t.Cleanup(server.Close)
				manager := claudeCompatibilityManager(t, server.URL, compat)
				raw := []byte(`{"messages":[{"role":"user","content":"question","reasoning_content":"untrusted"},{"role":"assistant","content":"answer","reasoning_content":"kept","tool_calls":[{"type":"function","id":"paired","function":{"name":"lookup","arguments":"{}"}}]},{"role":"tool","tool_call_id":"paired","content":"result"}]}`)
				before := bytes.Clone(raw)
				runClaudeCompatibilityRequest(t, manager, operation, raw, translator.FormatOpenAI)
				select {
				case valid := <-captured:
					if !valid {
						t.Fatal("selected compatibility changed thinking ownership or tool order")
					}
				default:
					t.Fatal("request did not reach the local upstream")
				}
				if !bytes.Equal(raw, before) {
					t.Fatal("compatibility changed the original request")
				}
			})
		}
	}
}

func TestClaudeModelCompatibilityReplaysResponsesAcrossOperations(t *testing.T) {
	const native = "CAISFwoVCBAqAQEyDmNsYXVkZS1maXh0dXJlGAE="
	for _, operation := range []string{"execute", "stream", "count"} {
		for _, compat := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/compat=%t", operation, compat), func(t *testing.T) {
				captured := make(chan bool, 2)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					body, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
						return
					}
					parts := gjson.GetBytes(body, "messages.1.content").Array()
					wantLength := 4
					if compat {
						wantLength++
					}
					valid := gjson.GetBytes(body, "messages.#").Int() == 3 && len(parts) == wantLength
					if valid {
						valid = parts[0].Get("signature").String() == native && parts[0].Get("thinking").String() == "native" &&
							parts[1].Get("text").String() == "answer" && parts[len(parts)-2].Get("data").String() == " opaque data " &&
							parts[len(parts)-1].Get("id").String() == "paired" && parts[len(parts)-1].Get("name").String() == "lookup" &&
							gjson.GetBytes(body, "messages.2.content.0.tool_use_id").String() == "paired"
						if compat {
							valid = valid && parts[2].Get("thinking").String() == "unsigned" && parts[2].Get("signature").Raw == `""`
						}
					}
					captured <- valid
					responseKind := "stream"
					if operation == "count" {
						responseKind = "count"
					}
					writeClaudeCompatibilityFixture(w, responseKind)
				}))
				t.Cleanup(server.Close)
				manager := claudeCompatibilityManager(t, server.URL, compat)
				raw := []byte(fmt.Sprintf(`{"input":[{"role":"user","content":"question"},{"type":"reasoning","encrypted_content":%q,"summary":[{"text":"native"}]},{"role":"assistant","content":"answer"},{"type":"reasoning","summary":[{"text":"unsigned"}]},{"type":"reasoning","encrypted_content":"claude-redacted-thinking: opaque data "},{"type":"function_call","name":"lookup","call_id":"paired","arguments":"{}"},{"type":"function_call_output","call_id":"paired","output":"done"}]}`, native))
				before := bytes.Clone(raw)
				runClaudeCompatibilityRequest(t, manager, operation, raw, translator.FormatOpenAIResponse)
				select {
				case valid := <-captured:
					if !valid {
						t.Fatal("Responses replay lost a signature or ignored the selected model policy")
					}
				default:
					t.Fatal("Responses request did not reach the local upstream")
				}
				if !bytes.Equal(raw, before) {
					t.Fatal("Responses replay mutated the original request")
				}
			})
		}
	}
}
