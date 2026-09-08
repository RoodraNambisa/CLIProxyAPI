package executor

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestClaudeTransportsUseSelectedModelCapabilities(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, scenario := range []string{"native", "responses", "unsupported"} {
			t.Run(fmt.Sprintf("stream=%t/%s", stream, scenario), func(t *testing.T) {
				var calls atomic.Int32
				type fields struct{ model, kind, effort string }
				observed := make(chan fields, 1)
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					calls.Add(1)
					body, errRead := io.ReadAll(req.Body)
					if errRead != nil {
						t.Error(errRead)
						return
					}
					select {
					case observed <- fields{gjson.GetBytes(body, "model").String(), gjson.GetBytes(body, "thinking.type").String(), gjson.GetBytes(body, "output_config.effort").String()}:
					default:
						t.Error("unexpected extra upstream request")
					}
					if stream {
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = io.WriteString(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_fixture\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-capability-private\",\"content\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
					} else {
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, `{"id":"msg_fixture","type":"message","role":"assistant","model":"claude-capability-private","content":[],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`)
					}
				}))
				t.Cleanup(upstream.Close)
				declared, aggregate := "max", "low"
				if scenario == "unsupported" {
					declared, aggregate = "low", "max"
				}
				cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}, ClaudeKey: []config.ClaudeKey{{APIKey: "fixture", BaseURL: upstream.URL, Models: []config.ClaudeModel{{Name: "claude-capability-private", Alias: "bound-claude", Thinking: &registry.ThinkingSupport{Levels: []string{declared}}}}}}}
				manager := coreauth.NewManager(nil, nil, nil)
				manager.SetConfig(cfg)
				manager.RegisterExecutor(NewClaudeExecutor(cfg))
				if _, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{ID: t.Name(), Provider: "claude", Attributes: map[string]string{"api_key": "fixture", "base_url": upstream.URL}}); errRegister != nil {
					t.Fatal(errRegister)
				}
				reg := registry.GetGlobalRegistry()
				reg.RegisterClient(t.Name(), "claude", []*registry.ModelInfo{{ID: "bound-claude"}, {ID: "claude-capability-private", Type: "claude", Thinking: &registry.ThinkingSupport{Levels: []string{aggregate}}}})
				t.Cleanup(func() { reg.UnregisterClient(t.Name()) })
				source := translator.FormatClaude
				payload := []byte(`{"model":"bound-claude","messages":[{"role":"user","content":"fixture"}],"max_tokens":1024,"thinking":{"type":"adaptive"},"output_config":{"effort":"max"}}`)
				if scenario == "responses" {
					source = translator.FormatOpenAIResponse
					payload = []byte(`{"model":"bound-claude","input":"fixture","reasoning":{"effort":"xhigh"}}`)
				}
				request := core.Request{Model: "bound-claude", Payload: payload}
				opts := core.Options{SourceFormat: source, OriginalRequest: payload}
				var err error
				if stream {
					var result *core.StreamResult
					result, err = manager.ExecuteStream(t.Context(), []string{"claude"}, request, opts)
					if result != nil {
						for chunk := range result.Chunks {
							if chunk.Err != nil {
								err = chunk.Err
							}
						}
					}
				} else {
					_, err = manager.Execute(t.Context(), []string{"claude"}, request, opts)
				}
				if scenario == "unsupported" {
					var issue *thinking.ThinkingError
					if !errors.As(err, &issue) || issue.Code != thinking.ErrLevelNotSupported || calls.Load() != 0 {
						t.Fatalf("native unsupported level lost validation: %v", err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				select {
				case got := <-observed:
					if got.model != "claude-capability-private" || got.kind != "adaptive" || got.effort != "max" || calls.Load() != 1 {
						t.Fatalf("incorrect selected Claude declaration: %+v", got)
					}
				default:
					t.Fatal("no upstream request")
				}
			})
		}
	}
}
