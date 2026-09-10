package executor

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestOpenAICompatToolResultsTextOnlyActualHTTPAndStream(t *testing.T) {
	for _, mode := range []string{"http", "stream", "compact", "chat", "chat-stream"} {
		for _, policy := range []string{"inherit", "text", "vision", "text-audio", "cleared", "pool-retry", "direct-text"} {
			t.Run(mode+"/"+policy, func(t *testing.T) {
				var bodies [][]byte
				var bodiesMu sync.Mutex
				stream := strings.HasSuffix(mode, "stream")
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					body, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
						return
					}
					bodiesMu.Lock()
					bodies = append(bodies, body)
					call := len(bodies)
					bodiesMu.Unlock()
					if policy == "pool-retry" && call == 1 {
						w.WriteHeader(http.StatusInternalServerError)
						_, _ = fmt.Fprint(w, `{"error":{"message":"fixture failure"}}`)
						return
					}
					if mode == "compact" {
						_, _ = fmt.Fprint(w, `{"id":"compact_fixture","object":"response.compaction","output":[]}`)
					} else if stream {
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = fmt.Fprint(w, "data: {\"id\":\"fixture\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"}}]}\n\ndata: {\"id\":\"fixture\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
					} else {
						_, _ = fmt.Fprint(w, `{"id":"fixture","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`)
					}
				}))
				t.Cleanup(server.Close)
				modalities := []string{"text"}
				if policy == "inherit" {
					modalities = nil
				} else if policy == "vision" {
					modalities = []string{"text", "image"}
				} else if policy == "text-audio" {
					modalities = []string{"text", "audio"}
				}
				models := []config.OpenAICompatibilityModel{{Name: "kimi-k2", Alias: "shared", InputModalities: modalities}}
				if policy == "pool-retry" {
					models = append(models, config.OpenAICompatibilityModel{Name: "gpt-5.5", Alias: "shared", InputModalities: []string{"text", "image"}})
				}
				cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}, OpenAICompatibility: []config.OpenAICompatibility{{Name: "fixture", BaseURL: server.URL, Models: models}}}
				executor := NewOpenAICompatExecutor("fixture", cfg)
				manager := coreauth.NewManager(nil, &coreauth.FillFirstSelector{}, nil)
				manager.SetConfig(cfg)
				manager.RegisterExecutor(executor)
				if policy == "cleared" {
					manager.SetConfig(&config.Config{OpenAICompatibility: []config.OpenAICompatibility{{Name: "fixture", Models: []config.OpenAICompatibilityModel{{Name: "kimi-k2", Alias: "shared"}}}}})
				}
				auth := &coreauth.Auth{ID: t.Name(), Provider: "fixture", Attributes: map[string]string{"api_key": "test", "base_url": server.URL, "compat_name": "fixture", "provider_key": "fixture"}}
				if _, err := manager.Register(coreauth.WithSkipPersist(t.Context()), auth); err != nil {
					t.Fatal(err)
				}
				registry.GetGlobalRegistry().RegisterClient(auth.ID, "fixture", []*registry.ModelInfo{{ID: "shared"}})
				t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
				body := []byte(`{"model":"shared","input":[{"type":"function_call","id":"fc_fixture","call_id":"call_fixture","name":"fixture","arguments":"{}"},{"type":"function_call_output","call_id":"call_fixture","output":[{"type":"input_text","text":"before"},{"type":"input_image","image_url":"data:image/png;base64,AA=="},{"type":"input_text","text":"after"}]}]}`)
				opts := core.Options{SourceFormat: translator.FormatOpenAIResponse}
				if strings.HasPrefix(mode, "chat") {
					body = []byte(`{"model":"shared","messages":[{"role":"assistant","tool_calls":[{"id":"call_fixture","type":"function","function":{"name":"fixture","arguments":"{}"}}]},{"role":"tool","tool_call_id":"call_fixture","content":[{"type":"text","text":"before"},{"type":"image_url","image_url":{"url":"data:image/png;base64,AA=="}},{"type":"text","text":"after"}]}]}`)
					opts.SourceFormat = translator.FormatOpenAI
				}
				original := bytes.Clone(body)
				req := core.Request{Model: "shared", Payload: body}
				if mode == "compact" {
					opts.Alt = "responses/compact"
				}
				var err error
				if stream {
					var result *core.StreamResult
					if policy == "direct-text" {
						result, err = executor.ExecuteStream(t.Context(), auth, req, opts)
					} else {
						result, err = manager.ExecuteStream(t.Context(), []string{"fixture"}, req, opts)
					}
					if result != nil {
						for chunk := range result.Chunks {
							if chunk.Err != nil {
								err = chunk.Err
							}
						}
					}
				} else if policy == "direct-text" {
					_, err = executor.Execute(t.Context(), auth, req, opts)
				} else {
					_, err = manager.Execute(t.Context(), []string{"fixture"}, req, opts)
				}
				if err != nil {
					t.Fatal(err)
				}
				bodiesMu.Lock()
				observedBodies := append([][]byte(nil), bodies...)
				bodiesMu.Unlock()
				wantCalls := 1
				if policy == "pool-retry" {
					wantCalls = 2
				}
				if len(observedBodies) != wantCalls || !bytes.Equal(body, original) {
					t.Fatal("text-only handling changed upstream call count or source request ownership")
				}
				for attempt, body := range observedBodies {
					path, callPath := `messages.#(role=="tool").content`, `messages.#(role=="tool").tool_call_id`
					if mode == "compact" {
						path, callPath = "input.1.output", "input.1.call_id"
					}
					content := gjson.GetBytes(body, path)
					textOnly := policy == "text" || policy == "direct-text" || (policy == "pool-retry" && attempt == 0)
					if (content.Type == gjson.String) != textOnly || gjson.GetBytes(body, callPath).Str != "call_fixture" {
						t.Fatal("outbound content used the wrong selected model policy or changed pairing")
					}
					if textOnly && (!strings.Contains(content.Str, "[image omitted: unsupported by upstream]") || !strings.HasPrefix(content.Str, "before") || !strings.HasSuffix(content.Str, "after")) {
						t.Fatal("outbound text lost ordered text or the image placeholder")
					}
					if !textOnly && !strings.Contains(content.Raw, "data:image/png;base64,AA==") {
						t.Fatal("an inherited, cleared or multimodal model lost its tool image")
					}
				}
			})
		}
	}
}
