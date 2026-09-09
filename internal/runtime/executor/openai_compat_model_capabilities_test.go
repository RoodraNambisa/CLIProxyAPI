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

func compatCapabilityManager(t *testing.T, baseURL string, rules config.PayloadConfig, levels []string) *coreauth.Manager {
	t.Helper()
	cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}, Payload: rules, OpenAICompatibility: []config.OpenAICompatibility{{Name: "capability-compat", BaseURL: baseURL, Models: []config.OpenAICompatibilityModel{{Name: "private-compat", Alias: "bound-compat", Thinking: &registry.ThinkingSupport{Levels: levels}}}}}}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetConfig(cfg)
	manager.RegisterExecutor(NewOpenAICompatExecutor("capability-compat", cfg))
	if _, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{ID: t.Name(), Provider: "capability-compat", Attributes: map[string]string{"api_key": "fixture", "base_url": baseURL, "compat_name": "capability-compat"}}); errRegister != nil {
		t.Fatal(errRegister)
	}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(t.Name(), "capability-compat", []*registry.ModelInfo{{ID: "bound-compat"}, {ID: "private-compat", Type: "openai", Thinking: &registry.ThinkingSupport{Levels: []string{"low"}}}})
	t.Cleanup(func() { reg.UnregisterClient(t.Name()) })
	return manager
}

func compatCapabilityPayload(source translator.Format, effort string) []byte {
	if source == translator.FormatOpenAIResponse {
		if effort == "" {
			return []byte(`{"model":"bound-compat","input":"fixture"}`)
		}
		return []byte(fmt.Sprintf(`{"model":"bound-compat","input":"fixture","reasoning":{"effort":%q}}`, effort))
	}
	if effort == "" {
		return []byte(`{"model":"bound-compat","messages":[{"role":"user","content":"fixture"}]}`)
	}
	return []byte(fmt.Sprintf(`{"model":"bound-compat","messages":[{"role":"user","content":"fixture"}],"reasoning_effort":%q}`, effort))
}

func TestOpenAICompatCapabilitiesPreservePayloadAndSuffixAuthority(t *testing.T) {
	for _, mode := range []string{"http", "sse", "responses", "compact"} {
		for _, scenario := range []string{"native", "source", "override", "same-override", "filter", "filter-absent", "unrelated", "suffix"} {
			t.Run(mode+"/"+scenario, func(t *testing.T) {
				effortPath := "reasoning_effort"
				source := translator.FormatOpenAI
				if mode == "responses" || mode == "compact" {
					source = translator.FormatOpenAIResponse
				}
				if mode == "compact" {
					effortPath = "reasoning.effort"
				}
				var calls atomic.Int32
				observed := make(chan string, 1)
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					calls.Add(1)
					body, errRead := io.ReadAll(req.Body)
					if errRead != nil {
						t.Error(errRead)
						return
					}
					select {
					case observed <- gjson.GetBytes(body, effortPath).String():
					default:
						t.Error("unexpected extra upstream attempt")
					}
					if mode == "sse" {
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = io.WriteString(w, "data: {\"id\":\"chatcmpl_fixture\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"fixture\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
					} else {
						w.Header().Set("Content-Type", "application/json")
						if mode == "compact" {
							_, _ = io.WriteString(w, `{"id":"resp_fixture","object":"response.compaction","output":[]}`)
						} else {
							_, _ = io.WriteString(w, `{"id":"chatcmpl_fixture","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"fixture"},"finish_reason":"stop"}]}`)
						}
					}
				}))
				t.Cleanup(upstream.Close)
				models := []config.PayloadModelRule{{Name: "bound-compat"}}
				rules := config.PayloadConfig{}
				requestEffort, want := "max", "max"
				model := "bound-compat"
				switch scenario {
				case "source":
					requestEffort = "low"
				case "override":
					want = "high"
					rules.Override = []config.PayloadRule{{Models: models, Params: map[string]any{effortPath: "high"}}}
				case "same-override":
					requestEffort, want = "low", "low"
					rules.Override = []config.PayloadRule{{Models: models, Params: map[string]any{effortPath: "low"}}}
				case "filter", "filter-absent":
					want = ""
					if scenario == "filter-absent" {
						requestEffort = ""
					}
					rules.Filter = []config.PayloadFilterRule{{Models: models, Params: []string{effortPath}}}
				case "unrelated":
					requestEffort = "low"
					rules.Override = []config.PayloadRule{{Models: models, Params: map[string]any{"metadata.reasoning_effort": "low"}}}
				case "suffix":
					model, want = "bound-compat(high)", "high"
					rules.Override = []config.PayloadRule{{Models: models, Params: map[string]any{effortPath: "low"}}}
				}
				manager := compatCapabilityManager(t, upstream.URL+"/v1", rules, []string{"low", "high", "max"})
				request := core.Request{Model: model, Payload: compatCapabilityPayload(source, requestEffort)}
				opts := core.Options{SourceFormat: source, OriginalRequest: compatCapabilityPayload(source, "max")}
				if mode == "compact" {
					opts.Alt = "responses/compact"
				}
				if mode == "sse" {
					result, errStream := manager.ExecuteStream(t.Context(), []string{"capability-compat"}, request, opts)
					if errStream != nil {
						t.Fatal(errStream)
					}
					for chunk := range result.Chunks {
						if chunk.Err != nil {
							t.Fatal(chunk.Err)
						}
					}
				} else if _, errExecute := manager.Execute(t.Context(), []string{"capability-compat"}, request, opts); errExecute != nil {
					t.Fatal(errExecute)
				}
				select {
				case got := <-observed:
					if got != want || calls.Load() != 1 {
						t.Fatalf("effort=%q, want=%q, attempts=%d", got, want, calls.Load())
					}
				default:
					t.Fatal("missing upstream request")
				}
			})
		}
	}
}

func TestOpenAICompatCountUsesSelectedCapabilities(t *testing.T) {
	manager := compatCapabilityManager(t, "http://127.0.0.1:1", config.PayloadConfig{}, []string{"low"})
	registry.GetGlobalRegistry().RegisterClient(t.Name(), "capability-compat", []*registry.ModelInfo{{ID: "bound-compat"}, {ID: "private-compat", Type: "openai", Thinking: &registry.ThinkingSupport{Levels: []string{"high"}}}})
	_, err := manager.ExecuteCount(t.Context(), []string{"capability-compat"}, core.Request{Model: "bound-compat", Payload: compatCapabilityPayload(translator.FormatOpenAI, "high")}, core.Options{SourceFormat: translator.FormatOpenAI})
	var issue *thinking.ThinkingError
	if !errors.As(err, &issue) || issue.Code != thinking.ErrLevelNotSupported {
		t.Fatalf("count skipped selected capability validation: %v", err)
	}
}
