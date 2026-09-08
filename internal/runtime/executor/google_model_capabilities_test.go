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

func googleCapabilityManager(t *testing.T, provider, baseURL string, levels []string) *coreauth.Manager {
	t.Helper()
	support := &registry.ThinkingSupport{Levels: levels}
	cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}}
	var executor coreauth.ProviderExecutor
	switch provider {
	case "vertex":
		cfg.VertexCompatAPIKey = []config.VertexCompatKey{{APIKey: "fixture", BaseURL: baseURL, Models: []config.VertexCompatModel{{Name: "gemini-capability-private", Alias: "bound-google", Thinking: support}}}}
		executor = NewGeminiVertexExecutor(cfg)
	case "gemini-interactions":
		cfg.InteractionsKey = []config.GeminiKey{{APIKey: "fixture", BaseURL: baseURL, Models: []config.GeminiModel{{Name: "gemini-capability-private", Alias: "bound-google", Thinking: support}}}}
		executor = NewGeminiInteractionsExecutor(cfg)
	default:
		cfg.GeminiKey = []config.GeminiKey{{APIKey: "fixture", BaseURL: baseURL, Models: []config.GeminiModel{{Name: "gemini-capability-private", Alias: "bound-google", Thinking: support}}}}
		executor = NewGeminiExecutor(cfg)
	}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetConfig(cfg)
	manager.RegisterExecutor(executor)
	if _, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{ID: t.Name(), Provider: provider, Attributes: map[string]string{"api_key": "fixture", "base_url": baseURL}}); errRegister != nil {
		t.Fatal(errRegister)
	}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(t.Name(), provider, []*registry.ModelInfo{{ID: "bound-google"}, {ID: "gemini-capability-private", Thinking: &registry.ThinkingSupport{Levels: []string{"low"}}}})
	t.Cleanup(func() { reg.UnregisterClient(t.Name()) })
	return manager
}

func TestGoogleTransportsUseSelectedModelCapabilities(t *testing.T) {
	for _, provider := range []string{"gemini", "gemini-interactions", "vertex"} {
		for _, stream := range []bool{false, true} {
			for _, responses := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/stream=%t/responses=%t", provider, stream, responses), func(t *testing.T) {
					var calls atomic.Int32
					type fields struct{ model, level string }
					observed := make(chan fields, 1)
					upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
						calls.Add(1)
						body, errRead := io.ReadAll(req.Body)
						if errRead != nil {
							t.Error(errRead)
							return
						}
						path := "generationConfig.thinkingConfig.thinkingLevel"
						if provider == "gemini-interactions" {
							path = "generation_config.thinking_level"
						}
						select {
						case observed <- fields{gjson.GetBytes(body, "model").String(), gjson.GetBytes(body, path).String()}:
						default:
							t.Error("unexpected extra upstream request")
						}
						result := `{"candidates":[{"content":{"role":"model","parts":[{"text":"fixture"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}`
						if provider == "gemini-interactions" {
							result = `{"id":"interaction_fixture","status":"completed","outputs":[{"type":"text","text":"fixture"}],"usage":{"total_input_tokens":1,"total_output_tokens":1,"total_tokens":2}}`
						}
						if stream {
							w.Header().Set("Content-Type", "text/event-stream")
							if provider == "gemini-interactions" {
								_, _ = fmt.Fprintf(w, "event: interaction.completed\ndata: {\"event_type\":\"interaction.completed\",\"interaction\":%s}\n\n", result)
							} else {
								_, _ = fmt.Fprintf(w, "data: %s\n\n", result)
							}
						} else {
							w.Header().Set("Content-Type", "application/json")
							_, _ = io.WriteString(w, result)
						}
					}))
					t.Cleanup(upstream.Close)
					manager := googleCapabilityManager(t, provider, upstream.URL, []string{"high"})
					source := translator.FormatGemini
					payload := []byte(`{"model":"bound-google","contents":[{"role":"user","parts":[{"text":"fixture"}]}],"generationConfig":{"thinkingConfig":{"thinkingLevel":"high"}}}`)
					if provider == "gemini-interactions" {
						source = translator.FormatInteractions
						payload = []byte(`{"model":"bound-google","input":"fixture","generation_config":{"thinking_level":"high"}}`)
					}
					if responses {
						source = translator.FormatOpenAIResponse
						payload = []byte(`{"model":"bound-google","input":"fixture","reasoning":{"effort":"max"}}`)
					}
					request := core.Request{Model: "bound-google", Payload: payload}
					opts := core.Options{SourceFormat: source, OriginalRequest: payload}
					if stream {
						result, errStream := manager.ExecuteStream(t.Context(), []string{provider}, request, opts)
						if errStream != nil {
							t.Fatal(errStream)
						}
						for chunk := range result.Chunks {
							if chunk.Err != nil {
								t.Fatal(chunk.Err)
							}
						}
					} else if _, errExecute := manager.Execute(t.Context(), []string{provider}, request, opts); errExecute != nil {
						t.Fatal(errExecute)
					}
					select {
					case got := <-observed:
						if got.model != "gemini-capability-private" || got.level != "high" || calls.Load() != 1 {
							t.Fatalf("wrong selected model declaration: %+v, calls=%d", got, calls.Load())
						}
					default:
						t.Fatal("missing upstream request")
					}
				})
			}
		}
	}
}

func TestGoogleCountsValidateSelectedThinkingBeforeNetwork(t *testing.T) {
	for _, provider := range []string{"gemini", "gemini-interactions", "vertex"} {
		t.Run(provider, func(t *testing.T) {
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
			t.Cleanup(upstream.Close)
			manager := googleCapabilityManager(t, provider, upstream.URL, []string{"low"})
			registry.GetGlobalRegistry().RegisterClient(t.Name(), provider, []*registry.ModelInfo{{ID: "bound-google"}, {ID: "gemini-capability-private", Thinking: &registry.ThinkingSupport{Levels: []string{"high"}}}})
			request := core.Request{Model: "bound-google", Payload: []byte(`{"contents":[{"role":"user","parts":[{"text":"fixture"}]}],"generationConfig":{"thinkingConfig":{"thinkingLevel":"high"}}}`)}
			_, err := manager.ExecuteCount(t.Context(), []string{provider}, request, core.Options{SourceFormat: translator.FormatGemini})
			var issue *thinking.ThinkingError
			if !errors.As(err, &issue) || issue.Code != thinking.ErrLevelNotSupported || calls.Load() != 0 {
				t.Fatalf("count skipped selected native validation: %v, calls=%d", err, calls.Load())
			}
		})
	}
}
