package executor

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestStructuredStreamPolicyRefusalDoesNotRetryOrCoolCredentials(t *testing.T) {
	for _, provider := range []string{"claude", "policy-compat"} {
		for _, code := range []string{"misalignment_policy_violation", "cyber_policy", "content_policy_violation"} {
			for _, field := range []string{"code", "type"} {
				for _, preamble := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/%s/preamble=%t", provider, code, field, preamble), func(t *testing.T) {
						var calls atomic.Int64
						server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							calls.Add(1)
							if _, err := io.Copy(io.Discard, r.Body); err != nil {
								t.Error(err)
								return
							}
							w.Header().Set("Content-Type", "text/event-stream")
							if preamble {
								prefix := `{"id":"refused","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant"}}]}`
								if provider == "claude" {
									prefix = `{"type":"message_start","message":{"id":"refused","role":"assistant"}}`
								}
								_, _ = io.WriteString(w, "data: "+prefix+"\n\n")
							}
							_, _ = fmt.Fprintf(w, "data: "+`{"type":"error","error":{"message":"request denied",%q:%q}}`+"\n\n", field, code)
						}))
						defer server.Close()
						cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}}
						manager := coreauth.NewManager(nil, &coreauth.FillFirstSelector{}, nil)
						manager.SetConfig(cfg)
						manager.SetRetryConfig(2, 0, 0)
						if provider == "claude" {
							manager.RegisterExecutor(NewClaudeExecutor(cfg))
						} else {
							manager.RegisterExecutor(NewOpenAICompatExecutor(provider, cfg))
						}
						var ids []string
						for i := range 2 {
							id := fmt.Sprintf("%s/%d", t.Name(), i)
							ids = append(ids, id)
							if _, err := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{ID: id, Provider: provider, Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}); err != nil {
								t.Fatal(err)
							}
							registry.GetGlobalRegistry().RegisterClient(id, provider, []*registry.ModelInfo{{ID: "policy-model"}})
							t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
						}
						result, err := manager.ExecuteStream(t.Context(), []string{provider}, core.Request{Model: "policy-model", Payload: []byte(`{"model":"policy-model","input":"question"}`)}, core.Options{SourceFormat: translator.FormatOpenAIResponse})
						if result != nil {
							for chunk := range result.Chunks {
								if chunk.Err != nil && !core.IsSuccessfulStreamTerminalChunk(chunk) {
									err = chunk.Err
								}
							}
						}
						if err == nil || !coreauth.IsPolicyRefusalError(err) || calls.Load() != 1 {
							t.Fatalf("policy error lost its class or made %d upstream calls", calls.Load())
						}
						for _, id := range ids {
							auth, ok := manager.GetByID(id)
							if !ok || auth.Unavailable || !auth.NextRetryAfter.IsZero() {
								t.Fatal("policy refusal cooled a credential")
							}
							for _, state := range auth.ModelStates {
								if state != nil && (state.Unavailable || !state.NextRetryAfter.IsZero()) {
									t.Fatal("policy refusal cooled a model")
								}
							}
						}
					})
				}
			}
		}
	}
}
