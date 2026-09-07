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
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestGeminiResponsesToolIdentityAndMultiAgentAfterRelease(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%t", enabled), func(t *testing.T) {
			runGoogleResponsesToolIdentity(t, "gemini", enabled)
		})
	}
}

func runGoogleResponsesToolIdentity(t *testing.T, provider string, enabled bool) {
	t.Helper()
	runGoogleResponsesToolIdentityWithNamespaceField(t, provider, enabled, "tools")
}

func runGoogleResponsesToolIdentityWithNamespaceField(t *testing.T, provider string, enabled bool, namespaceField string) {
	t.Helper()
	runGoogleResponsesToolOutputFixture(t, provider, enabled, namespaceField, "function")
}

func runGoogleResponsesToolOutputFixture(t *testing.T, provider string, enabled bool, namespaceField, toolKind string) {
	t.Helper()
	isAntigravity := strings.HasPrefix(provider, "antigravity")
	for _, stream := range []bool{false, true} {
		for _, explicitOriginal := range []bool{false, true} {
			t.Run(fmt.Sprintf("stream=%t/original=%t", stream, explicitOriginal), func(t *testing.T) {
				cfg := &config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: enabled}}
				controller := core.NewRequestBodyReleaseController(1, []byte("<released>"))
				tokenCalls := 0
				upstreamCalls := 0
				ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", streamTerminalRoundTripFunc(func(r *http.Request) (*http.Response, error) {
					if r.URL.Host == "oauth-fixture.invalid" {
						tokenCalls++
						return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"access_token":"fixture-token","token_type":"Bearer","expires_in":3600}`))}, nil
					}
					body, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
					}
					upstreamCalls++
					namePath := "tools.0.functionDeclarations.0.name"
					if provider == "gemini-interactions" {
						namePath = "tools.0.name"
					}
					if isAntigravity {
						namePath = "request." + namePath
					}
					if gjson.GetBytes(body, namePath).String() != "collaboration__spawn_agent" {
						t.Error("namespace declaration did not reach the fake upstream")
					}
					cfg.Codex.OptimizeMultiAgentV2 = !enabled
					if provider == "antigravity-credits" && upstreamCalls == 1 {
						w := httptest.NewRecorder()
						w.WriteHeader(http.StatusTooManyRequests)
						_, _ = io.WriteString(w, `{"error":{"status":"RESOURCE_EXHAUSTED","message":"QUOTA_EXHAUSTED"}}`)
						return w.Result(), nil
					}
					if provider == "antigravity-credits" && !strings.Contains(string(body), `"enabledCreditTypes":["GOOGLE_ONE_AI"]`) {
						t.Error("credits fallback was not exercised")
					}
					controller.Release()
					w := httptest.NewRecorder()
					response := `{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"collaboration__spawn_agent","args":{"message":"work"}}}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}`
					if toolKind == "custom" {
						response = strings.Replace(response, `"args":{"message":"work"}`, `"args":{"input":"work"}`, 1)
					}
					if isAntigravity {
						response = `{"response":` + response + `}`
					}
					if provider == "gemini-interactions" {
						step := `{"id":"pair","type":"function_call","name":"collaboration__spawn_agent","arguments":{"message":"work"}}`
						response = `{"id":"result","status":"completed","steps":[` + step + `],"usage":{"total_input_tokens":1,"total_output_tokens":1,"total_tokens":2}}`
						if stream {
							w.Header().Set("Content-Type", "text/event-stream")
							for _, event := range []string{`{"event_type":"step.start","index":0,"step":` + step + `}`, `{"event_type":"step.stop","index":0}`, `{"event_type":"interaction.completed","interaction":` + response + `}`} {
								_, _ = io.WriteString(w, "data: "+event+"\n\n")
							}
							return w.Result(), nil
						}
					}
					if stream || provider == "antigravity-claude" {
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = io.WriteString(w, "data: "+response+"\n\n")
					} else {
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, response)
					}
					return w.Result(), nil
				}))
				executor, auth := newGoogleMultiAgentFixtureExecutor(t, provider, cfg)
				req := core.Request{Model: "gemini-2.5-flash", Payload: []byte(`{"input":[],"tools":[{"type":"namespace","name":"collaboration","` + namespaceField + `":[{"type":"` + toolKind + `","name":"spawn_agent","parameters":{"type":"object"}}]}]}`)}
				if provider == "antigravity-claude" {
					req.Model = "claude-sonnet-4-6"
				}
				opts := core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Headers: http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}}, Metadata: map[string]any{core.BodyReleaseControllerMetadataKey: controller}}
				if explicitOriginal {
					opts.OriginalRequest = bytes.Clone(req.Payload)
				}
				var items []gjson.Result
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
							event := gjson.ParseBytes(helps.JSONPayload(line))
							if kind := event.Get("item.type").String(); kind == "function_call" || kind == "custom_tool_call" {
								items = append(items, event.Get("item"))
							} else if event.Get("type").String() == "response.completed" {
								items = append(items, event.Get("response.output.0"))
							}
						}
					}
				} else {
					result, err := executor.Execute(ctx, auth, req, opts)
					if err != nil {
						t.Fatal(err)
					}
					items = append(items, gjson.GetBytes(result.Payload, "output.0"))
				}
				if len(items) != 1 && !stream || len(items) != 3 && stream || !controller.Released() {
					t.Fatalf("identity events = %d, released = %t", len(items), controller.Released())
				}
				wantTokens := 0
				if provider == "vertex-service-account" {
					wantTokens = 1
				}
				if tokenCalls != wantTokens {
					t.Fatalf("credential acquisition calls = %d, want %d", tokenCalls, wantTokens)
				}
				wantCalls := 1
				if provider == "antigravity-credits" {
					wantCalls = 2
				}
				if upstreamCalls != wantCalls {
					t.Fatalf("upstream calls = %d, want %d", upstreamCalls, wantCalls)
				}
				for _, item := range items {
					wantMarker := enabled && toolKind == "function"
					if item.Get("encrypted_function_args").Exists() != wantMarker || wantMarker && item.Get("encrypted_function_args").Raw != "[]" {
						t.Fatal("plaintext marker did not keep the request snapshot after release and configuration update")
					}
					if toolKind == "custom" && (item.Get("type").String() != "custom_tool_call" || item.Get("id").String() != "ctc_"+item.Get("call_id").String() || item.Get("arguments").Exists() || item.Get("status").String() == "completed" && item.Get("input").String() != "work") {
						t.Fatal("released custom output retained the function representation or lost input")
					}
					if item.Get("name").String() != "spawn_agent" || item.Get("namespace").String() != "collaboration" || item.Get("call_id").String() == "" || item.Get("call_id").String() != items[0].Get("call_id").String() {
						t.Fatal("tool identity was lost after request release")
					}
				}
			})
		}
	}
}

func TestVertexResponsesToolIdentityAndMultiAgentAfterRelease(t *testing.T) {
	for _, provider := range []string{"vertex", "vertex-service-account"} {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/enabled=%t", provider, enabled), func(t *testing.T) {
				runGoogleResponsesToolIdentity(t, provider, enabled)
			})
		}
	}
}

func TestInteractionsResponsesToolIdentityAfterRelease(t *testing.T) {
	for _, field := range []string{"tools", "children"} {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/enabled=%t", field, enabled), func(t *testing.T) {
				runGoogleResponsesToolIdentityWithNamespaceField(t, "gemini-interactions", enabled, field)
			})
		}
	}
}

func TestAntigravityResponsesToolIdentityAfterRelease(t *testing.T) {
	resetAntigravityCreditsRetryState()
	t.Cleanup(resetAntigravityCreditsRetryState)
	for _, provider := range []string{"antigravity", "antigravity-claude", "antigravity-credits"} {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/enabled=%t", provider, enabled), func(t *testing.T) {
				runGoogleResponsesToolIdentity(t, provider, enabled)
			})
		}
	}
}

func TestGoogleCustomToolOutputAfterRelease(t *testing.T) {
	resetAntigravityCreditsRetryState()
	t.Cleanup(resetAntigravityCreditsRetryState)
	for _, provider := range []string{"gemini", "vertex", "vertex-service-account", "antigravity", "antigravity-claude", "antigravity-credits"} {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/enabled=%t", provider, enabled), func(t *testing.T) {
				runGoogleResponsesToolOutputFixture(t, provider, enabled, "tools", "custom")
			})
		}
	}
}
