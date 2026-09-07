package executor

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestGeminiMultiAgentHistoryAndClientGate(t *testing.T) {
	runGeminiMultiAgentHistory(t, "gemini")
}

func TestGeminiInteractionsMultiAgentHistoryAndClientGate(t *testing.T) {
	runGeminiMultiAgentHistory(t, "gemini-interactions")
}

func TestVertexMultiAgentHistoryAndClientGate(t *testing.T) {
	for _, provider := range []string{"vertex", "vertex-service-account"} {
		t.Run(provider, func(t *testing.T) { runGeminiMultiAgentHistory(t, provider) })
	}
}

func runGeminiMultiAgentHistory(t *testing.T, provider string) {
	t.Helper()
	interactions := provider == "gemini-interactions"
	for _, operation := range []string{"execute", "stream", "count"} {
		for _, mode := range []string{"disabled", "enabled", "other-client"} {
			t.Run(operation+"/"+mode, func(t *testing.T) {
				var calls atomic.Int32
				var tokenCalls atomic.Int32
				ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", streamTerminalRoundTripFunc(func(r *http.Request) (*http.Response, error) {
					if r.URL.Host == "oauth-fixture.invalid" {
						tokenCalls.Add(1)
						return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"access_token":"fixture-token","token_type":"Bearer","expires_in":3600}`))}, nil
					}
					calls.Add(1)
					body, err := io.ReadAll(r.Body)
					if err != nil {
						return nil, err
					}
					content := gjson.GetBytes(body, "contents").Raw
					if interactions && operation != "count" {
						content = gjson.GetBytes(body, "input").Raw
					}
					before, worker, after := strings.Index(content, `"before"`), strings.Index(content, `"worker result"`), strings.Index(content, `"after"`)
					// Interactions already retained plaintext through its generic content fallback.
					wantWorker := mode == "enabled" || interactions && operation != "count"
					if before < 0 || after <= before || (worker >= 0) != wantWorker || wantWorker && (worker <= before || worker >= after) {
						t.Error("Gemini lost collaboration order or client gating")
					}
					result := `{"candidates":[{"content":{"role":"model","parts":[{"text":"ok"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}`
					contentType := "application/json"
					if operation == "stream" {
						contentType = "text/event-stream"
						result = "data: " + result + "\n\n"
					}
					if operation == "count" {
						result = `{"totalTokens":12}`
					}
					if interactions && operation != "count" {
						result = `{"id":"i1","status":"completed","outputs":[{"type":"text","text":"ok"}],"usage":{"total_input_tokens":1,"total_output_tokens":1,"total_tokens":2}}`
						if operation == "stream" {
							result = "event: interaction.completed\ndata: {\"event_type\":\"interaction.completed\",\"interaction\":" + result + "}\n\n"
						}
					}
					return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(strings.NewReader(result))}, nil
				}))
				executor, auth := newGoogleMultiAgentFixtureExecutor(t, provider, &config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: mode != "disabled"}})
				userAgent := "codex_cli_rs/0.153.4"
				if mode == "other-client" {
					userAgent = "other/1"
				}
				req := core.Request{Model: "gemini-2.5-flash", Payload: []byte(codexPlainAgentHistory)}
				opts := core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, OriginalRequest: req.Payload, Headers: http.Header{"User-Agent": {userAgent}}}
				if _, err := executeMultiAgentTranslation(ctx, executor, operation, auth, req, opts); err != nil {
					t.Fatal(err)
				}
				if calls.Load() != 1 {
					t.Fatal("translation changed upstream call count")
				}
				if (tokenCalls.Load() == 1) != (provider == "vertex-service-account") {
					t.Fatal("service-account authentication was not exercised through the fixture")
				}
			})
		}
	}
}

func TestGeminiMultiAgentCiphertextAndCancellation(t *testing.T) {
	runGeminiMultiAgentCiphertext(t, "gemini")
}

func TestGeminiInteractionsMultiAgentCiphertextAndCancellation(t *testing.T) {
	runGeminiMultiAgentCiphertext(t, "gemini-interactions")
}

func TestVertexMultiAgentCiphertextAndCancellation(t *testing.T) {
	for _, provider := range []string{"vertex", "vertex-service-account"} {
		t.Run(provider, func(t *testing.T) { runGeminiMultiAgentCiphertext(t, provider) })
	}
}

func runGeminiMultiAgentCiphertext(t *testing.T, provider string) {
	t.Helper()
	var calls atomic.Int32
	ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", streamTerminalRoundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, fmt.Errorf("unexpected fixture request")
	}))
	executor, auth := newGoogleMultiAgentFixtureExecutor(t, provider, &config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: true}})
	for _, operation := range []string{"execute", "stream", "count"} {
		for _, originalOnly := range []bool{false, true} {
			if originalOnly && operation == "count" {
				continue
			}
			t.Run(fmt.Sprintf("%s/original=%t", operation, originalOnly), func(t *testing.T) {
				req := core.Request{Model: "gemini-2.5-flash", Payload: []byte(codexEncryptedAgentHistory)}
				opts := core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Headers: http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}}}
				if originalOnly {
					opts.OriginalRequest = req.Payload
					req.Payload = []byte(codexPlainAgentHistory)
				}
				_, err := executeMultiAgentTranslation(ctx, executor, operation, auth, req, opts)
				var local interface {
					SkipAuthResult() bool
					RetryOtherAuth() bool
				}
				if err == nil || !errors.As(err, &local) || !local.SkipAuthResult() || local.RetryOtherAuth() {
					t.Fatal("unsupported content lost its request-scoped error")
				}
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				if _, err := executeMultiAgentTranslation(canceled, executor, operation, auth, req, opts); !errors.Is(err, context.Canceled) {
					t.Fatal("normalization replaced cancellation")
				}
			})
		}
	}
	if calls.Load() != 0 {
		t.Fatal("unsupported content reached the transport")
	}
}

func newGoogleMultiAgentFixtureExecutor(t *testing.T, provider string, cfg *config.Config) (multiAgentTranslationExecutor, *cliproxyauth.Auth) {
	t.Helper()
	auth := &cliproxyauth.Auth{Provider: provider, Attributes: map[string]string{"api_key": "fixture"}}
	switch provider {
	case "antigravity":
		auth = antigravityStreamTestAuth()
		auth.ID = "tool-output-" + t.Name()
		return NewAntigravityExecutor(cfg), auth
	case "gemini-interactions":
		return NewGeminiInteractionsExecutor(cfg), auth
	case "vertex", "vertex-service-account":
		if provider == "vertex-service-account" {
			key, err := rsa.GenerateKey(rand.Reader, 2048)
			if err != nil {
				t.Fatal(err)
			}
			delete(auth.Attributes, "api_key")
			auth.Metadata = map[string]any{
				"project_id": "fixture-project",
				"service_account": map[string]any{
					"type": "service_account", "client_email": "fixture@example.invalid",
					"private_key": string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})),
					"token_uri":   "https://oauth-fixture.invalid/token",
				},
			}
		}
		return NewGeminiVertexExecutor(cfg), auth
	default:
		return NewGeminiExecutor(cfg), auth
	}
}
