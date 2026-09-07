package executor

import (
	"context"
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
	for _, operation := range []string{"execute", "stream", "count"} {
		for _, mode := range []string{"disabled", "enabled", "other-client"} {
			t.Run(operation+"/"+mode, func(t *testing.T) {
				var calls atomic.Int32
				ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", streamTerminalRoundTripFunc(func(r *http.Request) (*http.Response, error) {
					calls.Add(1)
					body, err := io.ReadAll(r.Body)
					if err != nil {
						return nil, err
					}
					content := gjson.GetBytes(body, "contents").Raw
					before, worker, after := strings.Index(content, `"before"`), strings.Index(content, `"worker result"`), strings.Index(content, `"after"`)
					enabled := mode == "enabled"
					if before < 0 || after <= before || (worker >= 0) != enabled || enabled && (worker <= before || worker >= after) {
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
					return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(strings.NewReader(result))}, nil
				}))
				executor := NewGeminiExecutor(&config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: mode != "disabled"}})
				auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "fixture"}}
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
			})
		}
	}
}

func TestGeminiMultiAgentCiphertextAndCancellation(t *testing.T) {
	var calls atomic.Int32
	ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", streamTerminalRoundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, fmt.Errorf("unexpected fixture request")
	}))
	executor := NewGeminiExecutor(&config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: true}})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "fixture"}}
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
