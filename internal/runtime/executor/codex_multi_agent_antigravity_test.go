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

func TestAntigravityMultiAgentHistoryAcrossModelPaths(t *testing.T) {
	for _, model := range []string{"gemini-2.5-flash", "claude-sonnet-4-6"} {
		for _, operation := range []string{"execute", "stream", "count"} {
			for _, enabled := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/%t", model, operation, enabled), func(t *testing.T) {
					var calls atomic.Int32
					ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", streamTerminalRoundTripFunc(func(r *http.Request) (*http.Response, error) {
						calls.Add(1)
						body, err := io.ReadAll(r.Body)
						if err != nil {
							return nil, err
						}
						contents := gjson.GetBytes(body, "request.contents").Raw
						before, worker, after := strings.Index(contents, `"before"`), strings.Index(contents, `"worker result"`), strings.Index(contents, `"after"`)
						if before < 0 || after <= before || (worker >= 0) != enabled || enabled && (worker <= before || worker >= after) {
							t.Error("Antigravity lost collaboration order or default behavior")
						}
						result := `{"response":{"candidates":[{"content":{"role":"model","parts":[{"text":"ok"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}}`
						contentType := "application/json"
						if operation == "stream" || operation != "count" && strings.Contains(model, "claude") {
							contentType = "text/event-stream"
							result = "data: " + result + "\n\n"
						}
						if operation == "count" {
							result = `{"totalTokens":12}`
						}
						return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(strings.NewReader(result))}, nil
					}))
					executor := NewAntigravityExecutor(&config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: enabled}})
					auth := antigravityStreamTestAuth()
					auth.ID = "multi-agent-" + t.Name()
					req := core.Request{Model: model, Payload: []byte(codexPlainAgentHistory)}
					opts := core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, OriginalRequest: req.Payload, Headers: http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}}}
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
}

func TestAntigravityMultiAgentRejectsBeforeRefreshingCredential(t *testing.T) {
	var calls atomic.Int32
	ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", streamTerminalRoundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, fmt.Errorf("unexpected fixture connection")
	}))
	executor := NewAntigravityExecutor(&config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: true}})
	for _, model := range []string{"gemini-2.5-flash", "claude-sonnet-4-6"} {
		for _, operation := range []string{"execute", "stream", "count"} {
			auth := &cliproxyauth.Auth{ID: "expired-multi-agent", Metadata: map[string]any{"refresh_token": "fixture"}}
			req := core.Request{Model: model, Payload: []byte(codexEncryptedAgentHistory)}
			opts := core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Headers: http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}}}
			_, err := executeMultiAgentTranslation(ctx, executor, operation, auth, req, opts)
			var local interface {
				SkipAuthResult() bool
				RetryOtherAuth() bool
			}
			if err == nil || !errors.As(err, &local) || !local.SkipAuthResult() || local.RetryOtherAuth() {
				t.Fatalf("%s/%s did not reject before refreshing the fixture credential", model, operation)
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if _, err := executeMultiAgentTranslation(canceled, executor, operation, auth, req, opts); !errors.Is(err, context.Canceled) {
				t.Fatal("compatibility error replaced cancellation")
			}
		}
	}
	if calls.Load() != 0 {
		t.Fatal("unsupported ciphertext triggered refresh or generation")
	}
}
