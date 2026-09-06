package executor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestProviderHTTPAttemptObservationPreservesDispatchAndCancellation(t *testing.T) {
	cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}}
	executors := []auth.ProviderExecutor{NewCodexExecutor(cfg), NewAntigravityExecutor(cfg), NewClaudeExecutor(cfg), NewGeminiExecutor(cfg), NewGeminiVertexExecutor(cfg), NewKimiExecutor(cfg), NewOpenAICompatExecutor("openai", cfg), NewXAIExecutor(cfg)}
	for _, exec := range executors {
		t.Run(exec.Identifier(), func(t *testing.T) {
			var calls atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusTooManyRequests)
			}))
			defer server.Close()
			credential := &auth.Auth{Provider: exec.Identifier(), Attributes: map[string]string{"api_key": "test"}, Metadata: map[string]any{"access_token": "test", "expired": time.Now().Add(time.Hour).Format(time.RFC3339)}}
			for _, tracked := range []bool{false, true} {
				ctx := t.Context()
				if tracked {
					ctx = core.WithUpstreamAttempt(ctx)
				}
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
				if err != nil {
					t.Fatal(err)
				}
				response, err := exec.HttpRequest(ctx, credential, req)
				if err != nil {
					t.Fatal(err)
				}
				_ = response.Body.Close()
				if response.StatusCode != 429 || core.IsUpstreamAttemptError(core.ErrorFromUpstreamAttempt(ctx, errors.New("observed response"))) != tracked {
					t.Fatal("tracking changed dispatch or missed the upstream response")
				}
			}
			ctx, cancel := context.WithCancel(core.WithUpstreamAttempt(t.Context()))
			cancel()
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
			_, err := exec.HttpRequest(ctx, credential, req)
			if !errors.Is(err, context.Canceled) || calls.Load() != 2 {
				t.Fatalf("cancellation or request count changed: calls=%d", calls.Load())
			}
		})
	}
}
