package executor

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestCodexEntrypointsRecordActualUpstreamAttempt(t *testing.T) {
	for _, operation := range []string{"http", "stream", "compact", "image", "image-stream", "websocket", "websocket-stream"} {
		t.Run(operation, func(t *testing.T) {
			var calls atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasPrefix(operation, "websocket") {
					upgrader := websocket.Upgrader{}
					conn, err := upgrader.Upgrade(w, r, nil)
					if err != nil {
						t.Error(err)
						return
					}
					defer func() { _ = conn.Close() }()
					if _, _, err = conn.ReadMessage(); err != nil {
						t.Error(err)
						return
					}
					calls.Add(1)
					_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","status":503,"error":{"code":"server_error","message":"upstream unavailable"}}`))
					_, _, _ = conn.ReadMessage()
					return
				}
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"error":{"code":"server_error","message":"upstream unavailable"}}`))
			}))
			defer server.Close()
			cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}}
			var exec auth.ProviderExecutor = NewCodexExecutor(cfg)
			if strings.HasPrefix(operation, "websocket") {
				exec = NewCodexWebsocketsExecutor(cfg)
			}
			credential := &auth.Auth{Provider: "codex", Attributes: map[string]string{"api_key": "test", "base_url": server.URL}}
			req := core.Request{Model: "gpt-5.4", Payload: []byte(`{"input":[]}`)}
			opts := core.Options{SourceFormat: translator.FromString("codex")}
			if operation == "compact" {
				opts.Alt = "responses/compact"
			}
			if strings.HasPrefix(operation, "image") {
				req = core.Request{Model: "gpt-image-2", Payload: []byte(`{"prompt":"draw"}`)}
				opts.SourceFormat = translator.FromString(codexOpenAIImageSourceFormat)
				opts.Alt = codexOpenAIImageGenerations
			}
			ctx := core.WithUpstreamAttempt(t.Context())
			var err error
			if strings.HasSuffix(operation, "stream") {
				var result *core.StreamResult
				result, err = exec.ExecuteStream(ctx, credential, req, opts)
				if result != nil {
					for chunk := range result.Chunks {
						if chunk.Err != nil {
							err = chunk.Err
						}
					}
				}
			} else {
				_, err = exec.Execute(ctx, credential, req, opts)
			}
			marked := core.ErrorFromUpstreamAttempt(ctx, err)
			var status core.StatusError
			if calls.Load() != 1 || !core.IsUpstreamAttemptError(marked) || !errors.As(marked, &status) || status.StatusCode() != 503 {
				t.Fatalf("upstream evidence, status or attempt count changed: calls=%d, marked=%t", calls.Load(), core.IsUpstreamAttemptError(marked))
			}
		})
	}
}
