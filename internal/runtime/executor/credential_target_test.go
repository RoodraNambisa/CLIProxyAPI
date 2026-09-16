package executor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestCredentialTargetCodexDoesNotFallbackFromWebsocket(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "execute", true: "stream"}[stream], func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusUpgradeRequired)
				_, _ = w.Write([]byte(`{"error":{"message":"use HTTP"}}`))
			}))
			defer server.Close()
			cfg := &config.Config{}
			cfg.ProxyURL = "direct"
			exec := NewCodexWebsocketsExecutor(cfg)
			auth := &coreauth.Auth{ID: "fixed-auth", Provider: "codex", Metadata: map[string]any{"access_token": "fixture"}, Attributes: map[string]string{"base_url": server.URL}}
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Set(sdkaccess.CredentialTargetAuthIDContextKey, auth.ID)
			ctx := context.WithValue(t.Context(), "gin", c)
			req := core.Request{Model: "gpt-5.4", Payload: []byte(`{"model":"gpt-5.4","input":"test"}`)}
			opts := core.Options{SourceFormat: sdktranslator.FromString("codex")}
			var err error
			if stream {
				_, err = exec.ExecuteStream(ctx, auth, req, opts)
			} else {
				_, err = exec.Execute(ctx, auth, req, opts)
			}
			if err == nil || calls.Load() != 1 {
				t.Fatalf("fallback occurred: calls=%d err=%v", calls.Load(), err)
			}
			status, ok := err.(interface{ StatusCode() int })
			if !ok || status.StatusCode() != 426 {
				t.Fatalf("handshake error changed: %v", err)
			}
		})
	}
}
