package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexstate"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

func TestCodexStateMissing429PreservesConfiguredErrorWithoutCooldown(t *testing.T) {
	server := newTestServerWithConfig(t, func(cfg *config.Config) {
		cfg.Codex.StateOverride = config.CodexStateOverrideConfig{Enabled: true, MissingPolicy: "error"}
		cfg.ErrorResponseRewrites = []config.ErrorResponseRewriteRule{{StatusCode: 429, ResponseStatusCode: 418}}
	})
	m := server.handlers.AuthManager
	m.RegisterExecutor(executor.NewCodexExecutor(server.currentConfig()))
	a, err := m.Register(auth.WithSkipPersist(t.Context()), &auth.Auth{ID: "state-api", Provider: "codex", Metadata: map[string]any{"access_token": "fixture", "account_id": "state-owner"}})
	if err != nil {
		t.Fatal(err)
	}
	r := registry.GetGlobalRegistry()
	r.RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "state-test-model"}})
	defer r.UnregisterClient(a.ID)
	codexstate.Default.Sync(server.currentConfig().Codex.StateOverride, []codexstate.Credential{helps.StateCredential(a, "state-test-model")})
	defer codexstate.Default.Sync(config.CodexStateOverrideConfig{}, nil)
	for _, stream := range []string{"false", "true"} {
		req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"state-test-model","input":"OK","stream":`+stream+`}`))
		req.Header.Set("Authorization", "Bearer test-key")
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		server.engine.ServeHTTP(w, req)
		if w.Code != 429 || gjson.GetBytes(w.Body.Bytes(), "error.type").String() != "rate_limit_exceeded" || gjson.GetBytes(w.Body.Bytes(), "error.message").String() != "Rate limit exceeded for image_generation. Please try again later." {
			t.Fatalf("admission response=%d %s", w.Code, w.Body.String())
		}
	}
	current, _ := m.GetByID(a.ID)
	if !current.NextRetryAfter.IsZero() || current.Disabled || current.Quota.Exceeded {
		t.Fatal("local state admission changed credential quota or cooldown")
	}
	if summary := codexstate.Default.Snapshots(a.ID, time.Now()); len(summary) != 1 || summary[0].Misses != 2 {
		t.Fatalf("missing state diagnostic=%+v", summary)
	}
}
