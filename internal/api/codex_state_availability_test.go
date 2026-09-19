package api

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexstate"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	runtimeexecutor "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestCredentialTargetWithoutStateReachesUpstream(t *testing.T) {
	for _, policy := range []string{"error", "hide"} {
		t.Run(policy, func(t *testing.T) {
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Header.Get("X-Codex-Turn-State") != "" {
					t.Error("missing State test unexpectedly sent a State")
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_fixture\",\"model\":\"gpt-target-state\",\"status\":\"completed\",\"output\":[]}}\n\n")
			}))
			defer upstream.Close()
			s := newTestServerWithConfig(t, func(cfg *config.Config) {
				cfg.APIKeyGroups = []config.APIKeyGroup{{APIKey: "test-key", Providers: []string{"codex"}, AllowCredentialTargeting: true}}
				cfg.ProxyURL = "direct"
				cfg.Codex.StateOverride = config.CodexStateOverrideConfig{Enabled: true, MissingPolicy: policy, Acquisition: "manual"}
			})
			s.handlers.AuthManager.RegisterExecutor(runtimeexecutor.NewCodexExecutor(s.currentConfig()))
			a, err := s.handlers.AuthManager.Register(t.Context(), &coreauth.Auth{ID: "target-state-auto", Provider: "codex", Attributes: map[string]string{"base_url": upstream.URL}, Metadata: map[string]any{"access_token": "fixture"}})
			if err != nil {
				t.Fatal(err)
			}
			reg := registry.GetGlobalRegistry()
			reg.RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "gpt-target-state"}})
			codexstate.Default.Sync(s.currentConfig().Codex.StateOverride, []codexstate.Credential{helps.StateCredential(a, "gpt-target-state")})
			t.Cleanup(func() {
				codexstate.Default.Sync(config.CodexStateOverrideConfig{}, nil)
				reg.UnregisterClient(a.ID)
			})
			for _, stream := range []bool{false, true} {
				req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(fmt.Sprintf(`{"model":"gpt-target-state","input":"test","stream":%t}`, stream)))
				req.Header.Set("Authorization", "Bearer test-key-auth-"+a.Index)
				w := httptest.NewRecorder()
				s.engine.ServeHTTP(w, req)
				if w.Code != http.StatusOK {
					t.Fatalf("fixed test stopped at local State: %d %s", w.Code, w.Body.String())
				}
			}
			if calls.Load() != 2 {
				t.Fatalf("fixed tests sent %d upstream calls, want 2", calls.Load())
			}
		})
	}
}

func TestHiddenStateCatalogAndExhaustedRoutingReturns503(t *testing.T) {
	s := newTestServerWithConfig(t, func(cfg *config.Config) {
		cfg.APIKeyGroups = []config.APIKeyGroup{{APIKey: "test-key", Providers: []string{"codex"}}}
		cfg.Codex.StateOverride = config.CodexStateOverrideConfig{Enabled: true, MissingPolicy: "hide", Acquisition: "manual"}
	})
	exec := &credentialTargetExecutor{provider: "codex"}
	s.handlers.AuthManager.RegisterExecutor(exec)
	a, err := s.handlers.AuthManager.Register(t.Context(), &coreauth.Auth{ID: "state-hidden-api", Provider: "codex", Metadata: map[string]any{"access_token": "fixture"}})
	if err != nil {
		t.Fatal(err)
	}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "state-hidden-model"}})
	codexstate.Default.Sync(s.currentConfig().Codex.StateOverride, []codexstate.Credential{helps.StateCredential(a, "state-hidden-model")})
	t.Cleanup(func() {
		codexstate.Default.Sync(config.CodexStateOverrideConfig{}, nil)
		reg.UnregisterClient(a.ID)
	})
	for _, path := range []string{"/v1/models", "/v1/responses", "/v1/chat/completions"} {
		method := http.MethodPost
		if path == "/v1/models" {
			method = http.MethodGet
		}
		req := httptest.NewRequest(method, path, strings.NewReader(`{"model":"state-hidden-model","input":"test","messages":[{"role":"user","content":"test"}]}`))
		req.Header.Set("Authorization", "Bearer test-key")
		w := httptest.NewRecorder()
		s.engine.ServeHTTP(w, req)
		if method == http.MethodGet {
			if w.Code != 200 || strings.Contains(w.Body.String(), "state-hidden-model") {
				t.Fatalf("hidden model remained in catalog: %d %s", w.Code, w.Body.String())
			}
		} else if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), "auth_") {
			t.Fatalf("exhausted route did not return availability 503: %d %s", w.Code, w.Body.String())
		}
	}
	if len(exec.calls) != 0 || !reg.ClientSupportsModel(a.ID, "state-hidden-model") {
		t.Fatal("hidden model called upstream or lost its acquisition registration")
	}
}
