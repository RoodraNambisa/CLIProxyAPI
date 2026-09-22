package api

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

func TestResponseGuardHTTP429AcrossClientProtocols(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: {\"type\":\"response.created\",\"response\":{\"model\":\"wrong\"}}\n\n")
	}))
	defer upstream.Close()
	s := newTestServerWithConfig(t, func(c *config.Config) {
		c.Codex.ResponseGuard = config.CodexResponseGuardConfig{Enabled: true, CodexResponseGuardSettings: config.CodexResponseGuardSettings{Mode: new("enforce"), ErrorType: new("guard_type"), ErrorCode: new("guard_code"), ErrorMessage: new("configured message")}}
		c.ErrorResponseRewrites = []config.ErrorResponseRewriteRule{{StatusCode: 429, ResponseStatusCode: 418}}
	})
	m := s.handlers.AuthManager
	m.RegisterExecutor(executor.NewCodexExecutor(s.currentConfig()))
	a, err := m.Register(auth.WithSkipPersist(t.Context()), &auth.Auth{ID: "guard-http", Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"api_key": "fixture", "base_url": upstream.URL}})
	if err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "guard-model"}})
	defer registry.GetGlobalRegistry().UnregisterClient(a.ID)
	for _, path := range []string{"/v1/responses", "/v1/chat/completions", "/v1/messages"} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprint(path, stream), func(t *testing.T) {
				body := fmt.Sprintf(`{"model":"guard-model","max_tokens":32,"input":"question","messages":[{"role":"user","content":"question"}],"stream":%t}`, stream)
				r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
				r.Header.Set("Authorization", "Bearer test-key")
				r.Header.Set("Content-Type", "application/json")
				w := httptest.NewRecorder()
				s.engine.ServeHTTP(w, r)
				if w.Code != 429 || gjson.GetBytes(w.Body.Bytes(), "error.type").String() != "guard_type" || gjson.GetBytes(w.Body.Bytes(), "error.code").String() != "guard_code" || gjson.GetBytes(w.Body.Bytes(), "error.message").String() != "configured message" {
					t.Fatalf("lost custom error: %d %s", w.Code, w.Body.String())
				}
			})
		}
	}
	current, _ := m.GetByID(a.ID)
	if current.Disabled || !current.NextRetryAfter.IsZero() || current.Quota.Exceeded {
		t.Fatal("local guard changed credential availability")
	}
	if calls.Load() != 6 {
		t.Fatalf("unexpected automatic retries: %d", calls.Load())
	}
	stats := m.AuthResponseModelRewriteSummary(a, true)
	if stats.Blocked != 6 || stats.Total != 0 || len(stats.Recent) != 6 {
		t.Fatalf("guard-only history: %+v", stats)
	}
}

func TestResponseGuardFixedCredentialOptInIsTrustedAndSingleAttempt(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"done\",\"status\":\"completed\",\"model\":\"wrong\",\"output\":[]}}\n\n")
	}))
	defer upstream.Close()
	s := newTestServerWithConfig(t, func(c *config.Config) {
		c.APIKeys = []string{"enabled", "disabled"}
		c.APIKeyGroups = []config.APIKeyGroup{{APIKey: "enabled", AllowCredentialTargeting: true, CredentialTargetResponseGuard: true}, {APIKey: "disabled", AllowCredentialTargeting: true}}
		c.Codex.ResponseGuard = config.CodexResponseGuardConfig{Enabled: true, CodexResponseGuardSettings: config.CodexResponseGuardSettings{Mode: new("enforce"), OnReject: new("retry")}}
	})
	m := s.handlers.AuthManager
	m.RegisterExecutor(executor.NewCodexExecutor(s.currentConfig()))
	a, err := m.Register(auth.WithSkipPersist(t.Context()), &auth.Auth{ID: "fixed-guard", Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"api_key": "fixture", "base_url": upstream.URL}})
	if err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "guard-model"}})
	defer registry.GetGlobalRegistry().UnregisterClient(a.ID)
	for _, key := range []string{"disabled", "enabled"} {
		r := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"guard-model","input":"question","codex_response_guard":"off"}`))
		r.Header.Set("Authorization", "Bearer "+key+"-auth-"+a.Index)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("credential_target_response_guard", "true")
		w := httptest.NewRecorder()
		s.engine.ServeHTTP(w, r)
		want := 200
		if key == "enabled" {
			want = 429
		}
		if w.Code != want {
			t.Fatalf("%s: status=%d body=%s", key, w.Code, w.Body.String())
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("fixed tests retried: %d", calls.Load())
	}
}
