package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

type credentialTargetExecutor struct {
	coreauth.ProviderExecutor
	provider   string
	mu         sync.Mutex
	calls      []string
	fail       bool
	failStatus int
	chunkError bool
	refreshes  int
}

func (e *credentialTargetExecutor) Identifier() string { return e.provider }
func (e *credentialTargetExecutor) execute(ctx context.Context, auth *coreauth.Auth, opts core.Options) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls = append(e.calls, auth.ID)
	c, _ := ctx.Value("gin").(*gin.Context)
	if c == nil || c.GetString("apiKey") != "test-key" {
		return fmt.Errorf("issuer key was not preserved")
	}
	if opts.Metadata[core.PinnedAuthMetadataKey] != auth.ID {
		return fmt.Errorf("credential pin was not propagated")
	}
	if e.failStatus != 0 {
		return &coreauth.Error{HTTPStatus: e.failStatus, Message: `{"error":{"message":"fixture original","code":"fixture_code"}}`}
	}
	if e.fail {
		return &coreauth.Error{HTTPStatus: 503, Message: "fixture unavailable"}
	}
	return nil
}
func (e *credentialTargetExecutor) Execute(ctx context.Context, auth *coreauth.Auth, _ core.Request, opts core.Options) (core.Response, error) {
	if err := e.execute(ctx, auth, opts); err != nil {
		return core.Response{}, err
	}
	return core.Response{Payload: []byte(`{"id":"fixture","choices":[],"credential":"` + auth.ID + `"}`)}, nil
}
func (e *credentialTargetExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.refreshes++
	return auth, nil
}
func (e *credentialTargetExecutor) ExecuteStream(ctx context.Context, auth *coreauth.Auth, _ core.Request, opts core.Options) (*core.StreamResult, error) {
	if err := e.execute(ctx, auth, opts); err != nil {
		if e.chunkError {
			chunks := make(chan core.StreamChunk, 1)
			chunks <- core.StreamChunk{Err: err}
			close(chunks)
			return &core.StreamResult{Chunks: chunks}, nil
		}
		return nil, err
	}
	chunks := make(chan core.StreamChunk, 2)
	chunks <- core.StreamChunk{Payload: []byte("data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_fixture\",\"output\":[]}}\n\n")}
	chunks <- core.StreamChunk{Payload: []byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_fixture\",\"status\":\"completed\",\"output\":[]}}\n\n")}
	close(chunks)
	return &core.StreamResult{Chunks: chunks}, nil
}

func TestCredentialTargetRequestsNeverSwitchAccounts(t *testing.T) {
	for _, provider := range []string{"codex", "xai"} {
		t.Run(provider, func(t *testing.T) {
			s := newTestServerWithConfig(t, func(cfg *config.Config) {
				cfg.APIKeyGroups = []config.APIKeyGroup{{APIKey: "test-key", AllowCredentialTargeting: true}}
				cfg.RequestRetry = 1
			})
			exec := &credentialTargetExecutor{provider: provider}
			manager := s.handlers.AuthManager
			manager.RegisterExecutor(exec)
			var target *coreauth.Auth
			for _, name := range []string{"a", "b"} {
				id := "target-" + provider + "-" + name
				a, err := manager.Register(t.Context(), &coreauth.Auth{ID: id, FileName: id + ".json", Provider: provider, Metadata: map[string]any{coreauth.RoutingAliasMetadataKey: "alias-" + name}})
				if err != nil {
					t.Fatal(err)
				}
				registry.GetGlobalRegistry().RegisterClient(id, provider, []*registry.ModelInfo{{ID: "target-model", Object: "model", OwnedBy: provider}, {ID: "only-" + name, Object: "model", OwnedBy: provider}})
				t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
				if name == "b" {
					target = a
				}
			}
			request := func(selector, path, body string) *httptest.ResponseRecorder {
				method := http.MethodPost
				if body == "" {
					method = http.MethodGet
				}
				req := httptest.NewRequest(method, path, strings.NewReader(body))
				req.Header.Set("Authorization", "Bearer test-key-auth-"+selector)
				req.Header.Set("Content-Type", "application/json")
				w := httptest.NewRecorder()
				s.engine.ServeHTTP(w, req)
				return w
			}
			for _, selector := range []string{target.Index, "alias-b"} {
				w := request(selector, "/v1/chat/completions", `{"model":"target-model","messages":[{"role":"user","content":"test"}]}`)
				if w.Code != 200 || gjson.GetBytes(w.Body.Bytes(), "credential").String() != target.ID || w.Header().Get("X-CLIProxy-Auth-ID") != target.Index {
					t.Fatalf("target request: %d %s", w.Code, w.Body.String())
				}
			}
			models := request("alias-b", "/v1/models", "")
			if models.Code != 200 || strings.Contains(models.Body.String(), "only-a") || !strings.Contains(models.Body.String(), "only-b") {
				t.Fatalf("unscoped models: %s", models.Body.String())
			}
			missing := request("missing", "/v1/models", "")
			if missing.Code != 404 {
				t.Fatalf("missing target status=%d", missing.Code)
			}
			server := httptest.NewServer(s.engine)
			defer server.Close()
			conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", http.Header{"Authorization": {"Bearer test-key-auth-alias-b"}})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = conn.Close() }()
			_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			for range 2 {
				if err = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"target-model","input":[]}`)); err != nil {
					t.Fatal(err)
				}
				for {
					_, data, errRead := conn.ReadMessage()
					if errRead != nil {
						t.Fatal(errRead)
					}
					kind := gjson.GetBytes(data, "type").String()
					if kind == "error" {
						t.Fatalf("websocket request failed: %s", data)
					}
					if kind == "response.completed" {
						break
					}
				}
			}
			exec.mu.Lock()
			exec.fail = true
			before := len(exec.calls)
			exec.mu.Unlock()
			failed := request("alias-b", "/v1/chat/completions", `{"model":"target-model","messages":[]}`)
			if failed.Code != 503 || !strings.Contains(failed.Body.String(), "fixture unavailable") {
				t.Fatal("failed target request unexpectedly succeeded")
			}
			exec.mu.Lock()
			if len(exec.calls) != before+1 {
				t.Fatalf("test retried: %d attempts", len(exec.calls)-before)
			}
			calls := append([]string(nil), exec.calls...)
			exec.fail = false
			exec.mu.Unlock()
			for _, id := range calls {
				if id != target.ID {
					t.Fatalf("target failure switched to %s", id)
				}
			}
			exec.mu.Lock()
			defer exec.mu.Unlock()
			for _, id := range exec.calls {
				if id != target.ID {
					t.Fatalf("websocket switched to %s", id)
				}
			}
		})
	}
}

func TestCredentialTargetFailuresReturnFirstOutcome(t *testing.T) {
	for _, status := range []int{401, 429, 503} {
		for _, mode := range []string{"http", "sse-error", "sse-bootstrap"} {
			t.Run(fmt.Sprintf("%d/%s", status, mode), func(t *testing.T) {
				s := newTestServerWithConfig(t, func(cfg *config.Config) {
					cfg.APIKeyGroups = []config.APIKeyGroup{{APIKey: "test-key", AllowCredentialTargeting: true}}
					cfg.RequestRetry = 3
					cfg.Streaming.BootstrapRetries = 3
					cfg.ErrorResponseRewrites = []config.ErrorResponseRewriteRule{{StatusCode: status, ResponseStatusCode: 418}}
				})
				exec := &credentialTargetExecutor{provider: "xai", failStatus: status, chunkError: mode == "sse-bootstrap"}
				manager := s.handlers.AuthManager
				manager.RegisterExecutor(exec)
				id := "test-first-outcome"
				a, err := manager.Register(t.Context(), &coreauth.Auth{ID: id, FileName: id + ".json", Provider: "xai", Metadata: map[string]any{"refresh_token": "fixture"}})
				if err != nil {
					t.Fatal(err)
				}
				registry.GetGlobalRegistry().RegisterClient(id, "xai", []*registry.ModelInfo{{ID: "target-model", Object: "model", OwnedBy: "xai"}})
				t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
				req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(fmt.Sprintf(`{"model":"target-model","input":"test","stream":%t}`, mode != "http")))
				req.Header.Set("Authorization", "Bearer test-key-auth-"+a.Index)
				req.Header.Set("Content-Type", "application/json")
				w := httptest.NewRecorder()
				s.engine.ServeHTTP(w, req)
				if w.Code != status || !strings.Contains(w.Body.String(), "fixture original") {
					t.Fatalf("outcome rewritten: %d %s", w.Code, w.Body.String())
				}
				if len(exec.calls) != 1 || exec.refreshes != 0 {
					t.Fatalf("retried: calls=%d refreshes=%d", len(exec.calls), exec.refreshes)
				}
			})
		}
	}
}

func TestCredentialTargetStillEnforcesAccess(t *testing.T) {
	for _, scenario := range []string{"provider", "priority", "disabled", "model", "live"} {
		t.Run(scenario, func(t *testing.T) {
			s := newTestServerWithConfig(t, func(cfg *config.Config) {
				group := config.APIKeyGroup{APIKey: "test-key", AllowCredentialTargeting: true}
				if scenario == "provider" {
					group.Providers = []string{"codex"}
				}
				if scenario == "priority" {
					group.AllowedPriorities = []int{3}
				}
				cfg.APIKeyGroups = []config.APIKeyGroup{group}
			})
			exec := &credentialTargetExecutor{provider: "xai"}
			s.handlers.AuthManager.RegisterExecutor(exec)
			a, err := s.handlers.AuthManager.Register(t.Context(), &coreauth.Auth{ID: "access-target", FileName: "access-target.json", Provider: "xai", Disabled: scenario == "disabled"})
			if err != nil {
				t.Fatal(err)
			}
			registry.GetGlobalRegistry().RegisterClient(a.ID, "xai", []*registry.ModelInfo{{ID: "target-model", Object: "model", OwnedBy: "xai"}})
			t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(a.ID) })
			model := "target-model"
			if scenario == "model" {
				model = "missing-model"
			}
			path := "/v1/responses"
			if scenario == "live" {
				path = "/v1/realtime/calls"
			}
			req := httptest.NewRequest("POST", path, strings.NewReader(fmt.Sprintf(`{"model":%q,"input":"test"}`, model)))
			req.Header.Set("Authorization", "Bearer test-key-auth-"+a.Index)
			w := httptest.NewRecorder()
			s.engine.ServeHTTP(w, req)
			if w.Code < 400 || len(exec.calls) != 0 {
				t.Fatalf("restriction bypassed: %d %s", w.Code, w.Body.String())
			}
		})
	}
}
