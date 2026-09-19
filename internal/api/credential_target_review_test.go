package api

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	runtimeexecutor "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

func TestCredentialTargetModelResolution(t *testing.T) {
	for _, tc := range []struct {
		name, provider, executor, prefix, model, want string
		attributes                                    map[string]string
	}{
		{name: "unregistered", provider: "codex", executor: "codex", model: "unknown", want: "unknown"},
		{name: "suffix", provider: "codex", executor: "codex", model: "unknown(high)", want: "unknown(high)"},
		{name: "prefix", provider: "codex", executor: "codex", prefix: "team", model: "team/unknown(high)", want: "unknown(high)"},
		{name: "api-key-alias", provider: "codex", executor: "codex", prefix: "team", model: "team/friendly(high)", want: "mapped-upstream(high)", attributes: map[string]string{"api_key": "fixture"}},
		{name: "auto", provider: "codex", executor: "codex", model: "auto(high)", want: "target-new(high)"},
		{name: "named-compatibility", provider: "openai-compatibility", executor: "named", model: "unknown", want: "unknown", attributes: map[string]string{"compat_name": "named", "provider_key": "named", "api_key": "fixture"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestServerWithConfig(t, func(cfg *config.Config) {
				cfg.APIKeyGroups = []config.APIKeyGroup{{APIKey: "test-key", Providers: []string{tc.executor}, AllowCredentialTargeting: true}}
				cfg.CodexKey = []config.CodexKey{{APIKey: "fixture", Models: []config.CodexModel{{Name: "mapped-upstream", Alias: "friendly"}}}}
			})
			exec := &credentialTargetExecutor{provider: tc.executor, wantModel: tc.want}
			s.handlers.AuthManager.RegisterExecutor(exec)
			a, err := s.handlers.AuthManager.Register(t.Context(), &coreauth.Auth{ID: "target-resolution", Provider: tc.provider, Prefix: tc.prefix, Attributes: tc.attributes})
			if err != nil {
				t.Fatal(err)
			}
			reg := registry.GetGlobalRegistry()
			reg.RegisterClient(a.ID, tc.executor, []*registry.ModelInfo{{ID: "target-old", Created: 1}, {ID: "target-new", Created: 2}})
			reg.RegisterClient("foreign-resolution", "foreign", []*registry.ModelInfo{{ID: "foreign-model", Created: 3}})
			t.Cleanup(func() { reg.UnregisterClient(a.ID); reg.UnregisterClient("foreign-resolution") })
			for _, stream := range []bool{false, true} {
				req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(fmt.Sprintf(`{"model":%q,"input":"hello","stream":%t}`, tc.model, stream)))
				req.Header.Set("Authorization", "Bearer test-key-auth-"+a.Index)
				w := httptest.NewRecorder()
				before := len(exec.calls)
				s.engine.ServeHTTP(w, req)
				if w.Code != http.StatusOK || len(exec.calls) != before+1 {
					t.Fatalf("stream=%v status=%d calls=%v body=%s", stream, w.Code, exec.calls, w.Body.String())
				}
			}
		})
	}
}

func TestCredentialTargetAutoRequiresItsOwnCatalog(t *testing.T) {
	s := newTestServerWithConfig(t, func(cfg *config.Config) {
		cfg.APIKeyGroups = []config.APIKeyGroup{{APIKey: "test-key", AllowCredentialTargeting: true}}
	})
	exec := &credentialTargetExecutor{provider: "codex"}
	s.handlers.AuthManager.RegisterExecutor(exec)
	a, err := s.handlers.AuthManager.Register(t.Context(), &coreauth.Auth{ID: "target-empty-catalog", Provider: "codex"})
	if err != nil {
		t.Fatal(err)
	}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient("foreign-auto", "xai", []*registry.ModelInfo{{ID: "foreign-model", Created: 100}})
	t.Cleanup(func() { reg.UnregisterClient("foreign-auto") })
	request := func(key, model string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(fmt.Sprintf(`{"model":%q,"input":"test"}`, model)))
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("X-CLIProxy-Auth-ID", a.ID)
		req.Header.Set("Pinned-Auth-ID", a.ID)
		w := httptest.NewRecorder()
		s.engine.ServeHTTP(w, req)
		return w
	}
	if w := request("test-key-auth-"+a.Index, "auto"); w.Code != http.StatusBadGateway || len(exec.calls) != 0 || !strings.Contains(w.Body.String(), "specify a model explicitly") {
		t.Fatalf("auto escaped target catalog: %d %s", w.Code, w.Body.String())
	}
	if w := request("test-key", "raw-unknown"); w.Code != http.StatusBadGateway || len(exec.calls) != 0 {
		t.Fatal("untrusted pin headers bypassed catalog")
	}
	if w := request("test-key-auth-"+a.Index, "raw-unknown"); w.Code != http.StatusOK || len(exec.calls) != 1 {
		t.Fatalf("explicit model required local catalog: %d %s", w.Code, w.Body.String())
	}
}

type targetWebsocketCaptureExecutor struct {
	*credentialTargetExecutor
	payloads [][]byte
}

func (e *targetWebsocketCaptureExecutor) ExecuteStream(ctx context.Context, a *coreauth.Auth, req core.Request, opts core.Options) (*core.StreamResult, error) {
	e.mu.Lock()
	e.payloads = append(e.payloads, bytes.Clone(req.Payload))
	e.mu.Unlock()
	return e.credentialTargetExecutor.ExecuteStream(ctx, a, req, opts)
}

func TestCredentialTargetUnregisteredWebsocketContinuation(t *testing.T) {
	for _, provider := range []string{"codex", "xai"} {
		t.Run(provider, func(t *testing.T) {
			s := newTestServerWithConfig(t, func(cfg *config.Config) {
				cfg.APIKeyGroups = []config.APIKeyGroup{{APIKey: "test-key", Providers: []string{provider}, AllowCredentialTargeting: true}}
			})
			exec := &targetWebsocketCaptureExecutor{credentialTargetExecutor: &credentialTargetExecutor{provider: provider, wantModel: "unregistered-ws"}}
			s.handlers.AuthManager.RegisterExecutor(exec)
			a, err := s.handlers.AuthManager.Register(t.Context(), &coreauth.Auth{ID: "target-ws", Provider: provider, Attributes: map[string]string{"websockets": "true"}})
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(s.engine)
			defer server.Close()
			conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", http.Header{"Authorization": {"Bearer test-key-auth-" + a.Index}})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = conn.Close() }()
			for turn := range 3 {
				previous := ""
				if turn > 0 {
					previous = `,"previous_response_id":"resp_fixture"`
				}
				body := fmt.Sprintf(`{"type":"response.create","model":"unregistered-ws","input":[{"role":"user","content":"turn %d"}]%s}`, turn, previous)
				if err := conn.WriteMessage(websocket.TextMessage, []byte(body)); err != nil {
					t.Fatal(err)
				}
				_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
				for {
					_, data, errRead := conn.ReadMessage()
					if errRead != nil {
						t.Fatal(errRead)
					}
					if gjson.GetBytes(data, "type").String() == "error" {
						t.Fatalf("websocket error: %s", data)
					}
					if gjson.GetBytes(data, "type").String() == "response.completed" {
						break
					}
				}
				exec.mu.Lock()
				payloads := append([][]byte(nil), exec.payloads...)
				exec.mu.Unlock()
				if len(payloads) != turn+1 {
					t.Fatalf("turn %d executed %d times", turn, len(payloads))
				}
				if turn > 0 && (gjson.GetBytes(payloads[turn], "previous_response_id").String() != "resp_fixture" || gjson.GetBytes(payloads[turn], "input.#").Int() != 1) {
					t.Fatalf("turn %d lost incremental context: %s", turn, payloads[turn])
				}
			}
		})
	}
}

func TestCredentialTargetCodexHTTPBypassesRequestLimitWithoutConsumingIt(t *testing.T) {
	for _, path := range []string{"/v1/chat/completions", "/v1/responses"} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%t", path, stream), func(t *testing.T) {
				var calls atomic.Int32
				captured := make(chan []byte, 8)
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					body, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
						return
					}
					captured <- body
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"OK\"}\n\n")
					_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_mock\",\"object\":\"response\",\"model\":\"actual-upstream-model\",\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"OK\"}]}]}}\n\n")
				}))
				defer upstream.Close()
				s := newTestServerWithConfig(t, func(cfg *config.Config) {
					cfg.APIKeyGroups = []config.APIKeyGroup{{APIKey: "test-key", Providers: []string{"codex"}, AllowCredentialTargeting: true}}
					cfg.Routing.PerAuthRequestLimit = 1
					cfg.Routing.PerAuthRequestWindowMinutes = 1
					cfg.ProxyURL = "direct"
				})
				s.handlers.AuthManager.RegisterExecutor(runtimeexecutor.NewCodexExecutor(s.currentConfig()))
				a, err := s.handlers.AuthManager.Register(t.Context(), &coreauth.Auth{ID: "target-protocol", Provider: "codex", Prefix: "team", Attributes: map[string]string{"api_key": "fixture", "base_url": upstream.URL}})
				if err != nil {
					t.Fatal(err)
				}
				body := fmt.Sprintf(`{"model":"team/unregistered-protocol(high)","input":[{"role":"user","content":"test"}],"messages":[{"role":"user","content":"test"}],"stream":%t}`, stream)
				registry.GetGlobalRegistry().RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "team/registered", UpstreamID: "registered"}})
				t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(a.ID) })
				// Tests neither consume ordinary capacity nor fail when it is already exhausted.
				for attempt, targeted := range []bool{true, true, false, true, false} {
					requestBody, key := body, "test-key-auth-"+a.Index
					if !targeted {
						requestBody = strings.ReplaceAll(body, "unregistered-protocol", "registered")
						key = "test-key"
					}
					req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(requestBody))
					req.Header.Set("Authorization", "Bearer "+key)
					w := httptest.NewRecorder()
					s.engine.ServeHTTP(w, req)
					if attempt < 4 {
						if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "OK") {
							t.Fatalf("protocol failed: %d %s", w.Code, w.Body.String())
						}
					} else if w.Code != http.StatusTooManyRequests || !strings.Contains(w.Body.String(), "auth_request_limited") {
						t.Fatalf("request limit bypassed: %d %s", w.Code, w.Body.String())
					}
				}
				if calls.Load() != 4 {
					t.Fatalf("upstream received %d attempts", calls.Load())
				}
				requestBody := <-captured
				if gjson.GetBytes(requestBody, "model").String() != "unregistered-protocol" || gjson.GetBytes(requestBody, "reasoning.effort").String() != "high" {
					t.Fatalf("request model or thinking suffix changed: %s", requestBody)
				}
			})
		}
	}
}
