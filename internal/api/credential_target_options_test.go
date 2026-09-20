package api

import (
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
	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexstate"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	runtimeexecutor "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

func TestCredentialTargetStatePolicyIsPerKey(t *testing.T) {
	for _, policy := range []string{"continue", "error", "hide"} {
		t.Run(policy, func(t *testing.T) {
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_fixture\",\"model\":\"gpt-target\",\"status\":\"completed\",\"output\":[]}}\n\n")
			}))
			defer upstream.Close()
			s := newTestServerWithConfig(t, func(cfg *config.Config) {
				cfg.APIKeys = []string{"raw", "obey"}
				cfg.APIKeyGroups = []config.APIKeyGroup{
					{APIKey: "raw", AllowCredentialTargeting: true},
					{APIKey: "obey", AllowCredentialTargeting: true, CredentialTargetRespectStatePolicy: true},
				}
				cfg.ProxyURL = "direct"
				cfg.Codex.StateOverride = config.CodexStateOverrideConfig{Enabled: true, MissingPolicy: policy, Acquisition: "manual"}
			})
			s.handlers.AuthManager.RegisterExecutor(runtimeexecutor.NewCodexExecutor(s.currentConfig()))
			a, err := s.handlers.AuthManager.Register(t.Context(), &coreauth.Auth{ID: "target-state-options", Provider: "codex", Attributes: map[string]string{"base_url": upstream.URL}, Metadata: map[string]any{"access_token": "fixture"}})
			if err != nil {
				t.Fatal(err)
			}
			reg := registry.GetGlobalRegistry()
			reg.RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "gpt-target"}})
			c := helps.StateCredential(a, "gpt-target")
			codexstate.Default.Sync(s.currentConfig().Codex.StateOverride, []codexstate.Credential{c})
			t.Cleanup(func() {
				codexstate.Default.Sync(config.CodexStateOverrideConfig{}, nil)
				reg.UnregisterClient(a.ID)
			})
			wantStatus := func(key string) int {
				if key == "obey" {
					if policy == "hide" {
						return 503
					}
					if policy == "error" {
						return 429
					}
				}
				return 200
			}
			for _, path := range []string{"/v1/responses", "/v1/chat/completions"} {
				for _, stream := range []bool{false, true} {
					for _, key := range []string{"raw", "obey"} {
						for _, model := range []string{"gpt-target", "gpt-unregistered"} {
							before := calls.Load()
							req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(fmt.Sprintf(`{"model":%q,"input":"test","messages":[{"role":"user","content":"test"}],"stream":%t}`, model, stream)))
							req.Header.Set("Authorization", "Bearer "+key+"-auth-"+a.Index)
							// Header text cannot override the authenticated key setting.
							req.Header.Set("credential_target_respect_state_policy", fmt.Sprint(key == "raw"))
							w := httptest.NewRecorder()
							s.engine.ServeHTTP(w, req)
							want := wantStatus(key)
							if w.Code != want || (calls.Load() == before+1) != (want == 200) || calls.Load()-before > 1 {
								t.Fatalf("%s stream=%t key=%s model=%s: status=%d calls=%d body=%s", path, stream, key, model, w.Code, calls.Load()-before, w.Body.String())
							}
						}
					}
				}
			}
			server := httptest.NewServer(s.engine)
			defer server.Close()
			for _, key := range []string{"raw", "obey"} {
				before := calls.Load()
				conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", http.Header{"Authorization": {"Bearer " + key + "-auth-" + a.Index}})
				if err != nil {
					t.Fatal(err)
				}
				_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
				if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"gpt-target","input":[]}`)); err != nil {
					t.Fatal(err)
				}
				want := wantStatus(key)
				for {
					_, data, err := conn.ReadMessage()
					if err != nil {
						t.Fatal(err)
					}
					typ := gjson.GetBytes(data, "type").String()
					if typ == "error" {
						if want == 200 || gjson.GetBytes(data, "status").Int() != int64(want) {
							t.Fatalf("WS error: %s", data)
						}
						break
					}
					if typ == "response.completed" {
						if want != 200 {
							t.Fatalf("WS bypassed State: %s", data)
						}
						break
					}
				}
				_ = conn.Close()
				if (calls.Load() == before+1) != (want == 200) {
					t.Fatal("WS reached the wrong State policy")
				}
			}
			// Recovery permits the same strict test key without changing its options.
			codexstate.Default.Action(c.ID, c.Model, "acquire")
			codexstate.Default.Tick(t.Context(), time.Now(), func(context.Context, codexstate.Credential, config.CodexStateOverrideConfig) (codexstate.Result, error) {
				return codexstate.Result{State: strings.Repeat("s", 292), Model: c.Model, Completed: true}, nil
			})
			codexstate.Default.Wait()
			req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-target","input":"test"}`))
			req.Header.Set("Authorization", "Bearer obey-auth-"+a.Index)
			w := httptest.NewRecorder()
			s.engine.ServeHTTP(w, req)
			if w.Code != 200 {
				t.Fatalf("State recovery still rejected: %d %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestCredentialTargetResponseModelRewriteIsPerKey(t *testing.T) {
	s := newTestServerWithConfig(t, func(cfg *config.Config) {
		cfg.APIKeys = []string{"raw", "rewrite"}
		cfg.APIKeyGroups = []config.APIKeyGroup{
			{APIKey: "raw", AllowCredentialTargeting: true},
			{APIKey: "rewrite", AllowCredentialTargeting: true, CredentialTargetResponseModelRewrite: true},
		}
		cfg.ResponseModelRewrite = config.ResponseModelRewriteConfig{Enabled: true, Rules: []config.ResponseModelRewriteRule{{RequestModels: []string{"gpt-target"}}}}
	})
	m := s.handlers.AuthManager
	m.RegisterExecutor(&responseModelTestExecutor{})
	a, err := m.Register(t.Context(), &coreauth.Auth{ID: "rewrite-target-options", Provider: "codex"})
	if err != nil {
		t.Fatal(err)
	}
	var expectedCount uint64
	for _, path := range []string{"/v1/responses", "/v1/chat/completions"} {
		for _, stream := range []bool{false, true} {
			for _, key := range []string{"raw", "rewrite"} {
				for _, model := range []string{"gpt-target", "unmatched"} {
					req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(fmt.Sprintf(`{"model":%q,"input":"test","messages":[{"role":"user","content":"test"}],"stream":%t}`, model, stream)))
					req.Header.Set("Authorization", "Bearer "+key+"-auth-"+a.Index)
					req.Header.Set("credential_target_response_model_rewrite", "true")
					w := httptest.NewRecorder()
					s.engine.ServeHTTP(w, req)
					want := "gpt-5.6-luna"
					if key == "rewrite" && model == "gpt-target" {
						want = model
						expectedCount++
					}
					if w.Code != 200 || !strings.Contains(w.Body.String(), `"model":"`+want+`"`) {
						t.Fatalf("%s key=%s stream=%t: %d %s", path, key, stream, w.Code, w.Body.String())
					}
					if stats := m.AuthResponseModelRewriteSummary(a, true); stats.Total != expectedCount {
						t.Fatalf("rewrite counted incorrectly: %+v", stats)
					}
				}
			}
		}
	}
	server := httptest.NewServer(s.engine)
	defer server.Close()
	for _, key := range []string{"raw", "rewrite"} {
		conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", http.Header{"Authorization": {"Bearer " + key + "-auth-" + a.Index}})
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"gpt-target","input":[]}`))
		want := "gpt-5.6-luna"
		if key == "rewrite" {
			want = "gpt-target"
			expectedCount++
		}
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				t.Fatal(err)
			}
			if model := gjson.GetBytes(data, "response.model").String(); model != "" && model != want {
				t.Fatalf("WS model=%s, want %s", model, want)
			}
			if gjson.GetBytes(data, "type").String() == "response.completed" {
				break
			}
		}
		_ = conn.Close()
		if stats := m.AuthResponseModelRewriteSummary(a, true); stats.Total != expectedCount {
			t.Fatalf("WS duplicated count: %+v", stats)
		}
	}
	updated, _ := config.Clone(s.currentConfig())
	updated.ResponseModelRewrite.Enabled = false
	if err := s.UpdateClients(updated); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-target","input":[]}`))
	req.Header.Set("Authorization", "Bearer rewrite-auth-"+a.Index)
	w := httptest.NewRecorder()
	s.engine.ServeHTTP(w, req)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"model":"gpt-5.6-luna"`) {
		t.Fatal("key option forced a globally disabled rewrite")
	}
	if m.AuthResponseModelRewriteSummary(a, true).Total != expectedCount {
		t.Fatal("disabled rewrite added count")
	}
}

func TestCredentialTargetRequestLimitIsPerKey(t *testing.T) {
	for _, limitSource := range []string{"global", "priority", "subscription", "fill-first-rpm"} {
		for _, transport := range []string{"responses", "responses-stream", "chat", "chat-stream", "websocket"} {
			t.Run(limitSource+"/"+transport, func(t *testing.T) {
				var calls atomic.Int32
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_fixture\",\"model\":\"gpt-target\",\"status\":\"completed\",\"output\":[]}}\n\n")
				}))
				defer upstream.Close()
				s := newTestServerWithConfig(t, func(cfg *config.Config) {
					cfg.APIKeys = []string{"raw", "obey"}
					cfg.APIKeyGroups = []config.APIKeyGroup{
						{APIKey: "raw", AllowCredentialTargeting: true},
						{APIKey: "obey", AllowCredentialTargeting: true, CredentialTargetRespectRequestLimit: true},
					}
					cfg.ProxyURL = "direct"
					cfg.Routing.PerAuthRequestLimit = 2
					cfg.Routing.PerAuthRequestWindowMinutes = 5
					switch limitSource {
					case "priority":
						cfg.Routing.PerAuthRequestLimit = 9
						limit := 2
						cfg.Routing.PriorityOverrides = []config.RoutingPriorityOverride{{Priority: 3, PerAuthRequestLimit: &limit}}
					case "subscription":
						cfg.Routing.PerAuthRequestLimit = 9
						limit := 2
						cfg.Routing.PriorityOverrides = []config.RoutingPriorityOverride{{Priority: 3, SubscriptionOverrides: []config.RoutingSubscriptionOverride{{Providers: []string{"codex"}, PlanTypes: []string{"pro"}, PerAuthRequestLimit: &limit}}}}
					case "fill-first-rpm":
						cfg.Routing.PerAuthRequestLimit = 0
						cfg.Routing.Strategy = "fill-first"
						cfg.Routing.FillFirstPerAuthRPM = 2
					}
				})
				m := s.handlers.AuthManager
				if limitSource == "fill-first-rpm" {
					m.SetSelector(&coreauth.FillFirstSelector{})
				}
				m.RegisterExecutor(runtimeexecutor.NewCodexExecutor(s.currentConfig()))
				a, err := m.Register(t.Context(), &coreauth.Auth{ID: "target-limit-options", Provider: "codex", Attributes: map[string]string{"base_url": upstream.URL, "priority": "3"}, Metadata: map[string]any{"access_token": "fixture", "plan_type": "pro"}})
				if err != nil {
					t.Fatal(err)
				}
				reg := registry.GetGlobalRegistry()
				reg.RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "gpt-target"}})
				t.Cleanup(func() { reg.UnregisterClient(a.ID) })
				server := httptest.NewServer(s.engine)
				defer server.Close()
				request := func(key string, targeted bool, want int) {
					t.Helper()
					before := calls.Load()
					model := "gpt-target"
					if targeted {
						key += "-auth-" + a.Index
						model = "gpt-unregistered"
					}
					if transport == "websocket" {
						conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", http.Header{"Authorization": {"Bearer " + key}})
						if err != nil {
							t.Fatal(err)
						}
						defer conn.Close()
						_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
						if err := conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[]}`, model))); err != nil {
							t.Fatal(err)
						}
						for {
							_, data, err := conn.ReadMessage()
							if err != nil {
								t.Fatal(err)
							}
							typ := gjson.GetBytes(data, "type").String()
							if typ == "error" {
								if want == 200 || gjson.GetBytes(data, "status").Int() != int64(want) {
									t.Fatalf("unexpected WS limit error: %s", data)
								}
								break
							}
							if typ == "response.completed" {
								if want != 200 {
									t.Fatalf("WS bypassed enabled limit: %s", data)
								}
								break
							}
						}
					} else {
						path := "/v1/responses"
						if strings.HasPrefix(transport, "chat") {
							path = "/v1/chat/completions"
						}
						req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(fmt.Sprintf(`{"model":%q,"input":"test","messages":[{"role":"user","content":"test"}],"stream":%t}`, model, strings.HasSuffix(transport, "-stream"))))
						req.Header.Set("Authorization", "Bearer "+key)
						req.Header.Set("credential_target_respect_request_limit", "false")
						w := httptest.NewRecorder()
						s.engine.ServeHTTP(w, req)
						if w.Code != want {
							t.Fatalf("key=%s want=%d status=%d body=%s", key, want, w.Code, w.Body.String())
						}
					}
					if delta := calls.Load() - before; (delta == 1) != (want == 200) || delta > 1 {
						t.Fatalf("unexpected number of upstream attempts: %d (status %d)", delta, want)
					}
				}
				request("raw", true, 200)
				request("raw", true, 200)
				request("raw", false, 200)
				request("obey", true, 200)
				request("obey", true, 429)
				request("raw", true, 200)
				request("raw", false, 429)
				// Hot reload changes only the test option, preserving the ordinary counter.
				for _, enabled := range []bool{false, true} {
					updated, err := config.Clone(s.currentConfig())
					if err != nil {
						t.Fatal(err)
					}
					updated.APIKeyGroups[1].CredentialTargetRespectRequestLimit = enabled
					if err := s.UpdateClients(updated); err != nil {
						t.Fatal(err)
					}
					want := 200
					if enabled {
						want = 429
					}
					request("obey", true, want)
				}
			})
		}
	}
}

func TestCredentialTargetStateRejectionDoesNotConsumeRequestCapacity(t *testing.T) {
	for _, policy := range []string{"hide", "error"} {
		t.Run(policy, func(t *testing.T) {
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_fixture\",\"model\":\"gpt-target\",\"status\":\"completed\",\"output\":[]}}\n\n")
			}))
			defer upstream.Close()
			s := newTestServerWithConfig(t, func(cfg *config.Config) {
				cfg.APIKeyGroups = []config.APIKeyGroup{{APIKey: "test-key", AllowCredentialTargeting: true, CredentialTargetRespectRequestLimit: true, CredentialTargetRespectStatePolicy: true}}
				cfg.Routing.PerAuthRequestLimit = 1
				cfg.Routing.PerAuthRequestWindowMinutes = 5
				cfg.ProxyURL = "direct"
				cfg.Codex.StateOverride = config.CodexStateOverrideConfig{Enabled: true, MissingPolicy: policy, Acquisition: "manual"}
			})
			m := s.handlers.AuthManager
			m.RegisterExecutor(runtimeexecutor.NewCodexExecutor(s.currentConfig()))
			a, err := m.Register(t.Context(), &coreauth.Auth{ID: "target-state-and-limit", Provider: "codex", Attributes: map[string]string{"base_url": upstream.URL}, Metadata: map[string]any{"access_token": "fixture"}})
			if err != nil {
				t.Fatal(err)
			}
			reg := registry.GetGlobalRegistry()
			reg.RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "gpt-target"}})
			c := helps.StateCredential(a, "gpt-target")
			codexstate.Default.Sync(s.currentConfig().Codex.StateOverride, []codexstate.Credential{c})
			t.Cleanup(func() { codexstate.Default.Sync(config.CodexStateOverrideConfig{}, nil); reg.UnregisterClient(a.ID) })
			request := func(want int, code string) {
				t.Helper()
				req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-target","input":[]}`))
				req.Header.Set("Authorization", "Bearer test-key-auth-"+a.Index)
				w := httptest.NewRecorder()
				s.engine.ServeHTTP(w, req)
				if w.Code != want || (code != "" && !strings.Contains(w.Body.String(), code)) {
					t.Fatalf("want %d/%s, got %d: %s", want, code, w.Code, w.Body.String())
				}
			}
			status := 429
			if policy == "hide" {
				status = 503
			}
			request(status, "")
			request(status, "")
			if calls.Load() != 0 {
				t.Fatal("local State rejection reached upstream")
			}
			codexstate.Default.Action(c.ID, c.Model, "acquire")
			codexstate.Default.Tick(t.Context(), time.Now(), func(context.Context, codexstate.Credential, config.CodexStateOverrideConfig) (codexstate.Result, error) {
				return codexstate.Result{State: strings.Repeat("s", 292), Model: c.Model, Completed: true}, nil
			})
			codexstate.Default.Wait()
			request(200, "")
			request(429, "auth_request_limited")
			if calls.Load() != 1 {
				t.Fatalf("unexpected upstream calls: %d", calls.Load())
			}
		})
	}
}
