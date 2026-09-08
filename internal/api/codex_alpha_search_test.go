package api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	proxyconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	runtimeexecutor "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

func newAlphaSearchTestServer(t *testing.T, enabled, passthrough bool) (*Server, *atomic.Int32, <-chan string) {
	t.Helper()
	calls := &atomic.Int32{}
	models := make(chan string, 8)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		if r.URL.Path != "/v1/alpha/search" {
			t.Error("search entered a different upstream endpoint")
			return
		}
		models <- gjson.GetBytes(body, "model").String()
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Set-Cookie", "upstream=fixture")
		w.Header().Set("Connection", "X-Private")
		w.Header().Set("X-Private", "hop-only")
		w.Header().Set("X-Request-Id", "search-fixture")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, `{"output":"native search","encrypted_output":"opaque","results":[{"future":true}]}`)
	}))
	t.Cleanup(upstream.Close)
	server := newTestServerWithConfig(t, func(cfg *proxyconfig.Config) {
		cfg.Debug = false
		cfg.ProxyURL = "direct"
		cfg.PassthroughHeaders = passthrough
		cfg.APIKeys = []string{"test-key", "blocked-key"}
		cfg.APIKeyGroups = []proxyconfig.APIKeyGroup{{APIKey: "test-key", Providers: []string{"codex"}}, {APIKey: "blocked-key", Providers: []string{"claude"}}}
		cfg.CodexKey = []proxyconfig.CodexKey{{APIKey: "upstream-fixture", BaseURL: upstream.URL + "/v1", Prefix: "team", AlphaSearch: enabled, Models: []proxyconfig.CodexModel{{Name: "gpt-5.5", Alias: "search-model"}}}}
	})
	manager := server.handlers.AuthManager
	manager.RegisterExecutor(runtimeexecutor.NewCodexAutoExecutor(server.currentConfig()))
	a := &auth.Auth{ID: "search-" + t.Name(), Provider: "codex", Prefix: "team", Attributes: map[string]string{"api_key": "upstream-fixture", "base_url": upstream.URL + "/v1"}}
	if enabled {
		a.Attributes[auth.CodexAlphaSearchAttributeKey] = "true"
	}
	if _, err := manager.Register(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "team/search-model"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(a.ID) })
	return server, calls, models
}

func TestCodexAlphaSearchRoutesAuthenticationAndAlias(t *testing.T) {
	for _, path := range []string{"/v1/alpha/search", "/backend-api/codex/alpha/search"} {
		for _, passthrough := range []bool{false, true} {
			t.Run(path+map[bool]string{false: "/default", true: "/headers"}[passthrough], func(t *testing.T) {
				server, calls, models := newAlphaSearchTestServer(t, true, passthrough)
				if calls.Load() != 0 {
					t.Fatal("search opened a connection at startup")
				}
				for _, key := range []string{"", "blocked-key", "test-key"} {
					r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"model":"team/search-model","id":"fixture","commands":[{"search_query":[{"q":"fixture"}]}]}`))
					if key != "" {
						r.Header.Set("Authorization", "Bearer "+key)
					}
					w := httptest.NewRecorder()
					server.engine.ServeHTTP(w, r)
					if key != "test-key" {
						want := http.StatusUnauthorized
						if key == "blocked-key" {
							want = http.StatusForbidden
						}
						if w.Code != want || calls.Load() != 0 {
							t.Fatalf("access denial: status=%d, calls=%d", w.Code, calls.Load())
						}
						continue
					}
					if w.Code != http.StatusAccepted || calls.Load() != 1 {
						t.Fatalf("search status=%d, calls=%d", w.Code, calls.Load())
					}
					if <-models != "gpt-5.5" {
						t.Fatal("selected credential alias was not resolved")
					}
					if w.Header().Get("Content-Type") != "application/json; charset=utf-8" || gjson.GetBytes(w.Body.Bytes(), "encrypted_output").String() != "opaque" {
						t.Fatal("native response was converted")
					}
					if w.Header().Get("Set-Cookie") != "" || w.Header().Get("X-Private") != "" {
						t.Fatal("unsafe upstream header escaped")
					}
					if (w.Header().Get("X-Request-Id") != "") != passthrough {
						t.Fatal("upstream header preference was ignored")
					}
				}
			})
		}
	}
}

func TestCodexAlphaSearchUnavailableAndInvalidRequestsDoNotConnect(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		server, calls, _ := newAlphaSearchTestServer(t, enabled, false)
		bodies := []string{`{`, `[]`, `{"model":1}`, `{"model":""}`, `{"model":"unregistered"}`}
		if !enabled {
			bodies = append(bodies, `{"model":"team/search-model"}`)
		}
		for _, body := range bodies {
			r := httptest.NewRequest(http.MethodPost, "/v1/alpha/search", strings.NewReader(body))
			r.Header.Set("Authorization", "Bearer test-key")
			w := httptest.NewRecorder()
			server.engine.ServeHTTP(w, r)
			if w.Code < 400 || w.Code == 404 || calls.Load() != 0 {
				t.Fatalf("invalid/unavailable request status=%d calls=%d", w.Code, calls.Load())
			}
		}
	}
}

func TestCodexAlphaSearchAliasesShareReadinessAndBodyAudit(t *testing.T) {
	for _, path := range []string{"/v1/alpha/search", "/backend-api/codex/alpha/search"} {
		startup := NewStartupState()
		server := newTestServerWithConfigAndOptions(t, func(cfg *proxyconfig.Config) {
			cfg.Debug = false
			cfg.RequestBodyAudit = proxyconfig.RequestBodyAuditConfig{Enable: true, Keywords: []string{"blocked-search-fixture"}, Error: proxyconfig.RequestBodyAuditErrorConfig{StatusCode: 451, Message: "fixture block", Code: "fixture_block"}}
		}, WithStartupState(startup))
		for _, ready := range []bool{false, true} {
			if ready {
				startup.MarkReady()
			}
			r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"model":"search-model","input":"blocked-search-fixture"}`))
			r.Header.Set("Authorization", "Bearer test-key")
			w := httptest.NewRecorder()
			server.engine.ServeHTTP(w, r)
			want := 503
			if ready {
				want = 451
			}
			if w.Code != want {
				t.Fatalf("alias %s skipped policy: status %d", path, w.Code)
			}
		}
	}
}

func TestCodexAlphaSearchRequestLimitDoesNotTruncateAndForward(t *testing.T) {
	server, calls, _ := newAlphaSearchTestServer(t, true, false)
	for _, path := range []string{"/v1/alpha/search", "/backend-api/codex/alpha/search"} {
		r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(strings.Repeat(" ", helps.CodexAlphaSearchMaxRequestBytes+1)))
		r.Header.Set("Authorization", "Bearer test-key")
		w := httptest.NewRecorder()
		server.engine.ServeHTTP(w, r)
		if w.Code != http.StatusRequestEntityTooLarge || calls.Load() != 0 {
			t.Fatal("oversized search was forwarded or misclassified")
		}
	}
}
