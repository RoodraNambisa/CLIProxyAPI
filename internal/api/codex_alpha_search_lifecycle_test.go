package api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	proxyconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	runtimeexecutor "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

func newSearchLifecycleFixture(t *testing.T, upstream http.HandlerFunc, count int, retry int, rule *proxyconfig.RequestScopedErrorRule) (*Server, []string) {
	t.Helper()
	remote := httptest.NewServer(upstream)
	t.Cleanup(remote.Close)
	server := newTestServerWithConfig(t, func(cfg *proxyconfig.Config) {
		cfg.Debug = false
		cfg.ProxyURL = "direct"
		cfg.RequestRetry = retry
		cfg.MaxRetryInterval = 0
	})
	manager := server.handlers.AuthManager
	manager.SetSelector(&auth.FillFirstSelector{})
	manager.RegisterExecutor(runtimeexecutor.NewCodexAutoExecutor(server.currentConfig()))
	ids := make([]string, 0, count)
	for index := range count {
		id := string(rune('a'+index)) + "-" + t.Name()
		credential := &auth.Auth{ID: id, Provider: "codex", Attributes: map[string]string{"api_key": "fixture", "base_url": remote.URL, auth.CodexAlphaSearchAttributeKey: "true"}}
		if rule != nil {
			credential.Metadata = map[string]any{"request_scoped_errors": []proxyconfig.RequestScopedErrorRule{*rule}}
		}
		if _, err := manager.Register(t.Context(), credential); err != nil {
			t.Fatal(err)
		}
		registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: "search-lifecycle-model"}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
		ids = append(ids, id)
	}
	return server, ids
}

func searchLifecycleRequest(ctx context.Context) *http.Request {
	r := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/alpha/search", strings.NewReader(`{"model":"search-lifecycle-model","id":"fixture"}`))
	r.Header.Set("Authorization", "Bearer test-key")
	return r
}

func TestCodexAlphaSearchKeepsRetryAndRefusalSemantics(t *testing.T) {
	for _, tc := range []struct {
		name          string
		status        int
		code, action  string
		negativeRetry bool
		wantCalls     int32
	}{
		{"policy refusal", 403, "misalignment_policy_violation", "continue-and-cooldown", false, 1},
		{"real rate limit", 429, "misalignment_policy_violation", "", false, 2},
		{"existing retry rounds", 500, "fixture_failure", "continue", false, 6},
		{"negative retry is zero", 500, "fixture_failure", "continue", true, 2},
		{"explicit stop", 500, "fixture_failure", "stop", false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			var rule *proxyconfig.RequestScopedErrorRule
			if tc.action != "" {
				rule = &proxyconfig.RequestScopedErrorRule{Status: tc.status, Match: []string{"fixture"}, Action: tc.action}
			}
			server, ids := newSearchLifecycleFixture(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				if tc.status == 429 {
					w.Header().Set("Retry-After", "3")
				}
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, `{"error":{"code":"`+tc.code+`","message":"fixture"}}`)
			}, 2, 2, rule)
			if tc.negativeRetry {
				for _, id := range ids {
					a, _ := server.handlers.AuthManager.GetByID(id)
					a.Metadata["request_retry"] = -1
					if _, err := server.handlers.AuthManager.Update(t.Context(), a); err != nil {
						t.Fatal(err)
					}
				}
			}
			w := httptest.NewRecorder()
			server.engine.ServeHTTP(w, searchLifecycleRequest(t.Context()))
			if w.Code != tc.status || calls.Load() != tc.wantCalls || gjson.GetBytes(w.Body.Bytes(), "error.code").String() != tc.code {
				t.Fatalf("status=%d calls=%d, want status=%d calls=%d", w.Code, calls.Load(), tc.status, tc.wantCalls)
			}
			if tc.status == 429 && w.Header().Get("Retry-After") != "3" {
				t.Fatal("final rate limit header was lost")
			}
			for _, id := range ids {
				a, _ := server.handlers.AuthManager.GetByID(id)
				if tc.status != 429 && (a.Unavailable || !a.NextRetryAfter.IsZero()) {
					t.Fatal("request fault or no-cooldown action changed credential availability")
				}
				if state := a.ModelStates["search-lifecycle-model"]; tc.status != 429 && state != nil && state.NextRetryAfter.After(time.Now()) {
					t.Fatal("no-cooldown action cooled the model")
				}
				if tc.status == 403 && len(a.ModelStates) != 0 {
					t.Fatal("policy refusal changed model health")
				}
			}
		})
	}
}

func TestCodexAlphaSearchCallerCancellationAndCredentialRetirement(t *testing.T) {
	for _, retire := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "retire"}[retire], func(t *testing.T) {
			started := make(chan struct{})
			upstreamCanceled := make(chan struct{})
			server, ids := newSearchLifecycleFixture(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				close(started)
				<-r.Context().Done()
				close(upstreamCanceled)
			}, 1, 2, nil)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan int, 1)
			go func() {
				w := httptest.NewRecorder()
				server.engine.ServeHTTP(w, searchLifecycleRequest(ctx))
				done <- w.Code
			}()
			select {
			case <-started:
			case <-time.After(3 * time.Second):
				t.Fatal("search did not start")
			}
			if retire {
				if err := server.handlers.AuthManager.Delete(t.Context(), ids[0]); err != nil {
					t.Fatal(err)
				}
			} else {
				cancel()
			}
			select {
			case <-upstreamCanceled:
			case <-time.After(3 * time.Second):
				t.Fatal("upstream connection remained active")
			}
			select {
			case code := <-done:
				if code < 400 {
					t.Fatal("unfinished search returned success")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("handler did not release retired/canceled execution")
			}
		})
	}
}

func TestCodexAlphaSearchCapabilityUpdateHonorsCredentialRetirement(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	server, ids := newSearchLifecycleFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		close(started)
		select {
		case <-release:
			_, _ = io.WriteString(w, `{"output":"ok"}`)
		case <-r.Context().Done():
		}
	}, 1, 0, nil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan int, 1)
	go func() {
		w := httptest.NewRecorder()
		server.engine.ServeHTTP(w, searchLifecycleRequest(ctx))
		done <- w.Code
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("search did not start")
	}
	a, _ := server.handlers.AuthManager.GetByID(ids[0])
	previous := a.Clone()
	a.Attributes[auth.CodexAlphaSearchAttributeKey] = "false"
	if _, err := server.handlers.AuthManager.Update(t.Context(), a); err != nil {
		t.Fatal(err)
	}
	if !previous.RuntimeInstanceRetired() {
		t.Fatal("fixture did not exercise the existing retirement path")
	}
	close(release)
	select {
	case code := <-done:
		if code < 400 {
			t.Fatal("retired credential completed an in-flight attempt")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("in-flight request did not finish")
	}
	w := httptest.NewRecorder()
	server.engine.ServeHTTP(w, searchLifecycleRequest(t.Context()))
	if w.Code < 400 || calls.Load() != 1 {
		t.Fatal("later request used disabled search capability")
	}
}

func TestCodexAlphaSearchInvalidCredentialEndpointUsesExistingBudget(t *testing.T) {
	for _, maximum := range []int{0, 1} {
		var calls atomic.Int32
		server, ids := newSearchLifecycleFixture(t, func(w http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			_, _ = io.WriteString(w, `{"output":"ok"}`)
		}, 2, 0, nil)
		manager := server.handlers.AuthManager
		manager.SetRetryConfig(0, 0, maximum)
		broken, _ := manager.GetByID(ids[0])
		broken.Attributes["base_url"] = "/invalid-relative-endpoint"
		if _, err := manager.Update(t.Context(), broken); err != nil {
			t.Fatal(err)
		}
		w := httptest.NewRecorder()
		server.engine.ServeHTTP(w, searchLifecycleRequest(t.Context()))
		if maximum == 0 && (w.Code != 200 || calls.Load() != 1) {
			t.Fatal("credential-local endpoint error blocked another eligible credential")
		}
		if maximum == 1 && (w.Code < 400 || calls.Load() != 0) {
			t.Fatal("endpoint fallback exceeded the credential budget")
		}
		broken, _ = manager.GetByID(ids[0])
		if broken.Unavailable || !broken.NextRetryAfter.IsZero() || len(broken.ModelStates) != 0 {
			t.Fatal("local endpoint error changed upstream credential health")
		}
	}
}
