package live

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type callObserverExecutor struct {
	*executor.CodexAutoExecutor
	after func(*http.Request, *http.Response)
}

func (e *callObserverExecutor) HttpRequest(ctx context.Context, a *auth.Auth, r *http.Request) (*http.Response, error) {
	response, errRequest := e.CodexAutoExecutor.HttpRequest(ctx, a, r)
	if response != nil && e.after != nil {
		e.after(r, response)
	}
	return response, errRequest
}

func newLiveCallsFixture(t *testing.T, cfg *config.Config, upstream http.HandlerFunc) (*Handler, *auth.Manager, *callObserverExecutor, string) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	cfg.ProxyURL = "direct"
	server := httptest.NewServer(upstream)
	t.Cleanup(server.Close)
	m := auth.NewManager(nil, nil, nil)
	m.SetConfig(cfg)
	e := &callObserverExecutor{CodexAutoExecutor: executor.NewCodexAutoExecutor(cfg)}
	m.RegisterExecutor(e)
	a := &auth.Auth{ID: "call-handler-" + t.Name(), Provider: "codex", Metadata: map[string]any{"access_token": "fixture-oauth", "account_id": "fixture-account"}}
	if _, errRegister := m.Register(t.Context(), a); errRegister != nil {
		t.Fatal(errRegister)
	}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(a.ID, "codex", registry.GetCodexRealtimeModels())
	t.Cleanup(func() { reg.UnregisterClient(a.ID) })
	h := NewHandler(cfg, m)
	h.callURL = server.URL + "/backend-api/codex/realtime/calls?intent=quicksilver&architecture=avas"
	h.websocketBaseURL = "ws" + strings.TrimPrefix(server.URL, "http") + "/v1"
	t.Cleanup(h.Close)
	return h, m, e, a.ID
}

func callHandlerRequest(t *testing.T, h *Handler, path, kind string, body io.Reader) *httptest.ResponseRecorder {
	t.Helper()
	c, w := liveHandlerRequest(t.Context(), "fixture-caller", "")
	c.Set("accessProvider", "fixture-access")
	c.Request = httptest.NewRequest(http.MethodPost, path, body)
	c.Request.Header.Set("Content-Type", kind)
	c.Request.Header.Set("Cookie", "downstream=private")
	c.Request.Header.Set("Authorization", "Bearer downstream-fixture")
	h.HandleCall(c)
	return w
}

func TestLiveCallCreatesNativeSessionAndPersistsAfterRequestAndDisable(t *testing.T) {
	var upstreamCalls atomic.Int32
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	h, _, _, _ := newLiveCallsFixture(t, cfg, func(w http.ResponseWriter, r *http.Request) {
		n := upstreamCalls.Add(1)
		body, _ := io.ReadAll(r.Body)
		if r.URL.Path != "/backend-api/codex/realtime/calls" || r.URL.Query().Get("architecture") != "avas" || r.Header.Get("Authorization") != "Bearer fixture-oauth" || r.Header.Get("Chatgpt-Account-Id") != "fixture-account" || r.Header.Get("Cookie") != "" || !bytes.Contains(body, []byte(registry.CodexLiveModelID)) {
			t.Error("native call request changed routing or leaked caller authentication")
		}
		w.Header().Set("Content-Type", "application/sdp")
		w.Header().Set("Location", fmt.Sprintf("https://upstream.invalid/v1/realtime/calls/call_%d", n))
		w.Header().Set("Set-Cookie", "upstream=private")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("v=0\r\nanswer"))
	})
	for _, path := range []string{"/v1/live", "/v1/realtime/calls", "/v1/realtime"} {
		w := callHandlerRequest(t, h, path, "application/json", strings.NewReader(`{"sdp":"v=0","session":{"model":"gpt-realtime","audio":{"output":{"voice":"marin"}}}}`))
		if w.Code != 201 || w.Body.String() != "v=0\r\nanswer" || w.Header().Get("Set-Cookie") != "" || !strings.HasPrefix(w.Header().Get("Location"), "/v1/") {
			t.Fatal("call response changed native payload or location isolation")
		}
	}
	h.UpdateConfig(&config.Config{})
	h.calls.mu.Lock()
	count := len(h.calls.entries)
	for _, entry := range h.calls.entries {
		if entry.call.lease.Context().Err() != nil || entry.call.lease.Context().Value("gin") != nil {
			t.Error("committed call retained request context or was cancelled")
		}
	}
	h.calls.mu.Unlock()
	if count != 3 {
		t.Fatal("calls did not survive request completion")
	}
	w := callHandlerRequest(t, h, "/v1/live", "application/json", strings.NewReader(`{}`))
	if w.Code != 503 || !strings.Contains(w.Body.String(), liveDisabledCode) || upstreamCalls.Load() != 3 {
		t.Fatal("disabled creation selected another credential")
	}
}

func TestLiveCallMultipartSDPAndResolvedAliasReachUpstream(t *testing.T) {
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	h, m, _, authID := newLiveCallsFixture(t, cfg, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !bytes.Contains(body, []byte(`"model":"gpt-live-1-codex"`)) || bytes.Contains(body, []byte("team/voice")) {
			t.Error("public model alias leaked into native call")
		}
		w.Header().Set("Location", "/v1/realtime/calls/call_alias")
		_, _ = w.Write([]byte("v=0"))
	})
	a, _ := m.GetByID(authID)
	a.Prefix = "team"
	if _, errUpdate := m.Update(t.Context(), a); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	m.SetOAuthModelAlias(map[string][]config.OAuthModelAlias{"codex": {{Name: registry.CodexLiveModelID, Alias: "voice"}}})
	registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: "team/voice", UpstreamID: registry.CodexLiveModelID, Type: registry.CodexRealtimeModelType}})
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	_ = writer.WriteField("sdp", "v=0")
	_ = writer.WriteField("session", `{"model":"team/voice","unknown":90071992547409931234}`)
	_ = writer.Close()
	w := callHandlerRequest(t, h, "/v1/realtime/calls", writer.FormDataContentType(), &body)
	if w.Code != 200 {
		t.Fatalf("native alias call failed: %d", w.Code)
	}
}

func TestLiveCallUpstreamRejectionPreservesStatusAndDoesNotRetry(t *testing.T) {
	for _, status := range []int{400, 401, 403, 404, 429, 500} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Codex.LiveEnabled = true
			cfg.RequestRetry = 5
			var attempts atomic.Int32
			h, _, _, _ := newLiveCallsFixture(t, cfg, func(w http.ResponseWriter, r *http.Request) {
				attempts.Add(1)
				w.Header().Set("Retry-After", "7")
				w.Header().Set("Set-Cookie", "private=fixture")
				w.WriteHeader(status)
				_, _ = w.Write([]byte(`{"error":{"code":"misalignment_policy_violation","message":"fixture"}}`))
			})
			w := callHandlerRequest(t, h, "/v1/live", "application/sdp", strings.NewReader("v=0"))
			if w.Code != status || attempts.Load() != 1 || !strings.Contains(w.Body.String(), "misalignment_policy_violation") || w.Header().Get("Set-Cookie") != "" {
				t.Fatal("native rejection changed status, leaked headers or retried")
			}
			if status == 429 && w.Header().Get("Retry-After") != "7" {
				t.Fatal("rate limit retry hint was lost")
			}
		})
	}
}

func TestLiveCallDisableAfterAllocationRollsBackUsingSameCredential(t *testing.T) {
	var creations, rollbacks atomic.Int32
	rolledBack := make(chan struct{}, 1)
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	h, _, e, _ := newLiveCallsFixture(t, cfg, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/hangup") {
			if r.Header.Get("Authorization") != "Bearer fixture-oauth" || r.URL.Path != "/v1/realtime/calls/call_rollback/hangup" {
				t.Error("rollback used another identity or call")
			}
			rollbacks.Add(1)
			w.WriteHeader(200)
			rolledBack <- struct{}{}
			return
		}
		creations.Add(1)
		w.Header().Set("Location", "/v1/realtime/calls/call_rollback")
		w.WriteHeader(201)
		_, _ = w.Write([]byte("v=0"))
	})
	e.after = func(r *http.Request, _ *http.Response) {
		if strings.Contains(r.URL.Path, "/backend-api/") {
			h.UpdateConfig(&config.Config{})
		}
	}
	w := callHandlerRequest(t, h, "/v1/live", "application/sdp", strings.NewReader("v=0"))
	if w.Code != 503 || !strings.Contains(w.Body.String(), liveDisabledCode) {
		t.Fatalf("allocated disabled call was committed: %d", w.Code)
	}
	select {
	case <-rolledBack:
	case <-time.After(time.Second):
		t.Fatal("allocated call was not rolled back")
	}
	if creations.Load() != 1 || rollbacks.Load() != 1 {
		t.Fatal("rollback added a creation attempt")
	}
	h.calls.mu.Lock()
	count := len(h.calls.entries)
	h.calls.mu.Unlock()
	if count != 0 {
		t.Fatal("failed setup left a registered call")
	}
}

type failedCallWriter struct{ gin.ResponseWriter }

func (w failedCallWriter) Write([]byte) (int, error) {
	return 0, errors.New("fixture delivery failure")
}

func TestLiveCallDeliveryFailureRollsBackAndReleasesRegistration(t *testing.T) {
	rolledBack := make(chan struct{}, 1)
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	h, _, _, _ := newLiveCallsFixture(t, cfg, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/hangup") {
			w.WriteHeader(200)
			rolledBack <- struct{}{}
			return
		}
		w.Header().Set("Location", "/v1/realtime/calls/call_delivery")
		_, _ = w.Write([]byte("v=0"))
	})
	c, _ := liveHandlerRequest(t.Context(), "fixture-caller", "")
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/live", strings.NewReader(`{"sdp":"v=0"}`))
	c.Writer = failedCallWriter{c.Writer}
	h.HandleCall(c)
	select {
	case <-rolledBack:
	case <-time.After(time.Second):
		t.Fatal("failed delivery did not hang up allocated call")
	}
	h.calls.mu.Lock()
	count := len(h.calls.entries)
	h.calls.mu.Unlock()
	if count != 0 {
		t.Fatal("failed delivery remained accessible")
	}
}

func TestLiveCallDuplicateIDDoesNotHangupExistingCall(t *testing.T) {
	var hangups atomic.Int32
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	h, _, _, _ := newLiveCallsFixture(t, cfg, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/hangup") {
			hangups.Add(1)
			return
		}
		w.Header().Set("Location", "/v1/realtime/calls/call_duplicate")
		_, _ = w.Write([]byte("v=0"))
	})
	for _, status := range []int{200, 503} {
		w := callHandlerRequest(t, h, "/v1/live", "application/sdp", strings.NewReader("v=0"))
		if w.Code != status {
			t.Fatalf("duplicate call status=%d, want %d", w.Code, status)
		}
	}
	h.calls.mu.Lock()
	old := h.calls.entries["call_duplicate"].call
	h.calls.mu.Unlock()
	if old.lease.Context().Err() != nil || hangups.Load() != 0 {
		t.Fatal("duplicate response terminated the established call")
	}
}

func TestLiveCallReadFailureAfterAllocationRollsBack(t *testing.T) {
	rolledBack := make(chan struct{}, 1)
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	h, _, e, _ := newLiveCallsFixture(t, cfg, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/hangup") {
			rolledBack <- struct{}{}
			return
		}
		w.Header().Set("Location", "/v1/realtime/calls/call_read_error")
		_, _ = w.Write([]byte("v=0"))
	})
	e.after = func(r *http.Request, response *http.Response) {
		if strings.Contains(r.URL.Path, "/backend-api/") {
			_ = response.Body.Close()
			response.Body = io.NopCloser(callFailingReader{io.ErrUnexpectedEOF})
		}
	}
	w := callHandlerRequest(t, h, "/v1/live", "application/sdp", strings.NewReader("v=0"))
	if w.Code != 502 {
		t.Fatal("truncated answer was reported successful")
	}
	select {
	case <-rolledBack:
	case <-time.After(time.Second):
		t.Fatal("truncated answer leaked allocated call")
	}
}

func TestLiveCallPendingHTTPIsCancelledByDisable(t *testing.T) {
	started, cancelled := make(chan struct{}), make(chan struct{})
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	h, _, _, _ := newLiveCallsFixture(t, cfg, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(started)
		<-r.Context().Done()
		close(cancelled)
	})
	finished := make(chan *httptest.ResponseRecorder, 1)
	go func() { finished <- callHandlerRequest(t, h, "/v1/live", "application/sdp", strings.NewReader("v=0")) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("request did not reach fake upstream")
	}
	h.UpdateConfig(&config.Config{})
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("disabled pending upstream was not cancelled")
	}
	select {
	case w := <-finished:
		if w.Code != 503 || !strings.Contains(w.Body.String(), liveDisabledCode) {
			t.Fatal("pending request lost disabled reason")
		}
	case <-time.After(time.Second):
		t.Fatal("pending creating handler did not finish")
	}
}

func TestLiveCallValidationDoesNotSelectOrCallUpstream(t *testing.T) {
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	var attempts atomic.Int32
	h, _, _, _ := newLiveCallsFixture(t, cfg, func(w http.ResponseWriter, r *http.Request) { attempts.Add(1); w.WriteHeader(500) })
	for _, test := range []struct {
		body   string
		status int
	}{{`{"session":null}`, 400}, {strings.Repeat("x", maxCallBodySize+1), 413}} {
		w := callHandlerRequest(t, h, "/v1/live", "application/json", strings.NewReader(test.body))
		if w.Code != test.status {
			t.Fatalf("invalid payload status=%d, want %d", w.Code, test.status)
		}
	}
	c, w := liveHandlerRequest(t.Context(), "", "")
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/live", strings.NewReader(`{}`))
	h.HandleCall(c)
	if w.Code != 401 || attempts.Load() != 0 {
		t.Fatal("invalid or unauthenticated request reached upstream")
	}
}
