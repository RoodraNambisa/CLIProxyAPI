package executor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	authcore "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestImageBootstrapTimeoutsReleaseConnections(t *testing.T) {
	for _, phase := range []string{"headers", "body", "redirect"} {
		t.Run(phase, func(t *testing.T) {
			var calls atomic.Int32
			closed := make(chan struct{}, 4)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if phase == "body" {
					_, _ = io.WriteString(w, "<html>")
					w.(http.Flusher).Flush()
				}
				if phase == "redirect" {
					w.Header().Set("Location", "/next")
					w.WriteHeader(302)
					w.(http.Flusher).Flush()
				}
				<-r.Context().Done()
				closed <- struct{}{}
			}))
			defer server.Close()
			e := NewChatGPTWebExecutor(nil, nil)
			defer e.Close()
			client, credential, err := e.newRuntimeClient(chatGPTWebRuntimeAuth())
			if err != nil {
				t.Fatal(err)
			}
			defer client.CloseIdleConnections()
			ctx := core.WithImageBootstrapPolicy(t.Context(), core.ImageBootstrapPolicy{Timeout: 150 * time.Millisecond, Retries: 1})
			registry := newChatGPTWebImageTaskRegistry(time.Now)
			ctx, task := registry.begin(ctx, "test")
			defer task.finish()
			started := time.Now()
			_, _, err = e.fetchChatGPTWebImageBootstrap(ctx, client, credential, server.URL+"/", nil)
			var timeout *core.ImageBootstrapError
			if !errors.As(err, &timeout) || !timeout.Timeout || timeout.StatusCode() != 504 || !timeout.SkipAuthResult() || !timeout.RetryOtherAuth() || core.IsImageRequestTimeout(err) {
				t.Fatalf("wrong error: %T %v", err, err)
			}
			if elapsed := time.Since(started); elapsed > 3*time.Second {
				t.Fatalf("cancellation took %v", elapsed)
			}
			if calls.Load() != 2 {
				t.Fatalf("homepage calls = %d", calls.Load())
			}
			for range 2 {
				select {
				case <-closed:
				case <-time.After(time.Second):
					t.Fatal("socket leaked")
				}
			}
			if got := registry.snapshot().Tasks[0]; got.BootstrapAttempt != 2 || got.BootstrapMaxAttempts != 2 {
				t.Fatalf("attempt state = %+v", got)
			}
		})
	}
}

func TestImageBootstrapRetryUsesFreshConnectionAndSharesCookies(t *testing.T) {
	var calls, committed atomic.Int32
	var mu sync.Mutex
	var addresses []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		addresses = append(addresses, r.RemoteAddr)
		mu.Unlock()
		if calls.Add(1) == 1 {
			w.Header().Set("Content-Length", "100")
			http.SetCookie(w, &http.Cookie{Name: "bootstrap", Value: "keep", Path: "/"})
			_, _ = io.WriteString(w, "short")
			return
		}
		if cookie, err := r.Cookie("bootstrap"); err != nil || cookie.Value != "keep" {
			t.Errorf("cookie missing: %v", err)
		}
		_, _ = io.WriteString(w, "<html>ok</html>")
	}))
	defer server.Close()
	e := NewChatGPTWebExecutor(nil, nil)
	defer e.Close()
	client, credential, err := e.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	client.SetBeforeRequestHook(func() { committed.Add(1) })
	observer := &chatGPTWebImagePhaseTestObserver{}
	ctx := core.WithRequestPhaseObserver(t.Context(), observer)
	ctx = core.WithImageBootstrapPolicy(ctx, core.ImageBootstrapPolicy{Retries: 1})
	before := core.ImageBootstrapSnapshot()
	_, body, err := e.fetchChatGPTWebImageBootstrap(ctx, client, credential, server.URL, nil)
	if err != nil || string(body) != "<html>ok</html>" {
		t.Fatalf("body %q: %v", body, err)
	}
	if calls.Load() != 2 || committed.Load() != 1 {
		t.Fatalf("calls/commits = %d/%d", calls.Load(), committed.Load())
	}
	mu.Lock()
	same := addresses[0] == addresses[1]
	mu.Unlock()
	if same {
		t.Fatal("connection reused")
	}
	if len(client.ExportCookies()) == 0 {
		t.Fatal("shared cookies lost")
	}
	after := core.ImageBootstrapSnapshot()
	if after.Attempts-before.Attempts != 2 || after.Retries-before.Retries != 1 || after.RetrySuccesses-before.RetrySuccesses != 1 {
		t.Fatalf("counters %v -> %v", before, after)
	}
	if observer.count(core.ImagePhaseBootstrapHTTP) != 2 || observer.count(core.ImagePhaseBootstrapBody) != 2 || observer.count(core.ImagePhaseBootstrapRetryWait) != 1 {
		t.Fatalf("phase counts: %v", observer.counts)
	}
}

func TestImageBootstrapNoRetryOnHTTPOrInvalidInput(t *testing.T) {
	for _, status := range []int{401, 403, 429, 500, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(status)
				_, _ = io.WriteString(w, "error")
			}))
			defer server.Close()
			e := NewChatGPTWebExecutor(nil, nil)
			defer e.Close()
			client, credential, err := e.newRuntimeClient(chatGPTWebRuntimeAuth())
			if err != nil {
				t.Fatal(err)
			}
			defer client.CloseIdleConnections()
			ctx := core.WithImageBootstrapPolicy(t.Context(), core.ImageBootstrapPolicy{Timeout: time.Second, Retries: 5})
			response, _, err := e.fetchChatGPTWebImageBootstrap(ctx, client, credential, server.URL, nil)
			if err != nil || response.StatusCode != status || calls.Load() != 1 {
				t.Fatalf("status/calls/error: %v/%d/%v", response, calls.Load(), err)
			}
		})
	}
}

func TestImageBootstrapParentBudgetStopsRetries(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); <-r.Context().Done() }))
	defer server.Close()
	e := NewChatGPTWebExecutor(nil, nil)
	defer e.Close()
	client, credential, err := e.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	budget := core.NewImageRequestBudget(t.Context(), time.Now(), 0, 250*time.Millisecond)
	defer budget.Close()
	ctx, cancel := budget.Bind(t.Context())
	defer cancel()
	if err := budget.Select("chatgpt-web"); err != nil {
		t.Fatal(err)
	}
	ctx = core.WithImageBootstrapPolicy(ctx, core.ImageBootstrapPolicy{Timeout: 100 * time.Millisecond, Retries: 5})
	_, _, err = e.fetchChatGPTWebImageBootstrap(ctx, client, credential, server.URL, nil)
	if !core.IsImageRequestTimeout(err) || calls.Load() != 1 {
		t.Fatalf("calls/error = %d/%v", calls.Load(), err)
	}
}

func TestImageBootstrapPolicyPinnedOnPrepare(t *testing.T) {
	cfg := &config.Config{}
	cfg.Images.ChatGPTWeb.BootstrapTimeoutSeconds = 10
	cfg.Images.ChatGPTWeb.BootstrapRetries = 1
	e := NewChatGPTWebExecutor(cfg, nil)
	defer e.Close()
	ctx := core.WithImageBootstrapPolicy(t.Context(), core.ImageBootstrapPolicy{Timeout: 3 * time.Second, Retries: 2})
	req := core.Request{Model: "gpt-image-2", Payload: []byte(`{"model":"gpt-image-2","input":"draw","tools":[{"type":"image_generation"}]}`)}
	opts := core.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex}
	prepared, err := e.prepareRuntimeRequestTemplate(ctx, req, opts, false)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.discardUsageProjection()
	if prepared.bootstrapPolicy.Timeout != 3*time.Second || prepared.bootstrapPolicy.Retries != 2 {
		t.Fatalf("not pinned: %+v", prepared.bootstrapPolicy)
	}
	zeroCtx := core.WithImageBootstrapPolicy(t.Context(), core.ImageBootstrapPolicy{})
	disabled, err := e.prepareRuntimeRequestTemplate(zeroCtx, req, opts, false)
	if err != nil {
		t.Fatal(err)
	}
	defer disabled.discardUsageProjection()
	if disabled.bootstrapPolicy.Enabled() {
		t.Fatal("disabled request adopted new config")
	}
}

func TestImageBootstrapCanceledParentDoesNotStart(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	ctx = core.WithImageBootstrapPolicy(ctx, core.ImageBootstrapPolicy{Timeout: time.Second, Retries: 5})
	_, _, err := (&ChatGPTWebExecutor{}).fetchChatGPTWebImageBootstrap(ctx, nil, nil, "http://127.0.0.1/", nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

func TestImageBootstrapRetriesDoNotReplayGeneration(t *testing.T) {
	fixture := newChatGPTWebImageFixture(t)
	defer fixture.Close()
	var mu sync.Mutex
	calls := map[string]int{}
	var baseURL string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls[r.URL.Path]++
		attempt := calls[r.URL.Path]
		mu.Unlock()
		if r.URL.Path == "/" && attempt == 1 {
			w.Header().Set("Content-Length", "100")
			_, _ = io.WriteString(w, "partial")
			return
		}
		recorder := httptest.NewRecorder()
		fixture.Config.Handler.ServeHTTP(recorder, r)
		for key, values := range recorder.Header() {
			w.Header()[key] = values
		}
		w.WriteHeader(recorder.Code)
		_, _ = w.Write(bytes.ReplaceAll(recorder.Body.Bytes(), []byte(fixture.URL), []byte(baseURL)))
	}))
	baseURL = server.URL
	defer server.Close()
	e := NewChatGPTWebExecutor(nil, nil)
	defer e.Close()
	e.runtimeBaseURL = server.URL
	disableChatGPTWebImagePollWaits(e)
	ctx := core.WithImageBootstrapPolicy(t.Context(), core.ImageBootstrapPolicy{Retries: 1})
	_, err := e.Execute(ctx, chatGPTWebRuntimeAuth(), core.Request{Model: "gpt-image-2", Payload: []byte(`{"model":"gpt-5.4","input":"draw","tools":[{"type":"image_generation","model":"gpt-image-2"}]}`)}, core.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex})
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls["/"] != 2 {
		t.Fatalf("homepages: %v", calls)
	}
	for _, path := range []string{"/backend-api/sentinel/chat-requirements/prepare", "/backend-api/sentinel/chat-requirements/finalize", "/backend-api/f/conversation", "/signed-image"} {
		if calls[path] != 1 {
			t.Errorf("%s called %d times", path, calls[path])
		}
	}
}

func TestImageBootstrapFullExecutorPreservesFailoverError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	e := NewChatGPTWebExecutor(nil, nil)
	defer e.Close()
	e.runtimeBaseURL = server.URL
	ctx := core.WithImageBootstrapPolicy(t.Context(), core.ImageBootstrapPolicy{Timeout: 150 * time.Millisecond})
	_, err := e.Execute(ctx, chatGPTWebRuntimeAuth(), core.Request{Model: "gpt-image-2", Payload: []byte(`{"model":"gpt-5.4","input":"draw","tools":[{"type":"image_generation","model":"gpt-image-2"}]}`)}, core.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex})
	var bootstrap *core.ImageBootstrapError
	if !errors.As(err, &bootstrap) || !bootstrap.RetryOtherAuth() || !bootstrap.SkipAuthResult() {
		t.Fatalf("error contract lost: %T %v", err, err)
	}
}

type bootstrapFailoverTestExecutor struct{ *ChatGPTWebExecutor }

func (e *bootstrapFailoverTestExecutor) Execute(ctx context.Context, auth *authcore.Auth, _ core.Request, _ core.Options) (core.Response, error) {
	client, credential, err := e.newRuntimeClient(auth)
	if err != nil {
		return core.Response{}, err
	}
	defer client.CloseIdleConnections()
	_, body, err := e.fetchChatGPTWebImageBootstrap(ctx, client, credential, e.runtimeBaseURL, map[string]string{"X-Test-Credential": auth.ID})
	return core.Response{Payload: body}, err
}

func TestImageBootstrapCredentialFailover(t *testing.T) {
	for _, limit := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("credential limit %d", limit), func(t *testing.T) {
			var first, second atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("X-Test-Credential") == "bootstrap-a" {
					first.Add(1)
					<-r.Context().Done()
					return
				}
				second.Add(1)
				_, _ = io.WriteString(w, `{"ok":true}`)
			}))
			defer server.Close()
			manager := authcore.NewManager(nil, &authcore.FillFirstSelector{}, nil)
			if limit == 3 {
				manager.SetConfig(&config.Config{NonRetryableErrors: []config.NonRetryableErrorRule{{StatusCode: 504}}})
			}
			manager.SetRetryConfig(0, 0, limit)
			e := &bootstrapFailoverTestExecutor{NewChatGPTWebExecutor(nil, manager)}
			e.runtimeBaseURL = server.URL
			defer e.Close()
			manager.RegisterExecutor(e)
			for _, id := range []string{"bootstrap-a", "bootstrap-b"} {
				auth := chatGPTWebRuntimeAuth()
				auth.ID = id
				auth.Metadata["email"] = id + "@example.com"
				registry.GetGlobalRegistry().RegisterClient(id, "chatgpt-web", []*registry.ModelInfo{{ID: "gpt-image-2"}})
				t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
				if _, err := manager.Register(authcore.WithSkipPersist(t.Context()), auth); err != nil {
					t.Fatal(err)
				}
				manager.RefreshSchedulerEntry(id)
			}
			ctx := core.WithImageBootstrapPolicy(t.Context(), core.ImageBootstrapPolicy{Timeout: 150 * time.Millisecond, Retries: 1})
			response, err := manager.Execute(ctx, []string{"chatgpt-web"}, core.Request{Model: "gpt-image-2", Payload: []byte(`{"model":"gpt-image-2","input":"draw","tools":[{"type":"image_generation"}]}`)}, core.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex})
			if first.Load() != 2 {
				t.Fatalf("first credential attempts=%d; err=%v", first.Load(), err)
			}
			if limit == 2 {
				if err != nil || string(response.Payload) != `{"ok":true}` || second.Load() != 1 {
					t.Fatalf("failover response=%s second=%d err=%v", response.Payload, second.Load(), err)
				}
			} else {
				var timeout *core.ImageBootstrapError
				if !errors.As(err, &timeout) || second.Load() != 0 {
					t.Fatalf("credential limit bypassed: second=%d err=%v", second.Load(), err)
				}
			}
			for _, auth := range manager.List() {
				if auth.Unavailable || auth.Disabled || !auth.NextRetryAfter.IsZero() {
					t.Fatalf("credential cooled: %+v", auth)
				}
			}
		})
	}
}
