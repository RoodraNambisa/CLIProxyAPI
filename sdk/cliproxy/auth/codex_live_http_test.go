package auth

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type liveHTTPExecutor struct {
	authFallbackExecutor
	prepare func(*http.Request, *Auth) error
	http    func(context.Context, *Auth, *http.Request) (*http.Response, error)
}

func (e *liveHTTPExecutor) PrepareRequest(req *http.Request, a *Auth) error {
	if e.prepare != nil {
		return e.prepare(req, a)
	}
	return nil
}

func (e *liveHTTPExecutor) HttpRequest(ctx context.Context, a *Auth, req *http.Request) (*http.Response, error) {
	return e.http(ctx, a, req)
}

type liveHTTPBody struct {
	io.Reader
	closed atomic.Bool
}

func (b *liveHTTPBody) Close() error { b.closed.Store(true); return nil }

func newLiveHTTPLease(t *testing.T, e *liveHTTPExecutor) (*Manager, *CodexLiveLease) {
	t.Helper()
	m, _, _ := newCodexLiveLeaseFixture(t)
	e.id = "codex"
	m.RegisterExecutor(e)
	lease, errAcquire := m.AcquireCodexLive(t.Context(), "live-model", core.Options{})
	if errAcquire != nil {
		t.Fatal(errAcquire)
	}
	t.Cleanup(lease.Close)
	return m, lease
}

func TestCodexLiveHTTPPreparationUsesSelectedExecutorAndRestoresContext(t *testing.T) {
	m, lease := newLiveHTTPLease(t, &liveHTTPExecutor{prepare: func(req *http.Request, a *Auth) error {
		req.Header.Set("X-Fixture-Executor", "original")
		if a.Metadata["access_token"] != "fixture" {
			t.Fatal("credential snapshot changed")
		}
		return nil
	}})
	m.RegisterExecutor(&liveHTTPExecutor{authFallbackExecutor: authFallbackExecutor{id: "codex"}, prepare: func(*http.Request, *Auth) error {
		t.Fatal("used replacement executor")
		return nil
	}})
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.invalid/realtime", nil)
	originalCtx := req.Context()
	if errPrepare := lease.PrepareHttpRequest(req); errPrepare != nil {
		t.Fatal(errPrepare)
	}
	if req.Header.Get("X-Fixture-Executor") != "original" || req.Context() != originalCtx {
		t.Fatal("preparation lost headers or replaced the caller context")
	}
	if lease.slot.Committed() {
		t.Fatal("header preparation consumed capacity")
	}
	lease.Close()
	if !errors.Is(lease.PrepareHttpRequest(req), context.Canceled) {
		t.Fatal("closed lease prepared credentials")
	}
}

func TestCodexLiveHTTPBodyOwnsRequestContextUntilEOFOrClose(t *testing.T) {
	for _, mode := range []string{"eof", "close", "error"} {
		t.Run(mode, func(t *testing.T) {
			body := &liveHTTPBody{Reader: strings.NewReader("fixture")}
			upstreamError := errors.New("fixture upstream failure")
			var requestCtx context.Context
			_, lease := newLiveHTTPLease(t, &liveHTTPExecutor{http: func(ctx context.Context, _ *Auth, req *http.Request) (*http.Response, error) {
				requestCtx = ctx
				req.Header.Set("X-Fixture-Upstream", "only-copy")
				resp := &http.Response{StatusCode: http.StatusAccepted, Body: body}
				if mode == "error" {
					return resp, upstreamError
				}
				return resp, nil
			}})
			req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, "https://example.invalid/calls", nil)
			resp, errRequest := lease.HttpRequest(req)
			if mode == "error" {
				if !errors.Is(errRequest, upstreamError) || !body.closed.Load() || requestCtx.Err() == nil {
					t.Fatal("failed response lost its error or leaked the body/context")
				}
				return
			}
			if errRequest != nil || requestCtx.Err() != nil || req.Header.Get("X-Fixture-Upstream") != "" {
				t.Fatal("response context ended early or caller headers changed")
			}
			if mode == "eof" {
				if _, errRead := io.ReadAll(resp.Body); errRead != nil {
					t.Fatal(errRead)
				}
				if requestCtx.Err() == nil {
					t.Fatal("EOF retained the per-request cancellation callback")
				}
			}
			if errClose := resp.Body.Close(); errClose != nil {
				t.Fatal(errClose)
			}
			if !body.closed.Load() || requestCtx.Err() == nil || lease.Context().Err() != nil {
				t.Fatal("response close leaked resources or ended the whole session")
			}
		})
	}
}

func TestCodexLiveHTTPHonorsBothCancellationSources(t *testing.T) {
	for _, mode := range []string{"request", "lease"} {
		t.Run(mode, func(t *testing.T) {
			entered := make(chan struct{})
			_, lease := newLiveHTTPLease(t, &liveHTTPExecutor{http: func(ctx context.Context, _ *Auth, _ *http.Request) (*http.Response, error) {
				close(entered)
				<-ctx.Done()
				return nil, context.Cause(ctx)
			}})
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://example.invalid/calls", nil)
			completed := make(chan error, 1)
			go func() { _, errRequest := lease.HttpRequest(req); completed <- errRequest }()
			<-entered
			cause := errors.New("fixture request cancelled")
			if mode == "request" {
				cancel(cause)
			} else {
				lease.Close()
				cause = context.Canceled
			}
			select {
			case errRequest := <-completed:
				if !errors.Is(errRequest, cause) {
					t.Fatal("cancellation cause was not preserved")
				}
			case <-time.After(time.Second):
				t.Fatal("HTTP request ignored cancellation")
			}
		})
	}
}

func TestCodexLiveHTTPDoesNotPrepareDuringCloseCancellationWindow(t *testing.T) {
	var calls atomic.Int32
	_, lease := newLiveHTTPLease(t, &liveHTTPExecutor{prepare: func(*http.Request, *Auth) error {
		calls.Add(1)
		return nil
	}})
	entered, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	cancel := lease.cancel
	lease.cancel = func() { close(entered); <-resume; cancel() }
	go func() { lease.Close(); close(done) }()
	<-entered
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.invalid/realtime", nil)
	errPrepare := lease.PrepareHttpRequest(req)
	close(resume)
	<-done
	if !errors.Is(errPrepare, context.Canceled) || calls.Load() != 0 {
		t.Fatal("closed lease invoked credential preparation before cancellation propagated")
	}
}
