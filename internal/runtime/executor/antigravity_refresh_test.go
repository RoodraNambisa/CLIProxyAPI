package executor

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type antigravityRefreshRoundTripper struct {
	accessToken  string
	refreshToken string
	started      chan struct{}
	release      <-chan struct{}
	canceled     chan struct{}
	startOnce    sync.Once
	cancelOnce   sync.Once
	calls        atomic.Int32
}

func (r *antigravityRefreshRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	r.calls.Add(1)
	r.startOnce.Do(func() { close(r.started) })
	select {
	case <-r.release:
	case <-req.Context().Done():
		if r.canceled != nil {
			r.cancelOnce.Do(func() { close(r.canceled) })
		}
		return nil, req.Context().Err()
	}
	body := `{"access_token":"` + r.accessToken + `","token_type":"Bearer","expires_in":3600`
	if r.refreshToken != "" {
		body += `,"refresh_token":"` + r.refreshToken + `"`
	}
	body += `}`
	return &http.Response{
		StatusCode:    http.StatusOK,
		Header:        make(http.Header),
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       req,
	}, nil
}

func useAntigravityRefreshTestTransport(t *testing.T, targetHost string) {
	t.Helper()
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, targetHost)
		},
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // test server only
		ForceAttemptHTTP2: false,
	}
	antigravityTransport = transport
	antigravityTransportOnce = sync.Once{}
	antigravityTransportOnce.Do(func() {})
	t.Cleanup(func() {
		antigravityTransport = nil
		antigravityTransportOnce = sync.Once{}
	})
}

func TestAntigravityRefreshDeduplicatesConcurrentRefresh(t *testing.T) {
	var tokenCalls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/token" {
			t.Errorf("unexpected request path: %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		tokenCalls.Add(1)
		once.Do(func() { close(started) })
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"access_token":"new-access","refresh_token":"new-refresh","token_type":"Bearer","expires_in":3600}`)
	}))
	defer server.Close()

	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse test server URL: %v", err)
	}
	useAntigravityRefreshTestTransport(t, serverURL.Host)

	executor := &AntigravityExecutor{}
	newAuth := func(id string) *cliproxyauth.Auth {
		return &cliproxyauth.Auth{
			ID:       id,
			Provider: "antigravity",
			Metadata: map[string]any{"refresh_token": "shared-refresh-token", "project_id": "project-" + id},
		}
	}
	results := make(chan *cliproxyauth.Auth, 2)
	errs := make(chan error, 2)
	runRefresh := func(auth *cliproxyauth.Auth) {
		updated, errRefresh := executor.Refresh(context.Background(), auth)
		results <- updated
		errs <- errRefresh
	}

	go runRefresh(newAuth("shared"))
	<-started
	go runRefresh(newAuth("shared"))
	time.Sleep(20 * time.Millisecond)
	if got := tokenCalls.Load(); got != 1 {
		t.Fatalf("token calls before release = %d, want 1", got)
	}
	close(release)

	for range 2 {
		if errRefresh := <-errs; errRefresh != nil {
			t.Fatalf("refresh error = %v", errRefresh)
		}
		updated := <-results
		if got := metaStringValue(updated.Metadata, "access_token"); got != "new-access" {
			t.Fatalf("access_token = %q", got)
		}
		if got := metaStringValue(updated.Metadata, "refresh_token"); got != "new-refresh" {
			t.Fatalf("refresh_token = %q", got)
		}
	}
	if got := tokenCalls.Load(); got != 1 {
		t.Fatalf("token calls = %d, want 1", got)
	}
}

func TestAntigravityRefreshCallerCancellationDoesNotWaitForSharedRequest(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	requestCanceled := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	roundTripper := &antigravityRefreshRoundTripper{
		accessToken: "new-access",
		started:     started,
		release:     release,
		canceled:    requestCanceled,
	}
	executor := &AntigravityExecutor{}

	ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", http.RoundTripper(roundTripper))
	ctx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		_, errRefresh := executor.Refresh(ctx, &cliproxyauth.Auth{
			ID:       "cancel-refresh",
			Provider: "antigravity",
			Metadata: map[string]any{"refresh_token": "cancel-refresh-token", "project_id": "project"},
		})
		errCh <- errRefresh
	}()
	<-started
	cancel()
	select {
	case errRefresh := <-errCh:
		if !errors.Is(errRefresh, context.Canceled) {
			t.Fatalf("Refresh() error = %v, want context.Canceled", errRefresh)
		}
	case <-time.After(time.Second):
		t.Fatal("Refresh() did not return after caller cancellation")
	}
	select {
	case <-requestCanceled:
	case <-time.After(time.Second):
		t.Fatal("last waiter cancellation did not cancel the upstream refresh")
	}
	releaseOnce.Do(func() { close(release) })
	updated, errRefresh := executor.Refresh(
		context.WithValue(t.Context(), "cliproxy.roundtripper", http.RoundTripper(roundTripper)),
		&cliproxyauth.Auth{
			ID:       "cancel-refresh",
			Provider: "antigravity",
			Metadata: map[string]any{"refresh_token": "cancel-refresh-token", "project_id": "project"},
		},
	)
	if errRefresh != nil {
		t.Fatalf("Refresh() after canceled flight error = %v", errRefresh)
	}
	if got := metaStringValue(updated.Metadata, "access_token"); got != "new-access" {
		t.Fatalf("access_token after canceled flight = %q, want new-access", got)
	}
	if got := roundTripper.calls.Load(); got != 2 {
		t.Fatalf("transport calls = %d, want a fresh second call", got)
	}
}

func TestAntigravityRefreshPreCanceledContextDoesNotStartRequest(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	close(release)
	roundTripper := &antigravityRefreshRoundTripper{
		accessToken: "unused-access",
		started:     started,
		release:     release,
	}
	ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", http.RoundTripper(roundTripper))
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	_, errRefresh := (&AntigravityExecutor{}).Refresh(ctx, &cliproxyauth.Auth{
		ID:       "pre-canceled-refresh",
		Provider: "antigravity",
		Metadata: map[string]any{"refresh_token": "pre-canceled-token", "project_id": "project"},
	})
	if !errors.Is(errRefresh, context.Canceled) {
		t.Fatalf("Refresh() error = %v, want context.Canceled", errRefresh)
	}
	if got := roundTripper.calls.Load(); got != 0 {
		t.Fatalf("transport calls = %d, want 0", got)
	}
	select {
	case <-started:
		t.Fatal("pre-canceled refresh started an upstream request")
	default:
	}
}

func TestAntigravityRefreshExecutorCloseCancelsSharedRequest(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	requestCanceled := make(chan struct{})
	roundTripper := &antigravityRefreshRoundTripper{
		accessToken: "unused-access",
		started:     started,
		release:     release,
		canceled:    requestCanceled,
	}
	executor := NewAntigravityExecutor(nil)
	errCh := make(chan error, 1)
	go func() {
		ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", http.RoundTripper(roundTripper))
		_, errRefresh := executor.Refresh(ctx, &cliproxyauth.Auth{
			ID:       "close-refresh",
			Provider: "antigravity",
			Metadata: map[string]any{"refresh_token": "close-refresh-token", "project_id": "project"},
		})
		errCh <- errRefresh
	}()
	<-started
	if errClose := executor.Close(); errClose != nil {
		t.Fatalf("Close() error = %v", errClose)
	}
	select {
	case errRefresh := <-errCh:
		if !errors.Is(errRefresh, context.Canceled) {
			t.Fatalf("Refresh() error = %v, want context.Canceled", errRefresh)
		}
	case <-time.After(time.Second):
		t.Fatal("Refresh() did not return after executor close")
	}
	select {
	case <-requestCanceled:
	case <-time.After(time.Second):
		t.Fatal("executor close did not cancel the upstream refresh")
	}
}

func TestAntigravityRefreshKeepsSharedRequestForRemainingWaiter(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	requestCanceled := make(chan struct{})
	var releaseOnce sync.Once
	releaseRequest := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseRequest)
	roundTripper := &antigravityRefreshRoundTripper{
		accessToken:  "new-access",
		refreshToken: "new-refresh",
		started:      started,
		release:      release,
		canceled:     requestCanceled,
	}

	executor := &AntigravityExecutor{}
	newAuth := func() *cliproxyauth.Auth {
		return &cliproxyauth.Auth{
			ID:       "shared-cancel-refresh",
			Provider: "antigravity",
			Metadata: map[string]any{"refresh_token": "shared-cancel-token", "project_id": "project"},
		}
	}
	baseCtx := context.WithValue(t.Context(), "cliproxy.roundtripper", http.RoundTripper(roundTripper))
	firstCtx, cancelFirst := context.WithCancel(baseCtx)
	firstErr := make(chan error, 1)
	secondResult := make(chan *cliproxyauth.Auth, 1)
	secondErr := make(chan error, 1)
	go func() {
		_, errRefresh := executor.Refresh(firstCtx, newAuth())
		firstErr <- errRefresh
	}()
	<-started
	go func() {
		updated, errRefresh := executor.Refresh(baseCtx, newAuth())
		secondResult <- updated
		secondErr <- errRefresh
	}()

	deadline := time.Now().Add(time.Second)
	for {
		executor.tokenRefreshMu.Lock()
		waiters := 0
		for _, call := range executor.tokenRefreshes {
			waiters += call.waiters
		}
		executor.tokenRefreshMu.Unlock()
		if waiters == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second caller did not join the shared refresh")
		}
		time.Sleep(time.Millisecond)
	}

	cancelFirst()
	if errRefresh := <-firstErr; !errors.Is(errRefresh, context.Canceled) {
		t.Fatalf("first Refresh() error = %v, want context.Canceled", errRefresh)
	}
	select {
	case <-requestCanceled:
		t.Fatal("canceling one waiter canceled the shared upstream refresh")
	case <-time.After(25 * time.Millisecond):
	}

	releaseRequest()
	if errRefresh := <-secondErr; errRefresh != nil {
		t.Fatalf("second Refresh() error = %v", errRefresh)
	}
	updated := <-secondResult
	if got := metaStringValue(updated.Metadata, "refresh_token"); got != "new-refresh" {
		t.Fatalf("second refresh_token = %q, want new-refresh", got)
	}
	if got := roundTripper.calls.Load(); got != 1 {
		t.Fatalf("token calls = %d, want 1", got)
	}
}

func TestAntigravityRefreshDoesNotCoalesceDifferentTransports(t *testing.T) {
	release := make(chan struct{})
	firstTransport := &antigravityRefreshRoundTripper{
		accessToken: "first-access",
		started:     make(chan struct{}),
		release:     release,
	}
	secondTransport := &antigravityRefreshRoundTripper{
		accessToken: "second-access",
		started:     make(chan struct{}),
		release:     release,
	}
	executor := &AntigravityExecutor{}
	newAuth := func() *cliproxyauth.Auth {
		return &cliproxyauth.Auth{
			ID:       "transport-isolation",
			Provider: "antigravity",
			Metadata: map[string]any{"refresh_token": "transport-refresh-token", "project_id": "project"},
		}
	}
	type result struct {
		auth *cliproxyauth.Auth
		err  error
	}
	results := make(chan result, 2)
	runRefresh := func(roundTripper http.RoundTripper) {
		ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", roundTripper)
		updated, errRefresh := executor.Refresh(ctx, newAuth())
		results <- result{auth: updated, err: errRefresh}
	}
	go runRefresh(firstTransport)
	go runRefresh(secondTransport)
	select {
	case <-firstTransport.started:
	case <-time.After(time.Second):
		t.Fatal("first transport was not used")
	}
	select {
	case <-secondTransport.started:
	case <-time.After(time.Second):
		t.Fatal("second transport was incorrectly coalesced")
	}
	close(release)

	seen := map[string]bool{}
	for range 2 {
		refreshResult := <-results
		if refreshResult.err != nil {
			t.Fatalf("Refresh() error = %v", refreshResult.err)
		}
		seen[metaStringValue(refreshResult.auth.Metadata, "access_token")] = true
	}
	if !seen["first-access"] || !seen["second-access"] {
		t.Fatalf("access tokens = %#v, want both transport-specific responses", seen)
	}
	if got := firstTransport.calls.Load(); got != 1 {
		t.Fatalf("first transport calls = %d, want 1", got)
	}
	if got := secondTransport.calls.Load(); got != 1 {
		t.Fatalf("second transport calls = %d, want 1", got)
	}
}

func TestAntigravityRefreshPreservesRetryAfterHeader(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"error":"temporarily unavailable"}`)
	}))
	defer server.Close()
	serverURL, errURL := url.Parse(server.URL)
	if errURL != nil {
		t.Fatalf("parse test server URL: %v", errURL)
	}
	useAntigravityRefreshTestTransport(t, serverURL.Host)

	_, errRefresh := (&AntigravityExecutor{}).Refresh(t.Context(), &cliproxyauth.Auth{
		ID:       "retry-after-refresh",
		Provider: "antigravity",
		Metadata: map[string]any{"refresh_token": "retry-after-refresh-token", "project_id": "project"},
	})
	var retryErr interface{ RetryAfter() *time.Duration }
	if !errors.As(errRefresh, &retryErr) || retryErr.RetryAfter() == nil || *retryErr.RetryAfter() != 7*time.Second {
		t.Fatalf("Refresh() error = %#v, want Retry-After 7s", errRefresh)
	}
}
