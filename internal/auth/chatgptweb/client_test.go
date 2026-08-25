package chatgptweb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestAcquisitionClientClosesActiveConnections(t *testing.T) {
	started := make(chan struct{}, 1)
	canceled := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		started <- struct{}{}
		<-request.Context().Done()
		canceled <- struct{}{}
	}))
	defer server.Close()

	client, errClient := NewAcquisitionClient(DefaultPersona(), "", nil, time.Second)
	if errClient != nil {
		t.Fatalf("NewAcquisitionClient() error = %v", errClient)
	}
	done := make(chan error, 1)
	go func() {
		_, _, errRequest := client.DoFollow(context.Background(), http.MethodGet, server.URL, nil, nil)
		done <- errRequest
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("acquisition request did not start")
	}
	client.acquisitionTracker.mu.Lock()
	activeConnections := len(client.acquisitionTracker.connections)
	client.acquisitionTracker.mu.Unlock()
	if activeConnections != 1 {
		t.Fatalf("tracked acquisition connections = %d, want 1", activeConnections)
	}
	client.CloseActiveAcquisitionConnections()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("active acquisition connection remained open")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("acquisition request did not return after closing its connection")
	}
}

func TestPollTransportSharesPersonaAndCookiesAndCancelsResponseBody(t *testing.T) {
	type observedRequest struct {
		path      string
		userAgent string
		cookie    string
	}
	observed := make(chan observedRequest, 2)
	pollCanceled := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		observed <- observedRequest{
			path:      request.URL.Path,
			userAgent: request.Header.Get("User-Agent"),
			cookie:    request.Header.Get("Cookie"),
		}
		if request.URL.Path != "/poll" {
			response.WriteHeader(http.StatusNoContent)
			return
		}
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusOK)
		if flusher, ok := response.(http.Flusher); ok {
			flusher.Flush()
		}
		<-request.Context().Done()
		pollCanceled <- struct{}{}
	}))
	defer server.Close()

	client, errClient := NewClient(DefaultPersona(), "", nil)
	if errClient != nil {
		t.Fatalf("NewClient() error = %v", errClient)
	}
	defer client.CloseIdleConnections()
	if errCookie := client.SetCookie(server.URL, "poll-test", "shared"); errCookie != nil {
		t.Fatalf("SetCookie() error = %v", errCookie)
	}

	pollCtx, cancelPoll := context.WithCancel(t.Context())
	response, errPoll := client.DoPollNoRedirectStream(pollCtx, http.MethodGet, server.URL+"/poll", nil, nil)
	if errPoll != nil {
		t.Fatalf("DoPollNoRedirectStream() error = %v", errPoll)
	}
	if client.pollTransport == nil || client.pollTransport.tracker == nil {
		t.Fatal("poll transport was not initialized with a connection tracker")
	}
	cancelPoll()
	select {
	case <-pollCanceled:
	case <-time.After(time.Second):
		t.Fatal("poll response body was not closed after cancellation")
	}
	if errClose := response.Body.Close(); errClose != nil {
		t.Fatalf("close poll response body: %v", errClose)
	}

	mainResponse, _, errMain := client.DoNoRedirect(t.Context(), http.MethodGet, server.URL+"/main", nil, nil)
	if errMain != nil {
		t.Fatalf("main transport request after poll cancellation: %v", errMain)
	}
	if mainResponse.StatusCode != http.StatusNoContent {
		t.Fatalf("main transport status = %d, want 204", mainResponse.StatusCode)
	}

	seen := map[string]observedRequest{}
	for range 2 {
		request := <-observed
		seen[request.path] = request
	}
	if seen["/poll"].userAgent == "" || seen["/poll"].userAgent != seen["/main"].userAgent {
		t.Fatalf("persona user agents differ: poll=%q main=%q", seen["/poll"].userAgent, seen["/main"].userAgent)
	}
	for _, path := range []string{"/poll", "/main"} {
		if !strings.Contains(seen[path].cookie, "poll-test=shared") {
			t.Fatalf("%s cookie = %q, want shared jar cookie", path, seen[path].cookie)
		}
	}
}

func TestPollTransportRetirementDoesNotCloseMainTransport(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client, errClient := NewClient(DefaultPersona(), "", nil)
	if errClient != nil {
		t.Fatalf("NewClient() error = %v", errClient)
	}
	defer client.CloseIdleConnections()
	client.pollCancelGrace = time.Millisecond
	poll, errPoll := client.currentPollTransport()
	if errPoll != nil {
		t.Fatalf("currentPollTransport() error = %v", errPoll)
	}
	control := &pollRequestControl{
		client:     client,
		generation: poll.generation,
		grace:      client.pollCancelGrace,
		done:       make(chan struct{}),
	}
	control.cancel()
	client.pollMu.Lock()
	retired := client.pollTransport == nil
	client.pollMu.Unlock()
	if !retired {
		t.Fatal("stuck poll transport was not retired")
	}
	replacement, errReplacement := client.currentPollTransport()
	if errReplacement != nil {
		t.Fatalf("recreate poll transport: %v", errReplacement)
	}
	if replacement.generation <= poll.generation {
		t.Fatalf("replacement generation = %d, want greater than %d", replacement.generation, poll.generation)
	}
	response, _, errMain := client.DoNoRedirect(t.Context(), http.MethodGet, server.URL, nil, nil)
	if errMain != nil {
		t.Fatalf("main transport request after poll retirement: %v", errMain)
	}
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("main transport status = %d, want 204", response.StatusCode)
	}
}

func TestAcquisitionClientAcceptsConfiguredProxyDialer(t *testing.T) {
	for _, proxyURL := range []string{
		"http://user:pass@127.0.0.1:18080",
		"socks5h://user:pass@127.0.0.1:1080",
	} {
		t.Run(proxyURL, func(t *testing.T) {
			client, errClient := NewAcquisitionClient(DefaultPersona(), proxyURL, nil, time.Second)
			if errClient != nil {
				t.Fatalf("NewAcquisitionClient() error = %v", errClient)
			}
			client.CloseIdleConnections()
		})
	}
}

func TestClientBeforeRequestHookRunsImmediatelyBeforeFirstDo(t *testing.T) {
	var hookCalls atomic.Int32
	var handlerCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		handlerCalls.Add(1)
		if got := hookCalls.Load(); got != 1 {
			t.Errorf("before-request hook calls at handler = %d, want 1", got)
		}
		response.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, errClient := NewClient(DefaultPersona(), "", nil)
	if errClient != nil {
		t.Fatalf("NewClient() error = %v", errClient)
	}
	defer client.CloseIdleConnections()
	client.SetBeforeRequestHook(func() { hookCalls.Add(1) })

	for attempt := 0; attempt < 2; attempt++ {
		response, _, errRequest := client.DoNoRedirect(t.Context(), http.MethodGet, server.URL, nil, nil)
		if errRequest != nil {
			t.Fatalf("DoNoRedirect() attempt %d error = %v", attempt+1, errRequest)
		}
		if response.StatusCode != http.StatusNoContent {
			t.Fatalf("DoNoRedirect() attempt %d status = %d, want 204", attempt+1, response.StatusCode)
		}
	}
	if got := hookCalls.Load(); got != 1 {
		t.Fatalf("before-request hook calls = %d, want 1", got)
	}
	if got := handlerCalls.Load(); got != 2 {
		t.Fatalf("handler calls = %d, want 2", got)
	}
}

func TestClientBeforeRequestHookDoesNotRunWhenRequestConstructionFails(t *testing.T) {
	client, errClient := NewClient(DefaultPersona(), "", nil)
	if errClient != nil {
		t.Fatalf("NewClient() error = %v", errClient)
	}
	defer client.CloseIdleConnections()
	var hookCalls atomic.Int32
	client.SetBeforeRequestHook(func() { hookCalls.Add(1) })

	if _, errRequest := client.DoNoRedirectStream(t.Context(), http.MethodGet, "://invalid", nil, nil); errRequest == nil {
		t.Fatal("DoNoRedirectStream() error = nil, want request construction failure")
	}
	if got := hookCalls.Load(); got != 0 {
		t.Fatalf("before-request hook calls = %d, want 0", got)
	}
}

func TestClientBeforeRequestHookDoesNotRunForPreCanceledContext(t *testing.T) {
	var hookCalls atomic.Int32
	var handlerCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		handlerCalls.Add(1)
		response.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, errClient := NewClient(DefaultPersona(), "", nil)
	if errClient != nil {
		t.Fatalf("NewClient() error = %v", errClient)
	}
	defer client.CloseIdleConnections()
	client.SetBeforeRequestHook(func() { hookCalls.Add(1) })

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, errRequest := client.DoNoRedirectStream(ctx, http.MethodGet, server.URL, nil, nil); !errors.Is(errRequest, context.Canceled) {
		t.Fatalf("DoNoRedirectStream() error = %v, want context.Canceled", errRequest)
	}
	if got := hookCalls.Load(); got != 0 {
		t.Fatalf("before-request hook calls = %d, want 0", got)
	}
	if got := handlerCalls.Load(); got != 0 {
		t.Fatalf("handler calls = %d, want 0", got)
	}
}

func TestClientBeforeRequestHookRemainsCommittedAfterUpstreamFailure(t *testing.T) {
	for _, statusCode := range []int{http.StatusBadRequest, http.StatusInternalServerError} {
		t.Run(http.StatusText(statusCode), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				response.WriteHeader(statusCode)
			}))
			defer server.Close()
			client, errClient := NewClient(DefaultPersona(), "", nil)
			if errClient != nil {
				t.Fatalf("NewClient() error = %v", errClient)
			}
			defer client.CloseIdleConnections()
			var hookCalls atomic.Int32
			client.SetBeforeRequestHook(func() { hookCalls.Add(1) })

			response, _, errRequest := client.DoNoRedirect(t.Context(), http.MethodGet, server.URL, nil, nil)
			if errRequest != nil {
				t.Fatalf("DoNoRedirect() error = %v", errRequest)
			}
			if response.StatusCode != statusCode {
				t.Fatalf("DoNoRedirect() status = %d, want %d", response.StatusCode, statusCode)
			}
			if got := hookCalls.Load(); got != 1 {
				t.Fatalf("before-request hook calls = %d, want 1", got)
			}
		})
	}
}

const (
	testReservationPending uint32 = iota
	testReservationCommitted
	testReservationReleased
)

type clientTestReservation struct {
	state atomic.Uint32
}

func (reservation *clientTestReservation) commit() bool {
	return reservation != nil && reservation.state.CompareAndSwap(testReservationPending, testReservationCommitted)
}

func (reservation *clientTestReservation) release() bool {
	return reservation != nil && reservation.state.CompareAndSwap(testReservationPending, testReservationReleased)
}

func TestClientBeforeRequestHookSettlesReservationAtDoBoundary(t *testing.T) {
	t.Run("request construction failure releases", func(t *testing.T) {
		client, errClient := NewClient(DefaultPersona(), "", nil)
		if errClient != nil {
			t.Fatalf("NewClient() error = %v", errClient)
		}
		defer client.CloseIdleConnections()
		reservation := &clientTestReservation{}
		client.SetBeforeRequestHook(func() { reservation.commit() })

		if _, errRequest := client.DoNoRedirectStream(t.Context(), http.MethodGet, "://invalid", nil, nil); errRequest == nil {
			t.Fatal("DoNoRedirectStream() error = nil, want request construction failure")
		}
		if !reservation.release() {
			t.Fatalf("release() = false, state = %d", reservation.state.Load())
		}
		if got := reservation.state.Load(); got != testReservationReleased {
			t.Fatalf("reservation state = %d, want released", got)
		}
	})

	for _, testCase := range []struct {
		name       string
		statusCode int
		closeFirst bool
	}{
		{name: "HTTP 400", statusCode: http.StatusBadRequest},
		{name: "HTTP 500", statusCode: http.StatusInternalServerError},
		{name: "transport error", closeFirst: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			reservation := &clientTestReservation{}
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				if got := reservation.state.Load(); got != testReservationCommitted {
					t.Errorf("reservation state at handler = %d, want committed", got)
				}
				response.WriteHeader(testCase.statusCode)
			}))
			serverURL := server.URL
			if testCase.closeFirst {
				server.Close()
			} else {
				defer server.Close()
			}

			client, errClient := NewClient(DefaultPersona(), "", nil)
			if errClient != nil {
				t.Fatalf("NewClient() error = %v", errClient)
			}
			defer client.CloseIdleConnections()
			client.SetBeforeRequestHook(func() { reservation.commit() })

			response, _, errRequest := client.DoNoRedirect(t.Context(), http.MethodGet, serverURL, nil, nil)
			if testCase.closeFirst {
				if errRequest == nil {
					t.Fatal("DoNoRedirect() error = nil, want transport error")
				}
			} else {
				if errRequest != nil {
					t.Fatalf("DoNoRedirect() error = %v", errRequest)
				}
				if response.StatusCode != testCase.statusCode {
					t.Fatalf("DoNoRedirect() status = %d, want %d", response.StatusCode, testCase.statusCode)
				}
			}
			if reservation.release() {
				t.Fatal("release() = true after upstream Do")
			}
			if got := reservation.state.Load(); got != testReservationCommitted {
				t.Fatalf("reservation state = %d, want committed", got)
			}
		})
	}
}

func TestCookieRoundTripAcrossClients(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/set":
			http.SetCookie(response, &http.Cookie{Name: "session", Value: "cookie-value", Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode})
			response.WriteHeader(http.StatusNoContent)
		case "/echo":
			cookie, err := request.Cookie("session")
			if err != nil {
				http.Error(response, err.Error(), http.StatusUnauthorized)
				return
			}
			_, _ = fmt.Fprint(response, cookie.Value)
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	first, err := NewClient(DefaultPersona(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer first.CloseIdleConnections()
	if response, _, err := first.DoFollow(context.Background(), http.MethodGet, server.URL+"/set", nil, nil); err != nil {
		t.Fatal(err)
	} else if response.StatusCode != http.StatusNoContent {
		t.Fatalf("set cookie status = %d", response.StatusCode)
	}
	exported := first.ExportCookies()
	if len(exported) != 1 || exported[0].Name != "session" || !exported[0].HTTPOnly {
		t.Fatalf("ExportCookies() = %#v", exported)
	}

	second, err := NewClient(first.Persona(), "", exported)
	if err != nil {
		t.Fatal(err)
	}
	defer second.CloseIdleConnections()
	response, payload, err := second.DoNoRedirect(context.Background(), http.MethodGet, server.URL+"/echo", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || string(payload) != "cookie-value" {
		t.Fatalf("restored cookie response = %d %q", response.StatusCode, payload)
	}
}

func TestAccessTokenClientWithholdsAndRetainsSessionCookies(t *testing.T) {
	t.Parallel()
	var cookieHeader string
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/seed":
			http.SetCookie(response, &http.Cookie{Name: "next-auth.session-token", Value: "initial", Path: "/"})
			http.SetCookie(response, &http.Cookie{Name: "oai-sc", Value: "sentinel", Path: "/"})
			response.WriteHeader(http.StatusNoContent)
		case "/check":
			cookieHeader = request.Header.Get("Cookie")
			http.SetCookie(response, &http.Cookie{Name: "next-auth.session-token", Value: "rotated", Path: "/"})
			response.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	client, errClient := NewAccessTokenClient(DefaultPersona(), "", nil)
	if errClient != nil {
		t.Fatal(errClient)
	}
	defer client.CloseIdleConnections()
	for _, path := range []string{"/seed", "/check"} {
		response, _, errRequest := client.DoFollow(context.Background(), http.MethodGet, server.URL+path, nil, nil)
		if errRequest != nil {
			t.Fatal(errRequest)
		}
		if response.StatusCode != http.StatusNoContent {
			t.Fatalf("%s status = %d", path, response.StatusCode)
		}
	}
	if strings.Contains(strings.ToLower(cookieHeader), "session-token") {
		t.Fatalf("access-token request sent a session cookie: %q", cookieHeader)
	}
	if !strings.Contains(cookieHeader, "oai-sc=sentinel") {
		t.Fatalf("access-token request cookie = %q, want non-session cookie", cookieHeader)
	}
	foundRotatedSession := false
	for _, cookie := range client.ExportCookies() {
		if isSessionCookieName(cookie.Name) && cookie.Value == "rotated" {
			foundRotatedSession = true
		}
	}
	if !foundRotatedSession {
		t.Fatalf("server-issued session cookie was not retained: %#v", client.ExportCookies())
	}
}

func TestBrowserClientSendsSessionCookiesForAuthenticationFlows(t *testing.T) {
	t.Parallel()
	var cookieHeader string
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/seed":
			http.SetCookie(response, &http.Cookie{Name: "next-auth.session-token", Value: "persisted", Path: "/"})
			response.WriteHeader(http.StatusNoContent)
		case "/check":
			cookieHeader = request.Header.Get("Cookie")
			response.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()
	client, errClient := NewClient(DefaultPersona(), "", nil)
	if errClient != nil {
		t.Fatal(errClient)
	}
	defer client.CloseIdleConnections()

	for _, path := range []string{"/seed", "/check"} {
		response, _, errRequest := client.DoFollow(context.Background(), http.MethodGet, server.URL+path, nil, nil)
		if errRequest != nil {
			t.Fatal(errRequest)
		}
		if response.StatusCode != http.StatusNoContent {
			t.Fatalf("%s status = %d", path, response.StatusCode)
		}
	}
	if !strings.Contains(cookieHeader, "next-auth.session-token=persisted") {
		t.Fatalf("authentication request cookie = %q", cookieHeader)
	}
}

func TestDomainCookieRoundTrip(t *testing.T) {
	t.Parallel()
	first, err := NewClient(DefaultPersona(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer first.CloseIdleConnections()
	if err := first.SetCookie(AuthBaseURL, "oai-did", "device"); err != nil {
		t.Fatal(err)
	}
	exported := first.ExportCookies()
	second, err := NewClient(first.Persona(), "", exported)
	if err != nil {
		t.Fatal(err)
	}
	defer second.CloseIdleConnections()
	restored := second.ExportCookies()
	if len(restored) != 1 || restored[0].Name != "oai-did" || restored[0].Value != "device" || restored[0].Domain != "auth.openai.com" {
		t.Fatalf("restored domain cookies = %#v", restored)
	}
}

func TestDoJSONStreamDoesNotFollowRedirects(t *testing.T) {
	t.Parallel()
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		targetCalls.Add(1)
		_, _ = io.Copy(io.Discard, request.Body)
		response.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Location", target.URL+"/leak")
		response.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer source.Close()

	client, err := NewClient(DefaultPersona(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	response, err := client.DoJSONStream(context.Background(), http.MethodPost, source.URL+"/stream", map[string]string{
		"openai-sentinel-proof-token": "secret-proof",
	}, map[string]string{"prompt": "secret-prompt"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if errClose := response.Body.Close(); errClose != nil {
			t.Errorf("close response body: %v", errClose)
		}
	}()
	if response.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusTemporaryRedirect)
	}
	if calls := targetCalls.Load(); calls != 0 {
		t.Fatalf("redirect target calls = %d, want 0", calls)
	}
}

func TestSameChatGPTWebOriginNormalizesDefaultPorts(t *testing.T) {
	tests := []struct {
		name  string
		left  string
		right string
		want  bool
	}{
		{
			name:  "https default port",
			left:  "https://chatgpt.com/backend-api/accounts/check",
			right: "https://CHATGPT.com:443/backend-api/accounts/check",
			want:  true,
		},
		{
			name:  "http default port",
			left:  "http://chatgpt.com/start",
			right: "http://chatgpt.com:080/done",
			want:  true,
		},
		{
			name:  "non-default port",
			left:  "https://chatgpt.com/start",
			right: "https://chatgpt.com:8443/done",
			want:  false,
		},
		{
			name:  "different scheme",
			left:  "https://chatgpt.com/start",
			right: "http://chatgpt.com:443/done",
			want:  false,
		},
		{
			name:  "different host",
			left:  "https://chatgpt.com/start",
			right: "https://example.com:443/done",
			want:  false,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			left, errLeft := url.Parse(testCase.left)
			if errLeft != nil {
				t.Fatalf("parse left URL: %v", errLeft)
			}
			right, errRight := url.Parse(testCase.right)
			if errRight != nil {
				t.Fatalf("parse right URL: %v", errRight)
			}
			if got := sameChatGPTWebOrigin(left, right); got != testCase.want {
				t.Fatalf("sameChatGPTWebOrigin(%q, %q) = %v, want %v", testCase.left, testCase.right, got, testCase.want)
			}
		})
	}
}

func TestDoFollowStreamLeavesBodyForCaller(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(response, "streamed")
	}))
	defer server.Close()

	client, err := NewClient(DefaultPersona(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	response, err := client.DoFollowStream(context.Background(), http.MethodGet, server.URL, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if err = response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if string(payload) != "streamed" {
		t.Fatalf("stream payload = %q", payload)
	}
}

func TestDoSameOriginRedirectStreamFollowsOnlyExactOrigin(t *testing.T) {
	t.Run("same origin", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			switch request.URL.Path {
			case "/start":
				http.Redirect(response, request, "/done", http.StatusFound)
			case "/done":
				if got := request.Header.Get("Oai-Device-Id"); got != "device" {
					t.Fatalf("device header = %q", got)
				}
				response.WriteHeader(http.StatusNoContent)
			default:
				http.NotFound(response, request)
			}
		}))
		defer server.Close()
		client, err := NewClient(DefaultPersona(), "", nil)
		if err != nil {
			t.Fatal(err)
		}
		defer client.CloseIdleConnections()
		response, err := client.DoSameOriginRedirectStream(context.Background(), http.MethodGet, server.URL+"/start", map[string]string{
			"oai-device-id": "device",
		}, 5)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusNoContent {
			t.Fatalf("status = %d", response.StatusCode)
		}
	})

	t.Run("cross origin", func(t *testing.T) {
		var targetCalls atomic.Int32
		target := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			targetCalls.Add(1)
			if request.Header.Get("Oai-Device-Id") != "" || request.Header.Get("Oai-Session-Id") != "" {
				t.Errorf("cross-origin request leaked identity headers")
			}
			response.WriteHeader(http.StatusNoContent)
		}))
		defer target.Close()
		source := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			http.Redirect(response, request, target.URL+"/capture", http.StatusTemporaryRedirect)
		}))
		defer source.Close()
		client, err := NewClient(DefaultPersona(), "", nil)
		if err != nil {
			t.Fatal(err)
		}
		defer client.CloseIdleConnections()
		response, err := client.DoSameOriginRedirectStream(context.Background(), http.MethodGet, source.URL, map[string]string{
			"authorization":  "Bearer secret",
			"oai-device-id":  "device",
			"oai-session-id": "session",
		}, 5)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusTemporaryRedirect {
			t.Fatalf("status = %d", response.StatusCode)
		}
		if calls := targetCalls.Load(); calls != 0 {
			t.Fatalf("target calls = %d", calls)
		}
	})
}
