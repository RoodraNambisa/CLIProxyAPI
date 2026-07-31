package chatgptweb

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
)

func TestCloudflareChallengeDetectionDoesNotRejectNormalEdgeResponse(t *testing.T) {
	response := &fhttp.Response{
		StatusCode: http.StatusOK,
		Header: fhttp.Header{
			"CF-Ray": {"normal-ray"},
			"Server": {"cloudflare"},
		},
	}
	if isCloudflareChallenge(response, []byte(`{"ok":true}`)) {
		t.Fatal("normal 2xx response with Cloudflare edge headers was classified as a challenge")
	}
	response.StatusCode = http.StatusForbidden
	response.Header.Set("Content-Type", "application/json")
	if isCloudflareChallenge(response, []byte(`{"error":"invalid_password"}`)) {
		t.Fatal("application 403 behind Cloudflare was classified as an edge challenge")
	}
	response.StatusCode = http.StatusOK
	response.Header.Set("Content-Type", "text/html")
	if !isCloudflareChallenge(response, []byte(`<html><title>Just a moment...</title><script src="/cdn-cgi/challenge-platform/x"></script></html>`)) {
		t.Fatal("2xx Challenge HTML was not classified as a Cloudflare challenge")
	}
}

func TestCloudflareChallengeDetectionUsesMitigatedHeader(t *testing.T) {
	response := &fhttp.Response{
		StatusCode: http.StatusOK,
		Header:     fhttp.Header{},
	}
	response.Header.Set("Content-Type", "application/json")
	response.Header.Set("CF-Mitigated", "challenge")
	if !isCloudflareChallenge(response, []byte(`{"unexpected":"body"}`)) {
		t.Fatal("Cloudflare cf-mitigated challenge header was not classified as a challenge")
	}
	response.Header.Set("CF-Mitigated", "not-a-challenge")
	if isCloudflareChallenge(response, []byte(`{"ok":true}`)) {
		t.Fatal("unknown cf-mitigated value was classified as a challenge")
	}
}

func TestLoginClientRotatesProxyAndPreservesCookies(t *testing.T) {
	var (
		calls          atomic.Int32
		proxyAuth      []string
		secondCookie   string
		observationMux sync.Mutex
	)
	upstream := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		call := calls.Add(1)
		if call == 2 {
			observationMux.Lock()
			secondCookie = request.Header.Get("Cookie")
			observationMux.Unlock()
		}
		if call == 1 {
			response.Header().Set("CF-Ray", "challenge-ray")
			response.Header().Set("Server", "cloudflare")
			response.Header().Set("Content-Type", "text/html")
			response.Header().Set("Set-Cookie", "flow=keep; Path=/")
			response.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(response, `<html><title>Just a moment...</title></html>`)
			return
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(response, `{"ok":true}`)
	}))
	defer upstream.Close()
	proxy := newLoginConnectProxy(t, func(request *http.Request) {
		observationMux.Lock()
		proxyAuth = append(proxyAuth, request.Header.Get("Proxy-Authorization"))
		observationMux.Unlock()
	})

	template := strings.Replace(proxy.URL, "http://", "http://session-{1}:secret@", 1)
	selector, errSelector := newLoginProxySelector(LoginProxyConfig{
		Enabled:            true,
		URLTemplate:        template,
		PlaceholderCharset: "ab",
		RotateOnRetry:      true,
		RequestAttempts:    2,
		FlowAttempts:       1,
	}, bytes.NewReader([]byte{0, 1}))
	if errSelector != nil {
		t.Fatalf("newLoginProxySelector() error = %v", errSelector)
	}
	client, errClient := newLoginClient(DefaultPersona(), nil, selector, time.Second)
	if errClient != nil {
		t.Fatalf("newLoginClient() error = %v", errClient)
	}
	defer client.CloseIdleConnections()

	response, payload, errRequest := client.DoNoRedirect(
		t.Context(),
		http.MethodGet,
		upstream.URL+"/login",
		nil,
		nil,
	)
	if errRequest != nil {
		t.Fatalf("DoNoRedirect() error = %v", errRequest)
	}
	if response.StatusCode != http.StatusOK || string(payload) != `{"ok":true}` || calls.Load() != 2 {
		t.Fatalf("response = %d %q, calls = %d", response.StatusCode, payload, calls.Load())
	}
	observationMux.Lock()
	defer observationMux.Unlock()
	if len(proxyAuth) != 2 || proxyAuth[0] == "" || proxyAuth[1] == "" || proxyAuth[0] == proxyAuth[1] {
		t.Fatalf("proxy authorization values did not rotate: %#v", proxyAuth)
	}
	if !strings.Contains(secondCookie, "flow=keep") {
		t.Fatalf("retry did not preserve the request cookie: %q", secondCookie)
	}
}

func TestLoginClientDoesNotReplayOneTimeRequest(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		response.Header().Set("CF-Ray", "challenge-ray")
		response.Header().Set("Content-Type", "text/html")
		response.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(response, `<html><script src="/cdn-cgi/challenge-platform/x"></script></html>`)
	}))
	defer upstream.Close()
	proxy := newLoginConnectProxy(t, nil)

	template := strings.Replace(proxy.URL, "http://", "http://session-{1}:secret@", 1)
	selector, errSelector := newLoginProxySelector(LoginProxyConfig{
		Enabled:            true,
		URLTemplate:        template,
		PlaceholderCharset: "ab",
		RotateOnRetry:      true,
		RequestAttempts:    3,
		FlowAttempts:       2,
	}, bytes.NewReader([]byte{0, 1, 0}))
	if errSelector != nil {
		t.Fatalf("newLoginProxySelector() error = %v", errSelector)
	}
	client, errClient := newLoginClient(DefaultPersona(), nil, selector, time.Second)
	if errClient != nil {
		t.Fatalf("newLoginClient() error = %v", errClient)
	}
	defer client.CloseIdleConnections()

	_, _, errRequest := client.DoJSONOnce(
		t.Context(),
		false,
		http.MethodPost,
		upstream.URL+"/api/accounts/oauth/token",
		nil,
		map[string]string{"code": "one-time"},
	)
	requestError, ok := loginRequestErrorDetails(errRequest)
	if !ok || !requestError.cloudflare {
		t.Fatalf("DoJSONOnce() error = %#v, want Cloudflare request error", errRequest)
	}
	if calls.Load() != 1 {
		t.Fatalf("one-time request calls = %d, want 1", calls.Load())
	}
}

func TestWaitLoginRetryRespondsToCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	started := time.Now()
	if errDelay := waitLoginRetry(ctx, time.Minute, 1); errDelay == nil {
		t.Fatal("waitLoginRetry() error = nil")
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("canceled retry delay took %s", elapsed)
	}
}

func TestLoginRequestLogFieldsExcludeQueryAndProxyMaterial(t *testing.T) {
	fields := loginRequestLogFields(
		"https://auth.example/api/accounts/authorize?token=secret-token&email=person@example.com",
		"authorize",
		http.StatusForbidden,
		1,
		3,
	)
	if fields["host"] != "auth.example" || fields["path"] != "/api/accounts/authorize" {
		t.Fatalf("log fields = %#v", fields)
	}
	serialized := ""
	for _, value := range fields {
		serialized += strings.TrimSpace(strings.ReplaceAll(strings.TrimSpace(toString(value)), "\n", ""))
	}
	for _, sensitive := range []string{"secret-token", "person@example.com", "session-", "proxy"} {
		if strings.Contains(serialized, sensitive) {
			t.Fatalf("log fields exposed %q: %#v", sensitive, fields)
		}
	}
}

func toString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return ""
	}
}

func newLoginConnectProxy(t *testing.T, observe func(*http.Request)) *httptest.Server {
	t.Helper()
	proxy := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if observe != nil {
			observe(request)
		}
		if request.Method != http.MethodConnect {
			http.Error(response, "CONNECT required", http.StatusMethodNotAllowed)
			return
		}
		upstream, errDial := net.DialTimeout("tcp", request.Host, time.Second)
		if errDial != nil {
			http.Error(response, "proxy target unavailable", http.StatusBadGateway)
			return
		}
		hijacker, ok := response.(http.Hijacker)
		if !ok {
			_ = upstream.Close()
			http.Error(response, "hijacking unavailable", http.StatusInternalServerError)
			return
		}
		client, buffered, errHijack := hijacker.Hijack()
		if errHijack != nil {
			_ = upstream.Close()
			return
		}
		if _, errWrite := buffered.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); errWrite != nil {
			_ = client.Close()
			_ = upstream.Close()
			return
		}
		if errFlush := buffered.Flush(); errFlush != nil {
			_ = client.Close()
			_ = upstream.Close()
			return
		}
		go func() {
			_, _ = io.Copy(upstream, client)
			_ = upstream.Close()
		}()
		_, _ = io.Copy(client, upstream)
		_ = client.Close()
		_ = upstream.Close()
	}))
	t.Cleanup(proxy.Close)
	return proxy
}
