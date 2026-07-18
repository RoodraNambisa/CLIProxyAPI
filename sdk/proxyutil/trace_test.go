package proxyutil

import (
	"context"
	"encoding/base64"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestTraceDefaults(t *testing.T) {
	if DefaultTraceEndpoint != "https://cloudflare.com/cdn-cgi/trace" {
		t.Fatalf("DefaultTraceEndpoint = %q", DefaultTraceEndpoint)
	}
	if DefaultTraceUserAgent != "Mozilla/5.0" {
		t.Fatalf("DefaultTraceUserAgent = %q", DefaultTraceUserAgent)
	}
	if DefaultTraceTimeout != 8*time.Second {
		t.Fatalf("DefaultTraceTimeout = %s", DefaultTraceTimeout)
	}
}

func TestCheckTraceSupportsDirectAndInherit(t *testing.T) {
	userAgents := make(chan string, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userAgents <- r.Header.Get("User-Agent")
		time.Sleep(2 * time.Millisecond)
		_, _ = w.Write([]byte("ip=1.2.3.4\nloc=US\nhttp=http/2\ntls=TLSv1.3\ncolo=LAX\n"))
	}))
	defer server.Close()

	for _, proxyURL := range []string{"direct", ""} {
		result := CheckTrace(context.Background(), proxyURL, TraceOptions{
			Endpoint: server.URL,
			Timeout:  time.Second,
		})
		if !result.OK || result.Error != "" || result.Message != "" {
			t.Fatalf("CheckTrace(%q) = %+v", proxyURL, result)
		}
		if result.IP != "1.2.3.4" || result.Location != "US" || result.HTTP != "http/2" || result.TLS != "TLSv1.3" || result.Colo != "LAX" {
			t.Fatalf("CheckTrace(%q) trace fields = %+v", proxyURL, result)
		}
		if result.Elapsed <= 0 {
			t.Fatalf("CheckTrace(%q) elapsed = %s", proxyURL, result.Elapsed)
		}
	}

	for range 2 {
		if got := <-userAgents; got != DefaultTraceUserAgent {
			t.Fatalf("user-agent = %q, want %q", got, DefaultTraceUserAgent)
		}
	}
}

func TestCheckTraceDisablesKeepAliveForDedicatedTransport(t *testing.T) {
	requestClosed := make(chan bool, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestClosed <- r.Close
		_, _ = w.Write([]byte("ip=1.2.3.4\nloc=US\n"))
	}))
	defer server.Close()

	result := CheckTrace(context.Background(), "direct", TraceOptions{Endpoint: server.URL, Timeout: time.Second})
	if !result.OK {
		t.Fatalf("CheckTrace() = %+v", result)
	}
	if !<-requestClosed {
		t.Fatal("trace request kept its dedicated transport connection alive")
	}
}

func TestCheckTraceTimeoutCoversResponseHeaders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	defer server.Close()

	const timeout = 50 * time.Millisecond
	result := CheckTrace(context.Background(), "direct", TraceOptions{Endpoint: server.URL, Timeout: timeout})
	if result.OK || result.Error != "request_failed" {
		t.Fatalf("CheckTrace() = %+v, want request_failed", result)
	}
	if result.Elapsed < timeout/2 || result.Elapsed > time.Second {
		t.Fatalf("elapsed = %s, want complete request timeout near %s", result.Elapsed, timeout)
	}
}

func TestCheckTraceTimeoutCoversResponseBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ip=1.2.3.4\n"))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	defer server.Close()

	const timeout = 50 * time.Millisecond
	result := CheckTrace(context.Background(), "direct", TraceOptions{Endpoint: server.URL, Timeout: timeout})
	if result.OK || result.Error != "read_failed" {
		t.Fatalf("CheckTrace() = %+v, want read_failed", result)
	}
	if result.Elapsed < timeout/2 || result.Elapsed > time.Second {
		t.Fatalf("elapsed = %s, want complete request timeout near %s", result.Elapsed, timeout)
	}
}

func TestCheckTraceSupportsConcreteHTTPProxy(t *testing.T) {
	type proxyRequest struct {
		url                string
		proxyAuthorization string
	}
	requests := make(chan proxyRequest, 1)
	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- proxyRequest{
			url:                r.URL.String(),
			proxyAuthorization: r.Header.Get("Proxy-Authorization"),
		}
		_, _ = w.Write([]byte("ip=5.6.7.8\nloc=JP\n"))
	}))
	defer proxyServer.Close()

	proxyURL := strings.Replace(proxyServer.URL, "http://", "http://user:s3cret@", 1)
	endpoint := "http://trace.invalid/cdn-cgi/trace"
	result := CheckTrace(context.Background(), proxyURL, TraceOptions{
		Endpoint: endpoint,
		Timeout:  time.Second,
	})
	if !result.OK || result.IP != "5.6.7.8" || result.Location != "JP" {
		t.Fatalf("CheckTrace() = %+v", result)
	}

	request := <-requests
	if request.url != endpoint {
		t.Fatalf("proxy request URL = %q, want %q", request.url, endpoint)
	}
	wantAuthorization := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:s3cret"))
	if request.proxyAuthorization != wantAuthorization {
		t.Fatalf("Proxy-Authorization = %q, want %q", request.proxyAuthorization, wantAuthorization)
	}
}

func TestTraceHTTPTransportUsesInjectedConnectTimeout(t *testing.T) {
	base := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	transport := traceHTTPTransport(base, 20*time.Millisecond)
	started := time.Now()
	conn, errDial := transport.DialContext(context.Background(), "tcp", "example.test:443")
	if conn != nil {
		_ = conn.Close()
		t.Fatal("DialContext() returned a connection after timeout")
	}
	if !errors.Is(errDial, context.DeadlineExceeded) {
		t.Fatalf("DialContext() error = %v, want deadline exceeded", errDial)
	}
	if elapsed := time.Since(started); elapsed < 10*time.Millisecond || elapsed > time.Second {
		t.Fatalf("elapsed = %s, want injected connection timeout near 20ms", elapsed)
	}
}

func TestCheckTraceAcceptsNilContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ip=1.2.3.4\nloc=US\n"))
	}))
	defer server.Close()

	result := CheckTrace(nil, "direct", TraceOptions{Endpoint: server.URL, Timeout: time.Second})
	if !result.OK || result.Error != "" {
		t.Fatalf("CheckTrace(nil) = %+v", result)
	}
}

func TestCheckTraceDiagnostics(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status":
			w.WriteHeader(http.StatusBadGateway)
		case "/invalid":
			_, _ = w.Write([]byte("warp=off\n"))
		}
	}))
	defer server.Close()

	tests := []struct {
		path      string
		wantError string
	}{
		{path: "/status", wantError: "unexpected_status"},
		{path: "/invalid", wantError: "invalid_trace"},
	}
	for _, test := range tests {
		result := CheckTrace(context.Background(), "direct", TraceOptions{
			Endpoint: server.URL + test.path,
			Timeout:  time.Second,
		})
		if result.OK || result.Error != test.wantError || result.Message == "" {
			t.Fatalf("CheckTrace(%q) = %+v", test.path, result)
		}
	}
}

func TestCheckTraceMasksProxyPasswordsInErrors(t *testing.T) {
	const proxyURL = "http://user:s3cret@proxy.example:8080"
	message := maskTraceMessage("proxy request via "+proxyURL+" failed", proxyURL)
	if strings.Contains(message, "user") || strings.Contains(message, "s3cret") || !strings.Contains(message, "********") {
		t.Fatalf("masked message = %q", message)
	}

	const usernameOnlyProxyURL = "http://session-token@proxy.example:8080"
	usernameOnlyMessage := maskTraceMessage("proxy userinfo session-token@ failed", usernameOnlyProxyURL)
	if strings.Contains(usernameOnlyMessage, "session-token") || !strings.Contains(usernameOnlyMessage, "********") {
		t.Fatalf("masked username-only message = %q", usernameOnlyMessage)
	}

	const encodedProxyURL = "http://session%2Dtoken:p%40ss@proxy.example:8080"
	encodedMessage := maskTraceMessage("proxy rejected session%2Dtoken:p%40ss@", encodedProxyURL)
	if strings.Contains(encodedMessage, "session%2Dtoken") || strings.Contains(encodedMessage, "p%40ss") {
		t.Fatalf("masked encoded credentials = %q", encodedMessage)
	}
	decodedMessage := maskTraceMessage("proxy rejected session-token:p@ss@", encodedProxyURL)
	if strings.Contains(decodedMessage, "session-token") || strings.Contains(decodedMessage, "p@ss") {
		t.Fatalf("masked decoded credentials = %q", decodedMessage)
	}
	const malformedProxyURL = "http://user:sec%ret@proxy.example:8080"
	malformedMessage := maskTraceMessage("proxy rejected user:sec%ret@", malformedProxyURL)
	if strings.Contains(malformedMessage, "user") || strings.Contains(malformedMessage, "sec%ret") {
		t.Fatalf("masked malformed credentials = %q", malformedMessage)
	}

	result := CheckTrace(context.Background(), malformedProxyURL)
	if strings.Contains(result.Message, "sec%ret") {
		t.Fatalf("CheckTrace() leaked malformed proxy password: %+v", result)
	}
	if result.Error != "invalid_proxy" {
		t.Fatalf("error = %q, want invalid_proxy", result.Error)
	}
}

func TestMaskTraceMessageDoesNotReplaceCredentialSubstrings(t *testing.T) {
	const proxyURL = "http://proxy:dial@proxy.example:8080"
	message := maskTraceMessage("dial proxy.example failed; proxy connection closed", proxyURL)
	if message != "dial proxy.example failed; proxy connection closed" {
		t.Fatalf("masked message corrupted diagnostics: %q", message)
	}
	explicitUserInfo := maskTraceMessage("proxy rejected proxy:dial@", proxyURL)
	if strings.Contains(explicitUserInfo, "proxy:dial@") || !strings.Contains(explicitUserInfo, "********:********@") {
		t.Fatalf("explicit proxy userinfo was not masked: %q", explicitUserInfo)
	}
}

func TestMaskTraceMessageIgnoresEmptyProxyUserInfo(t *testing.T) {
	const proxyURL = "http://@proxy.example:8080"
	const diagnostic = "notify ops@example.com about proxy failure"
	if message := maskTraceMessage(diagnostic, proxyURL); message != diagnostic {
		t.Fatalf("masked message corrupted diagnostics: %q", message)
	}
}

func TestMaskTraceMessagePreservesUnrelatedEmailWithMatchingUsername(t *testing.T) {
	const proxyURL = "http://ops@proxy.example:8080"
	const diagnostic = "notify ops@example.com; proxy rejected ops@proxy.example:8080"
	message := maskTraceMessage(diagnostic, proxyURL)
	if !strings.Contains(message, "ops@example.com") {
		t.Fatalf("masked message corrupted unrelated email: %q", message)
	}
	if strings.Contains(message, "ops@proxy.example:8080") || !strings.Contains(message, "********@proxy.example:8080") {
		t.Fatalf("masked message leaked proxy username: %q", message)
	}
}

func TestMaskTraceMessageMasksNormalizedProxyAuthority(t *testing.T) {
	const proxyURL = "http://user:secret@proxy.example:80"
	message := maskTraceMessage("proxy rejected user:secret@PROXY.EXAMPLE", proxyURL)
	if strings.Contains(message, "user:secret") || !strings.Contains(message, "********:********@PROXY.EXAMPLE") {
		t.Fatalf("normalized authority leaked credentials: %q", message)
	}

	const usernameOnlyProxyURL = "http://ops@proxy.example:80"
	usernameMessage := maskTraceMessage("notify ops@example.com; proxy rejected ops@PROXY.EXAMPLE", usernameOnlyProxyURL)
	if !strings.Contains(usernameMessage, "ops@example.com") || strings.Contains(usernameMessage, "ops@PROXY.EXAMPLE") {
		t.Fatalf("username-only normalized authority masking = %q", usernameMessage)
	}
}
