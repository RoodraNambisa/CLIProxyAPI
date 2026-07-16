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
	if strings.Contains(message, "s3cret") || !strings.Contains(message, "********") {
		t.Fatalf("masked message = %q", message)
	}

	result := CheckTrace(context.Background(), "http://user:sec%ret@proxy.example:8080")
	if strings.Contains(result.Message, "sec%ret") {
		t.Fatalf("CheckTrace() leaked malformed proxy password: %+v", result)
	}
	if result.Error != "invalid_proxy" {
		t.Fatalf("error = %q, want invalid_proxy", result.Error)
	}
}
