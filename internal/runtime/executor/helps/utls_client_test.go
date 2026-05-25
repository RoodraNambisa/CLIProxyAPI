package helps

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestStripHTTP2ConnectionHeaders(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "https://example.com/responses", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	req.Header.Set("Connection", "Keep-Alive, X-Drop")
	req.Header.Set("Keep-Alive", "timeout=5")
	req.Header.Set("Proxy-Connection", "keep-alive")
	req.Header.Set("Transfer-Encoding", "chunked")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("X-Drop", "1")
	req.Header.Set("X-Keep", "1")

	got := stripHTTP2ConnectionHeaders(req)

	for _, key := range []string{"Connection", "Keep-Alive", "Proxy-Connection", "Transfer-Encoding", "Upgrade", "X-Drop"} {
		if value := got.Header.Get(key); value != "" {
			t.Fatalf("%s = %q, want empty", key, value)
		}
	}
	if value := got.Header.Get("X-Keep"); value != "1" {
		t.Fatalf("X-Keep = %q, want 1", value)
	}
	if value := req.Header.Get("Connection"); value == "" {
		t.Fatalf("original request headers were mutated")
	}
}

func TestNewChromeUtlsHTTPClientUsesInjectedTransportWithoutExplicitProxy(t *testing.T) {
	wantTransport := &stubRoundTripper{}
	ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", http.RoundTripper(wantTransport))

	client := NewChromeUtlsHTTPClient(ctx, &config.Config{}, &cliproxyauth.Auth{}, 0, "chrome_133")

	if client.Transport != wantTransport {
		t.Fatalf("transport = %T, want injected context RoundTripper", client.Transport)
	}
}

func TestUtlsRoundTripperUsesEnvironmentProxy(t *testing.T) {
	setEnvironmentProxy(t, "http://env-proxy.example.com:8080")
	req, err := http.NewRequest(http.MethodGet, "https://example.com", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	rt := newUtlsRoundTripper("", "chrome_133", true)

	proxyURL, err := rt.proxyURLForRequest(req)
	if err != nil {
		t.Fatalf("proxyURLForRequest() error = %v", err)
	}
	if proxyURL != "http://env-proxy.example.com:8080" {
		t.Fatalf("proxyURL = %q, want env proxy", proxyURL)
	}
}

func TestUtlsConnectionCacheKeyIncludesPort(t *testing.T) {
	key443 := utlsConnectionCacheKey("example.com:443", "http://proxy.example.com:8080")
	key8443 := utlsConnectionCacheKey("example.com:8443", "http://proxy.example.com:8080")

	if key443 == key8443 {
		t.Fatalf("cache keys should differ by port: %q", key443)
	}
}

func TestUtlsPendingConnectionWaitHonorsContextCancellation(t *testing.T) {
	rt := newUtlsRoundTripper("", "chrome_133", false)
	cacheKey := utlsConnectionCacheKey("example.com:443", "")
	done := make(chan struct{})
	rt.mu.Lock()
	rt.pending[cacheKey] = done
	rt.mu.Unlock()
	defer close(done)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	conn, err := rt.getOrCreateConnection(ctx, cacheKey, "example.com", "example.com:443", "")
	if conn != nil {
		t.Fatalf("connection = %v, want nil", conn)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("getOrCreateConnection() error = %v, want context deadline exceeded", err)
	}
}

func TestEnvironmentProxyURLForHTTPSAddr(t *testing.T) {
	setEnvironmentProxy(t, "http://env-proxy.example.com:8080")

	proxyURL, err := environmentProxyURLForHTTPSAddr("chatgpt.com:443")
	if err != nil {
		t.Fatalf("environmentProxyURLForHTTPSAddr() error = %v", err)
	}
	if proxyURL != "http://env-proxy.example.com:8080" {
		t.Fatalf("proxyURL = %q, want env proxy", proxyURL)
	}
}

func TestHTTPConnectDialerHonorsContextCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer listener.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, errAccept := listener.Accept()
		if errAccept == nil {
			accepted <- conn
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	dialer := &httpConnectDialer{
		proxyURL: &url.URL{Scheme: "http", Host: listener.Addr().String()},
		dialer:   directContextDialer{},
	}

	conn, err := dialer.DialContext(ctx, "tcp", "example.com:443")
	if conn != nil {
		_ = conn.Close()
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("DialContext() error = %v, want context deadline exceeded", err)
	}
	select {
	case acceptedConn := <-accepted:
		_ = acceptedConn.Close()
	default:
	}
}
