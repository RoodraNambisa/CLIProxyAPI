package claude

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
	"golang.org/x/net/http2"
)

type claudeObservedCloseConn struct {
	net.Conn
	once   sync.Once
	closed chan struct{}
}

func (c *claudeObservedCloseConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

func newClaudeTestHTTP2ClientConn(t *testing.T) (*http2.ClientConn, <-chan struct{}) {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	observed := &claudeObservedCloseConn{Conn: clientConn, closed: make(chan struct{})}
	go func() {
		defer func() { _ = serverConn.Close() }()
		server := &http2.Server{}
		server.ServeConn(serverConn, &http2.ServeConnOpts{Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})})
	}()
	conn, errClient := (&http2.Transport{}).NewClientConn(observed)
	if errClient != nil {
		_ = observed.Close()
		t.Fatalf("NewClientConn() error = %v", errClient)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn, observed.closed
}

func waitForClaudeObservedClose(t *testing.T, closed <-chan struct{}) {
	t.Helper()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Claude HTTP/2 client connection was not closed")
	}
}

type claudeTestContextDialer struct {
	dialContext func(context.Context, string, string) (net.Conn, error)
}

func (d claudeTestContextDialer) Dial(network, addr string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, addr)
}

func (d claudeTestContextDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	return d.dialContext(ctx, network, addr)
}

func TestNewClaudeAuthWithProxyURL_OverrideDirectTakesPrecedence(t *testing.T) {
	cfg := &config.Config{SDKConfig: config.SDKConfig{ProxyURL: "socks5://proxy.example.com:1080"}}
	auth := NewClaudeAuthWithProxyURL(cfg, "direct")

	transport, ok := auth.httpClient.Transport.(*utlsRoundTripper)
	if !ok || transport == nil {
		t.Fatalf("expected utlsRoundTripper, got %T", auth.httpClient.Transport)
	}
	if transport.proxyMode != proxyutil.ModeDirect {
		t.Fatalf("proxy mode = %v, want direct", transport.proxyMode)
	}
}

func TestNewClaudeAuthWithProxyURL_OverrideProxyAppliedWithoutConfig(t *testing.T) {
	auth := NewClaudeAuthWithProxyURL(nil, "socks5://proxy.example.com:1080")

	transport, ok := auth.httpClient.Transport.(*utlsRoundTripper)
	if !ok || transport == nil {
		t.Fatalf("expected utlsRoundTripper, got %T", auth.httpClient.Transport)
	}
	if transport.proxyMode != proxyutil.ModeProxy {
		t.Fatalf("proxy mode = %v, want proxy", transport.proxyMode)
	}
}

func TestNewClaudeAuthWithProxyURL_InvalidProxyDoesNotFallBackDirect(t *testing.T) {
	auth := NewClaudeAuthWithProxyURL(nil, "http://proxy.example.com:0")
	transport, ok := auth.httpClient.Transport.(*utlsRoundTripper)
	if !ok || transport == nil {
		t.Fatalf("expected utlsRoundTripper, got %T", auth.httpClient.Transport)
	}
	if transport.configErr == nil {
		t.Fatal("configErr = nil, want invalid proxy error")
	}
}

func TestClaudeUTLSPendingConnectionWaitHonorsContext(t *testing.T) {
	transport := newUtlsRoundTripper(nil)
	done := make(chan struct{})
	transport.mu.Lock()
	transport.pending["example.com:443"] = done
	transport.mu.Unlock()
	defer close(done)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	conn, errGet := transport.getOrCreateConnection(ctx, "example.com:443", "example.com", "example.com:443")
	if conn != nil {
		t.Fatalf("connection = %v, want nil", conn)
	}
	if !errors.Is(errGet, context.DeadlineExceeded) {
		t.Fatalf("getOrCreateConnection() error = %v, want deadline exceeded", errGet)
	}
}

func TestClaudeUTLSRoundTripLimitsOnlyConnectionStage(t *testing.T) {
	transport := newUtlsRoundTripper(nil)
	transport.connectTimeout = 20 * time.Millisecond
	transport.dialer = claudeTestContextDialer{dialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	req, errRequest := http.NewRequest(http.MethodPost, "https://example.com/oauth/token", nil)
	if errRequest != nil {
		t.Fatalf("NewRequest() error = %v", errRequest)
	}
	_, errRoundTrip := transport.RoundTrip(req)
	if !errors.Is(errRoundTrip, context.DeadlineExceeded) {
		t.Fatalf("RoundTrip() error = %v, want connection deadline exceeded", errRoundTrip)
	}
}

func TestClaudeUTLSRoundTripUsesIPv6SafeAddress(t *testing.T) {
	transport := newUtlsRoundTripper(nil)
	addrCh := make(chan string, 1)
	transport.dialer = claudeTestContextDialer{dialContext: func(_ context.Context, _, addr string) (net.Conn, error) {
		addrCh <- addr
		return nil, errors.New("stop after address capture")
	}}
	req, errRequest := http.NewRequest(http.MethodPost, "https://[2001:db8::1]/oauth/token", nil)
	if errRequest != nil {
		t.Fatalf("NewRequest() error = %v", errRequest)
	}
	_, _ = transport.RoundTrip(req)
	if got := <-addrCh; got != "[2001:db8::1]:443" {
		t.Fatalf("dial address = %q, want IPv6-safe address", got)
	}
}

func TestClaudeUTLSRoundTripperClosesEvictedAndFailedConnections(t *testing.T) {
	t.Run("evicted", func(t *testing.T) {
		conn, closed := newClaudeTestHTTP2ClientConn(t)
		conn.SetDoNotReuse()
		transport := newUtlsRoundTripper(nil)
		transport.connections["example.com:443"] = conn
		transport.dialer = claudeTestContextDialer{dialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return nil, ctx.Err()
		}}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, _ = transport.getOrCreateConnection(ctx, "example.com:443", "example.com", "example.com:443")
		waitForClaudeObservedClose(t, closed)
	})

	t.Run("round trip error", func(t *testing.T) {
		conn, closed := newClaudeTestHTTP2ClientConn(t)
		transport := newUtlsRoundTripper(nil)
		transport.connections["example.com:443"] = conn
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		req, errRequest := http.NewRequestWithContext(ctx, http.MethodGet, "https://example.com", nil)
		if errRequest != nil {
			t.Fatalf("NewRequestWithContext() error = %v", errRequest)
		}
		_, _ = transport.RoundTrip(req)
		waitForClaudeObservedClose(t, closed)
	})
}
