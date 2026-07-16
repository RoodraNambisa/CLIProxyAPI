package proxyutil

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func mustDefaultTransport(t *testing.T) *http.Transport {
	t.Helper()

	transport, ok := http.DefaultTransport.(*http.Transport)
	if !ok || transport == nil {
		t.Fatal("http.DefaultTransport is not an *http.Transport")
	}
	return transport
}

func TestParseErrorDoesNotExposeProxyPassword(t *testing.T) {
	_, errParse := Parse("http://user:sec%ret@proxy.example:8080")
	if errParse == nil {
		t.Fatal("Parse() error = nil")
	}
	if strings.Contains(errParse.Error(), "sec%ret") {
		t.Fatalf("Parse() error leaked proxy password: %v", errParse)
	}
}

func TestParseRejectsUnbracketedIPv6Proxy(t *testing.T) {
	if _, errParse := Parse("socks5h://user:pass@2001:db8::1"); errParse == nil {
		t.Fatal("Parse() error = nil")
	}
}

func TestParseRejectsOutOfRangePort(t *testing.T) {
	for _, raw := range []string{"http://proxy.example:0", "http://proxy.example:65536"} {
		if _, errParse := Parse(raw); errParse == nil {
			t.Fatalf("Parse(%q) error = nil", raw)
		}
	}
}

func TestParse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    Mode
		wantErr bool
	}{
		{name: "inherit", input: "", want: ModeInherit},
		{name: "direct", input: "direct", want: ModeDirect},
		{name: "none", input: "none", want: ModeDirect},
		{name: "http", input: "http://proxy.example.com:8080", want: ModeProxy},
		{name: "https", input: "https://proxy.example.com:8443", want: ModeProxy},
		{name: "socks5", input: "socks5://proxy.example.com:1080", want: ModeProxy},
		{name: "socks5h", input: "socks5h://proxy.example.com:1080", want: ModeProxy},
		{name: "invalid", input: "bad-value", want: ModeInvalid, wantErr: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			setting, errParse := Parse(tt.input)
			if tt.wantErr && errParse == nil {
				t.Fatal("expected error, got nil")
			}
			if !tt.wantErr && errParse != nil {
				t.Fatalf("unexpected error: %v", errParse)
			}
			if setting.Mode != tt.want {
				t.Fatalf("mode = %d, want %d", setting.Mode, tt.want)
			}
		})
	}
}

func TestBuildHTTPTransportDirectBypassesProxy(t *testing.T) {
	t.Parallel()

	transport, mode, errBuild := BuildHTTPTransport("direct")
	if errBuild != nil {
		t.Fatalf("BuildHTTPTransport returned error: %v", errBuild)
	}
	if mode != ModeDirect {
		t.Fatalf("mode = %d, want %d", mode, ModeDirect)
	}
	if transport == nil {
		t.Fatal("expected transport, got nil")
	}
	if transport.Proxy != nil {
		t.Fatal("expected direct transport to disable proxy function")
	}
}

func TestBuildHTTPTransportHTTPProxy(t *testing.T) {
	t.Parallel()

	transport, mode, errBuild := BuildHTTPTransport("http://proxy.example.com:8080")
	if errBuild != nil {
		t.Fatalf("BuildHTTPTransport returned error: %v", errBuild)
	}
	if mode != ModeProxy {
		t.Fatalf("mode = %d, want %d", mode, ModeProxy)
	}
	if transport == nil {
		t.Fatal("expected transport, got nil")
	}

	req, errRequest := http.NewRequest(http.MethodGet, "https://example.com", nil)
	if errRequest != nil {
		t.Fatalf("http.NewRequest returned error: %v", errRequest)
	}

	proxyURL, errProxy := transport.Proxy(req)
	if errProxy != nil {
		t.Fatalf("transport.Proxy returned error: %v", errProxy)
	}
	if proxyURL == nil || proxyURL.String() != "http://proxy.example.com:8080" {
		t.Fatalf("proxy URL = %v, want http://proxy.example.com:8080", proxyURL)
	}

	defaultTransport := mustDefaultTransport(t)
	if transport.ForceAttemptHTTP2 != defaultTransport.ForceAttemptHTTP2 {
		t.Fatalf("ForceAttemptHTTP2 = %v, want %v", transport.ForceAttemptHTTP2, defaultTransport.ForceAttemptHTTP2)
	}
	if transport.IdleConnTimeout != defaultTransport.IdleConnTimeout {
		t.Fatalf("IdleConnTimeout = %v, want %v", transport.IdleConnTimeout, defaultTransport.IdleConnTimeout)
	}
	if transport.TLSHandshakeTimeout != defaultTransport.TLSHandshakeTimeout {
		t.Fatalf("TLSHandshakeTimeout = %v, want %v", transport.TLSHandshakeTimeout, defaultTransport.TLSHandshakeTimeout)
	}
}

func TestBuildHTTPTransportSOCKS5ProxyInheritsDefaultTransportSettings(t *testing.T) {
	t.Parallel()

	transport, mode, errBuild := BuildHTTPTransport("socks5://proxy.example.com:1080")
	if errBuild != nil {
		t.Fatalf("BuildHTTPTransport returned error: %v", errBuild)
	}
	if mode != ModeProxy {
		t.Fatalf("mode = %d, want %d", mode, ModeProxy)
	}
	if transport == nil {
		t.Fatal("expected transport, got nil")
	}
	if transport.Proxy != nil {
		t.Fatal("expected SOCKS5 transport to bypass http proxy function")
	}

	defaultTransport := mustDefaultTransport(t)
	if transport.ForceAttemptHTTP2 != defaultTransport.ForceAttemptHTTP2 {
		t.Fatalf("ForceAttemptHTTP2 = %v, want %v", transport.ForceAttemptHTTP2, defaultTransport.ForceAttemptHTTP2)
	}
	if transport.IdleConnTimeout != defaultTransport.IdleConnTimeout {
		t.Fatalf("IdleConnTimeout = %v, want %v", transport.IdleConnTimeout, defaultTransport.IdleConnTimeout)
	}
	if transport.TLSHandshakeTimeout != defaultTransport.TLSHandshakeTimeout {
		t.Fatalf("TLSHandshakeTimeout = %v, want %v", transport.TLSHandshakeTimeout, defaultTransport.TLSHandshakeTimeout)
	}
}

func TestBuildHTTPTransportSOCKS5HProxy(t *testing.T) {
	t.Parallel()

	transport, mode, errBuild := BuildHTTPTransport("socks5h://proxy.example.com:1080")
	if errBuild != nil {
		t.Fatalf("BuildHTTPTransport returned error: %v", errBuild)
	}
	if mode != ModeProxy {
		t.Fatalf("mode = %d, want %d", mode, ModeProxy)
	}
	if transport == nil {
		t.Fatal("expected transport, got nil")
	}
	if transport.Proxy != nil {
		t.Fatal("expected SOCKS5H transport to bypass http proxy function")
	}
	if transport.DialContext == nil {
		t.Fatal("expected SOCKS5H transport to have custom DialContext")
	}
}

func TestBuildHTTPTransportSOCKS5HDialHonorsContext(t *testing.T) {
	listener, errListen := net.Listen("tcp", "127.0.0.1:0")
	if errListen != nil {
		t.Fatalf("Listen() error = %v", errListen)
	}
	defer listener.Close()
	go func() {
		conn, errAccept := listener.Accept()
		if errAccept != nil {
			return
		}
		defer conn.Close()
		time.Sleep(500 * time.Millisecond)
	}()

	transport, _, errBuild := BuildHTTPTransport("socks5h://" + listener.Addr().String())
	if errBuild != nil {
		t.Fatalf("BuildHTTPTransport() error = %v", errBuild)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, errDial := transport.DialContext(ctx, "tcp", "upstream.example:443")
	var netErr net.Error
	if !errors.Is(errDial, context.DeadlineExceeded) && (!errors.As(errDial, &netErr) || !netErr.Timeout()) {
		t.Fatalf("DialContext() error = %v, want context timeout", errDial)
	}
	if elapsed := time.Since(started); elapsed > 300*time.Millisecond {
		t.Fatalf("DialContext() elapsed = %v, want prompt cancellation", elapsed)
	}
}

func TestBuildContextDialerHTTPConnectUsesTargetAndProxyAuth(t *testing.T) {
	listener, errListen := net.Listen("tcp", "127.0.0.1:0")
	if errListen != nil {
		t.Fatalf("Listen() error = %v", errListen)
	}
	defer listener.Close()

	requestCh := make(chan *http.Request, 1)
	serverErrCh := make(chan error, 1)
	go func() {
		conn, errAccept := listener.Accept()
		if errAccept != nil {
			serverErrCh <- errAccept
			return
		}
		defer conn.Close()
		req, errRead := http.ReadRequest(bufio.NewReader(conn))
		if errRead != nil {
			serverErrCh <- errRead
			return
		}
		requestCh <- req
		_, errWrite := io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\n")
		serverErrCh <- errWrite
	}()

	dialer, mode, errBuild := BuildContextDialer("http://user:secret@" + listener.Addr().String())
	if errBuild != nil {
		t.Fatalf("BuildContextDialer() error = %v", errBuild)
	}
	if mode != ModeProxy || dialer == nil {
		t.Fatalf("BuildContextDialer() = (%T, %v), want proxy dialer", dialer, mode)
	}
	conn, errDial := dialer.DialContext(context.Background(), "tcp", "upstream.example:443")
	if errDial != nil {
		t.Fatalf("DialContext() error = %v", errDial)
	}
	defer conn.Close()

	req := <-requestCh
	if req.Method != http.MethodConnect || req.Host != "upstream.example:443" {
		t.Fatalf("CONNECT request = %s %s, want upstream.example:443", req.Method, req.Host)
	}
	wantAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:secret"))
	if got := req.Header.Get("Proxy-Authorization"); got != wantAuth {
		t.Fatalf("Proxy-Authorization = %q, want %q", got, wantAuth)
	}
	if errServer := <-serverErrCh; errServer != nil {
		t.Fatalf("proxy server error = %v", errServer)
	}
}

func TestHTTPConnectDialerPreservesBufferedTunnelBytes(t *testing.T) {
	listener, errListen := net.Listen("tcp", "127.0.0.1:0")
	if errListen != nil {
		t.Fatalf("Listen() error = %v", errListen)
	}
	defer listener.Close()
	go func() {
		conn, errAccept := listener.Accept()
		if errAccept != nil {
			return
		}
		defer conn.Close()
		_, _ = http.ReadRequest(bufio.NewReader(conn))
		_, _ = io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\nhello")
	}()

	dialer, _, errBuild := BuildContextDialer("http://" + listener.Addr().String())
	if errBuild != nil {
		t.Fatalf("BuildContextDialer() error = %v", errBuild)
	}
	conn, errDial := dialer.DialContext(context.Background(), "tcp", "upstream.example:443")
	if errDial != nil {
		t.Fatalf("DialContext() error = %v", errDial)
	}
	defer conn.Close()
	payload := make([]byte, len("hello"))
	if _, errRead := io.ReadFull(conn, payload); errRead != nil {
		t.Fatalf("ReadFull() error = %v", errRead)
	}
	if string(payload) != "hello" {
		t.Fatalf("tunnel payload = %q, want hello", payload)
	}
}

func TestHTTPConnectDialerRejectsOversizedHeadersAsProxyFailure(t *testing.T) {
	listener, errListen := net.Listen("tcp", "127.0.0.1:0")
	if errListen != nil {
		t.Fatalf("Listen() error = %v", errListen)
	}
	defer listener.Close()
	go func() {
		conn, errAccept := listener.Accept()
		if errAccept != nil {
			return
		}
		defer conn.Close()
		_, _ = http.ReadRequest(bufio.NewReader(conn))
		_, _ = io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\nX-Fill: "+strings.Repeat("a", maxProxyConnectHeaderBytes)+"\r\n\r\n")
	}()

	dialer, _, errBuild := BuildContextDialer("http://" + listener.Addr().String())
	if errBuild != nil {
		t.Fatalf("BuildContextDialer() error = %v", errBuild)
	}
	_, errDial := dialer.DialContext(context.Background(), "tcp", "upstream.example:443")
	if errDial == nil {
		t.Fatal("DialContext() error = nil")
	}
	var proxyFailure interface{ ProxyInfrastructureError() bool }
	if !errors.As(errDial, &proxyFailure) || !proxyFailure.ProxyInfrastructureError() {
		t.Fatalf("DialContext() error = %T %v, want proxy infrastructure error", errDial, errDial)
	}
}

func TestHTTPConnectDialerReturnsTypedProxyStatusFailure(t *testing.T) {
	listener, errListen := net.Listen("tcp", "127.0.0.1:0")
	if errListen != nil {
		t.Fatalf("Listen() error = %v", errListen)
	}
	defer listener.Close()
	go func() {
		conn, errAccept := listener.Accept()
		if errAccept != nil {
			return
		}
		defer conn.Close()
		_, _ = http.ReadRequest(bufio.NewReader(conn))
		_, _ = io.WriteString(conn, "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n")
	}()

	dialer, _, errBuild := BuildContextDialer("http://" + listener.Addr().String())
	if errBuild != nil {
		t.Fatalf("BuildContextDialer() error = %v", errBuild)
	}
	_, errDial := dialer.DialContext(context.Background(), "tcp", "upstream.example:443")
	var proxyFailure interface{ ProxyInfrastructureError() bool }
	if !errors.As(errDial, &proxyFailure) || !proxyFailure.ProxyInfrastructureError() {
		t.Fatalf("DialContext() error = %T %v, want proxy infrastructure error", errDial, errDial)
	}
}

func TestHTTPSConnectDialerNegotiatesTLSBeforeCONNECT(t *testing.T) {
	requestCh := make(chan *http.Request, 1)
	tunnelDataCh := make(chan string, 1)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Error("response writer does not support hijacking")
			return
		}
		conn, rw, errHijack := hijacker.Hijack()
		if errHijack != nil {
			t.Errorf("Hijack() error = %v", errHijack)
			return
		}
		defer conn.Close()
		requestCh <- req.Clone(req.Context())
		if _, errWrite := rw.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); errWrite != nil {
			t.Errorf("write CONNECT response error = %v", errWrite)
			return
		}
		if errFlush := rw.Flush(); errFlush != nil {
			t.Errorf("flush CONNECT response error = %v", errFlush)
			return
		}
		payload := make([]byte, 4)
		if _, errRead := io.ReadFull(rw, payload); errRead != nil {
			t.Errorf("read tunnel payload error = %v", errRead)
			return
		}
		tunnelDataCh <- string(payload)
	}))
	defer server.Close()

	proxyURL, errParse := url.Parse(server.URL)
	if errParse != nil {
		t.Fatalf("url.Parse() error = %v", errParse)
	}
	dialer := &httpConnectDialer{
		proxyURL: proxyURL,
		dialer:   directContextDialer{},
		tlsConfig: &tls.Config{
			InsecureSkipVerify: true, // Test server certificate.
		},
	}
	conn, errDial := dialer.DialContext(context.Background(), "tcp", "upstream.example:443")
	if errDial != nil {
		t.Fatalf("DialContext() error = %v", errDial)
	}
	defer conn.Close()
	if _, errWrite := io.WriteString(conn, "ping"); errWrite != nil {
		t.Fatalf("write tunnel payload error = %v", errWrite)
	}
	req := <-requestCh
	if req.Method != http.MethodConnect || req.Host != "upstream.example:443" {
		t.Fatalf("CONNECT request = %s %s, want upstream.example:443", req.Method, req.Host)
	}
	if got := <-tunnelDataCh; got != "ping" {
		t.Fatalf("tunnel payload = %q, want ping", got)
	}
}

func TestBuildContextDialerHTTPConnectHonorsCancellation(t *testing.T) {
	listener, errListen := net.Listen("tcp", "127.0.0.1:0")
	if errListen != nil {
		t.Fatalf("Listen() error = %v", errListen)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, errAccept := listener.Accept()
		if errAccept == nil {
			accepted <- conn
		}
	}()

	dialer, _, errBuild := BuildContextDialer("http://" + listener.Addr().String())
	if errBuild != nil {
		t.Fatalf("BuildContextDialer() error = %v", errBuild)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	conn, errDial := dialer.DialContext(ctx, "tcp", "upstream.example:443")
	if conn != nil {
		_ = conn.Close()
	}
	if !errors.Is(errDial, context.DeadlineExceeded) {
		t.Fatalf("DialContext() error = %v, want context deadline exceeded", errDial)
	}
	select {
	case acceptedConn := <-accepted:
		_ = acceptedConn.Close()
	default:
	}
}

func TestBuildContextDialerSOCKS5HPreservesRemoteHostname(t *testing.T) {
	listener, errListen := net.Listen("tcp", "127.0.0.1:0")
	if errListen != nil {
		t.Fatalf("Listen() error = %v", errListen)
	}
	defer listener.Close()
	hostCh := make(chan string, 1)
	serverErrCh := make(chan error, 1)
	go func() {
		conn, errAccept := listener.Accept()
		if errAccept != nil {
			serverErrCh <- errAccept
			return
		}
		defer conn.Close()
		greeting := make([]byte, 3)
		if _, errRead := io.ReadFull(conn, greeting); errRead != nil {
			serverErrCh <- errRead
			return
		}
		if _, errWrite := conn.Write([]byte{5, 0}); errWrite != nil {
			serverErrCh <- errWrite
			return
		}
		header := make([]byte, 4)
		if _, errRead := io.ReadFull(conn, header); errRead != nil {
			serverErrCh <- errRead
			return
		}
		if header[0] != 5 || header[1] != 1 || header[3] != 3 {
			serverErrCh <- fmt.Errorf("unexpected SOCKS5 request header: %v", header)
			return
		}
		length := make([]byte, 1)
		if _, errRead := io.ReadFull(conn, length); errRead != nil {
			serverErrCh <- errRead
			return
		}
		hostAndPort := make([]byte, int(length[0])+2)
		if _, errRead := io.ReadFull(conn, hostAndPort); errRead != nil {
			serverErrCh <- errRead
			return
		}
		hostCh <- string(hostAndPort[:length[0]])
		_, errWrite := conn.Write([]byte{5, 0, 0, 1, 127, 0, 0, 1, 0, 0})
		serverErrCh <- errWrite
	}()

	dialer, _, errBuild := BuildContextDialer("socks5h://" + listener.Addr().String())
	if errBuild != nil {
		t.Fatalf("BuildContextDialer() error = %v", errBuild)
	}
	conn, errDial := dialer.DialContext(context.Background(), "tcp", "remote-only.invalid:443")
	if errDial != nil {
		t.Fatalf("DialContext() error = %v", errDial)
	}
	defer conn.Close()
	if got := <-hostCh; got != "remote-only.invalid" {
		t.Fatalf("SOCKS5 hostname = %q, want remote-only.invalid", got)
	}
	if errServer := <-serverErrCh; errServer != nil {
		t.Fatalf("SOCKS5 server error = %v", errServer)
	}
}

func TestProxyEndpointAddressAppliesSchemeDefaults(t *testing.T) {
	tests := []struct {
		raw         string
		defaultPort string
		want        string
	}{
		{raw: "http://proxy.example", defaultPort: "80", want: "proxy.example:80"},
		{raw: "https://proxy.example", defaultPort: "443", want: "proxy.example:443"},
		{raw: "socks5h://proxy.example", defaultPort: "1080", want: "proxy.example:1080"},
		{raw: "https://[2001:db8::1]:8443", defaultPort: "443", want: "[2001:db8::1]:8443"},
	}
	for _, tt := range tests {
		parsed, errParse := url.Parse(tt.raw)
		if errParse != nil {
			t.Fatalf("url.Parse(%q) error = %v", tt.raw, errParse)
		}
		if got := proxyEndpointAddress(parsed, tt.defaultPort); got != tt.want {
			t.Fatalf("proxyEndpointAddress(%q) = %q, want %q", tt.raw, got, tt.want)
		}
	}
}
