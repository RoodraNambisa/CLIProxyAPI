package helps

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	tls "github.com/refraction-networking/utls"
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

func TestCodexNativeTLS12ClientHelloSpec(t *testing.T) {
	spec := codexNativeTLS12ClientHelloSpec()

	wantCiphers := []uint16{0x00ff, 0xc02c, 0xc02b, 0xc024, 0xc023, 0xc00a, 0xc009, 0xc008, 0xc030, 0xc02f, 0xc028, 0xc027, 0xc014, 0xc013, 0xc012, 0x009d, 0x009c, 0x003d, 0x003c, 0x0035, 0x002f, 0x000a}
	if !sameUint16s(spec.CipherSuites, wantCiphers) {
		t.Fatalf("CipherSuites = %#v, want %#v", spec.CipherSuites, wantCiphers)
	}
	if spec.TLSVersMin != tls.VersionTLS12 || spec.TLSVersMax != tls.VersionTLS12 {
		t.Fatalf("TLS versions = %x/%x, want TLS 1.2 only", spec.TLSVersMin, spec.TLSVersMax)
	}
	if !sameUint8s(spec.CompressionMethods, []uint8{0}) {
		t.Fatalf("CompressionMethods = %#v, want [0]", spec.CompressionMethods)
	}

	wantExtOrder := []any{
		&tls.SNIExtension{},
		&tls.SupportedCurvesExtension{},
		&tls.SupportedPointsExtension{},
		&tls.SignatureAlgorithmsExtension{},
		&tls.StatusRequestExtension{},
		&tls.SCTExtension{},
		&tls.ExtendedMasterSecretExtension{},
	}
	if len(spec.Extensions) != len(wantExtOrder) {
		t.Fatalf("extensions len = %d, want %d", len(spec.Extensions), len(wantExtOrder))
	}
	for i, want := range wantExtOrder {
		if typeName(spec.Extensions[i]) != typeName(want) {
			t.Fatalf("extension %d = %s, want %s", i, typeName(spec.Extensions[i]), typeName(want))
		}
	}

	curves := spec.Extensions[1].(*tls.SupportedCurvesExtension).Curves
	if !sameCurveIDs(curves, []tls.CurveID{tls.CurveP256, tls.CurveP384, tls.CurveP521}) {
		t.Fatalf("curves = %#v, want P-256/P-384/P-521", curves)
	}
	sigAlgs := spec.Extensions[3].(*tls.SignatureAlgorithmsExtension).SupportedSignatureAlgorithms
	wantSigAlgs := []tls.SignatureScheme{0x0401, 0x0201, 0x0501, 0x0601, 0x0403, 0x0203, 0x0503, 0x0603}
	if !sameSignatureSchemes(sigAlgs, wantSigAlgs) {
		t.Fatalf("signature algorithms = %#v, want %#v", sigAlgs, wantSigAlgs)
	}
}

func TestWriteOrderedHTTP1RequestCodexOrder(t *testing.T) {
	body := []byte(`{"model":"gpt-5-codex"}`)
	req, err := http.NewRequest(http.MethodPost, "https://chatgpt.com/backend-api/codex/responses", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	req.Header.Set("X-Codex-Beta-Features", "terminal_resize_reflow")
	req.Header.Set("X-Codex-Turn-Metadata", `{"turn_id":"turn-1"}`)
	req.Header.Set("X-Codex-Window-Id", "window-1")
	req.Header.Set("X-Client-Request-Id", "request-1")
	req.Header.Set("Session_id", "session-1")
	req.Header.Set("Thread-Id", "thread-1")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Authorization", "Bearer token")
	req.Header.Set("Chatgpt-Account-Id", "account-1")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Originator", "Codex Desktop")
	req.Header.Set("User-Agent", "Codex Desktop/0.136.0-alpha.2")
	req.Header.Set("Connection", "Keep-Alive")
	req.ContentLength = int64(len(body))

	var out bytes.Buffer
	if err := writeOrderedHTTP1Request(req, &out, body, codexHTTP1HeaderOrder, true); err != nil {
		t.Fatalf("writeOrderedHTTP1Request() error = %v", err)
	}

	gotHeader := strings.Split(out.String(), "\r\n\r\n")[0]
	gotLines := strings.Split(gotHeader, "\r\n")
	wantLines := []string{
		"POST /backend-api/codex/responses HTTP/1.1",
		"x-codex-beta-features: terminal_resize_reflow",
		"x-codex-turn-metadata: {\"turn_id\":\"turn-1\"}",
		"x-codex-window-id: window-1",
		"x-client-request-id: request-1",
		"session-id: session-1",
		"thread-id: thread-1",
		"accept: text/event-stream",
		"authorization: Bearer token",
		"chatgpt-account-id: account-1",
		"content-type: application/json",
		"originator: Codex Desktop",
		"user-agent: Codex Desktop/0.136.0-alpha.2",
		"host: chatgpt.com",
		"content-length: 23",
	}
	if strings.Join(gotLines, "\n") != strings.Join(wantLines, "\n") {
		t.Fatalf("header lines:\n%s\nwant:\n%s", strings.Join(gotLines, "\n"), strings.Join(wantLines, "\n"))
	}
	if strings.Contains(gotHeader, "Connection:") {
		t.Fatalf("Connection header should be omitted in Codex ordered HTTP request:\n%s", gotHeader)
	}
}

func TestReorderHTTP1HeaderBlockCodexWebsocketOrder(t *testing.T) {
	raw := []byte("GET /backend-api/codex/responses HTTP/1.1\r\n" +
		"User-Agent: Codex Desktop/0.136.0-alpha.2\r\n" +
		"Host: chatgpt.com\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: key\r\n" +
		"Sec-WebSocket-Version: 13\r\n" +
		"Authorization: Bearer token\r\n" +
		"Chatgpt-Account-Id: account-1\r\n" +
		"Originator: Codex Desktop\r\n" +
		"OpenAI-Beta: responses_websockets=2026-02-06\r\n" +
		"Version: 0.136.0-alpha.2\r\n" +
		"X-Codex-Beta-Features: terminal_resize_reflow\r\n" +
		"X-Codex-Turn-Metadata: {}\r\n" +
		"X-Client-Request-Id: request-1\r\n" +
		"session_id: session-1\r\n" +
		"Thread-Id: thread-1\r\n" +
		"X-Codex-Window-Id: window-1\r\n" +
		"Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits\r\n\r\n")

	rewritten, err := reorderHTTP1HeaderBlock(raw, codexWebsocketHeaderOrder)
	if err != nil {
		t.Fatalf("reorderHTTP1HeaderBlock() error = %v", err)
	}
	gotLines := strings.Split(strings.TrimSuffix(string(rewritten), "\r\n\r\n"), "\r\n")
	wantLines := []string{
		"GET /backend-api/codex/responses HTTP/1.1",
		"Host: chatgpt.com",
		"Connection: Upgrade",
		"Upgrade: websocket",
		"Sec-WebSocket-Version: 13",
		"Sec-WebSocket-Key: key",
		"chatgpt-account-id: account-1",
		"authorization: Bearer token",
		"user-agent: Codex Desktop/0.136.0-alpha.2",
		"originator: Codex Desktop",
		"openai-beta: responses_websockets=2026-02-06",
		"version: 0.136.0-alpha.2",
		"x-codex-beta-features: terminal_resize_reflow",
		"x-codex-turn-metadata: {}",
		"x-client-request-id: request-1",
		"session-id: session-1",
		"thread-id: thread-1",
		"x-codex-window-id: window-1",
		"sec-websocket-extensions: permessage-deflate; client_max_window_bits",
	}
	if strings.Join(gotLines, "\n") != strings.Join(wantLines, "\n") {
		t.Fatalf("websocket header lines:\n%s\nwant:\n%s", strings.Join(gotLines, "\n"), strings.Join(wantLines, "\n"))
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

func TestNewChromeUtlsHTTP1ClientUsesInjectedTransportWithoutExplicitProxy(t *testing.T) {
	wantTransport := &stubRoundTripper{}
	ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", http.RoundTripper(wantTransport))

	client := NewChromeUtlsHTTP1Client(ctx, &config.Config{}, &cliproxyauth.Auth{}, 0, "chrome_133")

	if client.Transport != wantTransport {
		t.Fatalf("transport = %T, want injected context RoundTripper", client.Transport)
	}
}

func TestNewChromeUtlsHTTP1ClientUsesHTTP1RoundTripper(t *testing.T) {
	client := NewChromeUtlsHTTP1Client(context.Background(), &config.Config{}, &cliproxyauth.Auth{}, 0, "chrome_133")

	fallback, ok := client.Transport.(*fallbackRoundTripper)
	if !ok {
		t.Fatalf("transport type = %T, want *fallbackRoundTripper", client.Transport)
	}
	if _, ok := fallback.utls.(*utlsHTTP1RoundTripper); !ok {
		t.Fatalf("utls transport type = %T, want *utlsHTTP1RoundTripper", fallback.utls)
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

func TestUtlsHTTP1RoundTripperUsesEnvironmentProxy(t *testing.T) {
	setEnvironmentProxy(t, "http://env-proxy.example.com:8080")
	req, err := http.NewRequest(http.MethodGet, "https://example.com", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	rt := newUtlsHTTP1RoundTripper("", "chrome_133", true)

	proxyURL, err := rt.proxyURLForRequest(req)
	if err != nil {
		t.Fatalf("proxyURLForRequest() error = %v", err)
	}
	if proxyURL != "http://env-proxy.example.com:8080" {
		t.Fatalf("proxyURL = %q, want env proxy", proxyURL)
	}
}

func TestChromeSpecWithALPNForcesHTTP1(t *testing.T) {
	spec, err := chromeSpecWithALPN(tls.HelloChrome_133, []string{"http/1.1"})
	if err != nil {
		t.Fatalf("chromeSpecWithALPN() error = %v", err)
	}

	foundALPN := false
	for _, ext := range spec.Extensions {
		switch ext := ext.(type) {
		case *tls.ALPNExtension:
			foundALPN = true
			if len(ext.AlpnProtocols) != 1 || ext.AlpnProtocols[0] != "http/1.1" {
				t.Fatalf("ALPN protocols = %#v, want [http/1.1]", ext.AlpnProtocols)
			}
		case *tls.ApplicationSettingsExtension, *tls.ApplicationSettingsExtensionNew:
			t.Fatalf("unexpected h2 application settings extension: %T", ext)
		}
	}
	if !foundALPN {
		t.Fatal("ALPN extension was not found")
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

func typeName(v any) string {
	if v == nil {
		return "<nil>"
	}
	return reflect.TypeOf(v).String()
}

func sameUint16s(a, b []uint16) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sameUint8s(a, b []uint8) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sameCurveIDs(a, b []tls.CurveID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sameSignatureSchemes(a, b []tls.SignatureScheme) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
