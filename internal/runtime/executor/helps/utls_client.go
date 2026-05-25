package helps

import (
	"bufio"
	"context"
	stdtls "crypto/tls"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	tls "github.com/refraction-networking/utls"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/http2"
	"golang.org/x/net/proxy"
)

// utlsRoundTripper implements http.RoundTripper using a Chrome-like uTLS
// ClientHello.
type utlsRoundTripper struct {
	mu                  sync.Mutex
	connections         map[string]*http2.ClientConn
	pending             map[string]chan struct{}
	explicitProxyURL    string
	useEnvironmentProxy bool
	helloID             tls.ClientHelloID
}

func newUtlsRoundTripper(proxyURL string, profile string, useEnvironmentProxy bool) *utlsRoundTripper {
	return &utlsRoundTripper{
		connections:         make(map[string]*http2.ClientConn),
		pending:             make(map[string]chan struct{}),
		explicitProxyURL:    strings.TrimSpace(proxyURL),
		useEnvironmentProxy: useEnvironmentProxy,
		helloID:             chromeHelloID(profile),
	}
}

func (t *utlsRoundTripper) getOrCreateConnection(ctx context.Context, cacheKey, host, addr, proxyURL string) (*http2.ClientConn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		t.mu.Lock()
		if h2Conn, ok := t.connections[cacheKey]; ok && h2Conn.CanTakeNewRequest() {
			t.mu.Unlock()
			return h2Conn, nil
		}

		if done, ok := t.pending[cacheKey]; ok {
			t.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-done:
				continue
			}
		}

		done := make(chan struct{})
		t.pending[cacheKey] = done
		t.mu.Unlock()

		h2Conn, err := t.createConnection(ctx, host, addr, proxyURL)

		t.mu.Lock()
		delete(t.pending, cacheKey)
		if err == nil {
			t.connections[cacheKey] = h2Conn
		}
		close(done)
		t.mu.Unlock()

		if err != nil {
			return nil, err
		}
		return h2Conn, nil
	}
}

func (t *utlsRoundTripper) createConnection(ctx context.Context, host, addr, proxyURL string) (*http2.ClientConn, error) {
	dialer, err := buildUtlsContextDialerOrDirect(proxyURL)
	if err != nil {
		return nil, err
	}
	tlsConn, err := dialChromeUTLSWithDialer(ctx, dialer, "tcp", addr, host, t.helloID)
	if err != nil {
		return nil, err
	}

	tr := &http2.Transport{}
	h2Conn, err := tr.NewClientConn(tlsConn)
	if err != nil {
		tlsConn.Close()
		return nil, err
	}

	return h2Conn, nil
}

func (t *utlsRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	hostname := req.URL.Hostname()
	port := req.URL.Port()
	if port == "" {
		port = "443"
	}
	addr := net.JoinHostPort(hostname, port)
	proxyURL, err := t.proxyURLForRequest(req)
	if err != nil {
		return nil, err
	}
	cacheKey := utlsConnectionCacheKey(addr, proxyURL)

	h2Conn, err := t.getOrCreateConnection(req.Context(), cacheKey, hostname, addr, proxyURL)
	if err != nil {
		return nil, err
	}

	resp, err := h2Conn.RoundTrip(stripHTTP2ConnectionHeaders(req))
	if err != nil {
		t.mu.Lock()
		if cached, ok := t.connections[cacheKey]; ok && cached == h2Conn {
			delete(t.connections, cacheKey)
		}
		t.mu.Unlock()
		return nil, err
	}

	return resp, nil
}

func utlsConnectionCacheKey(addr, proxyURL string) string {
	return addr + "|" + proxyURL
}

func (t *utlsRoundTripper) proxyURLForRequest(req *http.Request) (string, error) {
	if t == nil || req == nil {
		return "", nil
	}
	if t.explicitProxyURL != "" {
		return t.explicitProxyURL, nil
	}
	if !t.useEnvironmentProxy {
		return "", nil
	}
	proxyURL, err := environmentProxyFunc()(req)
	if err != nil {
		return "", err
	}
	if proxyURL == nil {
		return "", nil
	}
	return proxyURL.String(), nil
}

func stripHTTP2ConnectionHeaders(req *http.Request) *http.Request {
	if req == nil || req.Header == nil {
		return req
	}
	cloned := req.Clone(req.Context())
	cloned.Header = req.Header.Clone()
	for _, token := range strings.Split(cloned.Header.Get("Connection"), ",") {
		if token = strings.TrimSpace(token); token != "" {
			cloned.Header.Del(token)
		}
	}
	for _, key := range []string{"Connection", "Keep-Alive", "Proxy-Connection", "Transfer-Encoding", "Upgrade"} {
		cloned.Header.Del(key)
	}
	return cloned
}

// anthropicHosts contains the hosts that should use utls Chrome TLS fingerprint.
var anthropicHosts = map[string]struct{}{
	"api.anthropic.com": {},
}

// fallbackRoundTripper uses uTLS for selected HTTPS requests and falls back to
// the standard transport for all other requests.
type fallbackRoundTripper struct {
	utls     *utlsRoundTripper
	fallback http.RoundTripper
	allHTTPS bool
}

func (f *fallbackRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme == "https" {
		if f.allHTTPS {
			return f.utls.RoundTrip(req)
		}
		if _, ok := anthropicHosts[strings.ToLower(req.URL.Hostname())]; ok {
			return f.utls.RoundTrip(req)
		}
	}
	return f.fallback.RoundTrip(req)
}

// NewUtlsHTTPClient creates an HTTP client using utls Chrome TLS fingerprint.
// Use this for Claude API requests to match real Claude Code's TLS behavior.
// Falls back to standard transport for non-HTTPS requests.
func NewUtlsHTTPClient(cfg *config.Config, auth *cliproxyauth.Auth, timeout time.Duration) *http.Client {
	proxyURL := explicitProxyURL(cfg, auth)
	utlsRT := newUtlsRoundTripper(proxyURL, "", proxyURL == "" && environmentProxyConfigured())
	standardTransport := proxyAwareFallbackTransport(nil, cfg, auth)

	client := &http.Client{
		Transport: &fallbackRoundTripper{
			utls:     utlsRT,
			fallback: standardTransport,
		},
	}
	if timeout > 0 {
		client.Timeout = timeout
	}
	return client
}

// NewChromeUtlsHTTPClient creates an HTTP client that uses a Chrome-like uTLS
// ClientHello for all HTTPS requests and falls back to the normal proxy-aware
// transport for non-HTTPS URLs.
func NewChromeUtlsHTTPClient(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth, timeout time.Duration, profile string) *http.Client {
	proxyURL := explicitProxyURL(cfg, auth)
	if proxyURL == "" {
		if transport := roundTripperFromContext(ctx); transport != nil {
			return newProxyHTTPClient(transport, timeout)
		}
	}
	client := &http.Client{
		Transport: &fallbackRoundTripper{
			utls:     newUtlsRoundTripper(proxyURL, profile, proxyURL == "" && environmentProxyConfigured()),
			fallback: proxyAwareFallbackTransport(ctx, cfg, auth),
			allHTTPS: true,
		},
	}
	if timeout > 0 {
		client.Timeout = timeout
	}
	return client
}

func explicitProxyURL(cfg *config.Config, auth *cliproxyauth.Auth) string {
	var proxyURL string
	if auth != nil {
		proxyURL = strings.TrimSpace(auth.ProxyURL)
	}
	if proxyURL == "" && cfg != nil {
		proxyURL = strings.TrimSpace(cfg.ProxyURL)
	}
	return proxyURL
}

func proxyAwareFallbackTransport(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth) http.RoundTripper {
	client := NewProxyAwareHTTPClient(ctx, cfg, auth, 0)
	if client != nil && client.Transport != nil {
		return client.Transport
	}
	return http.DefaultTransport
}

func chromeHelloID(profile string) tls.ClientHelloID {
	switch strings.ToLower(strings.TrimSpace(profile)) {
	case "chrome_120":
		return tls.HelloChrome_120
	case "chrome_120_pq":
		return tls.HelloChrome_120_PQ
	case "chrome_131":
		return tls.HelloChrome_131
	case "chrome_133":
		return tls.HelloChrome_133
	default:
		return tls.HelloChrome_Auto
	}
}

// DialChromeUTLSContext dials addr through the optional proxy URL and completes
// a Chrome-like uTLS handshake. It is used by transports that manage HTTP
// framing themselves, such as WebSocket handshakes.
func DialChromeUTLSContext(ctx context.Context, network, addr, serverName, proxyURL, profile string) (net.Conn, error) {
	proxyURL = strings.TrimSpace(proxyURL)
	if proxyURL == "" && environmentProxyConfigured() {
		envProxyURL, errEnvProxy := environmentProxyURLForHTTPSAddr(addr)
		if errEnvProxy != nil {
			return nil, errEnvProxy
		}
		proxyURL = envProxyURL
	}
	dialer, errBuild := buildUtlsContextDialerOrDirect(proxyURL)
	if errBuild != nil {
		return nil, errBuild
	}
	return dialChromeUTLSWithDialer(ctx, dialer, network, addr, serverName, chromeHelloID(profile))
}

// DialChromeUTLS is kept for call sites that do not have a request context.
func DialChromeUTLS(network, addr, serverName, proxyURL, profile string) (net.Conn, error) {
	return DialChromeUTLSContext(context.Background(), network, addr, serverName, proxyURL, profile)
}

type utlsContextDialer interface {
	DialContext(ctx context.Context, network, addr string) (net.Conn, error)
}

type directContextDialer struct {
	dialer net.Dialer
}

func (d directContextDialer) Dial(network, addr string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, addr)
}

func (d directContextDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	return d.dialer.DialContext(ctx, network, addr)
}

type proxyContextDialerAdapter struct {
	dialer proxy.Dialer
}

func (d proxyContextDialerAdapter) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if d.dialer == nil {
		return directContextDialer{}.DialContext(ctx, network, addr)
	}
	if contextDialer, ok := d.dialer.(proxy.ContextDialer); ok {
		return contextDialer.DialContext(ctx, network, addr)
	}
	return nil, fmt.Errorf("proxy dialer does not support context")
}

func dialChromeUTLSWithDialer(ctx context.Context, dialer utlsContextDialer, network, addr, serverName string, helloID tls.ClientHelloID) (net.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if dialer == nil {
		dialer = directContextDialer{}
	}
	conn, err := dialer.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{ServerName: serverName}
	tlsConn := tls.UClient(conn, tlsConfig, helloID)
	if errHandshake := tlsConn.HandshakeContext(ctx); errHandshake != nil {
		conn.Close()
		return nil, errHandshake
	}
	return tlsConn, nil
}

type httpConnectDialer struct {
	proxyURL *url.URL
	dialer   utlsContextDialer
}

func buildUtlsContextDialerOrDirect(raw string) (utlsContextDialer, error) {
	dialer, mode, errBuild := buildUtlsContextDialer(raw)
	if errBuild != nil {
		return nil, errBuild
	}
	if mode == proxyutil.ModeInherit || dialer == nil {
		return directContextDialer{}, nil
	}
	return dialer, nil
}

func buildUtlsContextDialer(raw string) (utlsContextDialer, proxyutil.Mode, error) {
	setting, errParse := proxyutil.Parse(raw)
	if errParse != nil {
		return nil, setting.Mode, errParse
	}
	switch setting.Mode {
	case proxyutil.ModeInherit:
		return nil, setting.Mode, nil
	case proxyutil.ModeDirect:
		return directContextDialer{}, setting.Mode, nil
	case proxyutil.ModeProxy:
		switch setting.URL.Scheme {
		case "http", "https":
			return &httpConnectDialer{proxyURL: setting.URL, dialer: directContextDialer{}}, setting.Mode, nil
		case "socks5", "socks5h":
			var proxyAuth *proxy.Auth
			if setting.URL.User != nil {
				username := setting.URL.User.Username()
				password, _ := setting.URL.User.Password()
				proxyAuth = &proxy.Auth{User: username, Password: password}
			}
			dialer, errSOCKS5 := proxy.SOCKS5("tcp", setting.URL.Host, proxyAuth, directContextDialer{})
			if errSOCKS5 != nil {
				return nil, setting.Mode, fmt.Errorf("create SOCKS5 dialer failed: %w", errSOCKS5)
			}
			return proxyContextDialerAdapter{dialer: dialer}, setting.Mode, nil
		default:
			return nil, setting.Mode, fmt.Errorf("unsupported proxy scheme: %s", setting.URL.Scheme)
		}
	default:
		return nil, setting.Mode, nil
	}
}

func environmentProxyURLForHTTPSAddr(addr string) (string, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", nil
	}
	req := &http.Request{URL: &url.URL{Scheme: "https", Host: addr}}
	proxyURL, err := environmentProxyFunc()(req)
	if err != nil {
		return "", err
	}
	if proxyURL == nil {
		return "", nil
	}
	return proxyURL.String(), nil
}

func (d *httpConnectDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if d == nil || d.proxyURL == nil {
		return nil, fmt.Errorf("http proxy dialer is not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if d.dialer == nil {
		d.dialer = directContextDialer{}
	}
	conn, err := d.dialer.DialContext(ctx, network, d.proxyURL.Host)
	if err != nil {
		return nil, err
	}
	if d.proxyURL.Scheme == "https" {
		tlsConn := stdtls.Client(conn, &stdtls.Config{ServerName: d.proxyURL.Hostname()})
		if errHandshake := tlsConn.HandshakeContext(ctx); errHandshake != nil {
			conn.Close()
			return nil, errHandshake
		}
		conn = tlsConn
	}

	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Opaque: addr},
		Host:   addr,
		Header: make(http.Header),
	}
	if d.proxyURL.User != nil {
		username := d.proxyURL.User.Username()
		password, _ := d.proxyURL.User.Password()
		encoded := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
		req.Header.Set("Proxy-Authorization", "Basic "+encoded)
	}
	if errWrite := writeHTTPConnectRequest(ctx, conn, req); errWrite != nil {
		conn.Close()
		return nil, errWrite
	}
	resp, errRead := readHTTPConnectResponse(ctx, conn, req)
	if errRead != nil {
		conn.Close()
		return nil, errRead
	}
	defer func() {
		if resp.Body != nil {
			if errClose := resp.Body.Close(); errClose != nil {
				log.Errorf("utls: close proxy CONNECT response body error: %v", errClose)
			}
		}
	}()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		conn.Close()
		return nil, fmt.Errorf("proxy CONNECT failed: %s", resp.Status)
	}
	return conn, nil
}

func writeHTTPConnectRequest(ctx context.Context, conn net.Conn, req *http.Request) error {
	return runConnOperationWithContext(ctx, conn, func() error {
		return req.Write(conn)
	})
}

func readHTTPConnectResponse(ctx context.Context, conn net.Conn, req *http.Request) (*http.Response, error) {
	type readResult struct {
		resp *http.Response
		err  error
	}
	resultCh := make(chan readResult, 1)
	go func() {
		resp, errRead := http.ReadResponse(bufio.NewReader(conn), req)
		resultCh <- readResult{resp: resp, err: errRead}
	}()
	select {
	case <-ctx.Done():
		conn.Close()
		return nil, ctx.Err()
	case result := <-resultCh:
		return result.resp, result.err
	}
}

func runConnOperationWithContext(ctx context.Context, conn net.Conn, op func() error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	resultCh := make(chan error, 1)
	go func() {
		resultCh <- op()
	}()
	select {
	case <-ctx.Done():
		conn.Close()
		return ctx.Err()
	case err := <-resultCh:
		return err
	}
}
