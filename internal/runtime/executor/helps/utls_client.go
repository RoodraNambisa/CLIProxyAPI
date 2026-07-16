package helps

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	tls "github.com/refraction-networking/utls"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
	"golang.org/x/net/http2"
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
		var stale *http2.ClientConn
		t.mu.Lock()
		if h2Conn, ok := t.connections[cacheKey]; ok {
			if h2Conn.CanTakeNewRequest() {
				t.mu.Unlock()
				return h2Conn, nil
			}
			delete(t.connections, cacheKey)
			stale = h2Conn
		}

		if done, ok := t.pending[cacheKey]; ok {
			t.mu.Unlock()
			closeHTTP2ClientConn(stale)
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
		closeHTTP2ClientConn(stale)

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
		closeHTTP2ClientConn(h2Conn)
		return nil, err
	}

	return resp, nil
}

func closeHTTP2ClientConn(conn *http2.ClientConn) {
	if conn != nil {
		_ = conn.Close()
	}
}

// utlsHTTP1RoundTripper implements one-shot HTTPS requests using a Chrome-like
// uTLS ClientHello while forcing HTTP/1.1 ALPN.
type utlsHTTP1RoundTripper struct {
	explicitProxyURL    string
	useEnvironmentProxy bool
	helloID             tls.ClientHelloID
}

type codexNativeTLSHTTP1RoundTripper struct {
	explicitProxyURL    string
	useEnvironmentProxy bool
}

type orderedHeaderSpec struct {
	wireName string
	keys     []string
}

var codexHTTP1HeaderOrder = []orderedHeaderSpec{
	{wireName: "x-codex-beta-features", keys: []string{"x-codex-beta-features"}},
	{wireName: "x-codex-turn-metadata", keys: []string{"x-codex-turn-metadata"}},
	{wireName: "x-codex-window-id", keys: []string{"x-codex-window-id", "x-codex-window_id"}},
	{wireName: "x-client-request-id", keys: []string{"x-client-request-id"}},
	{wireName: "session-id", keys: []string{"session-id", "session_id"}},
	{wireName: "thread-id", keys: []string{"thread-id", "thread_id"}},
	{wireName: "accept", keys: []string{"accept"}},
	{wireName: "authorization", keys: []string{"authorization"}},
	{wireName: "chatgpt-account-id", keys: []string{"chatgpt-account-id"}},
	{wireName: "content-type", keys: []string{"content-type"}},
	{wireName: "originator", keys: []string{"originator"}},
	{wireName: "user-agent", keys: []string{"user-agent"}},
	{wireName: "cookie", keys: []string{"cookie"}},
	{wireName: "host", keys: []string{"host"}},
	{wireName: "content-length", keys: []string{"content-length"}},
}

var codexWebsocketHeaderOrder = []orderedHeaderSpec{
	{wireName: "Host", keys: []string{"host"}},
	{wireName: "Connection", keys: []string{"connection"}},
	{wireName: "Upgrade", keys: []string{"upgrade"}},
	{wireName: "Sec-WebSocket-Version", keys: []string{"sec-websocket-version"}},
	{wireName: "Sec-WebSocket-Key", keys: []string{"sec-websocket-key"}},
	{wireName: "chatgpt-account-id", keys: []string{"chatgpt-account-id"}},
	{wireName: "authorization", keys: []string{"authorization"}},
	{wireName: "user-agent", keys: []string{"user-agent"}},
	{wireName: "originator", keys: []string{"originator"}},
	{wireName: "openai-beta", keys: []string{"openai-beta"}},
	{wireName: "version", keys: []string{"version"}},
	{wireName: "x-codex-beta-features", keys: []string{"x-codex-beta-features"}},
	{wireName: "x-codex-turn-metadata", keys: []string{"x-codex-turn-metadata"}},
	{wireName: "x-client-request-id", keys: []string{"x-client-request-id"}},
	{wireName: "session-id", keys: []string{"session-id", "session_id"}},
	{wireName: "thread-id", keys: []string{"thread-id", "thread_id"}},
	{wireName: "x-codex-window-id", keys: []string{"x-codex-window-id", "x-codex-window_id"}},
	{wireName: "sec-websocket-extensions", keys: []string{"sec-websocket-extensions"}},
}

func newUtlsHTTP1RoundTripper(proxyURL string, profile string, useEnvironmentProxy bool) *utlsHTTP1RoundTripper {
	return &utlsHTTP1RoundTripper{
		explicitProxyURL:    strings.TrimSpace(proxyURL),
		useEnvironmentProxy: useEnvironmentProxy,
		helloID:             chromeHelloID(profile),
	}
}

func newCodexNativeTLSHTTP1RoundTripper(proxyURL string, useEnvironmentProxy bool) *codexNativeTLSHTTP1RoundTripper {
	return &codexNativeTLSHTTP1RoundTripper{
		explicitProxyURL:    strings.TrimSpace(proxyURL),
		useEnvironmentProxy: useEnvironmentProxy,
	}
}

func (t *utlsHTTP1RoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil || req.URL == nil {
		return nil, fmt.Errorf("utls http1: request is nil")
	}
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
	dialer, err := buildUtlsContextDialerOrDirect(proxyURL)
	if err != nil {
		return nil, err
	}
	conn, err := dialChromeUTLSWithDialerNextProtos(req.Context(), dialer, "tcp", addr, hostname, t.helloID, []string{"http/1.1"})
	if err != nil {
		return nil, err
	}

	outReq := cloneRequestForHTTP1(req)
	if errWrite := runConnOperationWithContext(req.Context(), conn, func() error {
		return outReq.Write(conn)
	}); errWrite != nil {
		conn.Close()
		return nil, errWrite
	}
	resp, errRead := readHTTPResponse(req.Context(), conn, outReq)
	if errRead != nil {
		conn.Close()
		return nil, errRead
	}
	resp.Body = newConnBoundReadCloser(req.Context(), resp.Body, conn)
	return resp, nil
}

func (t *codexNativeTLSHTTP1RoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil || req.URL == nil {
		return nil, fmt.Errorf("codex native tls http1: request is nil")
	}
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
	dialer, err := buildUtlsContextDialerOrDirect(proxyURL)
	if err != nil {
		return nil, err
	}
	conn, err := dialCodexNativeTLSWithDialer(req.Context(), dialer, "tcp", addr, hostname)
	if err != nil {
		return nil, err
	}

	outReq := cloneRequestForHTTP1(req)
	body, err := readAndCloseRequestBody(outReq)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if errWrite := runConnOperationWithContext(req.Context(), conn, func() error {
		return writeOrderedHTTP1Request(outReq, conn, body, codexHTTP1HeaderOrder, true)
	}); errWrite != nil {
		conn.Close()
		return nil, errWrite
	}
	resp, errRead := readHTTPResponse(req.Context(), conn, outReq)
	if errRead != nil {
		conn.Close()
		return nil, errRead
	}
	resp.Body = newConnBoundReadCloser(req.Context(), resp.Body, conn)
	return resp, nil
}

func (t *utlsHTTP1RoundTripper) proxyURLForRequest(req *http.Request) (string, error) {
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

func (t *codexNativeTLSHTTP1RoundTripper) proxyURLForRequest(req *http.Request) (string, error) {
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

func cloneRequestForHTTP1(req *http.Request) *http.Request {
	cloned := req.Clone(req.Context())
	cloned.Header = req.Header.Clone()
	cloned.RequestURI = ""
	cloned.Proto = "HTTP/1.1"
	cloned.ProtoMajor = 1
	cloned.ProtoMinor = 1
	return cloned
}

func readAndCloseRequestBody(req *http.Request) ([]byte, error) {
	if req == nil || req.Body == nil || req.Body == http.NoBody {
		if req != nil {
			req.Body = http.NoBody
			req.ContentLength = 0
		}
		return nil, nil
	}
	body, err := io.ReadAll(req.Body)
	if errClose := req.Body.Close(); err == nil {
		err = errClose
	}
	if err != nil {
		return nil, err
	}
	req.Body = io.NopCloser(bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	return body, nil
}

func writeOrderedHTTP1Request(req *http.Request, w io.Writer, body []byte, order []orderedHeaderSpec, includeBody bool) error {
	if req == nil || req.URL == nil {
		return fmt.Errorf("ordered http1: request is nil")
	}
	method := req.Method
	if method == "" {
		method = http.MethodGet
	}
	if _, err := fmt.Fprintf(w, "%s %s HTTP/1.1\r\n", method, req.URL.RequestURI()); err != nil {
		return err
	}

	written := make(map[string]bool, len(order))
	for _, spec := range order {
		values, ok := orderedHeaderValues(req, body, spec)
		if !ok {
			continue
		}
		for _, value := range values {
			if err := writeHeaderLine(w, spec.wireName, value); err != nil {
				return err
			}
		}
		for _, key := range spec.keys {
			written[strings.ToLower(key)] = true
		}
		written[strings.ToLower(spec.wireName)] = true
	}

	for _, key := range sortedRemainingHeaderKeys(req.Header, written) {
		if skipOrderedHTTP1RemainderHeader(key) {
			continue
		}
		for _, value := range req.Header.Values(key) {
			if strings.TrimSpace(value) == "" {
				continue
			}
			if err := writeHeaderLine(w, key, value); err != nil {
				return err
			}
		}
	}

	if _, err := io.WriteString(w, "\r\n"); err != nil {
		return err
	}
	if includeBody && len(body) > 0 {
		_, err := w.Write(body)
		return err
	}
	return nil
}

func orderedHeaderValues(req *http.Request, body []byte, spec orderedHeaderSpec) ([]string, bool) {
	switch strings.ToLower(spec.wireName) {
	case "host":
		host := req.Host
		if strings.TrimSpace(host) == "" && req.URL != nil {
			host = req.URL.Host
		}
		if strings.TrimSpace(host) == "" {
			return nil, false
		}
		return []string{host}, true
	case "content-length":
		if body == nil && req.ContentLength < 0 {
			return nil, false
		}
		length := len(body)
		if body == nil {
			length = int(req.ContentLength)
		}
		return []string{fmt.Sprintf("%d", length)}, true
	default:
		values := headerValuesForOrderedKeys(req.Header, spec.keys)
		if len(values) == 0 {
			return nil, false
		}
		return values, true
	}
}

func headerValuesForOrderedKeys(headers http.Header, keys []string) []string {
	for _, wanted := range keys {
		for key, values := range headers {
			if !strings.EqualFold(key, wanted) {
				continue
			}
			out := make([]string, 0, len(values))
			for _, value := range values {
				if strings.TrimSpace(value) != "" {
					out = append(out, value)
				}
			}
			return out
		}
	}
	return nil
}

func sortedRemainingHeaderKeys(headers http.Header, written map[string]bool) []string {
	keys := make([]string, 0, len(headers))
	for key := range headers {
		if written[strings.ToLower(key)] {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func skipOrderedHTTP1RemainderHeader(key string) bool {
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade":
		return true
	default:
		return false
	}
}

func writeHeaderLine(w io.Writer, key string, value string) error {
	key = strings.TrimSpace(key)
	value = sanitizeHeaderValue(value)
	if key == "" || value == "" {
		return nil
	}
	_, err := fmt.Fprintf(w, "%s: %s\r\n", key, value)
	return err
}

func sanitizeHeaderValue(value string) string {
	value = strings.ReplaceAll(value, "\r", " ")
	value = strings.ReplaceAll(value, "\n", " ")
	return strings.TrimSpace(value)
}

func readHTTPResponse(ctx context.Context, conn net.Conn, req *http.Request) (*http.Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
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

type connBoundReadCloser struct {
	ctx    context.Context
	body   io.ReadCloser
	conn   net.Conn
	done   chan struct{}
	once   sync.Once
	cancel sync.Once
}

func newConnBoundReadCloser(ctx context.Context, body io.ReadCloser, conn net.Conn) io.ReadCloser {
	if body == nil {
		body = http.NoBody
	}
	wrapped := &connBoundReadCloser{
		ctx:  ctx,
		body: body,
		conn: conn,
		done: make(chan struct{}),
	}
	if ctx != nil && ctx.Done() != nil {
		go wrapped.closeConnOnContextDone()
	}
	return wrapped
}

func (c *connBoundReadCloser) Read(p []byte) (int, error) {
	return c.body.Read(p)
}

func (c *connBoundReadCloser) Close() error {
	var err error
	c.once.Do(func() {
		close(c.done)
		err = c.body.Close()
		if errClose := c.conn.Close(); err == nil {
			err = errClose
		}
	})
	return err
}

func (c *connBoundReadCloser) closeConnOnContextDone() {
	select {
	case <-c.ctx.Done():
		c.cancel.Do(func() {
			c.conn.Close()
		})
	case <-c.done:
	}
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
	utls     http.RoundTripper
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

// NewChromeUtlsHTTP1Client creates an HTTP client that uses a Chrome-like uTLS
// ClientHello for all HTTPS requests while forcing HTTP/1.1 framing.
func NewChromeUtlsHTTP1Client(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth, timeout time.Duration, profile string) *http.Client {
	proxyURL := explicitProxyURL(cfg, auth)
	if proxyURL == "" {
		if transport := roundTripperFromContext(ctx); transport != nil {
			return newProxyHTTPClient(transport, timeout)
		}
	}
	client := &http.Client{
		Transport: &fallbackRoundTripper{
			utls:     newUtlsHTTP1RoundTripper(proxyURL, profile, proxyURL == "" && environmentProxyConfigured()),
			fallback: proxyAwareFallbackTransport(ctx, cfg, auth),
			allHTTPS: true,
		},
	}
	if timeout > 0 {
		client.Timeout = timeout
	}
	return client
}

// NewCodexNativeTLSHTTP1Client creates an HTTP client that uses the native
// Codex TLS 1.2 fingerprint and writes HTTP/1.1 headers in Codex order.
func NewCodexNativeTLSHTTP1Client(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth, timeout time.Duration) *http.Client {
	proxyURL := explicitProxyURL(cfg, auth)
	if proxyURL == "" {
		if transport := roundTripperFromContext(ctx); transport != nil {
			return newProxyHTTPClient(transport, timeout)
		}
	}
	client := &http.Client{
		Transport: &fallbackRoundTripper{
			utls:     newCodexNativeTLSHTTP1RoundTripper(proxyURL, proxyURL == "" && environmentProxyConfigured()),
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
		proxyURL = auth.EffectiveProxyURL()
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

// DialCodexNativeTLSContext dials addr through the optional proxy URL and
// completes a Codex-native TLS 1.2 handshake without ALPN.
func DialCodexNativeTLSContext(ctx context.Context, network, addr, serverName, proxyURL string) (net.Conn, error) {
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
	return dialCodexNativeTLSWithDialer(ctx, dialer, network, addr, serverName)
}

// WrapCodexWebsocketHeaderOrder rewrites the initial HTTP/1.1 websocket
// handshake headers into the observed Codex order.
func WrapCodexWebsocketHeaderOrder(conn net.Conn) net.Conn {
	if conn == nil {
		return nil
	}
	return &orderedWebsocketHandshakeConn{Conn: conn}
}

// DialChromeUTLS is kept for call sites that do not have a request context.
func DialChromeUTLS(network, addr, serverName, proxyURL, profile string) (net.Conn, error) {
	return DialChromeUTLSContext(context.Background(), network, addr, serverName, proxyURL, profile)
}

type orderedWebsocketHandshakeConn struct {
	net.Conn
	mu        sync.Mutex
	buffer    bytes.Buffer
	rewritten bool
}

func (c *orderedWebsocketHandshakeConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rewritten {
		n, err := c.Conn.Write(p)
		return n, err
	}
	c.buffer.Write(p)
	buffered := c.buffer.Bytes()
	headerEnd := bytes.Index(buffered, []byte("\r\n\r\n"))
	if headerEnd < 0 {
		return len(p), nil
	}

	rawHeader := append([]byte(nil), buffered[:headerEnd+4]...)
	rest := append([]byte(nil), buffered[headerEnd+4:]...)
	rewritten, err := reorderHTTP1HeaderBlock(rawHeader, codexWebsocketHeaderOrder)
	if err != nil {
		return 0, err
	}
	payload := append(rewritten, rest...)
	if _, errWrite := c.Conn.Write(payload); errWrite != nil {
		return 0, errWrite
	}
	c.rewritten = true
	c.buffer.Reset()
	return len(p), nil
}

type parsedHeaderLine struct {
	name  string
	value string
}

func reorderHTTP1HeaderBlock(raw []byte, order []orderedHeaderSpec) ([]byte, error) {
	raw = bytes.TrimSuffix(raw, []byte("\r\n\r\n"))
	lines := bytes.Split(raw, []byte("\r\n"))
	if len(lines) == 0 || len(lines[0]) == 0 {
		return nil, fmt.Errorf("ordered websocket: empty request header")
	}

	headerLines := make([]parsedHeaderLine, 0, len(lines)-1)
	for _, line := range lines[1:] {
		if len(line) == 0 {
			continue
		}
		idx := bytes.IndexByte(line, ':')
		if idx <= 0 {
			return nil, fmt.Errorf("ordered websocket: malformed header line")
		}
		headerLines = append(headerLines, parsedHeaderLine{
			name:  string(line[:idx]),
			value: sanitizeHeaderValue(string(line[idx+1:])),
		})
	}

	var out bytes.Buffer
	out.Write(lines[0])
	out.WriteString("\r\n")
	written := make([]bool, len(headerLines))
	for _, spec := range order {
		for i, line := range headerLines {
			if written[i] || !orderedSpecMatchesHeader(spec, line.name) {
				continue
			}
			if err := writeHeaderLine(&out, spec.wireName, line.value); err != nil {
				return nil, err
			}
			written[i] = true
		}
	}
	for i, line := range headerLines {
		if written[i] {
			continue
		}
		if err := writeHeaderLine(&out, line.name, line.value); err != nil {
			return nil, err
		}
	}
	out.WriteString("\r\n")
	return out.Bytes(), nil
}

func orderedSpecMatchesHeader(spec orderedHeaderSpec, name string) bool {
	if strings.EqualFold(spec.wireName, name) {
		return true
	}
	for _, key := range spec.keys {
		if strings.EqualFold(key, name) {
			return true
		}
	}
	return false
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

func dialChromeUTLSWithDialer(ctx context.Context, dialer utlsContextDialer, network, addr, serverName string, helloID tls.ClientHelloID) (net.Conn, error) {
	return dialChromeUTLSWithDialerNextProtos(ctx, dialer, network, addr, serverName, helloID, nil)
}

func dialCodexNativeTLSWithDialer(ctx context.Context, dialer utlsContextDialer, network, addr, serverName string) (net.Conn, error) {
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

	tlsConn := tls.UClient(conn, &tls.Config{ServerName: serverName}, tls.HelloCustom)
	spec := codexNativeTLS12ClientHelloSpec()
	if errApply := tlsConn.ApplyPreset(&spec); errApply != nil {
		conn.Close()
		return nil, errApply
	}
	tlsConn.SetSNI(serverName)
	if errHandshake := tlsConn.HandshakeContext(ctx); errHandshake != nil {
		conn.Close()
		return nil, errHandshake
	}
	return tlsConn, nil
}

func dialChromeUTLSWithDialerNextProtos(ctx context.Context, dialer utlsContextDialer, network, addr, serverName string, helloID tls.ClientHelloID, nextProtos []string) (net.Conn, error) {
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
	clientHelloID := helloID
	if len(nextProtos) > 0 {
		tlsConfig.NextProtos = append([]string(nil), nextProtos...)
		clientHelloID = tls.HelloCustom
	}
	tlsConn := tls.UClient(conn, tlsConfig, clientHelloID)
	if len(nextProtos) > 0 {
		spec, errSpec := chromeSpecWithALPN(helloID, nextProtos)
		if errSpec != nil {
			conn.Close()
			return nil, errSpec
		}
		if errApply := tlsConn.ApplyPreset(&spec); errApply != nil {
			conn.Close()
			return nil, errApply
		}
	}
	if errHandshake := tlsConn.HandshakeContext(ctx); errHandshake != nil {
		conn.Close()
		return nil, errHandshake
	}
	return tlsConn, nil
}

func chromeSpecWithALPN(helloID tls.ClientHelloID, nextProtos []string) (tls.ClientHelloSpec, error) {
	spec, err := tls.UTLSIdToSpec(helloID)
	if err != nil {
		return tls.ClientHelloSpec{}, err
	}
	nextProtos = append([]string(nil), nextProtos...)
	extensions := make([]tls.TLSExtension, 0, len(spec.Extensions)+1)
	foundALPN := false
	for _, ext := range spec.Extensions {
		switch ext := ext.(type) {
		case *tls.ALPNExtension:
			foundALPN = true
			extensions = append(extensions, &tls.ALPNExtension{AlpnProtocols: nextProtos})
		case *tls.ApplicationSettingsExtension, *tls.ApplicationSettingsExtensionNew:
			continue
		default:
			extensions = append(extensions, ext)
		}
	}
	if !foundALPN {
		extensions = append(extensions, &tls.ALPNExtension{AlpnProtocols: nextProtos})
	}
	spec.Extensions = extensions
	return spec, nil
}

func codexNativeTLS12ClientHelloSpec() tls.ClientHelloSpec {
	return tls.ClientHelloSpec{
		TLSVersMin: tls.VersionTLS12,
		TLSVersMax: tls.VersionTLS12,
		CipherSuites: []uint16{
			tls.FAKE_TLS_EMPTY_RENEGOTIATION_INFO_SCSV,
			0xc02c, 0xc02b, 0xc024, 0xc023, 0xc00a, 0xc009, 0xc008,
			0xc030, 0xc02f, 0xc028, 0xc027, 0xc014, 0xc013, 0xc012,
			0x009d, 0x009c, 0x003d, 0x003c, 0x0035, 0x002f, 0x000a,
		},
		CompressionMethods: []uint8{0},
		Extensions: []tls.TLSExtension{
			&tls.SNIExtension{},
			&tls.SupportedCurvesExtension{Curves: []tls.CurveID{
				tls.CurveP256,
				tls.CurveP384,
				tls.CurveP521,
			}},
			&tls.SupportedPointsExtension{SupportedPoints: []uint8{0}},
			&tls.SignatureAlgorithmsExtension{SupportedSignatureAlgorithms: []tls.SignatureScheme{
				tls.SignatureScheme(0x0401),
				tls.SignatureScheme(0x0201),
				tls.SignatureScheme(0x0501),
				tls.SignatureScheme(0x0601),
				tls.SignatureScheme(0x0403),
				tls.SignatureScheme(0x0203),
				tls.SignatureScheme(0x0503),
				tls.SignatureScheme(0x0603),
			}},
			&tls.StatusRequestExtension{},
			&tls.SCTExtension{},
			&tls.ExtendedMasterSecretExtension{},
		},
	}
}

func buildUtlsContextDialerOrDirect(raw string) (utlsContextDialer, error) {
	dialer, mode, errBuild := proxyutil.BuildContextDialer(raw)
	if errBuild != nil {
		return nil, errBuild
	}
	if mode == proxyutil.ModeInherit || dialer == nil {
		return directContextDialer{}, nil
	}
	return dialer, nil
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
