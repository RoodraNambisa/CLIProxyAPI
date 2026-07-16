package proxyutil

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"

	"golang.org/x/net/proxy"
)

// Mode describes how a proxy setting should be interpreted.
type Mode int

const maxProxyConnectHeaderBytes = 64 * 1024

const (
	// ModeInherit means no explicit proxy behavior was configured.
	ModeInherit Mode = iota
	// ModeDirect means outbound requests must bypass proxies explicitly.
	ModeDirect
	// ModeProxy means a concrete proxy URL was configured.
	ModeProxy
	// ModeInvalid means the proxy setting is present but malformed or unsupported.
	ModeInvalid
)

type proxyConnectionError struct {
	operation string
	cause     error
}

func (e *proxyConnectionError) Error() string {
	if e == nil || e.cause == nil {
		return "proxy connection failed"
	}
	if strings.TrimSpace(e.operation) == "" {
		return "proxy connection failed: " + e.cause.Error()
	}
	return "proxy " + e.operation + " failed: " + e.cause.Error()
}

func (e *proxyConnectionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// ProxyInfrastructureError identifies failures produced before an upstream
// connection exists, so proxy pools can retry without cooling credentials.
func (*proxyConnectionError) ProxyInfrastructureError() bool { return true }

// Setting is the normalized interpretation of a proxy configuration value.
type Setting struct {
	Raw  string
	Mode Mode
	URL  *url.URL
}

// ContextDialer establishes outbound connections while honoring cancellation.
type ContextDialer interface {
	Dial(network, addr string) (net.Conn, error)
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

func (d proxyContextDialerAdapter) Dial(network, addr string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, addr)
}

func (d proxyContextDialerAdapter) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if d.dialer == nil {
		return directContextDialer{}.DialContext(ctx, network, addr)
	}
	if contextDialer, ok := d.dialer.(proxy.ContextDialer); ok {
		conn, errDial := contextDialer.DialContext(ctx, network, addr)
		if errDial != nil {
			return nil, &proxyConnectionError{operation: "SOCKS dial", cause: errDial}
		}
		return conn, nil
	}
	return nil, fmt.Errorf("proxy dialer does not support context")
}

type httpConnectDialer struct {
	proxyURL  *url.URL
	dialer    ContextDialer
	tlsConfig *tls.Config
}

func (d *httpConnectDialer) Dial(network, addr string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, addr)
}

func (d *httpConnectDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if d == nil || d.proxyURL == nil {
		return nil, fmt.Errorf("http proxy dialer is not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	forwardDialer := d.dialer
	if forwardDialer == nil {
		forwardDialer = directContextDialer{}
	}
	proxyAddr := proxyEndpointAddress(d.proxyURL, "80")
	if strings.EqualFold(d.proxyURL.Scheme, "https") {
		proxyAddr = proxyEndpointAddress(d.proxyURL, "443")
	}
	conn, errDial := forwardDialer.DialContext(ctx, network, proxyAddr)
	if errDial != nil {
		return nil, &proxyConnectionError{operation: "dial", cause: errDial}
	}
	if strings.EqualFold(d.proxyURL.Scheme, "https") {
		tlsConfig := d.tlsConfig
		if tlsConfig == nil {
			tlsConfig = &tls.Config{}
		} else {
			tlsConfig = tlsConfig.Clone()
		}
		if strings.TrimSpace(tlsConfig.ServerName) == "" {
			tlsConfig.ServerName = d.proxyURL.Hostname()
		}
		if len(tlsConfig.NextProtos) == 0 {
			tlsConfig.NextProtos = []string{"http/1.1"}
		}
		tlsConn := tls.Client(conn, tlsConfig)
		if errHandshake := tlsConn.HandshakeContext(ctx); errHandshake != nil {
			_ = conn.Close()
			return nil, &proxyConnectionError{operation: "TLS handshake", cause: errHandshake}
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
	if errWrite := runConnOperationWithContext(ctx, conn, func() error { return req.Write(conn) }); errWrite != nil {
		_ = conn.Close()
		return nil, &proxyConnectionError{operation: "CONNECT write", cause: errWrite}
	}
	resp, reader, errRead := readHTTPConnectResponse(ctx, conn, req)
	if errRead != nil {
		_ = conn.Close()
		return nil, &proxyConnectionError{operation: "CONNECT response", cause: errRead}
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		_ = conn.Close()
		return nil, &proxyConnectionError{operation: "CONNECT", cause: errors.New(resp.Status)}
	}
	if errContext := ctx.Err(); errContext != nil {
		_ = conn.Close()
		return nil, errContext
	}
	return &bufferedProxyConn{Conn: conn, reader: reader}, nil
}

func proxyEndpointAddress(proxyURL *url.URL, defaultPort string) string {
	if proxyURL == nil {
		return ""
	}
	host := strings.TrimSpace(proxyURL.Hostname())
	port := strings.TrimSpace(proxyURL.Port())
	if port == "" {
		port = defaultPort
	}
	return net.JoinHostPort(host, port)
}

type bufferedProxyConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedProxyConn) Read(buffer []byte) (int, error) {
	if c != nil && c.reader != nil && c.reader.Buffered() > 0 {
		return c.reader.Read(buffer)
	}
	return c.Conn.Read(buffer)
}

func readHTTPConnectResponse(ctx context.Context, conn net.Conn, req *http.Request) (*http.Response, *bufio.Reader, error) {
	type readResult struct {
		resp   *http.Response
		reader *bufio.Reader
		err    error
	}
	resultCh := make(chan readResult, 1)
	go func() {
		reader := bufio.NewReader(conn)
		header, errHeader := readProxyConnectHeader(reader)
		if errHeader != nil {
			resultCh <- readResult{reader: reader, err: errHeader}
			return
		}
		resp, errRead := http.ReadResponse(bufio.NewReader(bytes.NewReader(header)), req)
		resultCh <- readResult{resp: resp, reader: reader, err: errRead}
	}()
	select {
	case <-ctx.Done():
		_ = conn.Close()
		return nil, nil, ctx.Err()
	case result := <-resultCh:
		return result.resp, result.reader, result.err
	}
}

func readProxyConnectHeader(reader *bufio.Reader) ([]byte, error) {
	if reader == nil {
		return nil, errors.New("proxy CONNECT reader is nil")
	}
	header := make([]byte, 0, 1024)
	for {
		fragment, errRead := reader.ReadSlice('\n')
		if len(header)+len(fragment) > maxProxyConnectHeaderBytes {
			return nil, errors.New("proxy CONNECT response headers exceed limit")
		}
		header = append(header, fragment...)
		if bytes.HasSuffix(header, []byte("\r\n\r\n")) || bytes.HasSuffix(header, []byte("\n\n")) {
			return header, nil
		}
		if errRead == nil || errors.Is(errRead, bufio.ErrBufferFull) {
			continue
		}
		return nil, errRead
	}
}

func runConnOperationWithContext(ctx context.Context, conn net.Conn, op func() error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	resultCh := make(chan error, 1)
	go func() { resultCh <- op() }()
	select {
	case <-ctx.Done():
		_ = conn.Close()
		return ctx.Err()
	case err := <-resultCh:
		return err
	}
}

// Parse normalizes a proxy configuration value into inherit, direct, or proxy modes.
func Parse(raw string) (Setting, error) {
	trimmed := strings.TrimSpace(raw)
	setting := Setting{Raw: trimmed}

	if trimmed == "" {
		setting.Mode = ModeInherit
		return setting, nil
	}

	if strings.EqualFold(trimmed, "direct") || strings.EqualFold(trimmed, "none") {
		setting.Mode = ModeDirect
		return setting, nil
	}

	parsedURL, errParse := url.Parse(trimmed)
	if errParse != nil {
		setting.Mode = ModeInvalid
		return setting, fmt.Errorf("parse proxy URL failed")
	}
	if parsedURL.Scheme == "" || parsedURL.Host == "" {
		setting.Mode = ModeInvalid
		return setting, fmt.Errorf("proxy URL missing scheme/host")
	}
	if strings.Count(parsedURL.Host, ":") > 1 && !strings.HasPrefix(parsedURL.Host, "[") {
		setting.Mode = ModeInvalid
		return setting, fmt.Errorf("IPv6 proxy host must be enclosed in brackets")
	}
	if port := parsedURL.Port(); port != "" {
		if _, errPort := parsePort(port); errPort != nil {
			setting.Mode = ModeInvalid
			return setting, fmt.Errorf("proxy URL contains an invalid port")
		}
	}

	switch parsedURL.Scheme {
	case "socks5", "socks5h", "http", "https":
		setting.Mode = ModeProxy
		setting.URL = parsedURL
		return setting, nil
	default:
		setting.Mode = ModeInvalid
		return setting, fmt.Errorf("unsupported proxy scheme: %s", parsedURL.Scheme)
	}
}

func cloneDefaultTransport() *http.Transport {
	if transport, ok := http.DefaultTransport.(*http.Transport); ok && transport != nil {
		return transport.Clone()
	}
	return &http.Transport{}
}

// NewDirectTransport returns a transport that bypasses environment proxies.
func NewDirectTransport() *http.Transport {
	clone := cloneDefaultTransport()
	clone.Proxy = nil
	return clone
}

// BuildHTTPTransport constructs an HTTP transport for the provided proxy setting.
func BuildHTTPTransport(raw string) (*http.Transport, Mode, error) {
	setting, errParse := Parse(raw)
	if errParse != nil {
		return nil, setting.Mode, errParse
	}

	switch setting.Mode {
	case ModeInherit:
		return nil, setting.Mode, nil
	case ModeDirect:
		return NewDirectTransport(), setting.Mode, nil
	case ModeProxy:
		if setting.URL.Scheme == "socks5" || setting.URL.Scheme == "socks5h" {
			var proxyAuth *proxy.Auth
			if setting.URL.User != nil {
				username := setting.URL.User.Username()
				password, _ := setting.URL.User.Password()
				proxyAuth = &proxy.Auth{User: username, Password: password}
			}
			dialer, errSOCKS5 := proxy.SOCKS5("tcp", setting.URL.Host, proxyAuth, proxy.Direct)
			if errSOCKS5 != nil {
				return nil, setting.Mode, fmt.Errorf("create SOCKS5 dialer failed: %w", errSOCKS5)
			}
			transport := cloneDefaultTransport()
			transport.Proxy = nil
			if contextDialer, ok := dialer.(proxy.ContextDialer); ok {
				transport.DialContext = contextDialer.DialContext
			} else {
				transport.DialContext = func(_ context.Context, network, addr string) (net.Conn, error) {
					return dialer.Dial(network, addr)
				}
			}
			return transport, setting.Mode, nil
		}
		transport := cloneDefaultTransport()
		transport.Proxy = http.ProxyURL(setting.URL)
		return transport, setting.Mode, nil
	default:
		return nil, setting.Mode, nil
	}
}

// BuildContextDialer constructs a connection-layer proxy dialer with context support.
func BuildContextDialer(raw string) (ContextDialer, Mode, error) {
	setting, errParse := Parse(raw)
	if errParse != nil {
		return nil, setting.Mode, errParse
	}

	switch setting.Mode {
	case ModeInherit:
		return nil, setting.Mode, nil
	case ModeDirect:
		return directContextDialer{}, setting.Mode, nil
	case ModeProxy:
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
			dialer, errSOCKS5 := proxy.SOCKS5("tcp", proxyEndpointAddress(setting.URL, "1080"), proxyAuth, directContextDialer{})
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

// BuildDialer constructs a proxy dialer for settings that operate at the connection layer.
func BuildDialer(raw string) (proxy.Dialer, Mode, error) {
	dialer, mode, errBuild := BuildContextDialer(raw)
	if errBuild != nil {
		return nil, mode, errBuild
	}
	return dialer, mode, nil
}
