package helps

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/http/httpproxy"
)

var (
	proxyHTTPTransportCache        sync.Map // map[string]*cachedProxyTransport
	proxyHTTP1TransportCache       sync.Map // map[string]*cachedProxyTransport
	environmentProxyKeys           = []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"}
	environmentNoProxyKeys         = []string{"NO_PROXY", "no_proxy"}
	environmentProxyTransportCache sync.Map // map[string]*http.Transport
	environmentHTTP1TransportCache sync.Map // map[string]*http.Transport
	defaultHTTP1TransportOnce      sync.Once
	defaultHTTP1Transport          *http.Transport
)

type cachedProxyTransport struct {
	once      sync.Once
	transport *http.Transport
}

// NewProxyAwareHTTPClient creates an HTTP client with proper proxy configuration priority:
// 1. Use auth.ProxyURL if configured (highest priority)
// 2. Use cfg.ProxyURL if auth proxy is not configured
// 3. Use RoundTripper from context if no explicit proxy is configured
// 4. Use environment proxy settings if neither explicit nor injected transports are configured
//
// Parameters:
//   - ctx: The context containing optional RoundTripper
//   - cfg: The application configuration
//   - auth: The authentication information
//   - timeout: The client timeout (0 means no timeout)
//
// Returns:
//   - *http.Client: An HTTP client with configured proxy or transport
func NewProxyAwareHTTPClient(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth, timeout time.Duration) *http.Client {
	contextTransport := roundTripperFromContext(ctx)

	var proxyURL string
	if auth != nil {
		proxyURL = strings.TrimSpace(auth.ProxyURL)
	}
	if proxyURL == "" && cfg != nil {
		proxyURL = strings.TrimSpace(cfg.ProxyURL)
	}

	if proxyURL != "" {
		if transport := cachedTransportForProxyURL(proxyURL); transport != nil {
			return newProxyHTTPClient(transport, timeout)
		}
		log.Debugf("failed to setup proxy from URL: %s, falling back to injected/default transport", proxyURL)
	}

	if contextTransport != nil {
		return newProxyHTTPClient(contextTransport, timeout)
	}

	if environmentProxyConfigured() {
		return newProxyHTTPClient(newEnvironmentProxyTransport(), timeout)
	}

	return newProxyHTTPClient(nil, timeout)
}

// NewProxyAwareHTTP1Client creates a proxy-aware client with HTTP/2 disabled
// when the selected transport is a standard *http.Transport.
func NewProxyAwareHTTP1Client(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth, timeout time.Duration) *http.Client {
	contextTransport := roundTripperFromContext(ctx)

	var proxyURL string
	if auth != nil {
		proxyURL = strings.TrimSpace(auth.ProxyURL)
	}
	if proxyURL == "" && cfg != nil {
		proxyURL = strings.TrimSpace(cfg.ProxyURL)
	}

	if proxyURL != "" {
		if transport := cachedHTTP1TransportForProxyURL(proxyURL); transport != nil {
			return newProxyHTTPClient(transport, timeout)
		}
		log.Debugf("failed to setup proxy from URL: %s, falling back to injected/default transport", proxyURL)
	}

	if contextTransport != nil {
		if transport, ok := contextTransport.(*http.Transport); ok && transport != nil {
			clone := transport.Clone()
			disableHTTP2(clone)
			return newProxyHTTPClient(clone, timeout)
		}
		return newProxyHTTPClient(contextTransport, timeout)
	}

	if environmentProxyConfigured() {
		return newProxyHTTPClient(newEnvironmentProxyHTTP1Transport(), timeout)
	}

	return newProxyHTTPClient(newDefaultHTTP1Transport(), timeout)
}

func cloneDefaultHTTPTransport() *http.Transport {
	if transport, ok := http.DefaultTransport.(*http.Transport); ok && transport != nil {
		return transport.Clone()
	}
	return &http.Transport{}
}

func disableHTTP2(transport *http.Transport) {
	if transport == nil {
		return
	}
	transport.ForceAttemptHTTP2 = false
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	transport.Protocols = protocols
	transport.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	if transport.TLSClientConfig != nil {
		tlsConfig := transport.TLSClientConfig.Clone()
		tlsConfig.NextProtos = withoutHTTP2Proto(tlsConfig.NextProtos)
		transport.TLSClientConfig = tlsConfig
	}
}

func withoutHTTP2Proto(nextProtos []string) []string {
	if len(nextProtos) == 0 {
		return nextProtos
	}
	out := make([]string, 0, len(nextProtos))
	for _, proto := range nextProtos {
		if proto == "h2" {
			continue
		}
		out = append(out, proto)
	}
	if len(out) == 0 {
		return []string{"http/1.1"}
	}
	return out
}

func newDefaultHTTP1Transport() *http.Transport {
	defaultHTTP1TransportOnce.Do(func() {
		defaultHTTP1Transport = cloneDefaultHTTPTransport()
		disableHTTP2(defaultHTTP1Transport)
	})
	return defaultHTTP1Transport
}

func roundTripperFromContext(ctx context.Context) http.RoundTripper {
	if ctx == nil {
		return nil
	}
	rt, ok := ctx.Value("cliproxy.roundtripper").(http.RoundTripper)
	if !ok || rt == nil {
		return nil
	}
	return rt
}

func cachedTransportForProxyURL(proxyURL string) *http.Transport {
	proxyURL = strings.TrimSpace(proxyURL)
	if proxyURL == "" {
		return nil
	}
	entryAny, _ := proxyHTTPTransportCache.LoadOrStore(proxyURL, &cachedProxyTransport{})
	entry := entryAny.(*cachedProxyTransport)
	entry.once.Do(func() {
		entry.transport = buildProxyTransport(proxyURL)
	})
	return entry.transport
}

func cachedHTTP1TransportForProxyURL(proxyURL string) *http.Transport {
	proxyURL = strings.TrimSpace(proxyURL)
	if proxyURL == "" {
		return nil
	}
	entryAny, _ := proxyHTTP1TransportCache.LoadOrStore(proxyURL, &cachedProxyTransport{})
	entry := entryAny.(*cachedProxyTransport)
	entry.once.Do(func() {
		entry.transport = buildProxyTransport(proxyURL)
		disableHTTP2(entry.transport)
	})
	return entry.transport
}

func newProxyHTTPClient(transport http.RoundTripper, timeout time.Duration) *http.Client {
	client := &http.Client{Transport: transport}
	if timeout > 0 {
		client.Timeout = timeout
	}
	return client
}

// buildProxyTransport creates an HTTP transport configured for the given proxy URL.
// It supports SOCKS5, HTTP, and HTTPS proxy protocols.
//
// Parameters:
//   - proxyURL: The proxy URL string (e.g., "socks5://user:pass@host:port", "http://host:port")
//
// Returns:
//   - *http.Transport: A configured transport, or nil if the proxy URL is invalid
func buildProxyTransport(proxyURL string) *http.Transport {
	transport, _, errBuild := proxyutil.BuildHTTPTransport(proxyURL)
	if errBuild != nil {
		log.Errorf("%v", errBuild)
		return nil
	}
	return transport
}

func environmentProxyConfigured() bool {
	for _, key := range environmentProxyKeys {
		if strings.TrimSpace(os.Getenv(key)) != "" {
			return true
		}
	}
	return false
}

func newEnvironmentProxyTransport() *http.Transport {
	signature := environmentProxySignature()
	if cached, ok := environmentProxyTransportCache.Load(signature); ok {
		return cached.(*http.Transport)
	}

	proxyFunc := environmentProxyFunc()
	var transport *http.Transport
	if base, ok := http.DefaultTransport.(*http.Transport); ok && base != nil {
		clone := base.Clone()
		clone.Proxy = proxyFunc
		transport = clone
	} else {
		transport = &http.Transport{Proxy: proxyFunc}
	}
	actual, _ := environmentProxyTransportCache.LoadOrStore(signature, transport)
	return actual.(*http.Transport)
}

func newEnvironmentProxyHTTP1Transport() *http.Transport {
	signature := environmentProxySignature()
	if cached, ok := environmentHTTP1TransportCache.Load(signature); ok {
		return cached.(*http.Transport)
	}

	transport := newEnvironmentProxyTransport().Clone()
	disableHTTP2(transport)
	actual, _ := environmentHTTP1TransportCache.LoadOrStore(signature, transport)
	return actual.(*http.Transport)
}

func environmentProxySignature() string {
	values := make([]string, 0, len(environmentProxyKeys)+len(environmentNoProxyKeys))
	for _, key := range environmentProxyKeys {
		values = append(values, key+"="+strings.TrimSpace(os.Getenv(key)))
	}
	for _, key := range environmentNoProxyKeys {
		values = append(values, key+"="+strings.TrimSpace(os.Getenv(key)))
	}
	return strings.Join(values, "|")
}

func environmentProxyFunc() func(*http.Request) (*url.URL, error) {
	cfg := httpproxy.FromEnvironment()
	proxyFunc := cfg.ProxyFunc()
	return func(req *http.Request) (*url.URL, error) {
		if req == nil || req.URL == nil {
			return nil, nil
		}
		return proxyFunc(req.URL)
	}
}
