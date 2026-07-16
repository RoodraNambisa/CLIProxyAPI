package helps

import (
	"context"
	"crypto/sha256"
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
	proxyHTTPTransportCache        = newProxyTransportCache()
	proxyHTTP1TransportCache       = newProxyTransportCache()
	environmentProxyKeys           = []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"}
	environmentNoProxyKeys         = []string{"NO_PROXY", "no_proxy"}
	environmentProxyTransportCache = newEnvironmentTransportCache()
	environmentProxyHTTP1Cache     = newEnvironmentTransportCache()
	defaultHTTP1TransportOnce      sync.Once
	defaultHTTP1Transport          *http.Transport
)

type proxyTransportCache struct {
	mu         sync.Mutex
	generation uint64
	entries    map[string]proxyTransportCacheEntry
}

type proxyTransportCacheEntry struct {
	identity  string
	transport *http.Transport
}

type environmentTransportCache struct {
	mu        sync.Mutex
	signature string
	transport *http.Transport
}

type proxyConfigurationError struct {
	proxy string
}

func (e *proxyConfigurationError) Error() string {
	if e == nil || strings.TrimSpace(e.proxy) == "" {
		return "proxy configuration is unavailable"
	}
	return "proxy configuration is unavailable: " + e.proxy
}

func (*proxyConfigurationError) StatusCode() int                { return http.StatusServiceUnavailable }
func (*proxyConfigurationError) SkipAuthResult() bool           { return true }
func (*proxyConfigurationError) RetryOtherAuth() bool           { return true }
func (*proxyConfigurationError) ProxyInfrastructureError() bool { return true }

type proxyConfigurationErrorTransport struct {
	err error
}

func (t proxyConfigurationErrorTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, t.err
}

func newProxyTransportCache() *proxyTransportCache {
	return &proxyTransportCache{entries: make(map[string]proxyTransportCacheEntry)}
}

func newEnvironmentTransportCache() *environmentTransportCache {
	return &environmentTransportCache{}
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
		proxyURL = auth.EffectiveProxyURL()
	}
	if proxyURL == "" && cfg != nil {
		proxyURL = strings.TrimSpace(cfg.ProxyURL)
	}

	if proxyURL != "" {
		if transport := proxyHTTPTransportCache.transportFor(auth, proxyURL, buildProxyTransport); transport != nil {
			return newProxyHTTPClient(transport, timeout)
		}
		log.Errorf("failed to setup explicit proxy from URL: %s", proxyutil.MaskProxyURL(proxyURL))
		return newProxyConfigurationErrorClient(proxyURL, timeout)
	}

	if contextTransport != nil {
		return newProxyHTTPClient(contextTransport, timeout)
	}

	if environmentProxyConfigured() {
		return newProxyHTTPClient(newEnvironmentProxyTransport(), timeout)
	}

	return newProxyHTTPClient(nil, timeout)
}

// NewProxyAwareHTTP1Client creates an HTTP client with the same proxy priority
// as NewProxyAwareHTTPClient, but disables HTTP/2 on transports it controls.
func NewProxyAwareHTTP1Client(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth, timeout time.Duration) *http.Client {
	contextTransport := roundTripperFromContext(ctx)

	var proxyURL string
	if auth != nil {
		proxyURL = auth.EffectiveProxyURL()
	}
	if proxyURL == "" && cfg != nil {
		proxyURL = strings.TrimSpace(cfg.ProxyURL)
	}

	if proxyURL != "" {
		if transport := proxyHTTP1TransportCache.transportFor(auth, proxyURL, func(raw string) *http.Transport {
			if transport := buildProxyTransport(raw); transport != nil {
				return cloneTransportWithHTTP1(transport)
			}
			return nil
		}); transport != nil {
			return newProxyHTTPClient(transport, timeout)
		}
		log.Errorf("failed to setup explicit HTTP/1.1 proxy from URL: %s", proxyutil.MaskProxyURL(proxyURL))
		return newProxyConfigurationErrorClient(proxyURL, timeout)
	}

	if contextTransport != nil {
		return newProxyHTTPClient(contextTransport, timeout)
	}

	if environmentProxyConfigured() {
		return newProxyHTTPClient(newEnvironmentProxyHTTP1Transport(), timeout)
	}

	return newProxyHTTPClient(newDefaultHTTP1Transport(), timeout)
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

func (c *proxyTransportCache) transportFor(auth *cliproxyauth.Auth, proxyURL string, build func(string) *http.Transport) *http.Transport {
	proxyURL = strings.TrimSpace(proxyURL)
	if c == nil || proxyURL == "" || build == nil {
		return nil
	}
	cacheKey, identity := proxyTransportCacheIdentity(auth, proxyURL)
	for {
		c.mu.Lock()
		if entry, ok := c.entries[cacheKey]; ok && entry.identity == identity && entry.transport != nil {
			transport := entry.transport
			c.mu.Unlock()
			return transport
		}
		generation := c.generation
		c.mu.Unlock()

		built := build(proxyURL)
		if built == nil {
			return nil
		}
		c.mu.Lock()
		if c.generation != generation {
			built.DisableKeepAlives = true
			c.mu.Unlock()
			return built
		}
		if current, ok := c.entries[cacheKey]; ok && current.identity == identity && current.transport != nil {
			c.mu.Unlock()
			built.CloseIdleConnections()
			return current.transport
		}
		if previous, ok := c.entries[cacheKey]; ok && previous.transport != nil {
			previous.transport.CloseIdleConnections()
		}
		c.entries[cacheKey] = proxyTransportCacheEntry{identity: identity, transport: built}
		c.mu.Unlock()
		return built
	}
}

func (c *proxyTransportCache) closeMatching(match func(string) bool) {
	if c == nil || match == nil {
		return
	}
	c.mu.Lock()
	c.generation++
	toClose := make([]*http.Transport, 0)
	for key, entry := range c.entries {
		if !match(key) {
			continue
		}
		delete(c.entries, key)
		if entry.transport != nil {
			toClose = append(toClose, entry.transport)
		}
	}
	c.mu.Unlock()
	for _, transport := range toClose {
		transport.CloseIdleConnections()
	}
}

// CloseProxyTransportCachesForAuth releases idle transports scoped to one credential.
func CloseProxyTransportCachesForAuth(authID string) {
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return
	}
	key := "auth:" + authID
	match := func(candidate string) bool { return candidate == key }
	proxyHTTPTransportCache.closeMatching(match)
	proxyHTTP1TransportCache.closeMatching(match)
}

// CloseUnscopedProxyTransportCaches releases transports keyed only by proxy configuration.
func CloseUnscopedProxyTransportCaches() {
	match := func(candidate string) bool { return strings.HasPrefix(candidate, "proxy:") }
	proxyHTTPTransportCache.closeMatching(match)
	proxyHTTP1TransportCache.closeMatching(match)
	environmentProxyTransportCache.close()
	environmentProxyHTTP1Cache.close()
}

// CloseAllProxyTransportCaches releases all shared idle proxy transports.
func CloseAllProxyTransportCaches() {
	match := func(string) bool { return true }
	proxyHTTPTransportCache.closeMatching(match)
	proxyHTTP1TransportCache.closeMatching(match)
	environmentProxyTransportCache.close()
	environmentProxyHTTP1Cache.close()
}

func (c *environmentTransportCache) close() {
	if c == nil {
		return
	}
	c.mu.Lock()
	transport := c.transport
	c.signature = ""
	c.transport = nil
	c.mu.Unlock()
	if transport != nil {
		transport.CloseIdleConnections()
	}
}

func (c *environmentTransportCache) transportFor(signature string, build func() *http.Transport) *http.Transport {
	if c == nil || build == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.signature == signature && c.transport != nil {
		return c.transport
	}
	previous := c.transport
	transport := build()
	c.signature = signature
	c.transport = transport
	if previous != nil && previous != transport {
		previous.CloseIdleConnections()
	}
	return transport
}

func proxyTransportCacheIdentity(auth *cliproxyauth.Auth, proxyURL string) (string, string) {
	digest := sha256.Sum256([]byte(strings.TrimSpace(proxyURL)))
	proxyIdentity := string(digest[:])
	bindingID := ""
	authID := ""
	if auth != nil {
		bindingID = auth.EffectiveProxyBindingID()
		authID = strings.TrimSpace(auth.ID)
	}
	identity := bindingID + "\x00" + proxyIdentity
	if authID != "" && (bindingID != "" || strings.TrimSpace(auth.ProxyURL) != "") {
		return "auth:" + authID, identity
	}
	return "proxy:" + proxyIdentity, identity
}

func newProxyHTTPClient(transport http.RoundTripper, timeout time.Duration) *http.Client {
	client := &http.Client{Transport: transport}
	if timeout > 0 {
		client.Timeout = timeout
	}
	return client
}

func newProxyConfigurationErrorClient(proxyURL string, timeout time.Duration) *http.Client {
	errProxy := &proxyConfigurationError{proxy: proxyutil.MaskProxyURL(proxyURL)}
	return newProxyHTTPClient(proxyConfigurationErrorTransport{err: errProxy}, timeout)
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
	return environmentProxyTransportCache.transportFor(signature, func() *http.Transport {
		proxyFunc := environmentProxyFunc()
		if base, ok := http.DefaultTransport.(*http.Transport); ok && base != nil {
			clone := base.Clone()
			clone.Proxy = proxyFunc
			return clone
		}
		return &http.Transport{Proxy: proxyFunc}
	})
}

func newEnvironmentProxyHTTP1Transport() *http.Transport {
	signature := environmentProxySignature()
	return environmentProxyHTTP1Cache.transportFor(signature, func() *http.Transport {
		return cloneTransportWithHTTP1(newEnvironmentProxyTransport())
	})
}

func newDefaultHTTP1Transport() *http.Transport {
	defaultHTTP1TransportOnce.Do(func() {
		var base *http.Transport
		if transport, ok := http.DefaultTransport.(*http.Transport); ok && transport != nil {
			base = transport
		} else {
			base = &http.Transport{}
		}
		defaultHTTP1Transport = cloneTransportWithHTTP1(base)
	})
	return defaultHTTP1Transport
}

func cloneTransportWithHTTP1(base *http.Transport) *http.Transport {
	if base == nil {
		return nil
	}
	clone := base.Clone()
	clone.ForceAttemptHTTP2 = false
	clone.TLSNextProto = make(map[string]func(authority string, c *tls.Conn) http.RoundTripper)
	if clone.TLSClientConfig == nil {
		clone.TLSClientConfig = &tls.Config{}
	} else {
		clone.TLSClientConfig = clone.TLSClientConfig.Clone()
	}
	clone.TLSClientConfig.NextProtos = []string{"http/1.1"}
	return clone
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
