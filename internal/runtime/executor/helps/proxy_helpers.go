package helps

import (
	"context"
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
	environmentProxyKeys           = []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"}
	environmentNoProxyKeys         = []string{"NO_PROXY", "no_proxy"}
	environmentProxyTransportCache sync.Map // map[string]*http.Transport
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
	var contextTransport http.RoundTripper
	if ctx != nil {
		if rt, ok := ctx.Value("cliproxy.roundtripper").(http.RoundTripper); ok && rt != nil {
			contextTransport = rt
		}
	}

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
