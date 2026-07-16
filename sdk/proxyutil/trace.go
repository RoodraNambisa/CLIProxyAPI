package proxyutil

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	// DefaultTraceEndpoint is the Cloudflare endpoint used to inspect outbound connections.
	DefaultTraceEndpoint = "https://cloudflare.com/cdn-cgi/trace"
	// DefaultTraceUserAgent is sent with trace requests.
	DefaultTraceUserAgent = "Mozilla/5.0"
	// DefaultTraceTimeout bounds trace connection establishment.
	DefaultTraceTimeout = 8 * time.Second

	traceResponseLimit = 64 * 1024
)

// TraceOptions customizes a trace check. Zero values use the package defaults.
type TraceOptions struct {
	Endpoint string
	Timeout  time.Duration
}

// TraceResult describes the outbound connection observed by the trace endpoint.
type TraceResult struct {
	OK       bool
	IP       string
	Location string
	HTTP     string
	TLS      string
	Colo     string
	Elapsed  time.Duration
	Error    string
	Message  string
}

// CheckTrace requests Cloudflare-compatible trace data using the supplied proxy setting.
// An empty setting inherits the default HTTP transport, direct bypasses proxies, and a
// concrete proxy URL uses that proxy explicitly.
func CheckTrace(ctx context.Context, proxyURL string, options ...TraceOptions) (result TraceResult) {
	if ctx == nil {
		ctx = context.Background()
	}
	started := time.Now()
	proxyURL = strings.TrimSpace(proxyURL)
	defer func() {
		result.Elapsed = time.Since(started)
		result.Message = maskTraceMessage(result.Message, proxyURL)
	}()

	transport, mode, errTransport := BuildHTTPTransport(proxyURL)
	if errTransport != nil || mode == ModeInvalid {
		result.Error = "invalid_proxy"
		result.Message = "invalid proxy configuration"
		return result
	}

	traceOptions := resolveTraceOptions(options)
	req, errRequest := http.NewRequestWithContext(ctx, http.MethodGet, traceOptions.Endpoint, nil)
	if errRequest != nil {
		result.Error = "request_create_failed"
		result.Message = errRequest.Error()
		return result
	}
	req.Header.Set("User-Agent", DefaultTraceUserAgent)

	transport = traceHTTPTransport(transport, traceOptions.Timeout)
	client := &http.Client{Transport: transport}
	defer transport.CloseIdleConnections()
	resp, errDo := client.Do(req)
	if errDo != nil {
		result.Error = "request_failed"
		result.Message = errDo.Error()
		return result
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			logrus.WithError(errClose).Debug("failed to close proxy trace response body")
		}
	}()

	body, errRead := io.ReadAll(io.LimitReader(resp.Body, traceResponseLimit))
	if errRead != nil {
		result.Error = "read_failed"
		result.Message = errRead.Error()
		return result
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		result.Error = "unexpected_status"
		result.Message = http.StatusText(resp.StatusCode)
		if result.Message == "" {
			result.Message = "unexpected HTTP status"
		}
		return result
	}

	trace := parseCloudflareTrace(string(body))
	result.IP = trace["ip"]
	result.Location = trace["loc"]
	result.HTTP = trace["http"]
	result.TLS = trace["tls"]
	result.Colo = trace["colo"]
	result.OK = result.IP != "" || result.Location != ""
	if !result.OK {
		result.Error = "invalid_trace"
		result.Message = "trace response missing ip and loc"
	}
	return result
}

func traceHTTPTransport(base *http.Transport, connectTimeout time.Duration) *http.Transport {
	if base == nil {
		base = cloneDefaultTransport()
	} else {
		base = base.Clone()
	}
	base.DisableKeepAlives = true
	if connectTimeout <= 0 {
		return base
	}
	dialContext := base.DialContext
	if dialContext == nil {
		dialer := &net.Dialer{}
		dialContext = dialer.DialContext
	}
	base.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if ctx == nil {
			ctx = context.Background()
		}
		connectCtx, cancelConnect := context.WithTimeout(ctx, connectTimeout)
		defer cancelConnect()
		return dialContext(connectCtx, network, address)
	}
	return base
}

func resolveTraceOptions(options []TraceOptions) TraceOptions {
	resolved := TraceOptions{
		Endpoint: DefaultTraceEndpoint,
		Timeout:  DefaultTraceTimeout,
	}
	for _, option := range options {
		if endpoint := strings.TrimSpace(option.Endpoint); endpoint != "" {
			resolved.Endpoint = endpoint
		}
		if option.Timeout > 0 {
			resolved.Timeout = option.Timeout
		}
	}
	return resolved
}

func parseCloudflareTrace(body string) map[string]string {
	trace := make(map[string]string)
	for _, line := range strings.Split(body, "\n") {
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		if key == "" {
			continue
		}
		trace[key] = strings.TrimSpace(parts[1])
	}
	return trace
}

func maskTraceMessage(message, proxyURL string) string {
	if message == "" || proxyURL == "" {
		return message
	}

	maskedProxyURL := MaskProxyURL(proxyURL)
	message = strings.ReplaceAll(message, proxyURL, maskedProxyURL)

	parsedURL, errParse := url.Parse(proxyURL)
	if errParse != nil || parsedURL.User == nil {
		return message
	}
	if _, hasPassword := parsedURL.User.Password(); !hasPassword {
		return message
	}

	normalizedProxyURL := parsedURL.String()
	message = strings.ReplaceAll(message, normalizedProxyURL, MaskProxyURL(normalizedProxyURL))
	maskedUserInfo := url.User(parsedURL.User.Username()).String() + ":********@"
	return strings.ReplaceAll(message, parsedURL.User.String()+"@", maskedUserInfo)
}
