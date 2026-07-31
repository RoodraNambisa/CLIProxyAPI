package chatgptweb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
	log "github.com/sirupsen/logrus"
)

type loginProxySelector struct {
	config LoginProxyConfig
	source io.Reader
	fixed  string
}

type loginClientRetry struct {
	selector *loginProxySelector
	attempts int
	delay    time.Duration
}

func newLoginClient(persona Persona, cookies []Cookie, selector *loginProxySelector, timeout time.Duration) (*Client, error) {
	if selector == nil {
		return NewClient(persona, "", cookies)
	}
	proxyURL, errProxy := selector.next()
	if errProxy != nil {
		return nil, errProxy
	}
	client, errClient := newClient(persona, proxyURL, cookies, timeout)
	if errClient != nil {
		return nil, errClient
	}
	client.loginRetry = &loginClientRetry{
		selector: selector,
		attempts: max(1, selector.config.RequestAttempts),
		delay:    selector.config.RetryDelay,
	}
	return client, nil
}

func newLoginProxySelector(config LoginProxyConfig, source io.Reader) (*loginProxySelector, error) {
	if !config.Enabled {
		return nil, nil
	}
	config.URLTemplate = strings.TrimSpace(config.URLTemplate)
	config.PlaceholderCharset = strings.TrimSpace(config.PlaceholderCharset)
	if _, _, errValidate := proxyutil.ValidateURLTemplate(config.URLTemplate, "", config.PlaceholderCharset); errValidate != nil {
		return nil, fmt.Errorf("validate login proxy template: %w", errValidate)
	}
	if config.RequestAttempts < 1 {
		config.RequestAttempts = 1
	}
	if config.FlowAttempts < 1 {
		config.FlowAttempts = 1
	}
	if config.RetryDelay < 0 {
		config.RetryDelay = 0
	}
	selector := &loginProxySelector{config: config, source: source}
	if !config.RotateOnRetry {
		proxyURL, _, errExpand := proxyutil.ExpandURLTemplate(config.URLTemplate, config.PlaceholderCharset, source)
		if errExpand != nil {
			return nil, fmt.Errorf("expand login proxy template: %w", errExpand)
		}
		selector.fixed = proxyURL
	}
	return selector, nil
}

func (selector *loginProxySelector) next() (string, error) {
	if selector == nil {
		return "", nil
	}
	if selector.fixed != "" {
		return selector.fixed, nil
	}
	proxyURL, _, errExpand := proxyutil.ExpandURLTemplate(
		selector.config.URLTemplate,
		selector.config.PlaceholderCharset,
		selector.source,
	)
	if errExpand != nil {
		return "", fmt.Errorf("expand login proxy template: %w", errExpand)
	}
	return proxyURL, nil
}

type loginRequestError struct {
	stage      string
	status     int
	attempts   int
	cloudflare bool
	cause      error
}

func (requestError *loginRequestError) Error() string {
	if requestError == nil {
		return "login request failed"
	}
	if requestError.cloudflare {
		return "cloudflare challenge blocked login request"
	}
	return "login request failed"
}

func (requestError *loginRequestError) Unwrap() error {
	if requestError == nil {
		return nil
	}
	return requestError.cause
}

func loginRequestErrorDetails(err error) (*loginRequestError, bool) {
	var requestError *loginRequestError
	if !errors.As(err, &requestError) {
		return nil, false
	}
	return requestError, true
}

func waitLoginRetry(ctx context.Context, base time.Duration, retryNumber int) error {
	if retryNumber < 1 || base <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(time.Duration(retryNumber) * base)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func loginRequestStage(targetURL string) string {
	parsed, errParse := url.Parse(strings.TrimSpace(targetURL))
	if errParse != nil {
		return "authentication_request"
	}
	path := strings.ToLower(parsed.Path)
	switch {
	case strings.Contains(path, "/sentinel/"):
		return "sentinel"
	case strings.HasSuffix(path, "/authorize/continue"):
		return "authorize_continue"
	case strings.HasSuffix(path, "/password/verify"):
		return "password_verify"
	case strings.Contains(path, "/mfa/") || strings.Contains(path, "/otp/"):
		return "mfa_verify"
	case strings.HasSuffix(path, "/oauth/token"):
		return "token_exchange"
	case strings.Contains(path, "/authorize"):
		return "authorize"
	case strings.Contains(path, "/callback"):
		return "oauth_callback"
	default:
		return "oauth_redirect"
	}
}

func loginRequestLogFields(targetURL, stage string, status, attempt, attempts int) log.Fields {
	fields := log.Fields{
		"stage":    stage,
		"attempt":  attempt,
		"attempts": attempts,
	}
	if status > 0 {
		fields["status"] = status
	}
	parsed, errParse := url.Parse(strings.TrimSpace(targetURL))
	if errParse == nil {
		fields["host"] = parsed.Hostname()
		fields["path"] = parsed.EscapedPath()
	}
	return fields
}

func isCloudflareChallenge(response *fhttp.Response, payload []byte) bool {
	if response == nil {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(response.Header.Get("CF-Mitigated")), "challenge") {
		return true
	}
	statusCandidate := response.StatusCode == http.StatusForbidden ||
		response.StatusCode == http.StatusTooManyRequests ||
		response.StatusCode == http.StatusServiceUnavailable
	server := strings.ToLower(strings.TrimSpace(response.Header.Get("Server")))
	headerSignal := strings.TrimSpace(response.Header.Get("CF-Ray")) != "" ||
		strings.TrimSpace(response.Header.Get("CF-Cache-Status")) != "" ||
		strings.Contains(server, "cloudflare")
	pathSignal := false
	if response.Request != nil && response.Request.URL != nil {
		pathSignal = strings.Contains(strings.ToLower(response.Request.URL.Path), "/cdn-cgi/")
	}
	body := strings.ToLower(string(payload))
	bodySignal := strings.Contains(body, "/cdn-cgi/challenge-platform") ||
		strings.Contains(body, "cf-chl-") ||
		strings.Contains(body, "challenge-platform") ||
		strings.Contains(body, "just a moment") && strings.Contains(body, "cloudflare")
	contentType := strings.ToLower(response.Header.Get("Content-Type"))
	htmlResponse := strings.Contains(contentType, "text/html") || strings.Contains(body, "<html")
	if pathSignal {
		return true
	}
	if statusCandidate && (bodySignal || headerSignal && htmlResponse) {
		return true
	}
	if bodySignal && response.StatusCode >= 200 && response.StatusCode < 300 {
		return true
	}
	return statusCandidate && strings.Contains(contentType, "text/html") && strings.Contains(body, "cloudflare")
}
