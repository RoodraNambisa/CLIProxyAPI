package chatgptweb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	fhttpcookiejar "github.com/bogdanfinn/fhttp/cookiejar"
	tls_client "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
	log "github.com/sirupsen/logrus"
)

type Client struct {
	follow             tls_client.HttpClient
	noRedirect         tls_client.HttpClient
	jar                tls_client.CookieJar
	sendSessionCookies bool
	persona            Persona
	proxyURL           string
	acquisitionTimeout time.Duration
	acquisitionTracker *acquisitionConnectionTracker
	loginRetry         *loginClientRetry
	beforeRequestMu    sync.RWMutex
	beforeRequest      func()
	beforeRequestOnce  sync.Once
}

// accessTokenCookieJar retains all server-issued cookies but withholds browser
// session cookies from access-token-authenticated requests.
type accessTokenCookieJar struct {
	delegate tls_client.CookieJar
}

func NewClient(persona Persona, proxyURL string, cookies []Cookie) (*Client, error) {
	return newClient(persona, proxyURL, cookies, 0)
}

func NewAcquisitionClient(persona Persona, proxyURL string, cookies []Cookie, timeout time.Duration) (*Client, error) {
	if timeout <= 0 {
		timeout = DefaultAcquisitionTimeout
	}
	return newClient(persona, proxyURL, cookies, timeout)
}

// NewAccessTokenClient creates a runtime client that authenticates with an
// access token without sending the persisted browser session cookie.
func NewAccessTokenClient(persona Persona, proxyURL string, cookies []Cookie) (*Client, error) {
	return newClientWithSessionCookiePolicy(persona, proxyURL, cookies, 0, false)
}

// NewAccessTokenAcquisitionClient is the bounded-acquisition variant of
// NewAccessTokenClient.
func NewAccessTokenAcquisitionClient(persona Persona, proxyURL string, cookies []Cookie, timeout time.Duration) (*Client, error) {
	if timeout <= 0 {
		timeout = DefaultAcquisitionTimeout
	}
	return newClientWithSessionCookiePolicy(persona, proxyURL, cookies, timeout, false)
}

func newClient(persona Persona, proxyURL string, cookies []Cookie, timeout time.Duration) (*Client, error) {
	return newClientWithSessionCookiePolicy(persona, proxyURL, cookies, timeout, true)
}

func newClientWithSessionCookiePolicy(
	persona Persona,
	proxyURL string,
	cookies []Cookie,
	timeout time.Duration,
	sendSessionCookies bool,
) (*Client, error) {
	persona = canonicalPersona(persona)
	proxyURL = strings.TrimSpace(proxyURL)
	cookies, _ = normalizeSessionCookies(cookies)
	profile, ok := findTLSProfile(persona.Profile)
	if !ok {
		return nil, fmt.Errorf("unsupported TLS profile %q", persona.Profile)
	}

	baseJar := tls_client.NewCookieJar()
	var jar tls_client.CookieJar = baseJar
	if !sendSessionCookies {
		jar = &accessTokenCookieJar{delegate: baseJar}
	}
	var acquisitionTracker *acquisitionConnectionTracker
	if timeout > 0 {
		acquisitionTracker = newAcquisitionConnectionTracker()
	}
	newHTTPClient := func(followRedirect bool) (tls_client.HttpClient, error) {
		timeoutMilliseconds := 0
		if timeout > 0 {
			timeoutMilliseconds = max(1, int(timeout/time.Millisecond))
		}
		options := []tls_client.HttpClientOption{
			tls_client.WithClientProfile(profile),
			tls_client.WithCookieJar(jar),
			tls_client.WithRandomTLSExtensionOrder(),
			tls_client.WithTimeoutMilliseconds(timeoutMilliseconds),
		}
		if acquisitionTracker != nil {
			options = append(
				options,
				tls_client.WithProxyDialerFactory(acquisitionTracker.dialerFactory(proxyURL)),
				tls_client.WithTransportOptions(&tls_client.TransportOptions{DisableKeepAlives: true}),
			)
		} else if proxyURL != "" {
			options = append(options, tls_client.WithProxyUrl(proxyURL))
		}
		httpClient, err := tls_client.NewHttpClient(tls_client.NewNoopLogger(), options...)
		if err != nil {
			return nil, err
		}
		httpClient.SetFollowRedirect(followRedirect)
		return httpClient, nil
	}

	follow, err := newHTTPClient(true)
	if err != nil {
		return nil, fmt.Errorf("create redirect-following browser client: %w", err)
	}
	noRedirect, err := newHTTPClient(false)
	if err != nil {
		follow.CloseIdleConnections()
		return nil, fmt.Errorf("create no-redirect browser client: %w", err)
	}
	client := &Client{
		follow:             follow,
		noRedirect:         noRedirect,
		jar:                jar,
		sendSessionCookies: sendSessionCookies,
		persona:            persona,
		proxyURL:           proxyURL,
		acquisitionTimeout: timeout,
		acquisitionTracker: acquisitionTracker,
	}
	if err := client.RestoreCookies(cookies); err != nil {
		client.CloseIdleConnections()
		return nil, err
	}
	return client, nil
}

func (jar *accessTokenCookieJar) SetCookies(targetURL *url.URL, cookies []*fhttp.Cookie) {
	if jar == nil || jar.delegate == nil {
		return
	}
	jar.delegate.SetCookies(targetURL, cookies)
}

func (jar *accessTokenCookieJar) Cookies(targetURL *url.URL) []*fhttp.Cookie {
	if jar == nil || jar.delegate == nil {
		return nil
	}
	cookies := jar.delegate.Cookies(targetURL)
	filtered := make([]*fhttp.Cookie, 0, len(cookies))
	for _, cookie := range cookies {
		if cookie == nil || isSessionCookieName(cookie.Name) {
			continue
		}
		filtered = append(filtered, cookie)
	}
	return filtered
}

func (jar *accessTokenCookieJar) GetAllCookies() map[string][]*fhttp.Cookie {
	if jar == nil || jar.delegate == nil {
		return nil
	}
	return jar.delegate.GetAllCookies()
}

func findTLSProfile(name string) (profiles.ClientProfile, bool) {
	if strings.EqualFold(strings.TrimSpace(name), "chrome_146") {
		return profiles.Chrome_146, true
	}
	return profiles.ClientProfile{}, false
}

func (client *Client) Persona() Persona {
	if client == nil {
		return Persona{}
	}
	return client.persona
}

func (client *Client) ProxyURL() string {
	if client == nil {
		return ""
	}
	return client.proxyURL
}

func (client *Client) CloseIdleConnections() {
	if client == nil {
		return
	}
	if client.follow != nil {
		client.follow.CloseIdleConnections()
	}
	if client.noRedirect != nil {
		client.noRedirect.CloseIdleConnections()
	}
}

func (client *Client) CloseActiveAcquisitionConnections() {
	if client == nil || client.acquisitionTracker == nil {
		return
	}
	client.acquisitionTracker.closeAll()
}

func (client *Client) DoFollow(ctx context.Context, method, targetURL string, headers map[string]string, body io.Reader) (*fhttp.Response, []byte, error) {
	return client.do(ctx, true, true, method, targetURL, headers, body)
}

// DoFollowOnce executes a redirect-following request without replaying it.
func (client *Client) DoFollowOnce(ctx context.Context, method, targetURL string, headers map[string]string, body io.Reader) (*fhttp.Response, []byte, error) {
	return client.do(ctx, true, false, method, targetURL, headers, body)
}

// DoFollowStream executes a redirect-following request without buffering or
// closing the response body. The caller must close the body.
func (client *Client) DoFollowStream(ctx context.Context, method, targetURL string, headers map[string]string, body io.Reader) (*fhttp.Response, error) {
	return client.doStream(ctx, client.follow, method, targetURL, headers, body)
}

func (client *Client) DoNoRedirect(ctx context.Context, method, targetURL string, headers map[string]string, body io.Reader) (*fhttp.Response, []byte, error) {
	return client.do(ctx, false, true, method, targetURL, headers, body)
}

// DoNoRedirectOnce executes one no-redirect request without replaying it.
func (client *Client) DoNoRedirectOnce(ctx context.Context, method, targetURL string, headers map[string]string, body io.Reader) (*fhttp.Response, []byte, error) {
	return client.do(ctx, false, false, method, targetURL, headers, body)
}

// DoNoRedirectStream executes a request without following redirects, buffering,
// or closing the response body. The caller must close the body.
func (client *Client) DoNoRedirectStream(ctx context.Context, method, targetURL string, headers map[string]string, body io.Reader) (*fhttp.Response, error) {
	return client.doStream(ctx, client.noRedirect, method, targetURL, headers, body)
}

// DoSameOriginRedirectStream follows a bounded redirect chain only while every
// target remains on the exact original origin.
func (client *Client) DoSameOriginRedirectStream(ctx context.Context, method, targetURL string, headers map[string]string, maxRedirects int) (*fhttp.Response, error) {
	if maxRedirects < 0 {
		maxRedirects = 0
	}
	originalURL, err := url.Parse(strings.TrimSpace(targetURL))
	if err != nil || originalURL.Scheme == "" || originalURL.Host == "" {
		return nil, fmt.Errorf("invalid redirect origin %q", targetURL)
	}
	currentURL := originalURL
	for redirects := 0; ; redirects++ {
		response, errRequest := client.DoNoRedirectStream(ctx, method, currentURL.String(), headers, nil)
		if errRequest != nil {
			return nil, errRequest
		}
		if !isChatGPTWebRedirectStatus(response.StatusCode) {
			return response, nil
		}
		location := strings.TrimSpace(response.Header.Get("Location"))
		if location == "" {
			return response, nil
		}
		nextURL, errLocation := currentURL.Parse(location)
		if errLocation != nil || !sameChatGPTWebOrigin(originalURL, nextURL) {
			return response, nil
		}
		if redirects >= maxRedirects {
			_ = response.Body.Close()
			return nil, fmt.Errorf("chatgpt web redirect chain exceeds %d hops", maxRedirects)
		}
		if errClose := response.Body.Close(); errClose != nil {
			return nil, fmt.Errorf("close redirect response body: %w", errClose)
		}
		currentURL = nextURL
		if response.StatusCode == http.StatusSeeOther {
			method = http.MethodGet
		}
	}
}

func isChatGPTWebRedirectStatus(status int) bool {
	switch status {
	case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther,
		http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		return true
	default:
		return false
	}
}

func sameChatGPTWebOrigin(left, right *url.URL) bool {
	if left == nil || right == nil {
		return false
	}
	if !strings.EqualFold(left.Scheme, right.Scheme) ||
		!strings.EqualFold(left.Hostname(), right.Hostname()) {
		return false
	}
	return chatGPTWebOriginPort(left) == chatGPTWebOriginPort(right)
}

func chatGPTWebOriginPort(value *url.URL) string {
	if value == nil {
		return ""
	}
	port := value.Port()
	if port == "" {
		switch strings.ToLower(strings.TrimSpace(value.Scheme)) {
		case "http":
			return "80"
		case "https":
			return "443"
		}
		return ""
	}
	if number, errPort := strconv.Atoi(port); errPort == nil {
		return strconv.Itoa(number)
	}
	return port
}

func (client *Client) DoJSON(ctx context.Context, followRedirect bool, method, targetURL string, headers map[string]string, body any) (*fhttp.Response, []byte, error) {
	return client.doJSON(ctx, followRedirect, true, method, targetURL, headers, body)
}

// DoJSONOnce executes one JSON request without replaying it.
func (client *Client) DoJSONOnce(ctx context.Context, followRedirect bool, method, targetURL string, headers map[string]string, body any) (*fhttp.Response, []byte, error) {
	return client.doJSON(ctx, followRedirect, false, method, targetURL, headers, body)
}

func (client *Client) doJSON(ctx context.Context, followRedirect, replayable bool, method, targetURL string, headers map[string]string, body any) (*fhttp.Response, []byte, error) {
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return nil, nil, fmt.Errorf("encode request body: %w", err)
		}
		reader = bytes.NewReader(payload)
	}
	return client.do(ctx, followRedirect, replayable, method, targetURL, headers, reader)
}

// DoJSONStream executes a JSON request without buffering, closing the response
// body, or following redirects. SSE POST bodies and proof headers must not be
// replayed to a redirected origin.
func (client *Client) DoJSONStream(ctx context.Context, method, targetURL string, headers map[string]string, body any) (*fhttp.Response, error) {
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("encode request body: %w", err)
		}
		reader = bytes.NewReader(payload)
	}
	return client.doStream(ctx, client.noRedirect, method, targetURL, headers, reader)
}

func (client *Client) do(ctx context.Context, followRedirect, replayable bool, method, targetURL string, headers map[string]string, body io.Reader) (*fhttp.Response, []byte, error) {
	if client == nil {
		return nil, nil, fmt.Errorf("browser client is nil")
	}
	var bodyBytes []byte
	if body != nil {
		var errRead error
		bodyBytes, errRead = io.ReadAll(body)
		if errRead != nil {
			return nil, nil, fmt.Errorf("read request body: %w", errRead)
		}
	}
	attempts := 1
	if client.loginRetry != nil && replayable {
		attempts = max(1, client.loginRetry.attempts)
	}
	stage := loginRequestStage(targetURL)
	for attempt := 1; attempt <= attempts; attempt++ {
		var requestBody io.Reader
		if bodyBytes != nil {
			requestBody = bytes.NewReader(bodyBytes)
		}
		httpClient := client.noRedirect
		if followRedirect {
			httpClient = client.follow
		}
		response, errRequest := client.doStream(ctx, httpClient, method, targetURL, headers, requestBody)
		if errRequest != nil {
			if client.loginRetry != nil && replayable && attempt < attempts {
				log.WithFields(loginRequestLogFields(targetURL, stage, 0, attempt, attempts)).
					Warn("chatgpt web login request failed; rotating proxy")
				if errRetry := client.prepareLoginRequestRetry(ctx, attempt); errRetry == nil {
					continue
				} else {
					errRequest = errors.Join(errRequest, errRetry)
				}
			}
			if client.loginRetry != nil {
				return nil, nil, &loginRequestError{
					stage:    stage,
					attempts: attempt,
					cause:    errRequest,
				}
			}
			return nil, nil, errRequest
		}
		payload, errRead := io.ReadAll(response.Body)
		errClose := response.Body.Close()
		if errRead != nil || errClose != nil {
			errResponse := errors.Join(errRead, errClose)
			if client.loginRetry != nil && replayable && attempt < attempts {
				log.WithFields(loginRequestLogFields(targetURL, stage, response.StatusCode, attempt, attempts)).
					Warn("chatgpt web login response failed; rotating proxy")
				if errRetry := client.prepareLoginRequestRetry(ctx, attempt); errRetry == nil {
					continue
				} else {
					errResponse = errors.Join(errResponse, errRetry)
				}
			}
			if client.loginRetry != nil {
				return response, nil, &loginRequestError{
					stage:    stage,
					status:   response.StatusCode,
					attempts: attempt,
					cause:    errResponse,
				}
			}
			return response, nil, fmt.Errorf("read or close response body: %w", errResponse)
		}
		if client.loginRetry != nil && isCloudflareChallenge(response, payload) {
			log.WithFields(loginRequestLogFields(targetURL, stage, response.StatusCode, attempt, attempts)).
				Warn("chatgpt web login request encountered a Cloudflare challenge")
			if replayable && attempt < attempts {
				if errRetry := client.prepareLoginRequestRetry(ctx, attempt); errRetry == nil {
					continue
				} else {
					return response, payload, &loginRequestError{
						stage:      stage,
						status:     response.StatusCode,
						attempts:   attempt,
						cloudflare: true,
						cause:      errRetry,
					}
				}
			}
			return response, payload, &loginRequestError{
				stage:      stage,
				status:     response.StatusCode,
				attempts:   attempt,
				cloudflare: true,
			}
		}
		return response, payload, nil
	}
	return nil, nil, &loginRequestError{stage: stage, attempts: attempts}
}

func (client *Client) prepareLoginRequestRetry(ctx context.Context, retryNumber int) error {
	if client == nil || client.loginRetry == nil || client.loginRetry.selector == nil {
		return nil
	}
	if errDelay := waitLoginRetry(ctx, client.loginRetry.delay, retryNumber); errDelay != nil {
		return errDelay
	}
	proxyURL, errProxy := client.loginRetry.selector.next()
	if errProxy != nil {
		return errProxy
	}
	replacement, errClient := newClientWithSessionCookiePolicy(
		client.persona,
		proxyURL,
		client.ExportCookies(),
		client.acquisitionTimeout,
		client.sendSessionCookies,
	)
	if errClient != nil {
		return errClient
	}
	oldFollow := client.follow
	oldNoRedirect := client.noRedirect
	oldTracker := client.acquisitionTracker
	client.follow = replacement.follow
	client.noRedirect = replacement.noRedirect
	client.jar = replacement.jar
	client.proxyURL = replacement.proxyURL
	client.acquisitionTracker = replacement.acquisitionTracker
	replacement.follow = nil
	replacement.noRedirect = nil
	if oldFollow != nil {
		oldFollow.CloseIdleConnections()
	}
	if oldNoRedirect != nil {
		oldNoRedirect.CloseIdleConnections()
	}
	if oldTracker != nil {
		oldTracker.closeAll()
	}
	return nil
}

func (client *Client) doStream(ctx context.Context, httpClient tls_client.HttpClient, method, targetURL string, headers map[string]string, body io.Reader) (*fhttp.Response, error) {
	if client == nil || httpClient == nil {
		return nil, fmt.Errorf("browser client is nil")
	}
	request, err := fhttp.NewRequest(strings.ToUpper(strings.TrimSpace(method)), targetURL, body)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	request = request.WithContext(ctx)
	client.applyHeaders(request, headers)
	if errContext := ctx.Err(); errContext != nil {
		return nil, errContext
	}
	client.beforeRequestOnce.Do(func() {
		client.beforeRequestMu.RLock()
		hook := client.beforeRequest
		client.beforeRequestMu.RUnlock()
		if hook != nil {
			hook()
		}
	})
	return httpClient.Do(request)
}

// SetBeforeRequestHook installs a one-shot callback invoked immediately before
// this client performs its first upstream HTTP request.
func (client *Client) SetBeforeRequestHook(hook func()) {
	if client != nil {
		client.beforeRequestMu.Lock()
		client.beforeRequest = hook
		client.beforeRequestMu.Unlock()
	}
}

func (client *Client) applyHeaders(request *fhttp.Request, overrides map[string]string) {
	major := chromeMajor(client.persona.UserAgent)
	platform := secCHPlatform(client.persona.Platform)
	request.Header = fhttp.Header{
		"accept":             {"application/json"},
		"accept-encoding":    {"gzip, deflate, br"},
		"accept-language":    {client.persona.AcceptLanguage},
		"cache-control":      {"no-cache"},
		"dnt":                {"1"},
		"sec-ch-ua":          {fmt.Sprintf(`"Google Chrome";v="%s", "Chromium";v="%s", "Not.A/Brand";v="24"`, major, major)},
		"sec-ch-ua-mobile":   {"?0"},
		"sec-ch-ua-platform": {platform},
		"user-agent":         {client.persona.UserAgent},
		fhttp.HeaderOrderKey: {
			"accept", "content-type", "origin", "referer", "user-agent",
			"sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform",
			"sec-fetch-site", "sec-fetch-mode", "sec-fetch-dest",
			"accept-encoding", "accept-language",
		},
	}
	for key, value := range overrides {
		request.Header.Set(key, value)
	}
}

func chromeMajor(userAgent string) string {
	const marker = "Chrome/"
	start := strings.Index(userAgent, marker)
	if start < 0 {
		return "146"
	}
	value := userAgent[start+len(marker):]
	end := strings.IndexByte(value, '.')
	if end >= 0 {
		value = value[:end]
	}
	if _, err := strconv.Atoi(value); err != nil {
		return "146"
	}
	return value
}

func secCHPlatform(platform string) string {
	switch strings.ToLower(strings.TrimSpace(platform)) {
	case "macintel", "macos":
		return `"macOS"`
	case "linux", "linux x86_64":
		return `"Linux"`
	default:
		return `"Windows"`
	}
}

func (client *Client) SetCookie(rawURL, name, value string) error {
	if client == nil || client.follow == nil {
		return fmt.Errorf("browser client is nil")
	}
	parsedURL, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsedURL.Hostname() == "" {
		return fmt.Errorf("invalid cookie URL %q", rawURL)
	}
	client.follow.SetCookies(parsedURL, []*fhttp.Cookie{{
		Name:   name,
		Value:  value,
		Path:   "/",
		Domain: parsedURL.Hostname(),
		Secure: strings.EqualFold(parsedURL.Scheme, "https"),
	}})
	return nil
}

func (client *Client) ExportCookies() []Cookie {
	if client == nil || client.jar == nil {
		return []Cookie{}
	}
	allCookies := client.jar.GetAllCookies()
	hosts := make([]string, 0, len(allCookies))
	for host := range allCookies {
		hosts = append(hosts, host)
	}
	sort.Strings(hosts)

	result := make([]Cookie, 0)
	for _, host := range hosts {
		cookies := allCookies[host]
		sort.SliceStable(cookies, func(left, right int) bool {
			return cookies[left].Name < cookies[right].Name
		})
		for _, cookie := range cookies {
			if cookie == nil || cookie.Name == "" {
				continue
			}
			expires := ""
			if !cookie.Expires.IsZero() {
				expires = cookie.Expires.UTC().Format(time.RFC3339Nano)
			}
			result = append(result, Cookie{
				Name:       cookie.Name,
				Value:      cookie.Value,
				Path:       cookie.Path,
				Domain:     cookie.Domain,
				Host:       host,
				Expires:    expires,
				RawExpires: cookie.RawExpires,
				MaxAge:     cookie.MaxAge,
				Secure:     cookie.Secure,
				HTTPOnly:   cookie.HttpOnly,
				SameSite:   int(cookie.SameSite),
			})
		}
	}
	result, _ = normalizeSessionCookies(result)
	return result
}

func (client *Client) RestoreCookies(cookies []Cookie) error {
	if client == nil || client.follow == nil {
		return fmt.Errorf("browser client is nil")
	}
	return restoreCookies(client.jar, cookies)
}

func restoreCookies(jar fhttp.CookieJar, cookies []Cookie) error {
	if jar == nil {
		return fmt.Errorf("cookie jar is nil")
	}
	for _, persisted := range cookies {
		if strings.TrimSpace(persisted.Name) == "" {
			return fmt.Errorf("cookie name is empty")
		}
		host := strings.TrimPrefix(strings.TrimSpace(persisted.Domain), ".")
		if host == "" {
			host = strings.TrimPrefix(strings.TrimSpace(persisted.Host), ".")
		}
		if host == "" {
			return fmt.Errorf("cookie %q has no host or domain", persisted.Name)
		}
		scheme := "http"
		if persisted.Secure {
			scheme = "https"
		}
		targetURL, err := url.Parse(scheme + "://" + host)
		if err != nil {
			return fmt.Errorf("restore cookie %q: %w", persisted.Name, err)
		}
		expires := time.Time{}
		if persisted.Expires != "" {
			expires, err = time.Parse(time.RFC3339Nano, persisted.Expires)
			if err != nil {
				return fmt.Errorf("restore cookie %q expiration: %w", persisted.Name, err)
			}
		} else if strings.TrimSpace(persisted.RawExpires) != "" {
			if parsedExpiry, parseErr := http.ParseTime(strings.TrimSpace(persisted.RawExpires)); parseErr == nil {
				expires = parsedExpiry
			}
		}
		if !expires.IsZero() && !expires.After(time.Now()) {
			continue
		}
		path := persisted.Path
		if path == "" {
			path = "/"
		}
		jar.SetCookies(targetURL, []*fhttp.Cookie{{
			Name:       persisted.Name,
			Value:      persisted.Value,
			Path:       path,
			Domain:     persisted.Domain,
			Expires:    expires,
			RawExpires: persisted.RawExpires,
			MaxAge:     persisted.MaxAge,
			Secure:     persisted.Secure,
			HttpOnly:   persisted.HTTPOnly,
			SameSite:   fhttp.SameSite(persisted.SameSite),
		}})
	}
	return nil
}

func credentialCookieValueForURL(cookies []Cookie, rawURL, name string) (string, error) {
	targetURL, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || targetURL.Hostname() == "" {
		return "", fmt.Errorf("invalid cookie URL %q", rawURL)
	}
	jar, err := fhttpcookiejar.New(nil)
	if err != nil {
		return "", fmt.Errorf("create scoped cookie jar: %w", err)
	}
	if err = restoreCookies(jar, cookies); err != nil {
		return "", err
	}
	for _, cookie := range jar.Cookies(targetURL) {
		if cookie != nil && cookie.Name == name {
			if value := strings.TrimSpace(cookie.Value); value != "" {
				return value, nil
			}
		}
	}
	return "", nil
}

func (client *Client) CloneWithProxy(proxyURL string) (*Client, error) {
	if client == nil {
		return nil, fmt.Errorf("browser client is nil")
	}
	return newClientWithSessionCookiePolicy(
		client.persona,
		proxyURL,
		client.ExportCookies(),
		0,
		client.sendSessionCookies,
	)
}
