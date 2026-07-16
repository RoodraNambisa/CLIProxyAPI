package chatgptweb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	tls_client "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
)

type Client struct {
	follow     tls_client.HttpClient
	noRedirect tls_client.HttpClient
	jar        tls_client.CookieJar
	persona    Persona
	proxyURL   string
}

func NewClient(persona Persona, proxyURL string, cookies []Cookie) (*Client, error) {
	persona = normalizePersona(persona)
	profile, ok := findTLSProfile(persona.Profile)
	if !ok {
		return nil, fmt.Errorf("unsupported TLS profile %q", persona.Profile)
	}

	jar := tls_client.NewCookieJar()
	newHTTPClient := func(followRedirect bool) (tls_client.HttpClient, error) {
		options := []tls_client.HttpClientOption{
			tls_client.WithClientProfile(profile),
			tls_client.WithCookieJar(jar),
			tls_client.WithRandomTLSExtensionOrder(),
			tls_client.WithTimeoutSeconds(0),
		}
		if strings.TrimSpace(proxyURL) != "" {
			options = append(options, tls_client.WithProxyUrl(strings.TrimSpace(proxyURL)))
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
		follow:     follow,
		noRedirect: noRedirect,
		jar:        jar,
		persona:    persona,
		proxyURL:   strings.TrimSpace(proxyURL),
	}
	if err := client.RestoreCookies(cookies); err != nil {
		client.CloseIdleConnections()
		return nil, err
	}
	return client, nil
}

func findTLSProfile(name string) (profiles.ClientProfile, bool) {
	for profileName, profile := range profiles.MappedTLSClients {
		if strings.EqualFold(profileName, strings.TrimSpace(name)) {
			return profile, true
		}
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

func (client *Client) DoFollow(ctx context.Context, method, targetURL string, headers map[string]string, body io.Reader) (*fhttp.Response, []byte, error) {
	return client.do(ctx, client.follow, method, targetURL, headers, body)
}

func (client *Client) DoNoRedirect(ctx context.Context, method, targetURL string, headers map[string]string, body io.Reader) (*fhttp.Response, []byte, error) {
	return client.do(ctx, client.noRedirect, method, targetURL, headers, body)
}

func (client *Client) DoJSON(ctx context.Context, followRedirect bool, method, targetURL string, headers map[string]string, body any) (*fhttp.Response, []byte, error) {
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return nil, nil, fmt.Errorf("encode request body: %w", err)
		}
		reader = bytes.NewReader(payload)
	}
	if followRedirect {
		return client.DoFollow(ctx, method, targetURL, headers, reader)
	}
	return client.DoNoRedirect(ctx, method, targetURL, headers, reader)
}

func (client *Client) do(ctx context.Context, httpClient tls_client.HttpClient, method, targetURL string, headers map[string]string, body io.Reader) (*fhttp.Response, []byte, error) {
	if client == nil || httpClient == nil {
		return nil, nil, fmt.Errorf("browser client is nil")
	}
	request, err := fhttp.NewRequest(strings.ToUpper(strings.TrimSpace(method)), targetURL, body)
	if err != nil {
		return nil, nil, fmt.Errorf("create request: %w", err)
	}
	request = request.WithContext(ctx)
	client.applyHeaders(request, headers)

	response, err := httpClient.Do(request)
	if err != nil {
		return nil, nil, err
	}
	payload, errRead := io.ReadAll(response.Body)
	errClose := response.Body.Close()
	if errRead != nil {
		return response, nil, fmt.Errorf("read response body: %w", errRead)
	}
	if errClose != nil {
		return response, nil, fmt.Errorf("close response body: %w", errClose)
	}
	return response, payload, nil
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
	return result
}

func (client *Client) RestoreCookies(cookies []Cookie) error {
	if client == nil || client.follow == nil {
		return fmt.Errorf("browser client is nil")
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
		}
		path := persisted.Path
		if path == "" {
			path = "/"
		}
		client.follow.SetCookies(targetURL, []*fhttp.Cookie{{
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

func (client *Client) CloneWithProxy(proxyURL string) (*Client, error) {
	if client == nil {
		return nil, fmt.Errorf("browser client is nil")
	}
	return NewClient(client.persona, proxyURL, client.ExportCookies())
}
