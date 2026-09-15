package management

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

type apiCallProxyResolver struct {
	err error
}

type managementProxyError struct {
	headers http.Header
}

func (managementProxyError) Error() string        { return "proxy unavailable" }
func (managementProxyError) StatusCode() int      { return http.StatusServiceUnavailable }
func (managementProxyError) SkipAuthResult() bool { return true }
func (e managementProxyError) Headers() http.Header {
	return e.headers
}

type managementUpstreamUnavailableError struct{}

func (managementUpstreamUnavailableError) Error() string   { return "upstream unavailable" }
func (managementUpstreamUnavailableError) StatusCode() int { return http.StatusServiceUnavailable }

func (r apiCallProxyResolver) Resolve(context.Context, *coreauth.Auth) (coreauth.ResolvedProxy, error) {
	return coreauth.ResolvedProxy{}, r.err
}

func (apiCallProxyResolver) ReportFailure(_ context.Context, _ *coreauth.Auth, err error) error {
	return err
}

func TestAPICallTransportDirectBypassesGlobalProxy(t *testing.T) {
	t.Parallel()

	h := &Handler{
		cfg: &config.Config{
			SDKConfig: sdkconfig.SDKConfig{ProxyURL: "http://global-proxy.example.com:8080"},
		},
	}

	transport := h.apiCallTransport(&coreauth.Auth{ProxyURL: "direct"})
	httpTransport, ok := transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", transport)
	}
	if httpTransport.Proxy != nil {
		t.Fatal("expected direct transport to disable proxy function")
	}
}

func TestAPICallGrokProviderHeadersQuotaEndpoints(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodGet || r.Header.Get("Authorization") != "Bearer quota-fixture" {
			t.Error("quota request lost its method or credential")
		}
		if r.Header.Get("X-Global") != "inherited" || r.Header.Get("X-Override") != "credential" || r.Header.Get("x-grok-client-version") != "fixture-version" {
			t.Error("quota request lost global headers or credential precedence")
		}
		if r.Host == "cli-chat-proxy.grok.com" && r.Header.Get("X-XAI-Token-Auth") != "xai-grok-cli" {
			t.Error("subscription request lost Grok CLI authentication header")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"fixture":"quota"}`))
	}))
	defer upstream.Close()

	// Keep the real management request path, URL and TLS transport, but route
	// every connection to this test server instead of contacting real accounts.
	transport := upstream.Client().Transport.(*http.Transport).Clone()
	transport.DisableKeepAlives = true
	transport.TLSClientConfig.ServerName = upstream.Listener.Addr().(*net.TCPAddr).IP.String()
	transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, upstream.Listener.Addr().String())
	}
	previousTransport := http.DefaultTransport
	http.DefaultTransport = transport
	defer func() {
		http.DefaultTransport = previousTransport
		transport.CloseIdleConnections()
	}()

	manager := coreauth.NewManager(nil, nil, nil)
	registered, err := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{
		ID: "grok-quota-provider-headers", Provider: "xai",
		Attributes: map[string]string{"source": "config:xai[quota-fixture]", "api_key": "quota-fixture", "header:X-Override": "credential"},
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{XAI: config.XAIConfig{
		HeaderDefaults: config.XAIHeaderDefaults{ClientVersion: "fixture-version"},
		Headers:        map[string]string{"X-Global": "inherited", "X-Override": "global"},
	}}
	h := NewHandlerWithoutConfigFilePath(cfg, manager)
	for _, endpoint := range []string{
		"https://cli-chat-proxy.grok.com/v1/billing?format=credits",
		"https://cli-chat-proxy.grok.com/v1/billing",
		"https://cli-chat-proxy.grok.com/v1/settings",
		"https://api.x.ai/v1/models",
		"https://us-east-1.api.x.ai/v1/models",
	} {
		t.Run(endpoint, func(t *testing.T) {
			before := calls.Load()
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			body := fmt.Sprintf(`{"authIndex":%q,"provider_headers":true,"method":"GET","url":%q,"header":{"Authorization":"Bearer $TOKEN$"}}`, registered.EnsureIndex(), endpoint)
			ctx.Request = httptest.NewRequest(http.MethodPost, "/kimoji/v0/management/api-call", strings.NewReader(body))
			h.APICall(ctx)
			if recorder.Code != http.StatusOK || calls.Load() != before+1 || !strings.Contains(recorder.Body.String(), `"status_code":200`) {
				t.Fatalf("quota request rejected: status=%d calls=%d body=%s", recorder.Code, calls.Load()-before, recorder.Body.String())
			}
		})
	}
	for _, endpoint := range []string{
		"http://cli-chat-proxy.grok.com/v1/billing",
		"https://cli-chat-proxy.grok.com.example.org/v1/billing",
		"https://api.x.ai.example.org/v1/models",
	} {
		t.Run(endpoint, func(t *testing.T) {
			before := calls.Load()
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			body := fmt.Sprintf(`{"authIndex":%q,"provider_headers":true,"method":"GET","url":%q}`, registered.EnsureIndex(), endpoint)
			ctx.Request = httptest.NewRequest(http.MethodPost, "/kimoji/v0/management/api-call", strings.NewReader(body))
			h.APICall(ctx)
			if recorder.Code != http.StatusBadRequest || calls.Load() != before {
				t.Fatal("provider headers were allowed for an unrelated endpoint")
			}
		})
	}
}

func TestAPICallProxyResolutionErrorDoesNotReturnEmptySuccess(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	auth, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{
		ID:         "api-call-proxy-error",
		Provider:   "codex",
		Attributes: map[string]string{"source": "config:codex[proxy-error]"},
	})
	if errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	manager.SetProxyResolver(apiCallProxyResolver{err: errors.New("resolver failed")})
	h := NewHandlerWithoutConfigFilePath(&config.Config{}, manager)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body := fmt.Sprintf(`{"auth_index":%q,"method":"GET","url":"https://upstream.example"}`, auth.EnsureIndex())
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v0/management/api-call", strings.NewReader(body))
	ctx.Request.Header.Set("Content-Type", "application/json")

	h.APICall(ctx)

	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("APICall() status = %d, want %d; body=%s", recorder.Code, http.StatusBadGateway, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "proxy_resolution_failed") {
		t.Fatalf("APICall() body = %s, want proxy_resolution_failed", recorder.Body.String())
	}
}

func TestAPICallRejectsCodexRetainedForWebDependents(t *testing.T) {
	var upstreamCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		upstreamCalls.Add(1)
		response.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	manager := coreauth.NewManager(nil, nil, nil)
	retained := coreauth.RetainCodexAuthForChatGPTWebDependents(&coreauth.Auth{
		ID: "retained-codex", Provider: "codex", Status: coreauth.StatusActive,
		Metadata: map[string]any{"type": "codex", "credential_uid": "uid-a", "access_token": "secret"},
	}, time.Now())
	installed, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), retained)
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	h := &Handler{authManager: manager}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body := fmt.Sprintf(`{"auth_index":%q,"method":"GET","url":%q}`, installed.EnsureIndex(), upstream.URL)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v0/management/api-call", strings.NewReader(body))
	ctx.Request.Header.Set("Content-Type", "application/json")

	h.APICall(ctx)

	if recorder.Code != http.StatusConflict || upstreamCalls.Load() != 0 {
		t.Fatalf("APICall() status=%d calls=%d body=%s", recorder.Code, upstreamCalls.Load(), recorder.Body.String())
	}
}

func TestAPICallDisabledCodexQuotaPreservesDisabledState(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusUnauthorized} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != http.MethodGet || r.Header.Get("Authorization") != "Bearer disabled-quota-token-fixture" {
					t.Error("quota query lost its method or credential")
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				_, _ = w.Write([]byte(`{"fixture":"quota"}`))
			}))
			defer upstream.Close()
			manager := coreauth.NewManager(nil, nil, nil)
			auth, err := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{
				ID: "disabled-quota", Provider: "codex", Disabled: true, Status: coreauth.StatusDisabled,
				Attributes: map[string]string{"source": "config:codex[disabled-quota]"},
				Metadata:   map[string]any{"access_token": "disabled-quota-token-fixture", "disabled": true},
			})
			if err != nil {
				t.Fatal(err)
			}
			h := NewHandlerWithoutConfigFilePath(&config.Config{}, manager)
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			body := fmt.Sprintf(`{"auth_index":%q,"method":"GET","url":%q,"header":{"Authorization":"Bearer $TOKEN$"}}`, auth.EnsureIndex(), upstream.URL+"/usage")
			ctx.Request = httptest.NewRequest(http.MethodPost, "/v0/management/api-call", strings.NewReader(body))
			h.APICall(ctx)
			if recorder.Code != http.StatusOK || calls.Load() != 1 || !strings.Contains(recorder.Body.String(), fmt.Sprintf(`"status_code":%d`, status)) {
				t.Fatal("disabled credential could not perform the manual quota query")
			}
			current, _ := manager.GetByID(auth.ID)
			if !current.Disabled || current.Status != coreauth.StatusDisabled || current.Metadata["disabled"] != true {
				t.Fatal("manual quota query changed credential enablement")
			}
		})
	}
}

func TestWriteManagementProxyErrorOnlyCopiesRetryAfter(t *testing.T) {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	errProxy := managementProxyError{headers: http.Header{
		"Retry-After":   []string{"42"},
		"Set-Cookie":    []string{"secret=session"},
		"Authorization": []string{"Bearer secret"},
	}}

	if !writeManagementProxyError(ctx, errProxy) {
		t.Fatal("writeManagementProxyError() = false, want true")
	}
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
	if got := recorder.Header().Get("Retry-After"); got != "42" {
		t.Fatalf("Retry-After = %q, want 42", got)
	}
	if got := recorder.Header().Get("Set-Cookie"); got != "" {
		t.Fatalf("Set-Cookie leaked: %q", got)
	}
	if got := recorder.Header().Get("Authorization"); got != "" {
		t.Fatalf("Authorization leaked: %q", got)
	}
}

func TestWriteManagementProxyErrorDoesNotClaimGeneric503(t *testing.T) {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	if writeManagementProxyError(ctx, managementUpstreamUnavailableError{}) {
		t.Fatal("generic upstream 503 was classified as proxy_unavailable")
	}
}

func TestTokenValueForAuthPrefersAPIKeyOverLegacyOAuthMetadata(t *testing.T) {
	auth := &coreauth.Auth{
		Provider:   "gemini",
		Attributes: map[string]string{"api_key": "active-api-key"},
		Metadata: map[string]any{
			"type":         "gemini",
			"access_token": "retired-oauth-token",
		},
	}
	if token := tokenValueForAuth(auth); token != "active-api-key" {
		t.Fatalf("tokenValueForAuth() = %q, want API key", token)
	}
}

func TestProxyURLFromInteractionsConfigUsesExactCredentialIdentity(t *testing.T) {
	cfg := &config.Config{InteractionsKey: []config.GeminiKey{
		{APIKey: "KEY", BaseURL: "https://EXAMPLE.com/v1", ProxyURL: "http://wrong-proxy.example.com"},
		{APIKey: "key", BaseURL: "https://example.com/v1", ProxyURL: "http://right-proxy.example.com"},
	}}
	auth := &coreauth.Auth{
		Provider: "gemini-interactions",
		Attributes: map[string]string{
			"api_key":  "key",
			"base_url": "https://example.com/v1",
		},
	}

	if got := proxyURLFromAPIKeyConfig(cfg, auth); got != "http://right-proxy.example.com" {
		t.Fatalf("proxy URL = %q, want exact credential proxy", got)
	}
}

func TestAPICallConfigBackedAuthBypassesUnavailableAuthDir(t *testing.T) {
	authCases := []struct {
		name       string
		provider   string
		source     string
		apiKey     string
		mutate     func(*coreauth.Auth, string)
		wantStatus int
	}{
		{name: "gemini", provider: "gemini", source: "config:gemini[gemini-token]", apiKey: "gemini-key", wantStatus: http.StatusOK},
		{name: "claude", provider: "claude", source: "config:claude[claude-token]", apiKey: "claude-key", wantStatus: http.StatusOK},
		{name: "codex", provider: "codex", source: "config:codex[codex-token]", apiKey: "codex-key", wantStatus: http.StatusOK},
		{name: "openai compatibility", provider: "openrouter", source: "config:openrouter[compat-token]", apiKey: "compat-key", wantStatus: http.StatusOK},
		{name: "openai compatibility without api key", provider: "openrouter", source: "config:openrouter[keyless-token]", wantStatus: http.StatusOK},
		{name: "vertex api key", provider: "vertex", source: "config:vertex-apikey[vertex-token]", apiKey: "vertex-key", wantStatus: http.StatusOK},
		{
			name:     "config source with file name",
			provider: "codex",
			source:   "config:codex[file-name-token]",
			apiKey:   "file-name-key",
			mutate: func(auth *coreauth.Auth, _ string) {
				auth.FileName = "missing.json"
			},
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:     "config source with path",
			provider: "claude",
			source:   "config:claude[path-token]",
			apiKey:   "path-key",
			mutate: func(auth *coreauth.Auth, authDir string) {
				auth.Attributes["path"] = filepath.Join(authDir, "missing.json")
			},
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:     "managed file source",
			provider: "vertex",
			apiKey:   "source-key",
			mutate: func(auth *coreauth.Auth, authDir string) {
				auth.Attributes["source"] = filepath.Join(authDir, "missing.json")
			},
			wantStatus: http.StatusServiceUnavailable,
		},
	}

	authDirCases := []struct {
		name                   string
		makeAuthDirUnavailable func(*testing.T, string)
	}{
		{
			name: "missing",
			makeAuthDirUnavailable: func(t *testing.T, authDir string) {
				t.Helper()
				if errRemove := os.RemoveAll(authDir); errRemove != nil {
					t.Fatalf("remove auth dir: %v", errRemove)
				}
			},
		},
		{
			name: "not a directory",
			makeAuthDirUnavailable: func(t *testing.T, authDir string) {
				t.Helper()
				if errRemove := os.RemoveAll(authDir); errRemove != nil {
					t.Fatalf("remove auth dir: %v", errRemove)
				}
				if errWrite := os.WriteFile(authDir, []byte("unavailable"), 0o600); errWrite != nil {
					t.Fatalf("replace auth dir with file: %v", errWrite)
				}
			},
		},
	}

	for _, authCase := range authCases {
		for _, authDirCase := range authDirCases {
			t.Run(authCase.name+"/"+authDirCase.name, func(t *testing.T) {
				authDir := filepath.Join(t.TempDir(), "auths")
				if errMkdir := os.MkdirAll(authDir, 0o700); errMkdir != nil {
					t.Fatalf("create auth dir: %v", errMkdir)
				}

				auth := &coreauth.Auth{
					ID:       "test-auth",
					Provider: authCase.provider,
					Status:   coreauth.StatusActive,
					Attributes: map[string]string{
						"source": authCase.source,
					},
				}
				if authCase.apiKey != "" {
					auth.Attributes["api_key"] = authCase.apiKey
				}
				if authCase.mutate != nil {
					authCase.mutate(auth, authDir)
				}

				manager := coreauth.NewManager(nil, nil, nil)
				registered, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), auth)
				if errRegister != nil {
					t.Fatalf("register auth: %v", errRegister)
				}

				authDirCase.makeAuthDirUnavailable(t, authDir)
				var upstreamCalls atomic.Int32
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					upstreamCalls.Add(1)
					wantAuthorization := "Static"
					if authCase.apiKey != "" {
						wantAuthorization = "Bearer " + authCase.apiKey
					}
					if got := req.Header.Get("Authorization"); got != wantAuthorization {
						t.Errorf("Authorization = %q, want %q", got, wantAuthorization)
					}
					w.WriteHeader(http.StatusNoContent)
				}))
				defer upstream.Close()

				header := "Static"
				if authCase.apiKey != "" {
					header = "Bearer $TOKEN$"
				}
				h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
				recorder := httptest.NewRecorder()
				ctx, _ := gin.CreateTestContext(recorder)
				body := fmt.Sprintf(`{"auth_index":%q,"method":"GET","url":%q,"header":{"Authorization":%q}}`, registered.EnsureIndex(), upstream.URL, header)
				ctx.Request = httptest.NewRequest(http.MethodPost, "/v0/management/api-call", strings.NewReader(body))
				ctx.Request.Header.Set("Content-Type", "application/json")
				h.APICall(ctx)

				if recorder.Code != authCase.wantStatus {
					t.Fatalf("status = %d, want %d; body=%s", recorder.Code, authCase.wantStatus, recorder.Body.String())
				}
				wantUpstreamCalls := int32(0)
				if authCase.wantStatus == http.StatusOK {
					wantUpstreamCalls = 1
				}
				if got := upstreamCalls.Load(); got != wantUpstreamCalls {
					t.Fatalf("upstream calls = %d, want %d", got, wantUpstreamCalls)
				}
			})
		}
	}
}

func TestAPICallTransportInvalidAuthFallsBackToGlobalProxy(t *testing.T) {
	t.Parallel()

	h := &Handler{
		cfg: &config.Config{
			SDKConfig: sdkconfig.SDKConfig{ProxyURL: "http://global-proxy.example.com:8080"},
		},
	}

	transport := h.apiCallTransport(&coreauth.Auth{ProxyURL: "bad-value"})
	httpTransport, ok := transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", transport)
	}

	req, errRequest := http.NewRequest(http.MethodGet, "https://example.com", nil)
	if errRequest != nil {
		t.Fatalf("http.NewRequest returned error: %v", errRequest)
	}

	proxyURL, errProxy := httpTransport.Proxy(req)
	if errProxy != nil {
		t.Fatalf("httpTransport.Proxy returned error: %v", errProxy)
	}
	if proxyURL == nil || proxyURL.String() != "http://global-proxy.example.com:8080" {
		t.Fatalf("proxy URL = %v, want http://global-proxy.example.com:8080", proxyURL)
	}
}

func TestAPICallTransportAPIKeyAuthFallsBackToConfigProxyURL(t *testing.T) {
	t.Parallel()

	h := &Handler{
		cfg: &config.Config{
			SDKConfig: sdkconfig.SDKConfig{ProxyURL: "http://global-proxy.example.com:8080"},
			GeminiKey: []config.GeminiKey{{
				APIKey:   "gemini-key",
				ProxyURL: "http://gemini-proxy.example.com:8080",
			}},
			ClaudeKey: []config.ClaudeKey{{
				APIKey:   "claude-key",
				ProxyURL: "http://claude-proxy.example.com:8080",
			}},
			CodexKey: []config.CodexKey{{
				APIKey:   "codex-key",
				ProxyURL: "http://codex-proxy.example.com:8080",
			}},
			OpenAICompatibility: []config.OpenAICompatibility{{
				Name:    "bohe",
				BaseURL: "https://bohe.example.com",
				APIKeyEntries: []config.OpenAICompatibilityAPIKey{{
					APIKey:   "compat-key",
					ProxyURL: "http://compat-proxy.example.com:8080",
				}},
			}},
		},
	}

	cases := []struct {
		name      string
		auth      *coreauth.Auth
		wantProxy string
	}{
		{
			name: "gemini",
			auth: &coreauth.Auth{
				Provider:   "gemini",
				Attributes: map[string]string{"api_key": "gemini-key"},
			},
			wantProxy: "http://gemini-proxy.example.com:8080",
		},
		{
			name: "claude",
			auth: &coreauth.Auth{
				Provider:   "claude",
				Attributes: map[string]string{"api_key": "claude-key"},
			},
			wantProxy: "http://claude-proxy.example.com:8080",
		},
		{
			name: "codex",
			auth: &coreauth.Auth{
				Provider:   "codex",
				Attributes: map[string]string{"api_key": "codex-key"},
			},
			wantProxy: "http://codex-proxy.example.com:8080",
		},
		{
			name: "openai-compatibility",
			auth: &coreauth.Auth{
				Provider: "bohe",
				Attributes: map[string]string{
					"api_key":      "compat-key",
					"compat_name":  "bohe",
					"provider_key": "bohe",
				},
			},
			wantProxy: "http://compat-proxy.example.com:8080",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			transport := h.apiCallTransport(tc.auth)
			httpTransport, ok := transport.(*http.Transport)
			if !ok {
				t.Fatalf("transport type = %T, want *http.Transport", transport)
			}

			req, errRequest := http.NewRequest(http.MethodGet, "https://example.com", nil)
			if errRequest != nil {
				t.Fatalf("http.NewRequest returned error: %v", errRequest)
			}

			proxyURL, errProxy := httpTransport.Proxy(req)
			if errProxy != nil {
				t.Fatalf("httpTransport.Proxy returned error: %v", errProxy)
			}
			if proxyURL == nil || proxyURL.String() != tc.wantProxy {
				t.Fatalf("proxy URL = %v, want %s", proxyURL, tc.wantProxy)
			}
		})
	}
}

func TestAuthByIndexDistinguishesSharedAPIKeysAcrossProviders(t *testing.T) {
	t.Parallel()

	manager := coreauth.NewManager(nil, nil, nil)
	geminiAuth := &coreauth.Auth{
		ID:       "gemini:apikey:123",
		Provider: "gemini",
		Attributes: map[string]string{
			"api_key": "shared-key",
		},
	}
	compatAuth := &coreauth.Auth{
		ID:       "openai-compatibility:bohe:456",
		Provider: "bohe",
		Label:    "bohe",
		Attributes: map[string]string{
			"api_key":      "shared-key",
			"compat_name":  "bohe",
			"provider_key": "bohe",
		},
	}

	if _, errRegister := manager.Register(context.Background(), geminiAuth); errRegister != nil {
		t.Fatalf("register gemini auth: %v", errRegister)
	}
	if _, errRegister := manager.Register(context.Background(), compatAuth); errRegister != nil {
		t.Fatalf("register compat auth: %v", errRegister)
	}

	geminiIndex := geminiAuth.EnsureIndex()
	compatIndex := compatAuth.EnsureIndex()
	if geminiIndex == compatIndex {
		t.Fatalf("shared api key produced duplicate auth_index %q", geminiIndex)
	}

	h := &Handler{authManager: manager}

	gotGemini := h.authByIndex(geminiIndex)
	if gotGemini == nil {
		t.Fatal("expected gemini auth by index")
	}
	if gotGemini.ID != geminiAuth.ID {
		t.Fatalf("authByIndex(gemini) returned %q, want %q", gotGemini.ID, geminiAuth.ID)
	}

	gotCompat := h.authByIndex(compatIndex)
	if gotCompat == nil {
		t.Fatal("expected compat auth by index")
	}
	if gotCompat.ID != compatAuth.ID {
		t.Fatalf("authByIndex(compat) returned %q, want %q", gotCompat.ID, compatAuth.ID)
	}
}
