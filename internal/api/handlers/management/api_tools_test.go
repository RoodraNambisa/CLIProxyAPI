package management

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

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
