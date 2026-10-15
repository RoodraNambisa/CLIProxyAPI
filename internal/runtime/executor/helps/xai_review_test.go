package helps

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type xaiReviewTransport func(*http.Request) (*http.Response, error)

func (f xaiReviewTransport) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestXAIReviewBuildCommonHeadersAtAPIEndpoint(t *testing.T) {
	cfg := &config.Config{XAI: config.XAIConfig{HeaderDefaults: config.XAIHeaderDefaults{OfficialBuild: true}, ResolvedClientVersion: "1.0.52"}}
	for _, endpoint := range []string{"https://api.x.ai/v1/responses", "https://relay.example/v1/responses"} {
		req, _ := http.NewRequest(http.MethodPost, endpoint, nil)
		ApplyXAIResourceHeaders(req, nil, cfg)
		if !strings.HasPrefix(req.Header.Get("User-Agent"), "grok-shell/1.0.52 (") || req.Header.Get("x-grok-client-version") != "1.0.52" || req.Header.Get("x-grok-client-identifier") != "grok-shell" {
			t.Errorf("Build common headers missing at %s: %v", endpoint, req.Header)
		}
		if req.Header.Get("X-XAI-Token-Auth") != "" || req.Header.Get("x-authenticateresponse") != "" {
			t.Error("CLI authentication headers sent outside CLI")
		}
	}
}

func TestXAIReviewResourceIdentityAndCompressionNegotiation(t *testing.T) {
	auth := &coreauth.Auth{ID: "review-settings-identity", Provider: "xai", Metadata: map[string]any{"sub": "fixture-user"}}
	cfg := &config.Config{XAI: config.XAIConfig{RequestCompression: "auto", HeaderDefaults: config.XAIHeaderDefaults{OfficialBuild: true}}}
	for _, path := range []string{"models", "settings", "billing?format=credits"} {
		req, _ := http.NewRequest(http.MethodGet, "https://cli-chat-proxy.grok.com/v1/"+path, nil)
		ApplyXAIResourceHeaders(req, auth, cfg)
		if req.Header.Get("x-userid") != "fixture-user" {
			t.Errorf("resource %s omitted x-userid", path)
		}
	}
	body := []byte(`{"model":"grok-4.7","input":"` + strings.Repeat("x", XAICompressionMinimum) + `"}`)
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, "https://cli-chat-proxy.grok.com/v1/responses", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	ApplyXAIResourceHeaders(req, auth, cfg)
	client := &http.Client{Transport: xaiReviewTransport(func(query *http.Request) (*http.Response, error) {
		if query.URL.Path != "/v1/settings" || query.Header.Get("x-userid") != "fixture-user" {
			t.Errorf("settings negotiation used inference identity headers: %v", query.Header)
		}
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"accept_request_encodings":["zstd"]}`)), Request: query}, nil
	})}
	compressed := PrepareXAICompressedRequest(t.Context(), client, req, auth, cfg)
	if compressed.Header.Get("Content-Encoding") != "zstd" {
		t.Fatal("compression negotiation failed")
	}
	if err := compressed.Body.Close(); err != nil {
		t.Fatal(err)
	}
}
