package xai

import (
	"crypto/sha256"
	"encoding/base64"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

type pkceTestTransport func(*http.Request) (*http.Response, error)

func (f pkceTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestXAIPKCEChallengeAndBoundExchange(t *testing.T) {
	var submitted url.Values
	a := NewXAIAuth(nil)
	a.httpClient.Transport = pkceTestTransport(func(r *http.Request) (*http.Response, error) {
		body := `{"device_authorization_endpoint":"https://auth.x.ai/device","authorization_endpoint":"https://auth.x.ai/authorize","token_endpoint":"https://auth.x.ai/token"}`
		if r.URL.Path == "/token" {
			if err := r.ParseForm(); err != nil {
				t.Error(err)
			}
			submitted = r.PostForm
			body = `{"access_token":"fixture","refresh_token":"refresh-fixture","expires_in":3600}`
		}
		if r.Header.Get("x-grok-agent-id") != "" || r.Header.Get("x-grok-client-version") != "" {
			t.Error("inference identity leaked to OAuth")
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
	})
	flow, err := a.StartPKCE(t.Context(), "http://127.0.0.1:56121/callback")
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(flow.URL)
	q := u.Query()
	digest := sha256.Sum256([]byte(flow.Verifier))
	if q.Get("code_challenge") != base64.RawURLEncoding.EncodeToString(digest[:]) || q.Get("code_challenge_method") != "S256" || len(flow.State) < 32 || q.Get("scope") != Scope {
		t.Fatal("invalid authorization parameters")
	}
	second, err := a.StartPKCE(t.Context(), flow.RedirectURI)
	if err != nil || second.State == flow.State || second.Verifier == flow.Verifier {
		t.Fatal("login secrets were reused")
	}
	bundle, err := a.ExchangePKCE(t.Context(), flow, "fixture-code")
	if err != nil || bundle.TokenData.AccessToken != "fixture" || submitted.Get("code_verifier") != flow.Verifier || submitted.Get("redirect_uri") != flow.RedirectURI || submitted.Get("grant_type") != "authorization_code" {
		t.Fatalf("invalid exchange: %v", err)
	}
	if _, err = a.StartPKCE(t.Context(), "https://elsewhere.invalid/callback"); err == nil {
		t.Fatal("unbound redirect accepted")
	}
}
