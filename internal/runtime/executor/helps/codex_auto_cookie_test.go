package helps

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexcookie"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexstate"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type autoCookieRoundTrip func(*http.Request) (*http.Response, error)

func (f autoCookieRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

const autoCookieTarget = "https://chatgpt.com/backend-api/codex/responses"

func autoCookieFixture(t *testing.T) (*config.Config, *auth.Auth) {
	t.Helper()
	a := &auth.Auth{ID: "auto-cookie-" + t.Name(), Provider: "codex", Metadata: map[string]any{"account_id": "account"}}
	owner := StateCredential(a, "").Owner
	codexcookie.Default.Sync(true, map[string]string{a.ID: owner})
	t.Cleanup(func() { codexcookie.Default.Sync(false, nil) })
	return &config.Config{Codex: config.CodexConfig{AutoCookie: true}}, a
}
func autoResponse(r *http.Request, status int, headers http.Header) *http.Response {
	return &http.Response{StatusCode: status, Header: headers, Body: io.NopCloser(strings.NewReader("data: {\"type\":\"response.completed\"}\n\n")), Request: r}
}

func TestAutoCookieHTTPOverridesAndCapturesErrorsBeforeBody(t *testing.T) {
	for _, override := range []bool{true, false} {
		t.Run(map[bool]string{true: "override", false: "explicit"}[override], func(t *testing.T) {
			cfg, a := autoCookieFixture(t)
			cfg.Codex.AutoCookieOverride = &override
			var sent []string
			base := &http.Client{Transport: autoCookieRoundTrip(func(r *http.Request) (*http.Response, error) {
				sent = append(sent, r.Header.Get("Cookie"))
				return autoResponse(r, 429, http.Header{"Set-Cookie": {"__oailb=stored; Path=/; Max-Age=60"}}), nil
			})}
			client := AutoCookieHTTPClient(t.Context(), cfg, a, base)
			r, _ := http.NewRequestWithContext(t.Context(), "POST", autoCookieTarget, strings.NewReader("model-one"))
			r.Header.Set("Cookie", "__oailb=user; account=private")
			resp, err := client.Do(r)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if override && sent[0] != "" || !override && sent[0] != "__oailb=user; account=private" {
				t.Fatal(sent)
			}
			if r.Header.Get("Cookie") != "__oailb=user; account=private" {
				t.Fatal("caller header mutated")
			}
			// No body read is needed to make the response cookie available.
			next, _ := http.NewRequestWithContext(t.Context(), "POST", autoCookieTarget, strings.NewReader("model-two"))
			resp2, err := client.Do(next)
			if err != nil {
				t.Fatal(err)
			}
			defer resp2.Body.Close()
			if sent[1] != "__oailb=stored" {
				t.Fatal(sent)
			}
		})
	}
}

func TestAutoCookieRedirectRefreshAndScope(t *testing.T) {
	cfg, a := autoCookieFixture(t)
	store := codexcookie.Default.Acquire(a.ID, StateCredential(a, "").Owner)
	store.StoreResponse(autoCookieTarget, http.Header{"Set-Cookie": {"__oailb=first; Path=/"}})
	var sent []string
	client := AutoCookieHTTPClient(t.Context(), cfg, a, &http.Client{Transport: autoCookieRoundTrip(func(r *http.Request) (*http.Response, error) {
		sent = append(sent, r.Header.Get("Cookie"))
		switch r.URL.Path {
		case "/start":
			return autoResponse(r, 302, http.Header{"Location": {"/next"}, "Set-Cookie": {"__oailb=second; Path=/"}}), nil
		case "/next":
			return autoResponse(r, 302, http.Header{"Location": {"https://third-party.test/end"}, "Set-Cookie": {"__oailb=third; Path=/"}}), nil
		default:
			return autoResponse(r, 200, nil), nil
		}
	})})
	r, _ := http.NewRequestWithContext(t.Context(), "GET", "https://chatgpt.com/start", nil)
	response, err := client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if strings.Join(sent, ",") != "__oailb=first,__oailb=second," {
		t.Fatal(sent)
	}
}

func TestAutoCookieProbeBypassAndConflict(t *testing.T) {
	cfg, a := autoCookieFixture(t)
	store := codexcookie.Default.Acquire(a.ID, StateCredential(a, "").Owner)
	store.StoreResponse(autoCookieTarget, http.Header{"Set-Cookie": {"__oailb=original; Path=/"}})
	for _, ctx := range []context.Context{WithStateProbe(t.Context()), WithCodexCookieDiagnostic(t.Context(), "none", nil)} {
		base := &http.Client{Transport: autoCookieRoundTrip(func(r *http.Request) (*http.Response, error) {
			if r.Header.Get("Cookie") != "" {
				t.Fatal("probe sent cookie")
			}
			return autoResponse(r, 200, http.Header{"Set-Cookie": {"__oailb=probe; Path=/"}}), nil
		})}
		client := AutoCookieHTTPClient(ctx, cfg, a, base)
		r, _ := http.NewRequestWithContext(ctx, "GET", autoCookieTarget, nil)
		resp, err := client.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
	}
	if store.Header(autoCookieTarget) != "__oailb=original" {
		t.Fatal("probe changed passive store")
	}
	cfg.Codex.StateOverride = config.CodexStateOverrideConfig{Enabled: true, Strategy: "cookie-only"}
	_, _, err := PrepareAutoCookieHandshake(t.Context(), cfg, a, "wss://chatgpt.com/ws", nil)
	if err == nil {
		t.Fatal("missing runtime conflict guard")
	}
}

func TestAutoCookieHandshakeLastWriteAndNetworkFailure(t *testing.T) {
	cfg, a := autoCookieFixture(t)
	headA, saveA, err := PrepareAutoCookieHandshake(t.Context(), cfg, a, "wss://chatgpt.com/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	headB, saveB, err := PrepareAutoCookieHandshake(t.Context(), cfg, a, "wss://chatgpt.com/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	if headA.Get("Cookie") != "" || headB.Get("Cookie") != "" {
		t.Fatal("not initially empty")
	}
	saveB(&http.Response{StatusCode: 101, Header: http.Header{"Set-Cookie": {"__oailb=B; Path=/; Secure"}}})
	saveA(&http.Response{StatusCode: 403, Header: http.Header{"Set-Cookie": {"__oailb=A-late; Path=/; Secure"}}})
	headC, saveC, err := PrepareAutoCookieHandshake(t.Context(), cfg, a, "wss://chatgpt.com/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	if headC.Get("Cookie") != "__oailb=A-late" {
		t.Fatal(headC)
	}
	saveC(nil)
	client := AutoCookieHTTPClient(t.Context(), cfg, a, &http.Client{Transport: autoCookieRoundTrip(func(r *http.Request) (*http.Response, error) { return nil, errors.New("network failure") })})
	r, _ := http.NewRequestWithContext(t.Context(), "GET", autoCookieTarget, nil)
	_, _ = client.Do(r)
	headD, _, _ := PrepareAutoCookieHandshake(t.Context(), cfg, a, "wss://chatgpt.com/ws", nil)
	if headD.Get("Cookie") != headC.Get("Cookie") {
		t.Fatal("network failure cleared jar")
	}
}

func TestAutoCookieDiagnosticExplicitAndEmpty(t *testing.T) {
	cfg, a := autoCookieFixture(t)
	cfg.Codex.AutoCookieOverride = new(false)
	var source string
	ctx := WithCodexCookieDiagnostic(t.Context(), "configured", func(s string, c codexstate.CookieSelection) { source = s })
	_, _, err := PrepareAutoCookieHandshake(ctx, cfg, a, "wss://chatgpt.com/ws", http.Header{"cookie": {"__oailb=user"}})
	if err != nil || source != "explicit" {
		t.Fatal(source, err)
	}
	_, _, err = PrepareAutoCookieHandshake(ctx, cfg, a, "wss://chatgpt.com/ws", nil)
	if err != nil || source != "empty" {
		t.Fatal(source, err)
	}
}
