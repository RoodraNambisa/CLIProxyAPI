package helps

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexcookie"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexstate"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type codexAutoCookieTransport struct {
	base  http.RoundTripper
	cfg   *config.Config
	auth  *auth.Auth
	store *codexcookie.Store
}

func autoCookieBypass(ctx context.Context) bool {
	if IsStateProbe(ctx) {
		return true
	}
	if ctx != nil {
		d, _ := ctx.Value(cookieDiagnosticKey{}).(cookieDiagnostic)
		return d.mode == "none" || d.mode == "managed" || d.mode == "candidate"
	}
	return false
}

func automaticCookieStore(ctx context.Context, cfg *config.Config, a *auth.Auth) *codexcookie.Store {
	if cfg == nil || !cfg.Codex.AutoCookie || autoCookieBypass(ctx) || !StateCredentialAvailable(a) {
		return nil
	}
	c := StateCredential(a, "")
	return codexcookie.Default.Acquire(c.ID, c.Owner)
}

func cookieHeaderPresent(headers http.Header) bool {
	for key := range headers {
		if strings.EqualFold(key, "Cookie") {
			return true
		}
	}
	return false
}

func removeCookieHeader(headers http.Header) {
	for key := range headers {
		if strings.EqualFold(key, "Cookie") {
			delete(headers, key)
		}
	}
}

func applyAutomaticCookie(ctx context.Context, cfg *config.Config, a *auth.Auth, store *codexcookie.Store, target string, headers http.Header) error {
	if cfg == nil || !cfg.Codex.AutoCookie {
		return nil
	}
	if autoCookieBypass(ctx) {
		observeCookieSelection(ctx, "skipped", codexstate.CookieSelection{})
		return nil
	}
	if !StateCredentialAvailable(a) || codexcookie.URL(target) == nil {
		observeCookieSelection(ctx, "unsupported", codexstate.CookieSelection{})
		return nil
	}
	if err := cfg.ValidateCodexAutoCookie(); err != nil {
		return fmt.Errorf("Codex automatic Cookie configuration conflict: %w", err)
	}
	if cfg.Codex.OverridesAutoCookie() {
		removeCookieHeader(headers)
	} else if cookieHeaderPresent(headers) {
		var values []string
		for name, items := range headers {
			if strings.EqualFold(name, "Cookie") {
				values = append(values, items...)
			}
		}
		observeCookieSelection(ctx, "explicit", codexstate.CookieSelection{Header: strings.Join(values, "; ")})
		return nil
	}
	value := store.Header(target)
	if value != "" {
		headers.Set("Cookie", value)
		observeCookieSelection(ctx, "automatic", codexstate.CookieSelection{Header: value})
	} else {
		observeCookieSelection(ctx, "empty", codexstate.CookieSelection{})
	}
	return nil
}

func (t *codexAutoCookieTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Leave the caller's header map untouched so redirects and retries cannot
	// mistake our generated Cookie for an explicit user header.
	r := req.Clone(req.Context())
	if err := applyAutomaticCookie(r.Context(), t.cfg, t.auth, t.store, r.URL.String(), r.Header); err != nil {
		return nil, err
	}
	response, err := t.base.RoundTrip(r)
	if response != nil && !autoCookieBypass(r.Context()) {
		t.store.StoreResponse(r.URL.String(), response.Header)
	}
	return response, err
}

func AutoCookieHTTPClient(ctx context.Context, cfg *config.Config, a *auth.Auth, client *http.Client) *http.Client {
	if cfg == nil || !cfg.Codex.AutoCookie || client == nil || autoCookieBypass(ctx) {
		return client
	}
	copy := *client
	base := copy.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	copy.Jar = nil
	copy.Transport = &codexAutoCookieTransport{base: base, cfg: cfg, auth: a, store: automaticCookieStore(ctx, cfg, a)}
	return &copy
}

// PrepareAutoCookieHandshake owns only handshake headers; established sockets
// retain their connection even when this store changes or a cookie expires.
func PrepareAutoCookieHandshake(ctx context.Context, cfg *config.Config, a *auth.Auth, target string, headers http.Header) (http.Header, func(*http.Response), error) {
	copy := headers.Clone()
	if copy == nil {
		copy = make(http.Header)
	}
	store := automaticCookieStore(ctx, cfg, a)
	if err := applyAutomaticCookie(ctx, cfg, a, store, target, copy); err != nil {
		return nil, nil, err
	}
	return copy, func(response *http.Response) {
		if response != nil {
			store.StoreResponse(target, response.Header)
		}
	}, nil
}
