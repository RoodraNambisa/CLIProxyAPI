package helps

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexstate"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type cookieDiagnosticKey struct{}
type cookieDiagnostic struct {
	mode    string
	observe func(string, codexstate.CookieSelection)
}

func WithCodexCookieDiagnostic(ctx context.Context, mode string, observe func(string, codexstate.CookieSelection)) context.Context {
	return context.WithValue(ctx, cookieDiagnosticKey{}, cookieDiagnostic{mode: mode, observe: observe})
}

func applyCookieDiagnostic(ctx context.Context, cfg *config.Config, a *auth.Auth, model string, headers http.Header, target string) (bool, error) {
	d, _ := ctx.Value(cookieDiagnosticKey{}).(cookieDiagnostic)
	if d.mode == "none" {
		for name := range headers {
			if strings.EqualFold(name, "Cookie") {
				delete(headers, name)
			}
		}
		if d.observe != nil {
			d.observe("none", codexstate.CookieSelection{})
		}
		return false, nil
	}
	if d.mode != "managed" && d.mode != "candidate" {
		return false, nil
	}
	for name := range headers {
		if strings.EqualFold(name, "X-Codex-Turn-State") {
			delete(headers, name)
		}
	}
	codexstate.StripManagedCookies(headers)
	p := config.CodexStateOverrideConfig{}.Resolved()
	if cfg != nil {
		var matched bool
		p, _, matched = cfg.Codex.ManagedStateConfig().PolicyFor(StateCredential(a, model).Scope())
		if !matched {
			p, _ = codexstate.DiagnosticPolicy(cfg.Codex.ManagedStateConfig(), StateCredential(a, model))
		}
		p = p.Resolved()
	}
	c := StateCredential(a, model)
	var selected codexstate.CookieSelection
	if d.mode == "candidate" {
		selected = codexstate.Default.CookieCandidate(c, target, time.Now(), p)
		if selected.Header == "" {
			selected, _, _ = codexstate.Diagnostic.PickCookie(c, target, time.Now(), p)
		}
	} else {
		selected, _, _ = codexstate.Default.PickCookie(c, target, time.Now(), p)
	}
	if d.observe != nil {
		d.observe(d.mode, selected)
	}
	if selected.Header == "" {
		return true, errors.New("no usable Cookie is available for the selected diagnostic source")
	}
	if existing := headers.Get("Cookie"); existing != "" {
		headers.Set("Cookie", existing+"; "+selected.Header)
	} else {
		headers.Set("Cookie", selected.Header)
	}
	return true, nil
}

func observeCookieSelection(ctx context.Context, source string, selected codexstate.CookieSelection) {
	if d, ok := ctx.Value(cookieDiagnosticKey{}).(cookieDiagnostic); ok && d.observe != nil {
		d.observe(source, selected)
	}
}
