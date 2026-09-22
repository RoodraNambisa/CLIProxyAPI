package helps

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexstate"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

type cookieVerificationKey struct{}
type cookieVerification struct {
	bundle *codexstate.CookieBundle
	policy config.CodexStateOverrideConfig
}

func WithCookieVerification(ctx context.Context, b *codexstate.CookieBundle, policies ...config.CodexStateOverrideConfig) context.Context {
	v := cookieVerification{bundle: b}
	if len(policies) > 0 {
		v.policy = policies[0]
	}
	return context.WithValue(ctx, cookieVerificationKey{}, v)
}
func CaptureStateHeaders(ctx context.Context, response *http.Response) {
	capture, _ := ctx.Value(stateCaptureKey{}).(*codexstate.Result)
	if capture == nil || response == nil {
		return
	}
	capture.ReceivedAt = time.Now()
	capture.Status = response.StatusCode
	capture.State = response.Header.Get("X-Codex-Turn-State")
	if response.Request != nil && response.Request.URL != nil {
		capture.Cookies = codexstate.CaptureCookies(response.Request.URL.String(), response.Header, capture.ReceivedAt)
	}
}

func applyStateProbeHeaders(ctx context.Context, headers http.Header, rawURL string) error {
	for name := range headers {
		if strings.EqualFold(name, "Cookie") || strings.EqualFold(name, "X-Codex-Turn-State") {
			delete(headers, name)
		}
	}
	if v, ok := ctx.Value(cookieVerificationKey{}).(cookieVerification); ok && v.bundle != nil {
		selection := v.bundle.Select(rawURL, time.Now(), v.policy)
		if selection.Header == "" {
			return fmt.Errorf("candidate Cookie is unavailable for this request")
		}
		headers.Set("Cookie", selection.Header)
	}
	return nil
}

type stateProbeTransport struct {
	base http.RoundTripper
	ctx  context.Context
}

func (t stateProbeTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	copy := r.Clone(r.Context())
	if err := applyStateProbeHeaders(t.ctx, copy.Header, copy.URL.String()); err != nil {
		return nil, err
	}
	return t.base.RoundTrip(copy)
}

// State acquisition has no implicit cookie jar or redirect-based carryover.
// The transport check runs after all client middleware has populated headers.
func StateProbeHTTPClient(ctx context.Context, client *http.Client) *http.Client {
	if !IsStateProbe(ctx) || client == nil {
		return client
	}
	copy := *client
	copy.Jar = nil
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	base := copy.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	copy.Transport = stateProbeTransport{base: base, ctx: ctx}
	return &copy
}
