package helps

import (
	"context"
	"net/http"

	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

// ObserveCodexHTTPQuota borrows headers only while the request's sink runs.
// The logical request policy, rather than a transport's current config, owns
// admission so retries and fallback retain the same observation decision.
func ObserveCodexHTTPQuota(ctx context.Context, auth *coreauth.Auth, headers http.Header) {
	observer := core.CodexQuotaObserverFromContext(ctx)
	if observer == nil || auth == nil || len(headers) == 0 {
		return
	}
	observer(auth.ID, auth.RuntimeInstanceID(), "http", headers)
}

// ObserveCodexWebsocketQuota is called once when a text frame is consumed from
// the transport, before any identity rewriting or bootstrap buffer replay.
func ObserveCodexWebsocketQuota(ctx context.Context, auth *coreauth.Auth, payload []byte) {
	observer := core.CodexQuotaObserverFromContext(ctx)
	if observer == nil || auth == nil {
		return
	}
	if headers := ParseCodexQuotaEventHeaders(payload); len(headers) > 0 {
		observer(auth.ID, auth.RuntimeInstanceID(), "websocket", headers)
	}
}
