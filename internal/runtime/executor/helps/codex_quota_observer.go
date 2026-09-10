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
