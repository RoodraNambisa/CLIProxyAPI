package management

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

// readXAIResourceQuery retries one read-only query after OAuth token rejection.
// Close the previous response before rotating its credential runtime instance.
func readXAIResourceQuery(ctx context.Context, manager *coreauth.Manager, auth *coreauth.Auth, limit int64, query func(*coreauth.Auth) (*http.Response, error)) (*http.Response, []byte, *coreauth.Auth, error) {
	for attempt := 0; ; attempt++ {
		resp, err := query(auth)
		if err != nil || resp == nil || resp.Body == nil {
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
			if err == nil {
				err = fmt.Errorf("Grok query returned an empty response")
			}
			return resp, nil, auth, err
		}
		var reader io.Reader = resp.Body
		if limit > 0 {
			reader = io.LimitReader(reader, limit)
		}
		body, errRead := io.ReadAll(reader)
		if errClose := resp.Body.Close(); errClose != nil {
			log.WithError(errClose).Warn("close Grok query response")
		}
		if errRead != nil {
			return resp, nil, auth, errRead
		}
		rejected := resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden && helps.IsXAIBadCredentialsBody(body)
		credentialAuthorization := resp.Request == nil || resp.Request.Header.Get("Authorization") == "Bearer "+tokenValueForAuth(auth)
		if attempt > 0 || !rejected || !credentialAuthorization || manager == nil || auth == nil || !strings.EqualFold(auth.Provider, "xai") ||
			strings.TrimSpace(auth.Attributes["api_key"]) != "" || stringValue(auth.Metadata, "refresh_token") == "" {
			return resp, body, auth, nil
		}
		refreshed, errRefresh := manager.RefreshXAIAfterUnauthorized(ctx, auth)
		if errRefresh != nil {
			if ctx.Err() != nil {
				return resp, body, auth, ctx.Err()
			}
			return resp, body, auth, fmt.Errorf("Grok credential refresh failed after HTTP %d; sign in again if its refresh token was revoked", resp.StatusCode)
		}
		resolved, errProxy := manager.ResolveProxyAuth(ctx, refreshed)
		if errProxy != nil {
			return resp, body, refreshed, errProxy
		}
		auth = resolved
	}
}
