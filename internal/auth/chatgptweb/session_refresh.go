package chatgptweb

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

const sessionRefreshAttempts = 3

// A renewed session cookie can carry an expired token and RefreshAccessTokenError.
// Keep the cookie jar across bounded retries, but never publish that token as ready.
func (service *Service) refreshSessionToken(ctx context.Context, client *Client) (sessionPayload, *AuthError) {
	for attempt := 1; ; attempt++ {
		session, authError := service.fetchSessionToken(ctx, client)
		if authError == nil {
			return session, nil
		}
		authError.FailureStage = "session_refresh"
		authError.Attempts = attempt
		if attempt >= sessionRefreshAttempts || !ineffectiveSessionRefresh(authError.Code) {
			return session, authError
		}
		if err := waitLoginRetry(ctx, 200*time.Millisecond, attempt); err != nil {
			canceled := networkAuthError("session_refresh_network_error", LifecycleActive, err)
			canceled.FailureStage = "session_refresh"
			canceled.Attempts = attempt
			return sessionPayload{}, canceled
		}
	}
}

func ineffectiveSessionRefresh(code string) bool {
	return code == "session_refresh_failed" || code == "access_token_expired" || code == "access_token_missing"
}

func (service *Service) fetchSessionToken(ctx context.Context, client *Client) (sessionPayload, *AuthError) {
	var session sessionPayload
	response, payload, err := client.DoNoRedirect(ctx, http.MethodGet,
		service.options.SessionBaseURL+"/api/auth/session?refresh=true",
		map[string]string{
			"accept":         "application/json",
			"referer":        service.options.SessionBaseURL + "/",
			"sec-fetch-dest": "empty",
			"sec-fetch-mode": "cors",
			"sec-fetch-site": "same-origin",
		}, nil)
	if err != nil {
		return session, networkAuthError("session_refresh_network_error", LifecycleActive, err)
	}
	if isCloudflareChallenge(response, payload) {
		return session, newAuthError("cloudflare_challenge", LifecycleActive, response.StatusCode, true, false, "Cloudflare challenge blocked session refresh", nil)
	}
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden ||
		(response.StatusCode >= http.StatusMultipleChoices && response.StatusCode < http.StatusBadRequest) {
		return session, newAuthError("session_expired", LifecycleReauthRequired, response.StatusCode, false, true, "chatgpt session must be renewed", nil)
	}
	if authError := classifyHTTPResponse("session_refresh", response.StatusCode, payload, LifecycleActive); authError != nil {
		return session, authError
	}
	if err = json.Unmarshal(payload, &session); err != nil {
		return session, newAuthError("session_response_invalid", LifecycleActive, response.StatusCode, true, false, "session endpoint returned invalid JSON", err)
	}
	if raw := strings.TrimSpace(string(session.Error)); raw != "" && raw != "null" && raw != `""` {
		var code string
		if json.Unmarshal(session.Error, &code) == nil && code == "RefreshAccessTokenError" {
			return session, newAuthError("session_refresh_failed", LifecycleReauthRequired, response.StatusCode, false, true, "session endpoint returned RefreshAccessTokenError", nil)
		}
		// Unknown session errors are not evidence that login material is invalid.
		return session, newAuthError("session_response_error", LifecycleActive, response.StatusCode, true, false, "session endpoint returned an error", nil)
	}
	if strings.TrimSpace(session.AccessToken) == "" {
		return session, newAuthError("access_token_missing", LifecycleReauthRequired, response.StatusCode, false, true, "session endpoint did not return an access token", nil)
	}
	if expiry, ok := JWTExpiry(session.AccessToken); ok && !expiry.After(service.options.Now()) {
		return session, newAuthError("access_token_expired", LifecycleReauthRequired, response.StatusCode, false, true, "session endpoint returned an expired access token", nil)
	}
	return session, nil
}
