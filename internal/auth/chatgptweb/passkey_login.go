package chatgptweb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"html"
	"net/http"
	"net/url"
	"regexp"
	"strings"
)

const passkeyVerifyPath = "/api/accounts/passkey/verify"

var passkeyCallbackURLBodyPatterns = []*regexp.Regexp{
	regexp.MustCompile(`https?://[^\s"'<>]+code=[^\s"'<>]+state=[^\s"'<>]+`),
	regexp.MustCompile(`['"]([^'"]*code=[^'"]*state=[^'"]*)['"]`),
}

func (service *Service) loginWithPasskey(
	ctx context.Context,
	client *Client,
	credential *Credential,
	input LoginInput,
	pendingState LifecycleState,
) (*Credential, error) {
	if credential == nil || credential.WebAuthn == nil {
		return service.loginFailure(credential, input.Relogin, passkeyCredentialError("passkey_credential_invalid", "Passkey credential is unavailable", nil))
	}
	if errValidate := ValidateWebAuthnCredential(credential.WebAuthn); errValidate != nil {
		return service.loginFailure(credential, input.Relogin, passkeyCredentialError("passkey_credential_invalid", "Passkey credential is invalid", errValidate))
	}

	challengePayload, errChallenge := service.beginPasskeyLogin(ctx, client, credential, pendingState)
	if errChallenge != nil {
		return service.loginFailure(credential, input.Relogin, ensureAuthError(errChallenge, pendingState))
	}
	requestOptions, requestID, ok := extractPasskeyChallenge(challengePayload)
	if !ok {
		return service.loginFailure(credential, input.Relogin, passkeyCredentialError("passkey_challenge_unavailable", "Passkey challenge is unavailable", nil))
	}
	assertion, errAssertion := createWebAuthnAssertion(
		credential.WebAuthn,
		requestOptions,
		func(updated WebAuthnCredential) (WebAuthnCredential, error) {
			if input.PersistWebAuthn == nil {
				return WebAuthnCredential{}, errors.New("Passkey counter persistence is unavailable")
			}
			return input.PersistWebAuthn(ctx, updated)
		},
	)
	if errAssertion != nil {
		code := "passkey_credential_invalid"
		message := "Passkey assertion could not be created"
		switch {
		case errors.Is(errAssertion, errWebAuthnStatePersistence):
			return service.loginFailure(credential, input.Relogin, passkeyStatePersistenceError(pendingState, "passkey_verify", errAssertion))
		case errors.Is(errAssertion, errWebAuthnCredentialNotAllowed):
			code = "passkey_credential_not_allowed"
			message = "Passkey credential is not allowed by the challenge"
		case errors.Is(errAssertion, errWebAuthnRequestOptionsInvalid):
			code = "passkey_challenge_unavailable"
			message = "Passkey challenge is invalid"
		}
		return service.loginFailure(credential, input.Relogin, passkeyCredentialError(code, message, errAssertion))
	}

	response, payload, errVerify := client.DoJSONOnce(ctx, false, http.MethodPost,
		service.options.AuthBaseURL+passkeyVerifyPath,
		service.apiHeaders(credential.DeviceID, service.options.AuthBaseURL+"/log-in/passkey", ""),
		map[string]any{
			"mfa_request_id":             requestID,
			"passkey_challenge_response": assertion,
			"using_conditional_ui":       false,
		})
	if errVerify != nil {
		authError := networkAuthError("passkey_verification_failed", pendingState, errVerify)
		authError.FailureStage = "passkey_verify"
		return service.loginFailure(credential, input.Relogin, authError)
	}
	if isCloudflareChallenge(response, payload) {
		authError := newAuthError("cloudflare_challenge", pendingState, response.StatusCode, true, false, "Cloudflare challenge blocked Passkey verification", nil)
		authError.FailureStage = "passkey_verify"
		return service.loginFailure(credential, input.Relogin, authError)
	}
	if response.StatusCode != http.StatusOK {
		authError := classifyPasskeyVerificationResponse(response.StatusCode, payload, pendingState)
		return service.loginFailure(credential, input.Relogin, authError)
	}
	continueURL := parseAPIEnvelope(payload).ContinueURL
	if strings.TrimSpace(continueURL) == "" {
		return service.loginFailure(credential, input.Relogin, passkeyCredentialError("passkey_verification_failed", "Passkey verification did not return a callback", nil))
	}
	if errCallback := service.consumePasskeyCallback(ctx, client, continueURL, pendingState); errCallback != nil {
		return service.loginFailure(credential, input.Relogin, ensureAuthError(errCallback, pendingState))
	}
	return service.finishPasskeyLogin(ctx, client, credential, input, pendingState)
}

func (service *Service) beginPasskeyLogin(ctx context.Context, client *Client, credential *Credential, pendingState LifecycleState) ([]byte, error) {
	response, payload, errCSRF := client.DoFollow(ctx, http.MethodGet,
		service.options.SessionBaseURL+"/api/auth/csrf",
		service.sessionAPIHeaders(service.options.SessionBaseURL+"/"), nil)
	if errCSRF != nil {
		return nil, networkAuthError("passkey_challenge_unavailable", pendingState, errCSRF)
	}
	if isCloudflareChallenge(response, payload) {
		authError := newAuthError("cloudflare_challenge", pendingState, response.StatusCode, true, false, "Cloudflare challenge blocked Passkey login", nil)
		authError.FailureStage = "passkey_challenge"
		return nil, authError
	}
	if authError := classifyPasskeyChallengeResponse(response.StatusCode, payload, pendingState); authError != nil {
		return nil, authError
	}
	csrfToken := passkeyCSRFToken(payload, client.ExportCookies(), service.options.SessionBaseURL)
	if csrfToken == "" {
		return nil, passkeyCredentialError("passkey_challenge_unavailable", "Passkey login CSRF token is unavailable", nil)
	}
	authSessionID, errSessionID := GenerateDeviceID(service.options.Rand)
	if errSessionID != nil {
		return nil, newAuthError("random_generation_failed", pendingState, 0, false, true, "initialize Passkey login session", errSessionID)
	}
	query := url.Values{
		"prompt":                          {"login"},
		"ext-oai-did":                     {credential.DeviceID},
		"auth_session_logging_id":         {authSessionID},
		"ext-passkey-client-capabilities": {"11111"},
		"screen_hint":                     {"login"},
		"login_hint":                      {credential.Email},
	}
	form := url.Values{
		"csrfToken":   {csrfToken},
		"callbackUrl": {"/"},
		"json":        {"true"},
	}
	headers := service.sessionAPIHeaders(service.options.SessionBaseURL + "/auth/login")
	headers["content-type"] = "application/x-www-form-urlencoded"
	response, payload, errSignin := client.DoFollow(ctx, http.MethodPost,
		service.options.SessionBaseURL+"/api/auth/signin/openai?"+query.Encode(),
		headers, strings.NewReader(form.Encode()))
	if errSignin != nil {
		return nil, networkAuthError("passkey_challenge_unavailable", pendingState, errSignin)
	}
	if isCloudflareChallenge(response, payload) {
		authError := newAuthError("cloudflare_challenge", pendingState, response.StatusCode, true, false, "Cloudflare challenge blocked Passkey login", nil)
		authError.FailureStage = "passkey_challenge"
		return nil, authError
	}
	if authError := classifyPasskeyChallengeResponse(response.StatusCode, payload, pendingState); authError != nil {
		return nil, authError
	}
	var signin map[string]any
	if errDecode := json.Unmarshal(payload, &signin); errDecode != nil {
		return nil, passkeyCredentialError("passkey_challenge_unavailable", "Passkey sign-in response is invalid", errDecode)
	}
	authURL := strings.TrimSpace(stringValue(signin["url"]))
	if authURL == "" || validateOAuthContinuationOrigin(authURL, service.options.AuthBaseURL) != nil {
		return nil, passkeyCredentialError("passkey_challenge_unavailable", "Passkey sign-in response did not contain a trusted authorization URL", nil)
	}
	response, payload, errEntry := client.DoFollow(ctx, http.MethodGet, authURL, map[string]string{
		"accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
		"referer":                   service.options.SessionBaseURL + "/auth/login",
		"sec-fetch-dest":            "document",
		"sec-fetch-mode":            "navigate",
		"sec-fetch-site":            "cross-site",
		"sec-fetch-user":            "?1",
		"upgrade-insecure-requests": "1",
	}, nil)
	if errEntry != nil {
		return nil, networkAuthError("passkey_challenge_unavailable", pendingState, errEntry)
	}
	if isCloudflareChallenge(response, payload) {
		authError := newAuthError("cloudflare_challenge", pendingState, response.StatusCode, true, false, "Cloudflare challenge blocked Passkey login", nil)
		authError.FailureStage = "passkey_challenge"
		return nil, authError
	}
	if authError := classifyPasskeyChallengeResponse(response.StatusCode, payload, pendingState); authError != nil {
		return nil, authError
	}
	if deviceID, errCookie := credentialCookieValueForURL(client.ExportCookies(), service.options.AuthBaseURL, "oai-did"); errCookie == nil && strings.TrimSpace(deviceID) != "" {
		credential.DeviceID = strings.TrimSpace(deviceID)
		if errSet := client.SetCookie(service.options.SessionBaseURL, "oai-did", credential.DeviceID); errSet != nil {
			authError := newAuthError("cookie_initialization_failed", pendingState, 0, false, true, "synchronize Passkey device cookie", errSet)
			authError.FailureStage = "passkey_challenge"
			return nil, authError
		}
	}
	if _, _, ok := extractPasskeyChallenge(payload); ok {
		return payload, nil
	}

	sentinel, errSentinel := NewSentinel(client, service.options.SentinelBaseURL, service.options.AuthBaseURL, credential.DeviceID, service.options.Rand, service.options.Now)
	if errSentinel != nil {
		return nil, newAuthError("sentinel_initialization_failed", pendingState, 0, false, true, "initialize Passkey login sentinel", errSentinel)
	}
	authorizeSentinel, errToken := sentinel.Token(ctx, "authorize_continue")
	if errToken != nil {
		return nil, ensureAuthError(errToken, pendingState)
	}
	response, payload, errContinue := client.DoJSON(ctx, true, http.MethodPost,
		service.options.AuthBaseURL+"/api/accounts/authorize/continue",
		service.apiHeaders(credential.DeviceID, service.options.AuthBaseURL+"/sign-in", authorizeSentinel),
		map[string]any{
			"username":    map[string]string{"value": credential.Email, "kind": "email"},
			"screen_hint": "login",
		})
	if errContinue != nil {
		return nil, networkAuthError("passkey_challenge_unavailable", pendingState, errContinue)
	}
	if isCloudflareChallenge(response, payload) {
		authError := newAuthError("cloudflare_challenge", pendingState, response.StatusCode, true, false, "Cloudflare challenge blocked Passkey login", nil)
		authError.FailureStage = "passkey_challenge"
		return nil, authError
	}
	if authError := classifyPasskeyChallengeResponse(response.StatusCode, payload, pendingState); authError != nil {
		return nil, authError
	}
	return payload, nil
}

func (service *Service) consumePasskeyCallback(ctx context.Context, client *Client, rawURL string, pendingState LifecycleState) error {
	authBase, errAuth := url.Parse(service.options.AuthBaseURL)
	sessionBase, errSession := url.Parse(service.options.SessionBaseURL)
	targetURL := resolveURL(service.options.AuthBaseURL+"/", rawURL)
	if errAuth != nil || errSession != nil || !passkeyCallbackURLAllowed(targetURL, authBase, sessionBase) {
		return passkeyCallbackError("Passkey verification returned an untrusted callback")
	}
	referer := service.options.AuthBaseURL + "/log-in/passkey"
	callbackSeen := false
	for range 14 {
		parsedTarget, _ := url.Parse(targetURL)
		if sameOAuthEndpointOrigin(parsedTarget, sessionBase) && isPasskeyCallbackPath(parsedTarget.Path) {
			callbackSeen = true
		}
		response, payload, errCallback := client.DoNoRedirectOnce(ctx, http.MethodGet, targetURL, map[string]string{
			"accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
			"referer":                   referer,
			"sec-fetch-dest":            "document",
			"sec-fetch-mode":            "navigate",
			"sec-fetch-site":            "cross-site",
			"upgrade-insecure-requests": "1",
		}, nil)
		if errCallback != nil {
			authError := networkAuthError("passkey_verification_failed", pendingState, errCallback)
			authError.FailureStage = "passkey_callback"
			return authError
		}
		if isCloudflareChallenge(response, payload) {
			authError := newAuthError("cloudflare_challenge", pendingState, response.StatusCode, true, false, "Cloudflare challenge blocked Passkey callback", nil)
			authError.FailureStage = "passkey_callback"
			return authError
		}
		if response.StatusCode >= http.StatusBadRequest {
			return classifyPasskeyProtocolResponse(
				"passkey_callback",
				"passkey_verification_failed",
				"Passkey callback was rejected",
				response.StatusCode,
				payload,
				pendingState,
			)
		}
		if isChatGPTWebRedirectStatus(response.StatusCode) {
			location := strings.TrimSpace(response.Header.Get("Location"))
			if location == "" {
				return passkeyCallbackError("Passkey callback redirect is incomplete")
			}
			nextURL := resolveURL(targetURL, location)
			if !passkeyCallbackURLAllowed(nextURL, authBase, sessionBase) {
				return passkeyCallbackError("Passkey callback redirect is untrusted")
			}
			referer = targetURL
			targetURL = nextURL
			continue
		}
		parsed, _ := url.Parse(targetURL)
		if sameOAuthEndpointOrigin(parsed, sessionBase) && callbackSeen {
			return nil
		}
		continueURL := extractPasskeyCallbackContinuation(targetURL, payload)
		if continueURL == "" {
			return passkeyCallbackError("Passkey callback chain ended before the ChatGPT session")
		}
		nextURL := resolveURL(targetURL, continueURL)
		if !passkeyCallbackURLAllowed(nextURL, authBase, sessionBase) {
			return passkeyCallbackError("Passkey callback continuation is untrusted")
		}
		referer = targetURL
		targetURL = nextURL
	}
	return passkeyCallbackError("Passkey callback redirect limit exceeded")
}

func extractPasskeyCallbackContinuation(baseURL string, payload []byte) string {
	if continueURL := strings.TrimSpace(parseAPIEnvelope(payload).ContinueURL); continueURL != "" {
		return continueURL
	}
	body := strings.TrimSpace(string(payload))
	for _, pattern := range passkeyCallbackURLBodyPatterns {
		match := pattern.FindStringSubmatch(body)
		if len(match) == 0 {
			continue
		}
		candidate := match[0]
		if len(match) > 1 && strings.TrimSpace(match[1]) != "" {
			candidate = match[1]
		}
		resolved := resolveURL(baseURL, html.UnescapeString(strings.TrimSpace(candidate)))
		parsed, errParse := url.Parse(resolved)
		if errParse == nil && strings.TrimSpace(parsed.Query().Get("code")) != "" && strings.TrimSpace(parsed.Query().Get("state")) != "" {
			return resolved
		}
	}
	return ""
}

func isPasskeyCallbackPath(path string) bool {
	cleaned := strings.ToLower(strings.TrimSpace(path))
	return strings.Contains(cleaned, "/api/auth/callback/openai") || strings.HasPrefix(cleaned, "/auth/callback")
}

func passkeyCallbackError(message string) *AuthError {
	authError := passkeyCredentialError("passkey_verification_failed", message, nil)
	authError.FailureStage = "passkey_callback"
	return authError
}

func passkeyCallbackURLAllowed(rawURL string, authBase, sessionBase *url.URL) bool {
	parsed, errParse := url.Parse(strings.TrimSpace(rawURL))
	if errParse != nil || parsed == nil || parsed.User != nil || parsed.Fragment != "" {
		return false
	}
	return sameOAuthEndpointOrigin(parsed, authBase) || sameOAuthEndpointOrigin(parsed, sessionBase)
}

func (service *Service) finishPasskeyLogin(ctx context.Context, client *Client, credential *Credential, input LoginInput, pendingState LifecycleState) (*Credential, error) {
	response, payload, errSession := client.DoNoRedirect(ctx, http.MethodGet,
		service.options.SessionBaseURL+"/api/auth/session?refresh=true",
		service.sessionAPIHeaders(service.options.SessionBaseURL+"/"), nil)
	if errSession != nil {
		authError := networkAuthError("passkey_session_invalid", pendingState, errSession)
		authError.FailureStage = "passkey_session"
		return service.loginFailure(credential, input.Relogin, authError)
	}
	if isCloudflareChallenge(response, payload) {
		authError := newAuthError("cloudflare_challenge", pendingState, response.StatusCode, true, false, "Cloudflare challenge blocked Passkey session", nil)
		authError.FailureStage = "passkey_session"
		return service.loginFailure(credential, input.Relogin, authError)
	}
	if response.StatusCode != http.StatusOK {
		return service.loginFailure(credential, input.Relogin, classifyPasskeyProtocolResponse(
			"passkey_session",
			"passkey_session_invalid",
			"Passkey session is invalid",
			response.StatusCode,
			payload,
			pendingState,
		))
	}
	var session sessionPayload
	if errDecode := json.Unmarshal(payload, &session); errDecode != nil || strings.TrimSpace(session.AccessToken) == "" {
		return service.loginFailure(credential, input.Relogin, passkeyCredentialError("passkey_session_invalid", "Passkey session did not return an access token", errDecode))
	}
	incoming := &Credential{
		Email:       strings.TrimSpace(session.User.Email),
		AccountID:   strings.TrimSpace(session.Account.ID),
		UserID:      strings.TrimSpace(session.User.ID),
		AccessToken: strings.TrimSpace(session.AccessToken),
	}
	PopulateCredentialIdentity(credential)
	PopulateCredentialIdentity(incoming)
	if !passkeySessionIdentityMatches(credential, incoming) {
		return service.loginFailure(credential, input.Relogin, passkeyCredentialError("passkey_session_invalid", "Passkey session identity could not be verified", nil))
	}
	cookies := client.ExportCookies()
	cookies, sessionToken := normalizeSessionCookies(cookies)
	if !HasSessionCookieForURL(cookies, service.options.SessionBaseURL) {
		return service.loginFailure(credential, input.Relogin, passkeyCredentialError("passkey_session_invalid", "Passkey session cookie is unavailable", nil))
	}

	updatedWebAuthn := cloneWebAuthnCredential(credential.WebAuthn)
	lastUsedAt := service.timestamp()
	if CompareWebAuthnLastUsedAt(updatedWebAuthn.LastUsedAt, lastUsedAt) > 0 {
		lastUsedAt = updatedWebAuthn.LastUsedAt
	}
	updatedWebAuthn.LastUsedAt = lastUsedAt
	if input.PersistWebAuthn == nil {
		return service.loginFailure(credential, input.Relogin, passkeyStatePersistenceError(pendingState, "passkey_session", nil))
	}
	persisted, errPersist := input.PersistWebAuthn(ctx, *updatedWebAuthn)
	if errPersist != nil {
		return service.loginFailure(credential, input.Relogin, passkeyStatePersistenceError(pendingState, "passkey_session", errPersist))
	}
	if errValidate := ValidateWebAuthnCredential(&persisted); errValidate != nil ||
		!WebAuthnAuthenticatorMatches(&persisted, updatedWebAuthn) ||
		persisted.SignCount < updatedWebAuthn.SignCount ||
		CompareWebAuthnLastUsedAt(persisted.LastUsedAt, updatedWebAuthn.LastUsedAt) < 0 {
		return service.loginFailure(credential, input.Relogin, passkeyCredentialError("passkey_credential_invalid", "Persisted Passkey state is invalid", errValidate))
	}
	credential.WebAuthn = cloneWebAuthnCredential(&persisted)
	credential.AccessToken = strings.TrimSpace(session.AccessToken)
	credential.RefreshToken = ""
	credential.IDToken = strings.TrimSpace(session.IDToken)
	credential.RefreshStrategy = RefreshStrategyChatGPTSession
	credential.CredentialMode = CredentialModeNative
	credential.Expired = tokenExpiryString(credential.AccessToken, credential.IDToken)
	if credential.Expired == "" {
		credential.Expired = strings.TrimSpace(session.Expires)
	}
	if incoming.Email != "" {
		credential.Email = incoming.Email
	}
	if incoming.AccountID != "" {
		credential.AccountID = incoming.AccountID
	}
	if incoming.UserID != "" {
		credential.UserID = incoming.UserID
	}
	if planType := strings.TrimSpace(session.Account.PlanType); planType != "" {
		credential.PlanType = planType
	}
	PopulateCredentialIdentity(credential)
	credential.Cookies = cookies
	credential.SessionToken = sessionToken
	credential.Persona = client.Persona()
	now := service.timestamp()
	credential.LastLoginAt = now
	if input.Relogin {
		credential.LastReloginAt = now
	}
	service.updateLifecycle(credential, LifecycleActive, "")
	return credential, nil
}

func passkeySessionIdentityMatches(expected, incoming *Credential) bool {
	if expected == nil || incoming == nil || NormalizeEmail(incoming.Email) == "" ||
		NormalizeEmail(expected.Email) != NormalizeEmail(incoming.Email) ||
		credentialIdentitySetConflicts(credentialIdentityEvidence(expected)) ||
		credentialIdentitySetConflicts(credentialIdentityEvidence(incoming)) ||
		credentialIdentityConflicts(expected, incoming) {
		return false
	}
	for _, values := range [][2]string{
		{expected.AccountID, incoming.AccountID},
		{expected.UserID, incoming.UserID},
	} {
		if strings.TrimSpace(values[0]) != "" && strings.TrimSpace(values[1]) == "" {
			return false
		}
	}
	return true
}

func extractPasskeyChallenge(payload []byte) (map[string]any, string, bool) {
	var decoded any
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if errDecode := decoder.Decode(&decoded); errDecode != nil {
		return nil, "", false
	}
	var options map[string]any
	var requestID string
	var visit func(any)
	visit = func(value any) {
		if options != nil && requestID != "" {
			return
		}
		switch current := value.(type) {
		case map[string]any:
			if requestID == "" {
				requestID = strings.TrimSpace(stringValue(current["mfa_request_id"]))
			}
			if options == nil {
				if candidate, ok := current["passkey_request_options"].(map[string]any); ok {
					options = candidate
				}
			}
			for _, nested := range current {
				visit(nested)
			}
		case []any:
			for _, nested := range current {
				visit(nested)
			}
		}
	}
	visit(decoded)
	return options, requestID, options != nil && requestID != ""
}

func passkeyCSRFToken(payload []byte, cookies []Cookie, rawURL string) string {
	var response map[string]any
	_ = json.Unmarshal(payload, &response)
	result := strings.TrimSpace(stringValue(response["csrfToken"]))
	for _, name := range []string{
		"__Host-next-auth.csrf-token",
		"__Host-authjs.csrf-token",
		"next-auth.csrf-token",
		"authjs.csrf-token",
	} {
		value, errCookie := credentialCookieValueForURL(cookies, rawURL, name)
		if errCookie != nil || value == "" {
			continue
		}
		if decoded, errDecode := url.QueryUnescape(value); errDecode == nil {
			value = decoded
		}
		if token, _, found := strings.Cut(value, "|"); found {
			value = token
		}
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return result
}

func (service *Service) sessionAPIHeaders(referer string) map[string]string {
	return map[string]string{
		"accept":         "application/json",
		"origin":         service.options.SessionBaseURL,
		"referer":        referer,
		"sec-fetch-dest": "empty",
		"sec-fetch-mode": "cors",
		"sec-fetch-site": "same-origin",
	}
}

func classifyPasskeyVerificationResponse(status int, payload []byte, pendingState LifecycleState) *AuthError {
	classified := classifyHTTPResponse("passkey_verify", status, payload, pendingState)
	if classified == nil {
		return passkeyProtocolStatusError(
			"passkey_verify",
			"passkey_verification_failed",
			"Passkey verification returned an unexpected response",
			status,
		)
	}
	if classified.State == LifecycleDead || classified.State == LifecycleInteractionRequired {
		return classified
	}
	if classified.Retryable {
		classified.Code = "passkey_verification_failed"
		classified.Message = "Passkey verification service is temporarily unavailable"
		return classified
	}
	code, message := responseError(payload)
	normalized := strings.ToLower(code + " " + message)
	resultCode := "passkey_verification_failed"
	if strings.Contains(normalized, "not_allowed") || strings.Contains(normalized, "not allowed") || strings.Contains(normalized, "credential") {
		resultCode = "passkey_credential_not_allowed"
	}
	authError := passkeyCredentialError(resultCode, "Passkey credential was rejected", nil)
	authError.Status = status
	authError.StatusCode = status
	authError.FailureStage = "passkey_verify"
	return authError
}

func classifyPasskeyChallengeResponse(status int, payload []byte, pendingState LifecycleState) *AuthError {
	return classifyPasskeyProtocolResponse(
		"passkey_challenge",
		"passkey_challenge_unavailable",
		"Passkey challenge could not be acquired",
		status,
		payload,
		pendingState,
	)
}

func classifyPasskeyProtocolResponse(stage, code, message string, status int, payload []byte, defaultState LifecycleState) *AuthError {
	if status == http.StatusOK {
		return nil
	}
	classified := classifyHTTPResponse(stage, status, payload, defaultState)
	if classified == nil {
		return passkeyProtocolStatusError(stage, code, message, status)
	}
	if classified.State == LifecycleDead || classified.State == LifecycleInteractionRequired {
		return classified
	}
	if classified.Retryable {
		classified.Code = code
		classified.Message = message
		return classified
	}
	authError := passkeyCredentialError(code, message, nil)
	authError.Status = status
	authError.StatusCode = status
	authError.FailureStage = stage
	return authError
}

func passkeyProtocolStatusError(stage, code, message string, status int) *AuthError {
	authError := passkeyCredentialError(code, message, nil)
	authError.Status = status
	authError.StatusCode = status
	authError.FailureStage = stage
	return authError
}

func passkeyStatePersistenceError(state LifecycleState, stage string, cause error) *AuthError {
	authError := newAuthError(
		"passkey_state_persist_failed",
		state,
		0,
		true,
		false,
		"Passkey state could not be persisted",
		cause,
	)
	authError.FailureStage = stage
	return authError
}

func passkeyCredentialError(code, message string, cause error) *AuthError {
	authError := newAuthError(code, LifecycleReauthRequired, 0, false, true, message, cause)
	authError.FailureStage = loginFailureStage(code)
	return authError
}

func sameOAuthEndpointOrigin(left, right *url.URL) bool {
	if left == nil || right == nil {
		return false
	}
	return strings.EqualFold(left.Scheme, right.Scheme) && strings.EqualFold(left.Host, right.Host)
}
