package chatgptweb

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
)

type Service struct {
	options Options
}

type LoginService = Service

func NewService(options Options) *Service {
	if strings.TrimSpace(options.AuthBaseURL) == "" {
		options.AuthBaseURL = AuthBaseURL
	}
	if strings.TrimSpace(options.SessionBaseURL) == "" {
		options.SessionBaseURL = SessionBaseURL
	}
	if strings.TrimSpace(options.SentinelBaseURL) == "" {
		options.SentinelBaseURL = defaultSentinelBaseURL
	}
	if strings.TrimSpace(options.RedirectURL) == "" {
		options.RedirectURL = RedirectURL
	}
	if strings.TrimSpace(options.ClientID) == "" {
		options.ClientID = OAuthClientID
	}
	if strings.TrimSpace(options.Audience) == "" {
		options.Audience = AudienceURL
	}
	if options.AcquisitionTimeout == 0 {
		options.AcquisitionTimeout = DefaultAcquisitionTimeout
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	options.Rand = randomReader(options.Rand)
	options.Persona = normalizePersona(options.Persona)
	options.AuthBaseURL = strings.TrimRight(strings.TrimSpace(options.AuthBaseURL), "/")
	options.SessionBaseURL = strings.TrimRight(strings.TrimSpace(options.SessionBaseURL), "/")
	options.SentinelBaseURL = strings.TrimRight(strings.TrimSpace(options.SentinelBaseURL), "/")
	return &Service{options: options}
}

func NewLoginService(options Options) *LoginService {
	return NewService(options)
}

func (service *Service) Login(ctx context.Context, input LoginInput) (*Credential, error) {
	credential := service.loginCredential(input)
	pendingState := LifecycleLoginPending
	if input.Relogin {
		pendingState = LifecycleReloginPending
	}
	service.updateLifecycle(credential, pendingState, "")
	if credential.Email == "" || credential.Password == "" {
		authError := newAuthError("missing_credentials", LifecycleReauthRequired, 0, false, true, "email and password are required", nil)
		service.applyFailure(credential, authError, input.Relogin)
		return credential, authError
	}
	if err := EnsureCredentialRuntimeIDsForURL(credential, service.options.Rand, service.options.AuthBaseURL); err != nil {
		authError := newAuthError("random_generation_failed", pendingState, 0, false, true, "initialize browser identity", err)
		service.applyFailure(credential, authError, input.Relogin)
		return credential, authError
	}

	acquisitionContext, cancel := service.acquisitionContext(ctx)
	defer cancel()
	client, err := NewClient(credential.Persona, input.ProxyURL, credential.Cookies)
	if err != nil {
		authError := newAuthError("client_initialization_failed", pendingState, 0, true, false, "initialize browser client", err)
		service.applyFailure(credential, authError, input.Relogin)
		return credential, authError
	}
	defer client.CloseIdleConnections()

	pkce, state, nonce, err := service.oauthValues()
	if err != nil {
		authError := newAuthError("random_generation_failed", pendingState, 0, false, true, "initialize OAuth request", err)
		service.applyFailure(credential, authError, input.Relogin)
		return credential, authError
	}
	deviceID := strings.TrimSpace(credential.DeviceID)
	if err := client.SetCookie(service.options.AuthBaseURL, "oai-did", deviceID); err != nil {
		authError := newAuthError("cookie_initialization_failed", pendingState, 0, false, true, "initialize device cookie", err)
		service.applyFailure(credential, authError, input.Relogin)
		return credential, authError
	}
	sentinel, err := NewSentinel(client, service.options.SentinelBaseURL, service.options.AuthBaseURL, deviceID, service.options.Rand, service.options.Now)
	if err != nil {
		authError := newAuthError("sentinel_initialization_failed", pendingState, 0, false, true, "initialize sentinel", err)
		service.applyFailure(credential, authError, input.Relogin)
		return credential, authError
	}

	authorizeURL := service.authorizeURL(credential.Email, deviceID, state, nonce, pkce.CodeChallenge)
	response, payload, err := client.DoFollow(acquisitionContext, http.MethodGet, authorizeURL, map[string]string{
		"accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
		"referer":                   redirectOrigin(service.options.RedirectURL) + "/",
		"sec-fetch-dest":            "document",
		"sec-fetch-mode":            "navigate",
		"sec-fetch-site":            "cross-site",
		"sec-fetch-user":            "?1",
		"upgrade-insecure-requests": "1",
	}, nil)
	if err != nil {
		return service.loginFailure(credential, input.Relogin, networkAuthError("authorize_network_error", pendingState, err))
	}
	if authError := classifyHTTPResponse("authorize", response.StatusCode, payload, pendingState); authError != nil {
		return service.loginFailure(credential, input.Relogin, authError)
	}
	authorizeEnvelope := parseAuthorizationEnvelope(response, payload)
	authorizeRequestURL := responseRequestURL(response)
	if authError := classifyPermanentAccountPayload(payload); authError != nil {
		return service.loginFailure(credential, input.Relogin, authError)
	}
	if authError := classifyPageType(authorizeEnvelope.PageType); authError != nil && !isMFAChallenge(authorizeEnvelope.PageType, authorizeEnvelope.ContinueURL) {
		return service.loginFailure(credential, input.Relogin, authError)
	}
	if authorizeRequestURL != "" {
		if code, matched, callbackError := parseOAuthCallback(authorizeRequestURL, service.options.RedirectURL, state); matched {
			if callbackError != nil {
				return service.loginFailure(credential, input.Relogin, callbackError)
			}
			return service.finishLogin(acquisitionContext, client, credential, input.Relogin, code, pkce.CodeVerifier)
		}
	}
	continueAuthorization, navigationError := classifyAuthorizeNavigation(authorizeRequestURL)
	if navigationError != nil && (isPasswordChallenge(authorizeEnvelope.PageType, authorizeEnvelope.ContinueURL) || isMFAChallenge(authorizeEnvelope.PageType, authorizeEnvelope.ContinueURL)) {
		navigationError = nil
	}
	if navigationError != nil {
		return service.loginFailure(credential, input.Relogin, navigationError)
	}
	if continueAuthorization {
		authorizeSentinel, err := sentinel.Token(acquisitionContext, "authorize_continue")
		if err != nil {
			return service.loginFailure(credential, input.Relogin, ensureAuthError(err, pendingState))
		}
		response, payload, err = client.DoJSON(acquisitionContext, false, http.MethodPost,
			service.options.AuthBaseURL+"/api/accounts/authorize/continue",
			service.apiHeaders(deviceID, service.options.AuthBaseURL+"/sign-in", authorizeSentinel),
			map[string]any{
				"username":    map[string]string{"value": credential.Email, "kind": "email"},
				"screen_hint": "login",
			})
		if err != nil {
			return service.loginFailure(credential, input.Relogin, networkAuthError("authorize_continue_network_error", pendingState, err))
		}
		response, payload, authorizationCode, redirectError := service.followAuthorizationRedirects(
			acquisitionContext, client, response, payload, state, pendingState,
		)
		if redirectError != nil {
			return service.loginFailure(credential, input.Relogin, ensureAuthError(redirectError, pendingState))
		}
		if authorizationCode != "" {
			return service.finishLogin(acquisitionContext, client, credential, input.Relogin, authorizationCode, pkce.CodeVerifier)
		}
		if authError := classifyHTTPResponse("authorize_continue", response.StatusCode, payload, pendingState); authError != nil {
			return service.loginFailure(credential, input.Relogin, authError)
		}
		if authError := classifyPermanentAccountPayload(payload); authError != nil {
			return service.loginFailure(credential, input.Relogin, authError)
		}
		if code, matched, callbackError := parseOAuthCallback(responseRequestURL(response), service.options.RedirectURL, state); matched {
			if callbackError != nil {
				return service.loginFailure(credential, input.Relogin, callbackError)
			}
			return service.finishLogin(acquisitionContext, client, credential, input.Relogin, code, pkce.CodeVerifier)
		}
		authorizeEnvelope = parseAuthorizationEnvelope(response, payload)
		if !isMFAChallenge(authorizeEnvelope.PageType, authorizeEnvelope.ContinueURL) {
			if authError := classifyPageType(authorizeEnvelope.PageType); authError != nil {
				return service.loginFailure(credential, input.Relogin, authError)
			}
			if authorizeEnvelope.ContinueURL != "" {
				response, followPayload, authorizationCode, followError := service.openAuthorizationPage(
					acquisitionContext, client, authorizeEnvelope.ContinueURL, state, pendingState,
				)
				if followError != nil {
					return service.loginFailure(credential, input.Relogin, ensureAuthError(followError, pendingState))
				}
				if authorizationCode != "" {
					return service.finishLogin(acquisitionContext, client, credential, input.Relogin, authorizationCode, pkce.CodeVerifier)
				}
				if authError := classifyHTTPResponse("authorize_redirect", response.StatusCode, followPayload, pendingState); authError != nil {
					return service.loginFailure(credential, input.Relogin, authError)
				}
				if authError := classifyPermanentAccountPayload(followPayload); authError != nil {
					return service.loginFailure(credential, input.Relogin, authError)
				}
				authorizeEnvelope = parseAuthorizationEnvelope(response, followPayload)
				if authError := classifyPageType(authorizeEnvelope.PageType); authError != nil && !isMFAChallenge(authorizeEnvelope.PageType, authorizeEnvelope.ContinueURL) {
					return service.loginFailure(credential, input.Relogin, authError)
				}
				if code, matched, callbackError := parseOAuthCallback(responseRequestURL(response), service.options.RedirectURL, state); matched {
					if callbackError != nil {
						return service.loginFailure(credential, input.Relogin, callbackError)
					}
					return service.finishLogin(acquisitionContext, client, credential, input.Relogin, code, pkce.CodeVerifier)
				}
			}
		}
	}
	if isMFAChallenge(authorizeEnvelope.PageType, authorizeEnvelope.ContinueURL) {
		authorizeEnvelope, err = service.verifyTOTPChallenge(acquisitionContext, client, credential, deviceID, authorizeEnvelope)
		if err != nil {
			return service.loginFailure(credential, input.Relogin, ensureAuthError(err, pendingState))
		}
		if authorizeEnvelope.ContinueURL == "" {
			authError := newAuthError("authorization_completion_required", LifecycleInteractionRequired, response.StatusCode, false, true, "MFA verification did not return an OAuth continuation", nil)
			return service.loginFailure(credential, input.Relogin, authError)
		}
		code, followError := service.followOAuthCode(acquisitionContext, client, authorizeEnvelope.ContinueURL, state, pendingState)
		if followError != nil {
			return service.loginFailure(credential, input.Relogin, ensureAuthError(followError, pendingState))
		}
		return service.finishLogin(acquisitionContext, client, credential, input.Relogin, code, pkce.CodeVerifier)
	}

	passwordSentinel, err := sentinel.Token(acquisitionContext, "password_verify")
	if err != nil {
		return service.loginFailure(credential, input.Relogin, ensureAuthError(err, pendingState))
	}
	response, payload, err = client.DoJSON(acquisitionContext, false, http.MethodPost,
		service.options.AuthBaseURL+"/api/accounts/password/verify",
		service.apiHeaders(deviceID, service.options.AuthBaseURL+"/log-in/password", passwordSentinel),
		map[string]string{"password": credential.Password})
	if err != nil {
		return service.loginFailure(credential, input.Relogin, networkAuthError("password_verify_network_error", pendingState, err))
	}
	var redirectCode string
	response, payload, redirectCode, err = service.followPasswordRedirects(
		acquisitionContext,
		client,
		response,
		payload,
		credential.Password,
		deviceID,
		passwordSentinel,
		state,
		pendingState,
	)
	if err != nil {
		return service.loginFailure(credential, input.Relogin, ensureAuthError(err, pendingState))
	}
	if redirectCode != "" {
		return service.finishLogin(acquisitionContext, client, credential, input.Relogin, redirectCode, pkce.CodeVerifier)
	}
	if authError := classifyHTTPResponse("password_verify", response.StatusCode, payload, pendingState); authError != nil {
		return service.loginFailure(credential, input.Relogin, authError)
	}
	if authError := classifyPermanentAccountPayload(payload); authError != nil {
		return service.loginFailure(credential, input.Relogin, authError)
	}
	passwordEnvelope := parseAuthorizationEnvelope(response, payload)
	if isMFAChallenge(passwordEnvelope.PageType, passwordEnvelope.ContinueURL) {
		passwordEnvelope, err = service.verifyTOTPChallenge(acquisitionContext, client, credential, deviceID, passwordEnvelope)
		if err != nil {
			return service.loginFailure(credential, input.Relogin, ensureAuthError(err, pendingState))
		}
	} else if authError := classifyPageType(passwordEnvelope.PageType); authError != nil {
		return service.loginFailure(credential, input.Relogin, authError)
	}
	if passwordEnvelope.ContinueURL == "" {
		authError := newAuthError("authorization_completion_required", LifecycleInteractionRequired, response.StatusCode, false, true, "password verification did not return an OAuth continuation", nil)
		return service.loginFailure(credential, input.Relogin, authError)
	}

	code, err := service.followOAuthCode(acquisitionContext, client, passwordEnvelope.ContinueURL, state, pendingState)
	if err != nil {
		return service.loginFailure(credential, input.Relogin, ensureAuthError(err, pendingState))
	}
	return service.finishLogin(acquisitionContext, client, credential, input.Relogin, code, pkce.CodeVerifier)
}

func (service *Service) Refresh(ctx context.Context, credential Credential, proxyURL string) (*Credential, error) {
	refreshed := cloneCredential(&credential)
	refreshed.Type = Provider
	refreshed.Persona = normalizePersona(refreshed.Persona)
	if strings.TrimSpace(refreshed.RefreshToken) == "" {
		authError := newAuthError("refresh_token_missing", LifecycleReauthRequired, 0, false, true, "refresh token is required", nil)
		service.applyFailure(refreshed, authError, false)
		return refreshed, authError
	}

	service.updateLifecycle(refreshed, LifecycleRefreshing, "")
	if err := EnsureCredentialRuntimeIDsForURL(refreshed, service.options.Rand, service.options.AuthBaseURL); err != nil {
		authError := newAuthError("random_generation_failed", LifecycleActive, 0, false, true, "initialize browser identity", err)
		service.applyFailure(refreshed, authError, false)
		return refreshed, authError
	}
	acquisitionContext, cancel := service.acquisitionContext(ctx)
	defer cancel()
	client, err := NewClient(refreshed.Persona, proxyURL, refreshed.Cookies)
	if err != nil {
		authError := newAuthError("client_initialization_failed", LifecycleActive, 0, true, false, "initialize browser client", err)
		service.applyFailure(refreshed, authError, false)
		return refreshed, authError
	}
	defer client.CloseIdleConnections()
	if err := client.SetCookie(service.options.AuthBaseURL, "oai-did", refreshed.DeviceID); err != nil {
		authError := newAuthError("cookie_initialization_failed", LifecycleActive, 0, false, true, "initialize device cookie", err)
		service.applyFailure(refreshed, authError, false)
		return refreshed, authError
	}

	form := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {refreshed.RefreshToken},
		"client_id":     {service.options.ClientID},
	}
	response, payload, err := client.DoNoRedirect(acquisitionContext, http.MethodPost,
		service.options.AuthBaseURL+"/oauth/token",
		map[string]string{
			"accept":       "application/json",
			"content-type": "application/x-www-form-urlencoded",
		}, strings.NewReader(form.Encode()))
	if err != nil {
		authError := networkAuthError("token_refresh_network_error", LifecycleActive, err)
		service.applyFailure(refreshed, authError, false)
		return refreshed, authError
	}
	if authError := classifyHTTPResponse("token_refresh", response.StatusCode, payload, LifecycleActive); authError != nil {
		service.applyFailure(refreshed, authError, false)
		return refreshed, authError
	}
	var tokenResponse tokenPayload
	if err := json.Unmarshal(payload, &tokenResponse); err != nil {
		authError := newAuthError("token_response_invalid", LifecycleActive, response.StatusCode, true, false, "refresh endpoint returned invalid JSON", err)
		service.applyFailure(refreshed, authError, false)
		return refreshed, authError
	}
	if strings.TrimSpace(tokenResponse.AccessToken) == "" {
		authError := newAuthError("access_token_missing", LifecycleReauthRequired, response.StatusCode, false, true, "refresh endpoint did not return an access token", nil)
		service.applyFailure(refreshed, authError, false)
		return refreshed, authError
	}
	incomingIdentity := &Credential{
		AccessToken: strings.TrimSpace(tokenResponse.AccessToken),
		IDToken:     strings.TrimSpace(tokenResponse.IDToken),
	}
	PopulateCredentialIdentity(incomingIdentity)
	if credentialIdentityConflicts(&credential, incomingIdentity) {
		authError := newAuthError("identity_conflict", LifecycleReauthRequired, http.StatusConflict, false, true, "refreshed credential belongs to a different account", nil)
		service.applyFailure(refreshed, authError, false)
		return refreshed, authError
	}
	refreshed.AccessToken = strings.TrimSpace(tokenResponse.AccessToken)
	if strings.TrimSpace(tokenResponse.RefreshToken) != "" {
		refreshed.RefreshToken = strings.TrimSpace(tokenResponse.RefreshToken)
	}
	if strings.TrimSpace(tokenResponse.IDToken) != "" {
		refreshed.IDToken = strings.TrimSpace(tokenResponse.IDToken)
	}
	refreshed.Expired = tokenExpiryString(refreshed.AccessToken, refreshed.IDToken)
	PopulateCredentialIdentity(refreshed)
	refreshed.Cookies = client.ExportCookies()
	refreshed.LastRefreshAt = service.timestamp()
	service.updateLifecycle(refreshed, LifecycleActive, "")
	return refreshed, nil
}

// RefreshSession exchanges a persisted ChatGPT session cookie for a current
// access token without changing the browser identity or proxy.
func (service *Service) RefreshSession(ctx context.Context, credential Credential, proxyURL string) (*Credential, error) {
	refreshed := cloneCredential(&credential)
	refreshed.Type = Provider
	refreshed.RefreshStrategy = RefreshStrategyChatGPTSession
	refreshed.CredentialMode = CredentialModeNative
	refreshed.Persona = normalizePersona(refreshed.Persona)
	refreshed.Cookies = scopeUnscopedCookiesForURL(refreshed.Cookies, service.options.SessionBaseURL)
	if !HasSessionCookieForURL(refreshed.Cookies, service.options.SessionBaseURL) {
		authError := newAuthError("session_cookie_missing", LifecycleReauthRequired, 0, false, true, "chatgpt session cookie is required", nil)
		service.applyFailure(refreshed, authError, false)
		return refreshed, authError
	}

	service.updateLifecycle(refreshed, LifecycleRefreshing, "")
	if err := EnsureCredentialRuntimeIDsForURL(refreshed, service.options.Rand, service.options.SessionBaseURL); err != nil {
		authError := newAuthError("random_generation_failed", LifecycleActive, 0, false, true, "initialize browser identity", err)
		service.applyFailure(refreshed, authError, false)
		return refreshed, authError
	}
	acquisitionContext, cancel := service.acquisitionContext(ctx)
	defer cancel()
	client, err := NewClient(refreshed.Persona, proxyURL, refreshed.Cookies)
	if err != nil {
		authError := newAuthError("client_initialization_failed", LifecycleActive, 0, true, false, "initialize browser client", err)
		service.applyFailure(refreshed, authError, false)
		return refreshed, authError
	}
	defer client.CloseIdleConnections()
	if err = client.SetCookie(service.options.SessionBaseURL, "oai-did", refreshed.DeviceID); err != nil {
		authError := newAuthError("cookie_initialization_failed", LifecycleActive, 0, false, true, "initialize device cookie", err)
		service.applyFailure(refreshed, authError, false)
		return refreshed, authError
	}

	response, payload, err := client.DoNoRedirect(acquisitionContext, http.MethodGet,
		service.options.SessionBaseURL+"/api/auth/session?refresh=true",
		map[string]string{
			"accept":         "application/json",
			"referer":        service.options.SessionBaseURL + "/",
			"sec-fetch-dest": "empty",
			"sec-fetch-mode": "cors",
			"sec-fetch-site": "same-origin",
		}, nil)
	if err != nil {
		authError := networkAuthError("session_refresh_network_error", LifecycleActive, err)
		service.applyFailure(refreshed, authError, false)
		return refreshed, authError
	}
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden ||
		(response.StatusCode >= http.StatusMultipleChoices && response.StatusCode < http.StatusBadRequest) {
		authError := newAuthError("session_expired", LifecycleReauthRequired, response.StatusCode, false, true, "chatgpt session must be renewed", nil)
		service.applyFailure(refreshed, authError, false)
		return refreshed, authError
	}
	if authError := classifyHTTPResponse("session_refresh", response.StatusCode, payload, LifecycleActive); authError != nil {
		service.applyFailure(refreshed, authError, false)
		return refreshed, authError
	}
	var session sessionPayload
	if err = json.Unmarshal(payload, &session); err != nil {
		authError := newAuthError("session_response_invalid", LifecycleActive, response.StatusCode, true, false, "session endpoint returned invalid JSON", err)
		service.applyFailure(refreshed, authError, false)
		return refreshed, authError
	}
	if strings.TrimSpace(session.AccessToken) == "" {
		authError := newAuthError("access_token_missing", LifecycleReauthRequired, response.StatusCode, false, true, "session endpoint did not return an access token", nil)
		service.applyFailure(refreshed, authError, false)
		return refreshed, authError
	}
	incomingIdentity := &Credential{
		Email:       strings.TrimSpace(session.User.Email),
		AccountID:   strings.TrimSpace(session.Account.ID),
		UserID:      strings.TrimSpace(session.User.ID),
		AccessToken: strings.TrimSpace(session.AccessToken),
	}
	PopulateCredentialIdentity(incomingIdentity)
	if credentialIdentityConflicts(&credential, incomingIdentity) {
		authError := newAuthError("identity_conflict", LifecycleReauthRequired, http.StatusConflict, false, true, "chatgpt session belongs to a different account", nil)
		service.applyFailure(refreshed, authError, false)
		return refreshed, authError
	}
	refreshed.AccessToken = strings.TrimSpace(session.AccessToken)
	refreshed.Expired = tokenExpiryString(refreshed.AccessToken)
	if refreshed.Expired == "" {
		refreshed.Expired = strings.TrimSpace(session.Expires)
	}
	if email := strings.TrimSpace(session.User.Email); email != "" {
		refreshed.Email = email
	}
	if accountID := strings.TrimSpace(session.Account.ID); accountID != "" {
		refreshed.AccountID = accountID
	}
	if userID := strings.TrimSpace(session.User.ID); userID != "" {
		refreshed.UserID = userID
	}
	if planType := strings.TrimSpace(session.Account.PlanType); planType != "" {
		refreshed.PlanType = planType
	}
	refreshed.Cookies = client.ExportCookies()
	refreshed.LastRefreshAt = service.timestamp()
	service.updateLifecycle(refreshed, LifecycleActive, "")
	return refreshed, nil
}

type credentialIdentity struct {
	accountID string
	userID    string
	subject   string
	email     string
}

func credentialIdentityEvidence(credential *Credential) []credentialIdentity {
	if credential == nil {
		return nil
	}
	evidence := make([]credentialIdentity, 0, 3)
	explicit := credentialIdentity{
		accountID: strings.TrimSpace(credential.AccountID),
		userID:    strings.TrimSpace(credential.UserID),
		email:     NormalizeEmail(credential.Email),
	}
	if !credentialIdentityEmpty(explicit) {
		evidence = append(evidence, explicit)
	}
	for _, token := range []string{credential.AccessToken, credential.IDToken} {
		claims := jwtClaims(token)
		if len(claims) == 0 {
			continue
		}
		authClaims, _ := claims["https://api.openai.com/auth"].(map[string]any)
		identity := credentialIdentity{
			accountID: firstStringValue(authClaims, "chatgpt_account_id", "account_id"),
			userID:    firstStringValue(authClaims, "chatgpt_user_id", "user_id"),
			subject:   strings.TrimSpace(stringValue(claims["sub"])),
			email:     NormalizeEmail(stringValue(claims["email"])),
		}
		if identity.accountID == "" {
			identity.accountID = firstStringValue(claims, "chatgpt_account_id", "account_id")
		}
		if identity.userID == "" {
			identity.userID = firstStringValue(claims, "chatgpt_user_id", "user_id")
		}
		if !credentialIdentityEmpty(identity) {
			evidence = append(evidence, identity)
		}
	}
	return evidence
}

func credentialIdentityEmpty(identity credentialIdentity) bool {
	return identity.accountID == "" && identity.userID == "" && identity.subject == "" && identity.email == ""
}

func credentialIdentityEvidenceConflicts(current, next credentialIdentity) bool {
	if current.accountID != "" && next.accountID != "" {
		if current.accountID != next.accountID {
			return true
		}
		return (current.userID != "" && next.userID != "" && current.userID != next.userID) ||
			(current.subject != "" && next.subject != "" && current.subject != next.subject)
	}
	return (current.userID != "" && next.userID != "" && current.userID != next.userID) ||
		(current.subject != "" && next.subject != "" && current.subject != next.subject) ||
		(current.email != "" && next.email != "" && current.email != next.email)
}

func credentialIdentitySetConflicts(evidence []credentialIdentity) bool {
	for i := range evidence {
		for j := i + 1; j < len(evidence); j++ {
			if credentialIdentityEvidenceConflicts(evidence[i], evidence[j]) {
				return true
			}
		}
	}
	return false
}

func credentialIdentityConflicts(existing, incoming *Credential) bool {
	currentEvidence := credentialIdentityEvidence(existing)
	nextEvidence := credentialIdentityEvidence(incoming)
	if credentialIdentitySetConflicts(currentEvidence) || credentialIdentitySetConflicts(nextEvidence) {
		return true
	}
	for _, current := range currentEvidence {
		for _, next := range nextEvidence {
			if credentialIdentityEvidenceConflicts(current, next) {
				return true
			}
		}
	}
	return false
}

// EnsureCredentialRuntimeIDs restores or creates the stable browser identity
// fields required by login, refresh, and runtime requests.
func EnsureCredentialRuntimeIDs(credential *Credential, reader io.Reader) error {
	return EnsureCredentialRuntimeIDsForURL(credential, reader, AuthBaseURL)
}

// EnsureCredentialRuntimeIDsForURL restores a scoped device cookie or creates
// the stable browser identity required for the supplied ChatGPT origin.
func EnsureCredentialRuntimeIDsForURL(credential *Credential, reader io.Reader, rawURL string) error {
	if credential == nil {
		return nil
	}
	if strings.TrimSpace(credential.DeviceID) == "" {
		deviceID, err := credentialCookieValueForURL(credential.Cookies, rawURL, "oai-did")
		if err != nil {
			return err
		}
		credential.DeviceID = deviceID
	}
	var err error
	if strings.TrimSpace(credential.DeviceID) == "" {
		credential.DeviceID, err = GenerateDeviceID(reader)
		if err != nil {
			return err
		}
	}
	if strings.TrimSpace(credential.SessionID) == "" {
		credential.SessionID, err = GenerateDeviceID(reader)
		if err != nil {
			return err
		}
	}
	return nil
}

func (service *Service) finishLogin(ctx context.Context, client *Client, credential *Credential, relogin bool, code, verifier string) (*Credential, error) {
	tokens, authError := service.exchangeCode(ctx, client, code, verifier)
	if authError != nil {
		return service.loginFailure(credential, relogin, authError)
	}
	credential.AccessToken = tokens.AccessToken
	credential.RefreshToken = tokens.RefreshToken
	credential.IDToken = tokens.IDToken
	credential.RefreshStrategy = RefreshStrategyWebOAuthRT
	credential.CredentialMode = CredentialModeNative
	credential.Expired = tokenExpiryString(tokens.AccessToken, tokens.IDToken)
	PopulateCredentialIdentity(credential)
	credential.Cookies = client.ExportCookies()
	credential.Persona = client.Persona()
	now := service.timestamp()
	credential.LastLoginAt = now
	if relogin {
		credential.LastReloginAt = now
	}
	service.updateLifecycle(credential, LifecycleActive, "")
	return credential, nil
}

type tokenPayload struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	IDToken      string `json:"id_token"`
}

type sessionPayload struct {
	AccessToken string `json:"accessToken"`
	Expires     string `json:"expires"`
	User        struct {
		ID    string `json:"id"`
		Email string `json:"email"`
	} `json:"user"`
	Account struct {
		ID       string `json:"id"`
		PlanType string `json:"planType"`
	} `json:"account"`
}

func (service *Service) exchangeCode(ctx context.Context, client *Client, code, verifier string) (tokenPayload, *AuthError) {
	response, payload, err := client.DoJSON(ctx, false, http.MethodPost,
		service.options.AuthBaseURL+"/api/accounts/oauth/token",
		map[string]string{
			"accept":         "application/json",
			"auth0-client":   auth0Client,
			"content-type":   "application/json",
			"origin":         redirectOrigin(service.options.RedirectURL),
			"referer":        redirectOrigin(service.options.RedirectURL) + "/",
			"sec-fetch-dest": "empty",
			"sec-fetch-mode": "cors",
			"sec-fetch-site": "same-site",
		}, map[string]string{
			"client_id":     service.options.ClientID,
			"code_verifier": verifier,
			"grant_type":    "authorization_code",
			"code":          code,
			"redirect_uri":  service.options.RedirectURL,
		})
	if err != nil {
		return tokenPayload{}, networkAuthError("token_exchange_network_error", LifecycleLoginPending, err)
	}
	if authError := classifyHTTPResponse("token_exchange", response.StatusCode, payload, LifecycleLoginPending); authError != nil {
		return tokenPayload{}, authError
	}
	var tokens tokenPayload
	if err := json.Unmarshal(payload, &tokens); err != nil {
		return tokenPayload{}, newAuthError("token_response_invalid", LifecycleReauthRequired, response.StatusCode, false, true, "token endpoint returned invalid JSON", err)
	}
	tokens.AccessToken = strings.TrimSpace(tokens.AccessToken)
	tokens.RefreshToken = strings.TrimSpace(tokens.RefreshToken)
	tokens.IDToken = strings.TrimSpace(tokens.IDToken)
	if tokens.AccessToken == "" || tokens.RefreshToken == "" {
		return tokenPayload{}, newAuthError("token_response_incomplete", LifecycleReauthRequired, response.StatusCode, false, true, "token endpoint returned an incomplete credential", nil)
	}
	return tokens, nil
}

func (service *Service) followOAuthCode(ctx context.Context, client *Client, startURL, expectedState string, transientState LifecycleState) (string, error) {
	currentURL := resolveURL(service.options.AuthBaseURL, startURL)
	for range 10 {
		if code, matched, authError := parseOAuthCallback(currentURL, service.options.RedirectURL, expectedState); matched {
			if authError != nil {
				return "", authError
			}
			return code, nil
		}
		if authError := classifyOAuthContinuationURL(currentURL, service.options.AuthBaseURL); authError != nil {
			return "", authError
		}
		response, payload, err := client.DoNoRedirect(ctx, http.MethodGet, currentURL,
			map[string]string{"referer": service.options.AuthBaseURL + "/log-in/password"}, nil)
		if err != nil {
			return "", networkAuthError("oauth_redirect_network_error", transientState, err)
		}
		if response.StatusCode >= http.StatusBadRequest {
			if authError := classifyHTTPResponse("oauth_redirect", response.StatusCode, payload, transientState); authError != nil {
				return "", authError
			}
		}
		location := strings.TrimSpace(response.Header.Get("Location"))
		if response.StatusCode >= 300 && response.StatusCode < 400 && location != "" {
			currentURL = resolveURL(currentURL, location)
			continue
		}
		envelope := parseAPIEnvelope(payload)
		if authError := classifyPagePayload(payload); authError != nil {
			return "", authError
		}
		if envelope.ContinueURL != "" {
			currentURL = resolveURL(currentURL, envelope.ContinueURL)
			continue
		}
		return "", newAuthError("authorization_completion_required", LifecycleInteractionRequired, response.StatusCode, false, true, "OAuth redirect did not reach the callback", nil)
	}
	return "", newAuthError("oauth_redirect_limit", transientState, 0, true, false, "OAuth redirect limit exceeded", nil)
}

func (service *Service) openAuthorizationPage(ctx context.Context, client *Client, startURL, expectedState string, transientState LifecycleState) (*fhttp.Response, []byte, string, error) {
	targetURL := resolveURL(service.options.AuthBaseURL, startURL)
	if code, matched, authError := parseOAuthCallback(targetURL, service.options.RedirectURL, expectedState); matched {
		if authError != nil {
			return nil, nil, "", authError
		}
		return nil, nil, code, nil
	}
	if authError := validateOAuthContinuationOrigin(targetURL, service.options.AuthBaseURL); authError != nil {
		return nil, nil, "", authError
	}
	response, payload, err := client.DoNoRedirect(ctx, http.MethodGet, targetURL, map[string]string{
		"referer": service.options.AuthBaseURL + "/sign-in",
	}, nil)
	if err != nil {
		return nil, nil, "", networkAuthError("authorize_redirect_network_error", transientState, err)
	}
	return service.followAuthorizationRedirects(ctx, client, response, payload, expectedState, transientState)
}

func (service *Service) followAuthorizationRedirects(ctx context.Context, client *Client, response *fhttp.Response, payload []byte, expectedState string, transientState LifecycleState) (*fhttp.Response, []byte, string, error) {
	var replayBody []byte
	hasReplayBody := false
	for redirects := 0; response != nil && isChatGPTWebRedirectStatus(response.StatusCode); redirects++ {
		if redirects >= 10 {
			return response, payload, "", newAuthError("oauth_redirect_limit", transientState, response.StatusCode, true, false, "OAuth redirect limit exceeded", nil)
		}
		currentURL := responseRequestURL(response)
		location := strings.TrimSpace(response.Header.Get("Location"))
		if location == "" {
			return response, payload, "", newAuthError("authorization_completion_required", LifecycleInteractionRequired, response.StatusCode, false, true, "authorization redirect did not provide a destination", nil)
		}
		nextURL := resolveURL(currentURL, location)
		if code, matched, authError := parseOAuthCallback(nextURL, service.options.RedirectURL, expectedState); matched {
			if authError != nil {
				return response, payload, "", authError
			}
			return response, payload, code, nil
		}
		if authError := validateOAuthContinuationOrigin(nextURL, service.options.AuthBaseURL); authError != nil {
			return response, payload, "", authError
		}
		method := http.MethodGet
		headers := map[string]string{"referer": currentURL}
		var body io.Reader
		if response.StatusCode == http.StatusTemporaryRedirect || response.StatusCode == http.StatusPermanentRedirect {
			request := response.Request
			if request != nil && request.Method != "" {
				method = request.Method
			}
			if method != http.MethodGet && method != http.MethodHead {
				if request != nil && request.GetBody != nil {
					replayReader, errBody := request.GetBody()
					if errBody != nil {
						return response, payload, "", newAuthError("oauth_redirect_body_unavailable", transientState, response.StatusCode, false, true, "authorization redirect could not safely replay its request body", errBody)
					}
					replayBody, errBody = io.ReadAll(replayReader)
					if errClose := replayReader.Close(); errBody == nil {
						errBody = errClose
					}
					if errBody != nil {
						return response, payload, "", newAuthError("oauth_redirect_body_unavailable", transientState, response.StatusCode, false, true, "authorization redirect could not safely replay its request body", errBody)
					}
					hasReplayBody = true
				} else if !hasReplayBody {
					return response, payload, "", newAuthError("oauth_redirect_body_unavailable", transientState, response.StatusCode, false, true, "authorization redirect could not safely replay its request body", nil)
				}
				body = bytes.NewReader(replayBody)
				headers = authorizationReplayHeaders(request.Header, currentURL)
			}
		}
		var err error
		response, payload, err = client.DoNoRedirect(ctx, method, nextURL, headers, body)
		if err != nil {
			return response, payload, "", networkAuthError("authorize_redirect_network_error", transientState, err)
		}
	}
	return response, payload, "", nil
}

func authorizationReplayHeaders(source fhttp.Header, referer string) map[string]string {
	headers := make(map[string]string, len(source)+1)
	for key, values := range source {
		normalized := strings.ToLower(strings.TrimSpace(key))
		if key == fhttp.HeaderOrderKey {
			continue
		}
		switch normalized {
		case "", "host", "cookie", "content-length", "transfer-encoding":
			continue
		}
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}
	headers["referer"] = referer
	return headers
}

func (service *Service) followPasswordRedirects(
	ctx context.Context,
	client *Client,
	response *fhttp.Response,
	payload []byte,
	password, deviceID, sentinelToken, expectedState string,
	transientState LifecycleState,
) (*fhttp.Response, []byte, string, error) {
	method := http.MethodPost
	for redirects := 0; response != nil && isChatGPTWebRedirectStatus(response.StatusCode); redirects++ {
		if redirects >= 10 {
			return response, payload, "", newAuthError("oauth_redirect_limit", transientState, response.StatusCode, true, false, "OAuth redirect limit exceeded", nil)
		}
		currentURL := responseRequestURL(response)
		location := strings.TrimSpace(response.Header.Get("Location"))
		if location == "" {
			return response, payload, "", newAuthError("authorization_completion_required", LifecycleInteractionRequired, response.StatusCode, false, true, "password redirect did not provide a destination", nil)
		}
		nextURL := resolveURL(currentURL, location)
		if code, matched, callbackError := parseOAuthCallback(nextURL, service.options.RedirectURL, expectedState); matched {
			if callbackError != nil {
				return response, payload, "", callbackError
			}
			return response, payload, code, nil
		}
		if authError := validateOAuthContinuationOrigin(nextURL, service.options.AuthBaseURL); authError != nil {
			return response, payload, "", authError
		}

		if response.StatusCode != http.StatusTemporaryRedirect && response.StatusCode != http.StatusPermanentRedirect {
			method = http.MethodGet
		}
		var err error
		if method == http.MethodPost {
			response, payload, err = client.DoJSON(ctx, false, http.MethodPost, nextURL,
				service.apiHeaders(deviceID, currentURL, sentinelToken),
				map[string]string{"password": password})
		} else {
			response, payload, err = client.DoNoRedirect(ctx, http.MethodGet, nextURL,
				map[string]string{"referer": currentURL}, nil)
		}
		if err != nil {
			return response, payload, "", networkAuthError("oauth_redirect_network_error", transientState, err)
		}
	}
	return response, payload, "", nil
}

func (service *Service) authorizeURL(email, deviceID, state, nonce, challenge string) string {
	parameters := url.Values{
		"issuer":                {service.options.AuthBaseURL},
		"client_id":             {service.options.ClientID},
		"audience":              {service.options.Audience},
		"redirect_uri":          {service.options.RedirectURL},
		"device_id":             {deviceID},
		"screen_hint":           {"login_or_signup"},
		"max_age":               {"0"},
		"login_hint":            {email},
		"scope":                 {"openid profile email offline_access"},
		"response_type":         {"code"},
		"response_mode":         {"query"},
		"state":                 {state},
		"nonce":                 {nonce},
		"code_challenge":        {challenge},
		"code_challenge_method": {"S256"},
		"auth0Client":           {auth0Client},
	}
	return service.options.AuthBaseURL + "/api/accounts/authorize?" + parameters.Encode()
}

func (service *Service) apiHeaders(deviceID, referer, sentinelToken string) map[string]string {
	return map[string]string{
		"accept":                "application/json",
		"content-type":          "application/json",
		"oai-device-id":         deviceID,
		"openai-sentinel-token": sentinelToken,
		"origin":                service.options.AuthBaseURL,
		"referer":               referer,
		"sec-fetch-dest":        "empty",
		"sec-fetch-mode":        "cors",
		"sec-fetch-site":        "same-origin",
	}
}

func (service *Service) oauthValues() (PKCE, string, string, error) {
	pkce, err := GeneratePKCE(service.options.Rand)
	if err != nil {
		return PKCE{}, "", "", err
	}
	state, err := GenerateState(service.options.Rand)
	if err != nil {
		return PKCE{}, "", "", err
	}
	nonce, err := GenerateNonce(service.options.Rand)
	if err != nil {
		return PKCE{}, "", "", err
	}
	return pkce, state, nonce, nil
}

func (service *Service) acquisitionContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if service.options.AcquisitionTimeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, service.options.AcquisitionTimeout)
}

func (service *Service) loginCredential(input LoginInput) *Credential {
	credential := cloneCredential(input.Credential)
	credential.Type = Provider
	if strings.TrimSpace(input.Email) != "" {
		credential.Email = strings.TrimSpace(input.Email)
	}
	if input.Password != "" {
		credential.Password = input.Password
	}
	if strings.TrimSpace(input.TOTPSecret) != "" {
		credential.TOTPSecret = strings.TrimSpace(input.TOTPSecret)
	}
	if strings.TrimSpace(credential.Persona.Profile) == "" {
		credential.Persona = service.options.Persona
	}
	credential.Persona = normalizePersona(credential.Persona)
	if credential.Cookies == nil {
		credential.Cookies = []Cookie{}
	}
	return credential
}

func cloneCredential(source *Credential) *Credential {
	if source == nil {
		return &Credential{Type: Provider, Cookies: []Cookie{}}
	}
	clone := *source
	clone.Cookies = append([]Cookie(nil), source.Cookies...)
	return &clone
}

func (service *Service) loginFailure(credential *Credential, relogin bool, authError *AuthError) (*Credential, error) {
	service.applyFailure(credential, authError, relogin)
	return credential, authError
}

func (service *Service) applyFailure(credential *Credential, authError *AuthError, relogin bool) {
	if credential == nil || authError == nil {
		return
	}
	state := authError.State
	if relogin && state == LifecycleLoginPending {
		state = LifecycleReloginPending
		authError.State = state
		authError.LifecycleState = state
	}
	service.updateLifecycle(credential, state, authError.Code)
}

func (service *Service) updateLifecycle(credential *Credential, state LifecycleState, reason string) {
	credential.LifecycleState = state
	credential.LifecycleReason = reason
	credential.LifecycleUpdatedAt = service.timestamp()
}

func (service *Service) timestamp() string {
	return service.options.Now().UTC().Format(time.RFC3339)
}

func ensureAuthError(err error, defaultState LifecycleState) *AuthError {
	if authError, ok := AsAuthError(err); ok {
		return authError
	}
	return networkAuthError("authentication_network_error", defaultState, err)
}

func networkAuthError(code string, state LifecycleState, err error) *AuthError {
	message := "network request failed"
	if errors.Is(err, context.DeadlineExceeded) {
		code = "acquisition_deadline_exceeded"
		message = "authentication acquisition deadline exceeded"
	} else if errors.Is(err, context.Canceled) {
		code = "acquisition_canceled"
		message = "authentication acquisition was canceled"
	}
	return newAuthError(code, state, 0, true, false, message, err)
}

type apiEnvelope struct {
	ContinueURL string
	PageType    string
	Payload     map[string]any
}

func parseAPIEnvelope(payload []byte) apiEnvelope {
	var decoded map[string]any
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(&decoded); err != nil {
		return apiEnvelope{}
	}
	envelope := apiEnvelope{ContinueURL: stringValue(decoded["continue_url"])}
	if page, ok := decoded["page"].(map[string]any); ok {
		envelope.PageType = stringValue(page["type"])
		if payload, okPayload := page["payload"].(map[string]any); okPayload {
			envelope.Payload = payload
		}
	}
	if envelope.Payload == nil {
		envelope.Payload = decoded
	} else {
		mergeMFAHints(envelope.Payload, decoded)
	}
	return envelope
}

func parseAuthorizationEnvelope(response *fhttp.Response, payload []byte) apiEnvelope {
	envelope := parseAPIEnvelope(payload)
	if challenge, ok := mfaChallengeFromURL(envelope.ContinueURL); ok {
		if envelope.Payload == nil {
			envelope.Payload = make(map[string]any)
		}
		mergeMFAHints(envelope.Payload, challenge.Payload)
		if strings.TrimSpace(envelope.PageType) == "" {
			envelope.PageType = challenge.PageType
		}
	}
	requestURL := responseRequestURL(response)
	if challenge, ok := mfaChallengeFromURL(requestURL); ok {
		if envelope.Payload == nil {
			envelope.Payload = challenge.Payload
		} else {
			mergeMFAHints(envelope.Payload, challenge.Payload)
		}
		if strings.TrimSpace(envelope.ContinueURL) == "" {
			if strings.TrimSpace(envelope.PageType) == "" {
				envelope.ContinueURL = challenge.ContinueURL
				envelope.PageType = challenge.PageType
			} else if isMFAChallenge(envelope.PageType, "") {
				envelope.ContinueURL = challenge.ContinueURL
			}
		}
		return envelope
	}
	if strings.TrimSpace(envelope.PageType) == "" && strings.TrimSpace(envelope.ContinueURL) == "" {
		parsedURL, err := url.Parse(requestURL)
		if err != nil {
			return envelope
		}
		if authError := classifyPageType(parsedURL.Path); authError != nil {
			envelope.PageType = parsedURL.Path
			envelope.ContinueURL = requestURL
		}
	}
	return envelope
}

func mergeMFAHints(target, source map[string]any) {
	if target == nil || source == nil {
		return
	}
	for _, key := range []string{
		"mfa_request_id", "mfaRequestId", "factor_id", "factorId", "factor_type", "factorType",
		"factors", "mfa_factors", "mfa_challenge_factors",
		"oai-client-auth-session", "client_auth_session", "auth_session",
	} {
		if _, exists := target[key]; exists {
			continue
		}
		if value, found := source[key]; found {
			target[key] = value
		}
	}
	factorType := firstNonEmptyString(stringValue(source["factor_type"]), stringValue(source["factorType"]), stringValue(source["type"]))
	if isMFAFactorType(factorType) {
		for _, key := range []string{"id", "type"} {
			if _, exists := target[key]; exists {
				continue
			}
			if value, found := source[key]; found {
				target[key] = value
			}
		}
	}
}

func isMFAFactorType(value string) bool {
	return normalizeMFAFactorType(value) != ""
}

func (service *Service) verifyTOTPChallenge(ctx context.Context, client *Client, credential *Credential, deviceID string, challenge apiEnvelope) (apiEnvelope, error) {
	factorID, requestID := selectMFAFactor(challenge.Payload, "totp")
	if factorID == "" {
		return apiEnvelope{}, unsupportedMFAError(challenge.Payload)
	}
	if strings.TrimSpace(credential.TOTPSecret) == "" {
		return apiEnvelope{}, newAuthError("totp_required", LifecycleInteractionRequired, 0, false, true, "authentication requires a TOTP secret", nil)
	}
	code, err := GenerateTOTP(credential.TOTPSecret, service.options.Now())
	if err != nil {
		return apiEnvelope{}, newAuthError("invalid_totp_secret", LifecycleReauthRequired, 0, false, true, "TOTP secret is invalid", err)
	}
	body := map[string]any{
		"id":   factorID,
		"type": "totp",
		"code": code,
	}
	if requestID != "" {
		body["mfa_request_id"] = requestID
	}
	referer := resolveURL(service.options.AuthBaseURL, challenge.ContinueURL)
	if strings.TrimSpace(challenge.ContinueURL) == "" {
		referer = service.options.AuthBaseURL + "/mfa-challenge/" + url.PathEscape(factorID)
	}
	targetURL := service.options.AuthBaseURL + "/api/accounts/mfa/verify"
	var response *fhttp.Response
	var payload []byte
	for redirects := 0; ; redirects++ {
		response, payload, err = client.DoJSON(ctx, false, http.MethodPost,
			targetURL, service.mfaHeaders(deviceID, referer), body)
		if err != nil {
			return apiEnvelope{}, networkAuthError("mfa_verify_network_error", LifecycleLoginPending, err)
		}
		if response.StatusCode < http.StatusMultipleChoices || response.StatusCode >= http.StatusBadRequest {
			break
		}
		location := strings.TrimSpace(response.Header.Get("Location"))
		if location == "" {
			break
		}
		nextURL := resolveURL(targetURL, location)
		if response.StatusCode != http.StatusTemporaryRedirect && response.StatusCode != http.StatusPermanentRedirect {
			return apiEnvelope{ContinueURL: nextURL}, nil
		}
		if authError := validateOAuthContinuationOrigin(nextURL, service.options.AuthBaseURL); authError != nil {
			return apiEnvelope{ContinueURL: nextURL}, nil
		}
		if redirects >= 9 {
			return apiEnvelope{}, newAuthError("oauth_redirect_limit", LifecycleLoginPending, response.StatusCode, true, false, "MFA redirect limit exceeded", nil)
		}
		referer = targetURL
		targetURL = nextURL
	}
	if authError := classifyHTTPResponse("mfa_verify", response.StatusCode, payload, LifecycleLoginPending); authError != nil {
		return apiEnvelope{}, authError
	}
	if authError := classifyPermanentAccountPayload(payload); authError != nil {
		return apiEnvelope{}, authError
	}
	verified := parseAuthorizationEnvelope(response, payload)
	if isMFAChallenge(verified.PageType, verified.ContinueURL) {
		return apiEnvelope{}, newAuthError("invalid_totp", LifecycleReauthRequired, response.StatusCode, false, true, "TOTP was rejected", nil)
	}
	if authError := classifyPageType(verified.PageType); authError != nil {
		return apiEnvelope{}, authError
	}
	return verified, nil
}

func (service *Service) mfaHeaders(deviceID, referer string) map[string]string {
	return map[string]string{
		"accept":         "application/json",
		"content-type":   "application/json",
		"oai-device-id":  deviceID,
		"origin":         service.options.AuthBaseURL,
		"referer":        referer,
		"sec-fetch-dest": "empty",
		"sec-fetch-mode": "cors",
		"sec-fetch-site": "same-origin",
	}
}

func isMFAChallenge(pageType, continueURL string) bool {
	normalizedPage := normalizeCode(pageType)
	normalizedURL := strings.ToLower(strings.TrimSpace(continueURL))
	return normalizedPage == "mfa_challenge" || strings.Contains(normalizedPage, "totp") || strings.Contains(normalizedURL, "/mfa")
}

func mfaChallengeFromURL(rawURL string) (apiEnvelope, bool) {
	parsedURL, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return apiEnvelope{}, false
	}
	path := strings.Trim(parsedURL.EscapedPath(), "/")
	parts := strings.Split(path, "/")
	if len(parts) != 2 || !strings.EqualFold(parts[0], "mfa-challenge") {
		return apiEnvelope{}, false
	}
	factorID, err := url.PathUnescape(parts[1])
	if err != nil || strings.TrimSpace(factorID) == "" {
		return apiEnvelope{}, false
	}
	query := parsedURL.Query()
	factorType := firstNonEmptyString(query.Get("factor_type"), query.Get("factorType"), query.Get("type"))
	factorType = normalizeMFAFactorType(factorType)
	if factorType == "" {
		factorType = mfaFactorTypeFromID(factorID)
	}
	payload := map[string]any{
		"factor_id":   factorID,
		"factor_type": factorType,
	}
	if requestID := strings.TrimSpace(query.Get("mfa_request_id")); requestID != "" {
		payload["mfa_request_id"] = requestID
	}
	return apiEnvelope{
		ContinueURL: rawURL,
		PageType:    "mfa_challenge",
		Payload:     payload,
	}, true
}

func mfaFactorTypeFromID(factorID string) string {
	return normalizeMFAFactorType(factorID)
}

func normalizeMFAFactorType(value string) string {
	normalized := normalizeCode(value)
	switch {
	case normalized == "email":
		return "email"
	case normalized == "email_otp" || strings.HasPrefix(normalized, "email_otp_"):
		return "email"
	case normalized == "sms":
		return "sms"
	case normalized == "sms_otp" || strings.HasPrefix(normalized, "sms_otp_"):
		return "sms"
	case normalized == "phone":
		return "phone"
	case normalized == "phone_otp" || strings.HasPrefix(normalized, "phone_otp_"):
		return "phone"
	case normalized == "passkey" || strings.HasPrefix(normalized, "passkey_"):
		return "passkey"
	case normalized == "webauthn" || strings.HasPrefix(normalized, "webauthn_"):
		return "webauthn"
	case normalized == "totp" || strings.HasPrefix(normalized, "totp_") ||
		normalized == "authenticator" || strings.HasPrefix(normalized, "authenticator_"):
		return "totp"
	default:
		return ""
	}
}

func isPasswordChallenge(pageType, continueURL string) bool {
	normalizedPage := normalizeCode(pageType)
	normalizedURL := strings.ToLower(strings.TrimSpace(continueURL))
	return normalizedPage == "login_password" || strings.Contains(normalizedURL, "/log-in/password")
}

func selectMFAFactor(payload map[string]any, factorType string) (string, string) {
	wanted := normalizeMFAFactorType(factorType)
	if payload == nil || wanted == "" {
		return "", ""
	}
	candidates := []map[string]any{payload}
	for _, key := range []string{"oai-client-auth-session", "client_auth_session", "auth_session"} {
		if session, ok := payload[key].(map[string]any); ok {
			candidates = append(candidates, session)
		}
	}
	fallbackFactorID := ""
	fallbackRequestID := ""
	for _, candidate := range candidates {
		candidateRequestID := firstNonEmptyString(stringValue(candidate["mfa_request_id"]), stringValue(candidate["mfaRequestId"]))
		for _, factorListKey := range []string{"factors", "mfa_factors", "mfa_challenge_factors"} {
			factors, ok := candidate[factorListKey].([]any)
			if !ok {
				continue
			}
			for _, item := range factors {
				factor, okFactor := item.(map[string]any)
				if !okFactor {
					continue
				}
				actualType := normalizeMFAFactorType(firstNonEmptyString(stringValue(factor["factor_type"]), stringValue(factor["type"])))
				if actualType != wanted {
					continue
				}
				factorID := firstNonEmptyString(stringValue(factor["id"]), stringValue(factor["factor_id"]), stringValue(factor["factorId"]))
				if factorID == "" {
					continue
				}
				requestID := ""
				if metadata, okMetadata := factor["metadata"].(map[string]any); okMetadata {
					requestID = firstNonEmptyString(stringValue(metadata["mfa_request_id"]), stringValue(metadata["mfaRequestId"]), stringValue(factor["mfa_request_id"]), stringValue(factor["mfaRequestId"]), candidateRequestID)
				} else {
					requestID = firstNonEmptyString(stringValue(factor["mfa_request_id"]), stringValue(factor["mfaRequestId"]), candidateRequestID)
				}
				if requestID != "" {
					return strings.TrimSpace(factorID), strings.TrimSpace(requestID)
				}
				if fallbackFactorID == "" {
					fallbackFactorID = factorID
					fallbackRequestID = requestID
				}
			}
		}
		topType := normalizeMFAFactorType(firstNonEmptyString(stringValue(candidate["factor_type"]), stringValue(candidate["factorType"]), stringValue(candidate["type"])))
		if topType == wanted {
			factorID := firstNonEmptyString(stringValue(candidate["factor_id"]), stringValue(candidate["factorId"]), stringValue(candidate["id"]))
			if factorID != "" {
				if candidateRequestID != "" {
					return strings.TrimSpace(factorID), strings.TrimSpace(candidateRequestID)
				}
				if fallbackFactorID == "" {
					fallbackFactorID = factorID
					fallbackRequestID = candidateRequestID
				}
			}
		}
	}
	return strings.TrimSpace(fallbackFactorID), strings.TrimSpace(fallbackRequestID)
}

func unsupportedMFAError(payload map[string]any) *AuthError {
	for _, factorType := range []string{"email", "sms", "phone", "passkey", "webauthn"} {
		if factorID, _ := selectMFAFactor(payload, factorType); factorID != "" {
			code := factorType + "_otp_required"
			if factorType == "sms" || factorType == "phone" {
				code = "sms_otp_required"
			} else if factorType == "passkey" || factorType == "webauthn" {
				code = "passkey_required"
			}
			return newAuthError(code, LifecycleInteractionRequired, 0, false, true, "authentication requires user interaction", nil)
		}
	}
	return newAuthError("totp_factor_missing", LifecycleInteractionRequired, 0, false, true, "MFA challenge did not provide a TOTP factor", nil)
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func classifyPagePayload(payload []byte) *AuthError {
	if authError := classifyPermanentAccountPayload(payload); authError != nil {
		return authError
	}
	return classifyPageType(parseAPIEnvelope(payload).PageType)
}

func classifyPermanentAccountPayload(payload []byte) *AuthError {
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return nil
	}
	code, message := responseError(payload)
	_, hasStructuredError := decoded["error"]
	if code != "" || hasStructuredError {
		if authError := classifyPermanentAccountFields(code, message); authError != nil {
			return authError
		}
	}
	page, _ := decoded["page"].(map[string]any)
	pageType := stringValue(page["type"])
	if authError := classifyPageType(pageType); authError != nil && authError.State == LifecycleDead {
		return authError
	}
	pagePayload, _ := page["payload"].(map[string]any)
	pageCode := firstNonEmptyString(stringValue(pagePayload["code"]), stringValue(pagePayload["error"]), stringValue(pagePayload["type"]))
	pageMessage := stringValue(pagePayload["message"])
	if nestedError, ok := pagePayload["error"].(map[string]any); ok {
		pageCode = firstNonEmptyString(stringValue(nestedError["code"]), stringValue(nestedError["type"]), pageCode)
		pageMessage = firstNonEmptyString(stringValue(nestedError["message"]), pageMessage)
	}
	if errorList, ok := pagePayload["errors"].([]any); ok {
		for _, item := range errorList {
			switch typed := item.(type) {
			case map[string]any:
				code := firstNonEmptyString(stringValue(typed["code"]), stringValue(typed["type"]), stringValue(typed["error"]))
				message := firstNonEmptyString(stringValue(typed["message"]), stringValue(typed["detail"]))
				if authError := classifyPermanentAccountFields(code, message); authError != nil {
					return authError
				}
			case string:
				if authError := classifyPermanentAccountFields("", typed); authError != nil {
					return authError
				}
			}
		}
	}
	if authError := classifyPermanentAccountCode(pageCode, 0); authError != nil {
		return authError
	}
	if !strings.Contains(normalizeCode(pageType), "error") {
		return nil
	}
	return classifyPermanentAccountFields(pageCode, pageMessage)
}

func classifyPermanentAccountFields(code, message string) *AuthError {
	normalizedCode := normalizeCode(code)
	if authError := classifyPermanentAccountCode(normalizedCode, 0); authError != nil {
		return authError
	}
	message = strings.ToLower(strings.TrimSpace(message))
	normalizedMessage := normalizeCode(message)
	if normalizedMessage == "account_deactivated" ||
		strings.Contains(message, "deleted or deactivated") || strings.Contains(message, "account deactivated") ||
		strings.Contains(message, "account has been deactivated") || strings.Contains(message, "account was deactivated") {
		return newAuthError("account_deactivated", LifecycleDead, 0, false, true, "account is deactivated", nil)
	}
	if normalizedMessage == "account_deleted" ||
		strings.Contains(message, "account has been deleted") || strings.Contains(message, "account was deleted") ||
		strings.Contains(message, "account because it has been deleted") {
		return newAuthError("account_deleted", LifecycleDead, 0, false, true, "account is deleted", nil)
	}
	return nil
}

func classifyPermanentAccountCode(code string, status int) *AuthError {
	normalized := normalizeCode(code)
	if strings.Contains(normalized, "account_deleted") || normalized == "deleted" {
		return newAuthError("account_deleted", LifecycleDead, status, false, true, "account is deleted", nil)
	}
	if strings.Contains(normalized, "account_deactivated") || normalized == "deactivated" {
		return newAuthError("account_deactivated", LifecycleDead, status, false, true, "account is deactivated", nil)
	}
	return nil
}

func classifyPageType(pageType string) *AuthError {
	normalized := normalizeCode(pageType)
	if normalized == "" {
		return nil
	}
	var reason string
	switch {
	case strings.Contains(normalized, "totp") || strings.Contains(normalized, "authenticator") || strings.Contains(normalized, "mfa"):
		reason = "totp_required"
	case strings.Contains(normalized, "email") && (strings.Contains(normalized, "otp") || strings.Contains(normalized, "verification") || strings.Contains(normalized, "code")):
		reason = "email_otp_required"
	case strings.Contains(normalized, "sms") || (strings.Contains(normalized, "phone") && (strings.Contains(normalized, "otp") || strings.Contains(normalized, "verification") || strings.Contains(normalized, "code"))):
		reason = "sms_otp_required"
	case strings.Contains(normalized, "passkey") || strings.Contains(normalized, "webauthn"):
		reason = "passkey_required"
	case strings.Contains(normalized, "browser") && (strings.Contains(normalized, "confirm") || strings.Contains(normalized, "verification") || strings.Contains(normalized, "approval")):
		reason = "browser_confirmation_required"
	case strings.Contains(normalized, "turnstile"):
		reason = "turnstile_required"
	case strings.Contains(normalized, "arkose"):
		reason = "arkose_required"
	case strings.Contains(normalized, "account_deactivated") || normalized == "deactivated":
		return newAuthError("account_deactivated", LifecycleDead, 0, false, true, "account is deactivated", nil)
	case strings.Contains(normalized, "account_deleted") || normalized == "deleted":
		return newAuthError("account_deleted", LifecycleDead, 0, false, true, "account is deleted", nil)
	}
	if reason == "" {
		return nil
	}
	return newAuthError(reason, LifecycleInteractionRequired, 0, false, true, "authentication requires user interaction", nil)
}

func classifyHTTPResponse(stage string, status int, payload []byte, defaultState LifecycleState) *AuthError {
	if status >= 200 && status < 300 {
		return nil
	}
	if authError := classifyPermanentAccountPayload(payload); authError != nil {
		authError.Status = status
		authError.StatusCode = status
		return authError
	}
	code, message := responseError(payload)
	normalized := normalizeCode(code)
	if authError := classifyPermanentAccountCode(normalized, status); authError != nil {
		return authError
	}
	if normalized == "" && strings.TrimSpace(message) == "" {
		if authError := classifyPermanentAccountFields(string(payload), string(payload)); authError != nil {
			authError.Status = status
			authError.StatusCode = status
			return authError
		}
	}
	messageLower := strings.ToLower(message)
	responseTextLower := strings.ToLower(string(payload))
	if normalized == "" {
		normalized = normalizeCode(stage + "_failed")
	}
	if strings.Contains(messageLower, "deactivated") || strings.Contains(responseTextLower, "deleted or deactivated") || strings.Contains(responseTextLower, "account deactivated") {
		return newAuthError("account_deactivated", LifecycleDead, status, false, true, "account is deactivated", nil)
	}
	if strings.Contains(messageLower, "account has been deleted") || strings.Contains(responseTextLower, "account because it has been deleted") {
		return newAuthError("account_deleted", LifecycleDead, status, false, true, "account is deleted", nil)
	}
	if status == http.StatusTooManyRequests || status >= http.StatusInternalServerError {
		return newAuthError(normalized, defaultState, status, true, false, "upstream authentication service is temporarily unavailable", nil)
	}
	if normalized == "invalid_grant" || normalized == "app_session_terminated" {
		return newAuthError(normalized, LifecycleReauthRequired, status, false, true, "authentication must be renewed", nil)
	}
	if normalized == "interaction_required" {
		return newAuthError(normalized, LifecycleInteractionRequired, status, false, true, "authentication requires user interaction", nil)
	}
	if normalized == "reauth_required" {
		return newAuthError(normalized, LifecycleReauthRequired, status, false, true, "authentication must be renewed", nil)
	}
	if normalized == "invalid_password" || normalized == "invalid_credentials" || normalized == "wrong_password" ||
		strings.Contains(messageLower, "invalid credentials") || strings.Contains(messageLower, "wrong password") {
		return newAuthError("invalid_password", LifecycleReauthRequired, status, false, true, "password was rejected", nil)
	}
	if normalized == "invalid_totp" || strings.Contains(normalized, "invalid_otp") {
		return newAuthError("invalid_totp", LifecycleReauthRequired, status, false, true, "TOTP was rejected", nil)
	}
	if stage == "mfa_verify" && (status == http.StatusBadRequest || status == http.StatusUnauthorized || status == http.StatusForbidden) {
		return newAuthError("invalid_totp", LifecycleReauthRequired, status, false, true, "TOTP was rejected", nil)
	}
	if strings.Contains(normalized, "turnstile") || strings.Contains(normalized, "arkose") {
		return newAuthError(normalized, LifecycleInteractionRequired, status, false, true, "interactive challenge is required", nil)
	}
	if stage == "password_verify" && (status == http.StatusBadRequest || status == http.StatusUnauthorized || status == http.StatusForbidden) {
		return newAuthError("invalid_password", LifecycleReauthRequired, status, false, true, "password was rejected", nil)
	}
	return newAuthError(normalized, defaultState, status, false, true, "authentication request was rejected", nil)
}

func responseError(payload []byte) (string, string) {
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return "", ""
	}
	message := stringValue(decoded["error_description"])
	if message == "" {
		message = stringValue(decoded["message"])
	}
	switch value := decoded["error"].(type) {
	case string:
		return value, message
	case map[string]any:
		code := stringValue(value["code"])
		if code == "" {
			code = stringValue(value["type"])
		}
		if message == "" {
			message = stringValue(value["message"])
		}
		return code, message
	}
	return stringValue(decoded["code"]), message
}

func normalizeCode(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var builder strings.Builder
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' {
			builder.WriteRune(character)
		} else if builder.Len() > 0 && !strings.HasSuffix(builder.String(), "_") {
			builder.WriteByte('_')
		}
	}
	return strings.Trim(builder.String(), "_")
}

func parseOAuthCallback(rawURL, redirectURL, expectedState string) (string, bool, *AuthError) {
	parsedURL, err := url.Parse(strings.TrimSpace(rawURL))
	parsedRedirect, errRedirect := url.Parse(strings.TrimSpace(redirectURL))
	if err != nil || errRedirect != nil || !sameOAuthEndpoint(parsedURL, parsedRedirect) {
		return "", false, nil
	}
	query := parsedURL.Query()
	state := strings.TrimSpace(query.Get("state"))
	if state == "" || state != expectedState {
		return "", true, newAuthError("invalid_state", LifecycleReauthRequired, 0, false, true, "OAuth state did not match", nil)
	}
	if oauthCode := strings.TrimSpace(query.Get("error")); oauthCode != "" {
		normalizedCode := normalizeCode(oauthCode)
		lifecycleState := LifecycleReauthRequired
		if normalizedCode == "access_denied" || normalizedCode == "interaction_required" {
			lifecycleState = LifecycleInteractionRequired
		}
		return "", true, newAuthError(normalizedCode, lifecycleState, 0, false, true, "OAuth authorization failed", nil)
	}
	code := strings.TrimSpace(query.Get("code"))
	if code == "" {
		return "", false, nil
	}
	return code, true, nil
}

func sameOAuthEndpoint(candidate, expected *url.URL) bool {
	return sameOAuthOrigin(candidate, expected) &&
		normalizeOAuthPath(candidate.Path) == normalizeOAuthPath(expected.Path)
}

func sameOAuthOrigin(candidate, expected *url.URL) bool {
	if candidate == nil || expected == nil || candidate.User != nil || expected.User != nil {
		return false
	}
	candidateScheme := strings.ToLower(strings.TrimSpace(candidate.Scheme))
	expectedScheme := strings.ToLower(strings.TrimSpace(expected.Scheme))
	return (candidateScheme == "http" || candidateScheme == "https") && candidateScheme == expectedScheme &&
		candidate.Hostname() != "" && strings.EqualFold(candidate.Hostname(), expected.Hostname()) &&
		oauthEffectivePort(candidate, candidateScheme) == oauthEffectivePort(expected, expectedScheme)
}

func oauthEffectivePort(parsed *url.URL, scheme string) string {
	if parsed == nil {
		return ""
	}
	if port := parsed.Port(); port != "" {
		return port
	}
	switch scheme {
	case "http":
		return "80"
	case "https":
		return "443"
	default:
		return ""
	}
}

func normalizeOAuthPath(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "/"
	}
	return value
}

func classifyOAuthContinuationURL(rawURL, authBaseURL string) *AuthError {
	if authError := validateOAuthContinuationOrigin(rawURL, authBaseURL); authError != nil {
		return authError
	}
	parsedURL, _ := url.Parse(strings.TrimSpace(rawURL))
	return classifyPageType(parsedURL.Path)
}

func validateOAuthContinuationOrigin(rawURL, authBaseURL string) *AuthError {
	parsedURL, err := url.Parse(strings.TrimSpace(rawURL))
	parsedAuthBase, errBase := url.Parse(strings.TrimSpace(authBaseURL))
	if err != nil || errBase != nil || !sameOAuthOrigin(parsedURL, parsedAuthBase) {
		return newAuthError("oauth_redirect_untrusted", LifecycleInteractionRequired, 0, false, true, "OAuth continuation left the trusted authentication origin", nil)
	}
	return nil
}

func responseRequestURL(response *fhttp.Response) string {
	if response == nil || response.Request == nil || response.Request.URL == nil {
		return ""
	}
	return response.Request.URL.String()
}

func classifyAuthorizeNavigation(rawURL string) (bool, *AuthError) {
	parsedURL, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsedURL.Path == "" {
		return false, newAuthError("authorization_completion_required", LifecycleInteractionRequired, 0, false, true, "authorize request did not reach a recognized login page", err)
	}
	path := strings.ToLower(strings.TrimRight(parsedURL.Path, "/"))
	switch path {
	case "/log-in/password":
		return false, nil
	case "/log-in", "/sign-in":
		return true, nil
	}
	if authError := classifyPageType(path); authError != nil {
		return false, authError
	}
	return false, newAuthError("authorization_completion_required", LifecycleInteractionRequired, 0, false, true, "authorize request requires user interaction", nil)
}

func resolveURL(baseURL, reference string) string {
	parsedReference, err := url.Parse(strings.TrimSpace(reference))
	if err != nil {
		return strings.TrimSpace(reference)
	}
	if parsedReference.IsAbs() {
		return parsedReference.String()
	}
	parsedBase, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return strings.TrimSpace(reference)
	}
	return parsedBase.ResolveReference(parsedReference).String()
}

func redirectOrigin(rawURL string) string {
	parsedURL, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
		return "https://platform.openai.com"
	}
	return parsedURL.Scheme + "://" + parsedURL.Host
}

func JWTExpiry(token string) (time.Time, bool) {
	parts := strings.Split(strings.TrimSpace(token), ".")
	if len(parts) < 2 {
		return time.Time{}, false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, false
	}
	var claims struct {
		ExpiresAt json.Number `json:"exp"`
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(&claims); err != nil {
		return time.Time{}, false
	}
	expiresAt, err := claims.ExpiresAt.Int64()
	if err != nil || expiresAt <= 0 {
		return time.Time{}, false
	}
	return time.Unix(expiresAt, 0).UTC(), true
}

// PopulateCredentialIdentity fills non-secret account metadata from available
// JWTs without replacing values already returned by an authenticated endpoint.
func PopulateCredentialIdentity(credential *Credential) {
	if credential == nil {
		return
	}
	for _, token := range []string{credential.AccessToken, credential.IDToken} {
		claims := jwtClaims(token)
		if len(claims) == 0 {
			continue
		}
		authClaims, _ := claims["https://api.openai.com/auth"].(map[string]any)
		if credential.Email == "" {
			credential.Email = strings.TrimSpace(stringValue(claims["email"]))
		}
		if credential.AccountID == "" {
			credential.AccountID = firstStringValue(authClaims, "chatgpt_account_id", "account_id")
			if credential.AccountID == "" {
				credential.AccountID = firstStringValue(claims, "chatgpt_account_id", "account_id")
			}
		}
		if credential.UserID == "" {
			credential.UserID = firstStringValue(authClaims, "chatgpt_user_id", "user_id")
			if credential.UserID == "" {
				credential.UserID = firstStringValue(claims, "chatgpt_user_id", "user_id")
			}
		}
		if credential.PlanType == "" {
			credential.PlanType = firstStringValue(authClaims, "chatgpt_plan_type", "plan_type")
		}
	}
}

func jwtClaims(token string) map[string]any {
	parts := strings.Split(strings.TrimSpace(token), ".")
	if len(parts) < 2 {
		return nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil
	}
	var claims map[string]any
	if err = json.Unmarshal(payload, &claims); err != nil {
		return nil
	}
	return claims
}

func firstStringValue(values map[string]any, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(stringValue(values[key])); value != "" {
			return value
		}
	}
	return ""
}

func tokenExpiryString(tokens ...string) string {
	for _, token := range tokens {
		if expiresAt, ok := JWTExpiry(token); ok {
			return expiresAt.Format(time.RFC3339)
		}
	}
	return ""
}
