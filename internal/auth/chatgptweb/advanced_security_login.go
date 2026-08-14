package chatgptweb

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"

	http "github.com/bogdanfinn/fhttp"
)

const (
	advancedSecurityChallengeIssuePath   = "/api/accounts/auth_challenge/issue_method_challenge"
	advancedSecuritySessionDumpPath      = "/api/accounts/client_auth_session_dump"
	advancedSecurityChallengeVerifyPath  = "/api/accounts/auth_challenge/verify"
	advancedSecurityPostAuthFinalizePath = "/api/accounts/post-auth/finalize"
)

func (service *Service) loginWithAdvancedSecurityPasskey(
	ctx context.Context,
	client *Client,
	credential *Credential,
	input LoginInput,
	pendingState LifecycleState,
) (*Credential, error) {
	if credential == nil || ValidateAdvancedAccountSecurityCredential(credential.AdvancedAccountSecurity) != nil {
		return service.loginFailure(credential, input.Relogin, advancedSecurityCredentialError("advanced_security_credential_invalid", "Advanced account security credential is unavailable", nil))
	}
	if input.PersistAdvancedAccountSecurity == nil {
		return service.loginFailure(credential, input.Relogin, advancedSecurityStatePersistenceError(pendingState, "advanced_security_challenge", nil))
	}

	entryPayload, entryURL, errEntry := service.beginPasskeyLogin(ctx, client, credential, pendingState)
	if errEntry != nil {
		return service.loginFailure(credential, input.Relogin, ensureAuthError(errEntry, pendingState))
	}
	if !isAdvancedSecurityAuthChallenge(entryPayload, entryURL) {
		return service.loginFailure(credential, input.Relogin, advancedSecurityCredentialError("advanced_security_challenge_unavailable", "Advanced account security challenge is unavailable", nil))
	}
	if authError := service.issueAdvancedSecurityPasskeyChallenge(ctx, client, credential, pendingState); authError != nil {
		return service.loginFailure(credential, input.Relogin, authError)
	}
	requestOptions, requestID, authError := service.fetchAdvancedSecurityPasskeyChallenge(ctx, client, credential, pendingState)
	if authError != nil {
		return service.loginFailure(credential, input.Relogin, authError)
	}
	keyIndex, errSelect := selectAdvancedSecurityPasskey(credential.AdvancedAccountSecurity, requestOptions)
	if errSelect != nil {
		return service.loginFailure(credential, input.Relogin, advancedSecurityCredentialError("advanced_security_credential_not_allowed", "Advanced account security challenge did not allow a persisted credential", errSelect))
	}
	selectedAuthenticator := cloneWebAuthnCredential(&credential.AdvancedAccountSecurity.Passkeys[keyIndex].Credential)

	var persistedAAS *AdvancedAccountSecurityCredential
	assertion, errAssertion := createWebAuthnAssertion(
		&credential.AdvancedAccountSecurity.Passkeys[keyIndex].Credential,
		requestOptions,
		func(updated WebAuthnCredential) (WebAuthnCredential, error) {
			candidate := CloneAdvancedAccountSecurityCredential(credential.AdvancedAccountSecurity)
			candidate.Passkeys[keyIndex].Credential = *cloneWebAuthnCredential(&updated)
			persisted, errPersist := input.PersistAdvancedAccountSecurity(ctx, *candidate)
			if errPersist != nil {
				return WebAuthnCredential{}, errPersist
			}
			if errValidate := ValidateAdvancedAccountSecurityCredential(&persisted); errValidate != nil {
				return WebAuthnCredential{}, errValidate
			}
			persistedIndex := findAdvancedSecurityAuthenticator(&persisted, &updated)
			if persistedIndex < 0 || persisted.Passkeys[persistedIndex].Credential.SignCount != updated.SignCount {
				return WebAuthnCredential{}, errors.New("persisted advanced account security counter did not match the credential")
			}
			persistedAAS = CloneAdvancedAccountSecurityCredential(&persisted)
			return *cloneWebAuthnCredential(&persisted.Passkeys[persistedIndex].Credential), nil
		},
	)
	if errAssertion != nil {
		if errors.Is(errAssertion, errWebAuthnStatePersistence) {
			return service.loginFailure(credential, input.Relogin, advancedSecurityStatePersistenceError(pendingState, "advanced_security_verify", errAssertion))
		}
		return service.loginFailure(credential, input.Relogin, advancedSecurityCredentialError("advanced_security_assertion_invalid", "Advanced account security assertion could not be created", errAssertion))
	}
	if persistedAAS == nil {
		return service.loginFailure(credential, input.Relogin, advancedSecurityStatePersistenceError(pendingState, "advanced_security_verify", nil))
	}
	credential.AdvancedAccountSecurity = persistedAAS
	keyIndex = findAdvancedSecurityAuthenticator(persistedAAS, selectedAuthenticator)
	if keyIndex < 0 {
		return service.loginFailure(credential, input.Relogin, advancedSecurityCredentialError("advanced_security_credential_invalid", "Persisted advanced account security credential is unavailable", nil))
	}

	continueURL, authError := service.verifyAdvancedSecurityPasskeyChallenge(ctx, client, credential, requestID, assertion, pendingState)
	if authError != nil {
		return service.loginFailure(credential, input.Relogin, authError)
	}
	if !service.isAdvancedSecurityCallbackURL(continueURL) {
		continueURL, authError = service.finalizeAdvancedSecurityLogin(ctx, client, credential, pendingState)
		if authError != nil {
			return service.loginFailure(credential, input.Relogin, authError)
		}
	}
	if !service.isAdvancedSecurityCallbackURL(continueURL) {
		return service.loginFailure(credential, input.Relogin, advancedSecurityCredentialError("advanced_security_finalize_failed", "Advanced account security login did not return a callback", nil))
	}
	if errCallback := service.consumePasskeyCallback(ctx, client, continueURL, pendingState); errCallback != nil {
		return service.loginFailure(credential, input.Relogin, ensureAuthError(errCallback, pendingState))
	}
	return service.finishAdvancedSecurityLogin(ctx, client, credential, input, keyIndex, pendingState)
}

func (service *Service) issueAdvancedSecurityPasskeyChallenge(ctx context.Context, client *Client, credential *Credential, pendingState LifecycleState) *AuthError {
	endpoint := service.options.AuthBaseURL + advancedSecurityChallengeIssuePath
	response, payload, errRequest := client.DoJSON(ctx, true, http.MethodPost, endpoint,
		service.apiHeaders(credential.DeviceID, service.options.AuthBaseURL+"/auth-challenge", ""),
		map[string]string{"method_id": "passkey"})
	return classifyAdvancedSecurityProtocolResponse(response, payload, errRequest, pendingState, "advanced_security_challenge", "advanced_security_challenge_unavailable", endpoint)
}

func (service *Service) fetchAdvancedSecurityPasskeyChallenge(ctx context.Context, client *Client, credential *Credential, pendingState LifecycleState) (map[string]any, string, *AuthError) {
	endpoint := service.options.AuthBaseURL + advancedSecuritySessionDumpPath
	response, payload, errRequest := client.DoFollow(ctx, http.MethodGet, endpoint,
		service.apiHeaders(credential.DeviceID, service.options.AuthBaseURL+"/advanced-account-security/secure-methods", ""), nil)
	if authError := classifyAdvancedSecurityProtocolResponse(response, payload, errRequest, pendingState, "advanced_security_challenge", "advanced_security_challenge_unavailable", endpoint); authError != nil {
		return nil, "", authError
	}
	var decoded map[string]any
	if errDecode := json.Unmarshal(payload, &decoded); errDecode != nil {
		return nil, "", advancedSecurityCredentialError("advanced_security_challenge_unavailable", "Advanced account security session is invalid", errDecode)
	}
	session, _ := decoded["client_auth_session"].(map[string]any)
	enabled, _ := session["aas_enabled"].(bool)
	options, requestID, ok := extractPasskeyChallengeFromValue(session)
	if !enabled || !ok {
		return nil, "", advancedSecurityCredentialError("advanced_security_challenge_unavailable", "Advanced account security session did not contain Passkey options", nil)
	}
	return options, requestID, nil
}

func (service *Service) verifyAdvancedSecurityPasskeyChallenge(ctx context.Context, client *Client, credential *Credential, requestID string, assertion map[string]any, pendingState LifecycleState) (string, *AuthError) {
	endpoint := service.options.AuthBaseURL + advancedSecurityChallengeVerifyPath
	response, payload, errRequest := client.DoJSONOnce(ctx, false, http.MethodPost, endpoint,
		service.apiHeaders(credential.DeviceID, service.options.AuthBaseURL+"/auth-challenge", ""),
		map[string]any{
			"method_id": "passkey",
			"passkey_challenge_response": map[string]any{
				"mfa_request_id":          requestID,
				"signed_passkey_response": assertion,
			},
		})
	if shouldRecoverAdvancedSecurityVerify(ctx, response, errRequest) {
		continueURL, recoveryError := service.finalizeAdvancedSecurityLogin(ctx, client, credential, pendingState)
		if recoveryError == nil && service.isAdvancedSecurityCallbackURL(continueURL) {
			return continueURL, nil
		}
	}
	if authError := classifyAdvancedSecurityProtocolResponse(response, payload, errRequest, pendingState, "advanced_security_verify", "advanced_security_verification_failed", endpoint); authError != nil {
		return "", authError
	}
	return strings.TrimSpace(parseAPIEnvelope(payload).ContinueURL), nil
}

func shouldRecoverAdvancedSecurityVerify(ctx context.Context, response *http.Response, errRequest error) bool {
	if ctx == nil || ctx.Err() != nil {
		return false
	}
	if requestError, ok := loginRequestErrorDetails(errRequest); ok {
		if requestError.cloudflare || requestError.status == http.StatusTooManyRequests {
			return false
		}
		return requestError.status == 0 ||
			requestError.status >= http.StatusOK && requestError.status < http.StatusMultipleChoices ||
			requestError.status >= http.StatusInternalServerError
	}
	if errRequest != nil {
		return true
	}
	return response != nil && response.StatusCode >= http.StatusInternalServerError
}

func (service *Service) finalizeAdvancedSecurityLogin(ctx context.Context, client *Client, credential *Credential, pendingState LifecycleState) (string, *AuthError) {
	endpoint := service.options.AuthBaseURL + advancedSecurityPostAuthFinalizePath
	response, payload, errRequest := client.DoFollowOnce(ctx, http.MethodPost, endpoint,
		service.apiHeaders(credential.DeviceID, service.options.AuthBaseURL+"/advanced-account-security/enrolled", ""), nil)
	if authError := classifyAdvancedSecurityProtocolResponse(response, payload, errRequest, pendingState, "advanced_security_finalize", "advanced_security_finalize_failed", endpoint); authError != nil {
		return "", authError
	}
	return strings.TrimSpace(parseAPIEnvelope(payload).ContinueURL), nil
}

func classifyAdvancedSecurityProtocolResponse(response *http.Response, payload []byte, errRequest error, pendingState LifecycleState, stage, code, endpoint string) *AuthError {
	if errRequest != nil {
		authError := networkAuthError(code, pendingState, errRequest)
		authError.FailureStage = stage
		return authError
	}
	if isCloudflareChallenge(response, payload) {
		authError := newAuthError("cloudflare_challenge", pendingState, response.StatusCode, true, false, "Cloudflare challenge blocked advanced account security login", nil)
		authError.FailureStage = stage
		return attachPasskeyHTTPDiagnostic(authError, response, payload, endpoint)
	}
	if response == nil || response.StatusCode != http.StatusOK {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		return attachPasskeyHTTPDiagnostic(classifyPasskeyProtocolResponse(stage, code, "Advanced account security login was rejected", status, payload, pendingState), response, payload, endpoint)
	}
	return nil
}

func selectAdvancedSecurityPasskey(aas *AdvancedAccountSecurityCredential, rawOptions any) (int, error) {
	options, errOptions := decodeWebAuthnRequestOptions(rawOptions)
	if errOptions != nil || len(options.AllowCredentials) == 0 {
		return -1, errWebAuthnRequestOptionsInvalid
	}
	for index := range aas.Passkeys {
		if webAuthnRequestAllowsCredential(options.AllowCredentials, aas.Passkeys[index].Credential.CredentialID) {
			return index, nil
		}
	}
	return -1, errWebAuthnCredentialNotAllowed
}

func findAdvancedSecurityAuthenticator(aas *AdvancedAccountSecurityCredential, credential *WebAuthnCredential) int {
	if aas == nil || credential == nil {
		return -1
	}
	for index := range aas.Passkeys {
		if WebAuthnAuthenticatorMatches(&aas.Passkeys[index].Credential, credential) {
			return index
		}
	}
	return -1
}

func extractPasskeyChallengeFromValue(value any) (map[string]any, string, bool) {
	payload, errMarshal := json.Marshal(value)
	if errMarshal != nil {
		return nil, "", false
	}
	return extractPasskeyChallenge(payload)
}

func isAdvancedSecurityAuthChallenge(payload []byte, rawURL string) bool {
	combined := strings.ToLower(string(payload) + " " + rawURL)
	return strings.Contains(combined, "advanced_account_security") || strings.Contains(combined, "auth_challenge") || strings.Contains(combined, "auth-challenge")
}

func (service *Service) isAdvancedSecurityCallbackURL(rawURL string) bool {
	target := resolveURL(service.options.AuthBaseURL+"/", rawURL)
	parsed, errParse := url.Parse(target)
	if errParse != nil {
		return false
	}
	sessionBase, errSession := url.Parse(service.options.SessionBaseURL)
	return errSession == nil && sameOAuthEndpointOrigin(parsed, sessionBase) && isPasskeyCallbackPath(parsed.Path)
}

func advancedSecurityCredentialError(code, message string, cause error) *AuthError {
	authError := newAuthError(code, LifecycleReauthRequired, http.StatusBadRequest, false, true, message, cause)
	authError.FailureStage = "advanced_security_login"
	return authError
}

func advancedSecurityStatePersistenceError(state LifecycleState, stage string, cause error) *AuthError {
	authError := newAuthError("advanced_security_state_persist_failed", state, 0, true, false, "Advanced account security state could not be persisted", cause)
	authError.FailureStage = stage
	return authError
}
