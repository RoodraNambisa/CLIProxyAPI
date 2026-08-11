package chatgptweb

import (
	"errors"
	"fmt"
	"strings"
)

// ErrCredentialSuperseded reports that a re-login result no longer matches
// the credential generation that started the operation.
var ErrCredentialSuperseded = errors.New("chatgpt web credential changed during re-login")

type AuthError struct {
	Code                  string         `json:"code"`
	State                 LifecycleState `json:"state"`
	LifecycleState        LifecycleState `json:"lifecycle_state"`
	Status                int            `json:"status"`
	StatusCode            int            `json:"status_code"`
	Retryable             bool           `json:"retryable"`
	Terminal              bool           `json:"terminal"`
	FailureStage          string         `json:"failure_stage,omitempty"`
	Attempts              int            `json:"attempts,omitempty"`
	Message               string         `json:"message,omitempty"`
	Cause                 error          `json:"-"`
	DiagnosticCode        string         `json:"-"`
	ResponseType          string         `json:"-"`
	ContentType           string         `json:"-"`
	CFRay                 string         `json:"-"`
	TargetHost            string         `json:"-"`
	TargetPath            string         `json:"-"`
	ResponseBytes         int64          `json:"-"`
	ResponseBody          string         `json:"-"`
	ResponseBodyTruncated bool           `json:"-"`
	Cloudflare            bool           `json:"-"`
}

func (authError *AuthError) Error() string {
	if authError == nil {
		return "authentication failed"
	}
	if authError.Message != "" {
		return fmt.Sprintf("%s: %s", authError.Code, authError.Message)
	}
	return authError.Code
}

func (authError *AuthError) Unwrap() error {
	if authError == nil {
		return nil
	}
	return authError.Cause
}

func newAuthError(code string, state LifecycleState, status int, retryable, terminal bool, message string, cause error) *AuthError {
	return &AuthError{
		Code:           code,
		State:          state,
		LifecycleState: state,
		Status:         status,
		StatusCode:     status,
		Retryable:      retryable,
		Terminal:       terminal,
		Message:        message,
		Cause:          cause,
	}
}

func AsAuthError(err error) (*AuthError, bool) {
	var authError *AuthError
	if !errors.As(err, &authError) {
		return nil, false
	}
	return authError, true
}

func IsRetryable(err error) bool {
	authError, ok := AsAuthError(err)
	return ok && authError.Retryable
}

func IsTerminal(err error) bool {
	authError, ok := AsAuthError(err)
	return ok && authError.Terminal
}

func IsLifecycleState(err error, state LifecycleState) bool {
	authError, ok := AsAuthError(err)
	return ok && authError.State == state
}

func IsInteractionRequired(err error) bool {
	return IsLifecycleState(err, LifecycleInteractionRequired)
}

// ClassifyPermanentAccountResponse recognizes structured account termination
// errors returned by ChatGPT Web runtime endpoints.
func ClassifyPermanentAccountResponse(status int, payload []byte) *AuthError {
	if !hasExplicitRuntimeAccountTermination(payload) {
		return nil
	}
	classified := classifyPermanentAccountPayload(payload)
	if classified == nil || classified.State != LifecycleDead {
		return nil
	}
	result := *classified
	result.Status = status
	result.StatusCode = status
	return &result
}

// SafeLifecycleReason returns a stable, non-sensitive lifecycle error code.
func SafeLifecycleReason(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	if normalized == "" {
		return ""
	}
	switch normalized {
	case "authentication_failed",
		"ready",
		"awaiting_login",
		"access_denied",
		"account_deleted",
		"account_deactivated",
		"invalid_password",
		"invalid_totp",
		"invalid_totp_secret",
		"totp_required",
		"totp_factor_missing",
		"email_otp_required",
		"sms_otp_required",
		"passkey_required",
		"passkey_challenge_unavailable",
		"passkey_credential_invalid",
		"passkey_credential_not_allowed",
		"passkey_state_persist_failed",
		"passkey_verification_failed",
		"invalid_passkey_response",
		"passkey_session_invalid",
		"browser_confirmation_required",
		"turnstile_required",
		"arkose_required",
		"interaction_required",
		"reauth_required",
		"auto_relogin_exhausted",
		"missing_credentials",
		"login_method_invalid",
		"passkey_credential_missing",
		"password_missing",
		"api798_url_missing",
		"api798_url_invalid",
		"api798_authorization_failed",
		"api798_request_rejected",
		"api798_response_invalid",
		"api798_response_too_large",
		"api798_request_timeout",
		"api798_timeout",
		"api798_network_error",
		"api798_unavailable",
		"api798_email_otp_unavailable",
		"api798_email_otp_network_error",
		"api798_email_otp_rejected",
		"authorization_completion_required",
		"cloudflare_challenge",
		"login_proxy_invalid",
		"refresh_token_missing",
		"session_cookie_missing",
		"session_expired",
		"session_response_invalid",
		"session_refresh_network_error",
		"token_only_expired",
		"refresh_strategy_invalid",
		"source_auth_missing",
		"source_auth_invalid",
		"source_auth_disabled",
		"source_auth_replaced",
		"source_auth_changed",
		"source_identity_changed",
		"source_identity_mismatch",
		"identity_conflict",
		"source_refresh_unavailable",
		"source_token_unavailable",
		"access_token_missing",
		"invalid_grant",
		"app_session_terminated",
		"invalid_state",
		"oauth_redirect_limit",
		"oauth_redirect_untrusted",
		"token_response_invalid",
		"token_response_incomplete",
		"client_initialization_failed",
		"cookie_initialization_failed",
		"random_generation_failed",
		"sentinel_initialization_failed",
		"sentinel_generation_failed",
		"sentinel_network_error",
		"sentinel_transient_error",
		"sentinel_rejected",
		"sentinel_response_invalid",
		"sentinel_token_missing",
		"sentinel_pow_invalid",
		"sentinel_cookie_failed",
		"sentinel_session_observer_unavailable",
		"authentication_network_error",
		"acquisition_deadline_exceeded",
		"acquisition_canceled",
		"authorize_network_error",
		"authorize_continue_network_error",
		"authorize_redirect_network_error",
		"password_verify_network_error",
		"mfa_verify_network_error",
		"token_refresh_network_error",
		"token_exchange_network_error",
		"oauth_redirect_network_error",
		"authorize_failed",
		"authorize_continue_failed",
		"authorize_redirect_failed",
		"password_verify_failed",
		"mfa_verify_failed",
		"token_refresh_failed",
		"token_exchange_failed",
		"oauth_redirect_failed",
		"credential_invalid":
		return normalized
	default:
		return "authentication_failed"
	}
}

// SafeDiagnosticCode returns a stable, non-sensitive upstream or lifecycle code.
func SafeDiagnosticCode(value string) string {
	normalized := normalizeCode(value)
	switch normalized {
	case "cloudflare_challenge",
		"credential_unavailable",
		"dns_error",
		"forbidden",
		"identity_mismatch",
		"internal_server_error",
		"invalid_passkey_response",
		"invalid_response",
		"invalid_token",
		"network_error",
		"network_timeout",
		"proxy_error",
		"rate_limit_exceeded",
		"rate_limited",
		"refresh_failed",
		"request_canceled",
		"server_error",
		"service_unavailable",
		"temporarily_unavailable",
		"tls_error",
		"token_expired",
		"unauthorized",
		"upstream_challenge",
		"upstream_non_json",
		"upstream_request_error",
		"upstream_server_error",
		"upstream_unavailable":
		return normalized
	}
	safe := SafeLifecycleReason(normalized)
	if safe == "authentication_failed" && normalized != "authentication_failed" {
		return ""
	}
	return safe
}

// SafeLifecycleState returns a known lifecycle state and fails closed for
// malformed persisted or upstream values.
func SafeLifecycleState(value string) LifecycleState {
	normalized := LifecycleState(strings.ToLower(strings.TrimSpace(value)))
	switch normalized {
	case LifecycleLoginPending,
		LifecycleActive,
		LifecycleRefreshing,
		LifecycleReloginPending,
		LifecycleReauthRequired,
		LifecycleInteractionRequired,
		LifecycleDead:
		return normalized
	default:
		return LifecycleReauthRequired
	}
}
