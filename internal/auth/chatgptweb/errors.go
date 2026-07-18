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
	Code           string         `json:"code"`
	State          LifecycleState `json:"state"`
	LifecycleState LifecycleState `json:"lifecycle_state"`
	Status         int            `json:"status"`
	StatusCode     int            `json:"status_code"`
	Retryable      bool           `json:"retryable"`
	Terminal       bool           `json:"terminal"`
	Message        string         `json:"message,omitempty"`
	Cause          error          `json:"-"`
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
		"browser_confirmation_required",
		"turnstile_required",
		"arkose_required",
		"interaction_required",
		"missing_credentials",
		"authorization_completion_required",
		"refresh_token_missing",
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
