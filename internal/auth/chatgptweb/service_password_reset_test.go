package chatgptweb

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestServicePasswordVerificationTimeoutRemainsTransient(t *testing.T) {
	fixture := newLoginFixture(t, http.StatusOK, "")
	started, closed := make(chan struct{}), make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	fixture.passwordVerifyHandler = func(_ http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		close(started)
		select {
		case <-request.Context().Done():
		case <-release:
		}
		close(closed)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	credential, errLogin := NewService(fixture.options(time.Now())).Login(ctx, LoginInput{
		Relogin: true, Credential: &Credential{Email: "person@example.com", Password: "correct-password", LoginMethod: LoginMethodPasswordTOTP},
	})
	select {
	case <-started:
	default:
		t.Fatal("timeout occurred before the password request")
	}
	authError, ok := AsAuthError(errLogin)
	if !ok || !authError.Retryable || authError.Terminal || credential.LifecycleState != LifecycleReloginPending || authError.FailureStage != "password_verify" {
		t.Fatalf("timeout classification = %v, want temporary password-verification failure", errLogin)
	}
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out password connection was not closed")
	}
}

func TestServicePasswordSelectionPreservesVerificationFailure(t *testing.T) {
	for _, test := range []struct {
		name, body, code string
		status           int
		state            LifecycleState
		retryable        bool
	}{
		{"reset 401", `{"error":{"code":"password_reset_required","type":"invalid_request_error","message":"Reset your password"}}`, "password_reset_required", 401, LifecycleInteractionRequired, false},
		{"reset 400", `{"error":{"code":"password_reset_required","type":"invalid_request_error"}}`, "password_reset_required", 400, LifecycleInteractionRequired, false},
		{"wrong password", `{"error":{"code":"invalid_password"}}`, "invalid_password", 401, LifecycleReauthRequired, false},
		{"extra email after password", `{"page":{"type":"email_otp_verification"},"continue_url":"/email-verification"}`, "email_otp_required", 200, LifecycleInteractionRequired, false},
		{"plain HTML forbidden", `<html>Access denied</html>`, "authentication_forbidden", 403, LifecycleReloginPending, true},
		{"CF challenge", `<html><script src="/cdn-cgi/challenge-platform/h/g/orchestrate/chl_page/v1"></script></html>`, "cloudflare_challenge", 403, LifecycleReloginPending, true},
		{"server failure", `{"error":{"code":"server_error"}}`, "server_error", 500, LifecycleReloginPending, true},
		{"official dead", `{"error":{"code":"account_deactivated"}}`, "account_deactivated", 403, LifecycleDead, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newLoginFixture(t, test.status, test.body)
			fixture.authorizeBody = `{"page":{"type":"email_otp_verification"},"continue_url":"/email-verification"}`
			credential, errLogin := NewService(fixture.options(time.Now())).Login(t.Context(), LoginInput{
				Relogin:    true,
				Credential: &Credential{Email: "person@example.com", Password: "correct-password", TOTPSecret: "JBSWY3DPEHPK3PXP", LoginMethod: LoginMethodPasswordTOTP},
			})
			authError, ok := AsAuthError(errLogin)
			if !ok || authError.Code != test.code || authError.Retryable != test.retryable || credential.LifecycleState != test.state {
				t.Fatalf("classification = %v, want %s/%s/retryable=%t", errLogin, test.code, test.state, test.retryable)
			}
			if authError.FailureStage != "password_verify" {
				t.Fatalf("failure stage = %q, want password_verify", authError.FailureStage)
			}
			if test.status != http.StatusOK && authError.StatusCode != test.status {
				t.Fatalf("upstream status = %d, want %d", authError.StatusCode, test.status)
			}
			metadata := make(map[string]any)
			credential.ApplyToMetadata(metadata)
			if credential.LifecycleReason != test.code || metadata["lifecycle_reason"] != SafeLifecycleReason(test.code) || metadata["lifecycle_failure_code"] != SafeDiagnosticCode(test.code) {
				t.Fatal("specific failure did not survive credential persistence")
			}
			if test.state != LifecycleDead && DeadLifecycleDeletionAllowed(metadata) {
				t.Fatal("manual or transient failure became an automatic death deletion reason")
			}
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			if fixture.passwordPageCalls != 1 || fixture.passwordCalls != 1 || fixture.mfaVerifyCalls != 0 || fixture.tokenCalls != 0 || fixture.emailOTPCalls != 0 {
				t.Fatal("login stopped before selecting password or continued past the password failure")
			}
		})
	}
}

func TestServiceMFAIncorrectCodeKeepsStageAndStatus(t *testing.T) {
	now := time.Now().UTC()
	fixture := newLoginFixture(t, http.StatusOK, `{"page":{"type":"mfa_challenge","payload":{"mfa_request_id":"mfa-request","mfa_factors":[{"type":"totp","id":"totp-factor"}]}},"continue_url":"/mfa-challenge/totp-factor"}`)
	fixture.mfaStatus = http.StatusForbidden
	fixture.mfaBody = `{"error":{"code":"incorrect_code","message":"Incorrect code"}}`
	const secret = "JBSWY3DPEHPK3PXP"
	var errTOTP error
	fixture.wantTOTP, errTOTP = GenerateTOTP(secret, now)
	if errTOTP != nil {
		t.Fatal(errTOTP)
	}
	credential, errLogin := NewService(fixture.options(now)).Login(t.Context(), LoginInput{
		Relogin: true, Credential: &Credential{Email: "person@example.com", Password: "correct-password", TOTPSecret: secret, LoginMethod: LoginMethodPasswordTOTP},
	})
	authError, ok := AsAuthError(errLogin)
	if !ok || authError.Code != "incorrect_code" || authError.StatusCode != 403 || authError.FailureStage != "mfa_verify" || authError.Retryable || !authError.Terminal {
		t.Fatalf("MFA classification = %v, want terminal incorrect_code/403/mfa_verify", errLogin)
	}
	if credential.LifecycleState != LifecycleReauthRequired || credential.Password != "correct-password" || credential.TOTPSecret != secret {
		t.Fatal("MFA rejection changed authentication material or lifecycle incorrectly")
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.passwordCalls != 1 || fixture.mfaVerifyCalls != 1 || fixture.tokenCalls != 0 {
		t.Fatal("MFA rejection replayed authentication or exchanged tokens")
	}
}

func TestVerificationSpecificCodesDoNotOverrideTransientFailures(t *testing.T) {
	for _, stage := range []string{"password_verify", "mfa_verify"} {
		for _, status := range []int{408, 429, 500, 503} {
			for _, code := range []string{"password_reset_required", "incorrect_code"} {
				failure := classifyHTTPResponse(stage, status, []byte(`{"error":{"code":"`+code+`"}}`), LifecycleReloginPending)
				if !failure.Retryable || failure.Terminal || failure.State != LifecycleReloginPending || failure.StatusCode != status {
					t.Fatalf("%s/%d/%s lost temporary-failure semantics", stage, status, code)
				}
			}
		}
	}
	failure := classifyHTTPResponse("authorize", 403, []byte(`{"error":{"code":"incorrect_code"}}`), LifecycleReloginPending)
	if failure.Code != "authentication_forbidden" || !failure.Retryable {
		t.Fatal("non-MFA incorrect_code was interpreted as an authenticator rejection")
	}
}
