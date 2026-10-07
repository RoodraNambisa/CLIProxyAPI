package chatgptweb

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestServicePasswordTOTPSelectsPasswordAfterAccountIdentification(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(*loginFixture)
	}{
		{"continue envelope", func(f *loginFixture) {
			f.authorizeBody = `{"page":{"type":"email_otp_verification"},"continue_url":"/email-verification"}`
		}},
		{"continue passwordless envelope", func(f *loginFixture) {
			f.authorizeBody = `{"page":{"type":"passwordless_login"},"continue_url":"/email-verification"}`
		}},
		{"continue redirect", func(f *loginFixture) {
			f.authorizeRedirectURL = "/email-verification"
		}},
		{"followed continuation", func(f *loginFixture) {
			f.authorizeBody = `{"page":{"type":"password"},"continue_url":"/authorize-follow"}`
			f.authorizeFollowURL = "/email-verification"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixedNow := time.Date(2026, time.October, 8, 0, 0, 0, 0, time.UTC)
			const secret = "JBSWY3DPEHPK3PXP"
			fixture := newLoginFixture(t, http.StatusOK, `{
				"page":{"type":"mfa_challenge","payload":{
					"mfa_request_id":"mfa-request","mfa_factors":[{"type":"totp","id":"totp-factor"}]
				}},"continue_url":"/mfa-challenge/totp-factor"}`)
			test.setup(fixture)
			var errTOTP error
			fixture.wantTOTP, errTOTP = GenerateTOTP(secret, fixedNow)
			if errTOTP != nil {
				t.Fatal(errTOTP)
			}
			fixture.requirePasswordSelection = true
			fixture.passwordPageHandler = func(w http.ResponseWriter, r *http.Request) {
				if cookie, errCookie := r.Cookie("login_session"); errCookie != nil || cookie.Value != "session" {
					t.Error("password selection lost the authorize session")
				}
				http.SetCookie(w, &http.Cookie{Name: "password-selected", Value: "selected", Path: "/"})
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"page":{"type":"login_password"},"continue_url":"/log-in/password"}`)
			}
			credential, errLogin := NewService(fixture.options(fixedNow)).Login(t.Context(), LoginInput{
				Relogin: true,
				Credential: &Credential{
					Email: "person@example.com", Password: "correct-password", TOTPSecret: secret,
					LoginMethod: LoginMethodPasswordTOTP,
				},
			})
			if errLogin != nil {
				t.Fatal(errLogin)
			}
			if credential.LifecycleState != LifecycleActive || credential.LastReloginAt == "" {
				t.Fatalf("relogin state = %s, completed = %t", credential.LifecycleState, credential.LastReloginAt != "")
			}
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			if fixture.authorizeContinueCalls != 1 || fixture.passwordPageCalls != 1 || fixture.passwordCalls != 1 || fixture.mfaVerifyCalls != 1 || fixture.tokenCalls != 1 {
				t.Fatalf("calls authorize/password page/password/MFA/token = %d/%d/%d/%d/%d", fixture.authorizeContinueCalls, fixture.passwordPageCalls, fixture.passwordCalls, fixture.mfaVerifyCalls, fixture.tokenCalls)
			}
			if fixture.emailOTPCalls != 0 || fixture.emailOTPSendCalls != 0 || fixture.emailOTPResendCalls != 0 || fixture.mfaEmailVerifyCalls != 0 {
				t.Fatal("password relogin consumed an email OTP")
			}
		})
	}
}

func TestServicePasswordSelectionRejectsUntrustedEmailContinuation(t *testing.T) {
	fixture := newLoginFixture(t, http.StatusOK, "")
	fixture.authorizeResponseBody = `{"page":{"type":"email_otp_verification"},"continue_url":"https://untrusted.invalid/email-verification"}`
	credential, errLogin := NewService(fixture.options(time.Now())).Login(t.Context(), LoginInput{
		Credential: &Credential{Email: "person@example.com", Password: "correct-password", LoginMethod: LoginMethodPasswordTOTP},
	})
	authError, ok := AsAuthError(errLogin)
	if !ok || authError.Code != "oauth_redirect_untrusted" || credential.LifecycleState != LifecycleInteractionRequired {
		t.Fatalf("untrusted continuation error = %v", errLogin)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.passwordPageCalls != 0 || fixture.passwordCalls != 0 || fixture.tokenCalls != 0 {
		t.Fatal("untrusted continuation was silently discarded before authenticating")
	}
}

func TestServicePasswordSelectionDoesNotReplaceInitialEmailMFA(t *testing.T) {
	fixture := newLoginFixture(t, http.StatusOK, "")
	fixture.authorizeResponseBody = `{"page":{"type":"mfa_challenge","payload":{
		"mfa_request_id":"mfa-request","mfa_factors":[{"type":"email","id":"email-factor"}]
	}},"continue_url":"/email-verification"}`
	_, errLogin := NewService(fixture.options(time.Now())).Login(t.Context(), LoginInput{
		Credential: &Credential{Email: "person@example.com", Password: "correct-password", LoginMethod: LoginMethodPasswordTOTP},
	})
	authError, ok := AsAuthError(errLogin)
	if !ok || authError.Code != "email_otp_required" {
		t.Fatalf("email MFA error = %v", errLogin)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.passwordPageCalls != 0 || fixture.passwordCalls != 0 || fixture.emailOTPCalls != 0 {
		t.Fatal("password selection replaced a mandatory email MFA factor")
	}
}

func TestServicePasswordSelectionFailureDoesNotSubmitPassword(t *testing.T) {
	for _, test := range []struct {
		name      string
		status    int
		body      string
		location  string
		wantCode  string
		wantState LifecycleState
		wantStage string
	}{
		{name: "email still required", body: `{"page":{"type":"email_otp_verification"},"continue_url":"/email-verification"}`, wantCode: "email_otp_required", wantState: LifecycleInteractionRequired},
		{name: "email continuation overrides password page", body: `{"page":{"type":"login_password"},"continue_url":"/email-verification"}`, wantCode: "email_otp_required", wantState: LifecycleInteractionRequired},
		{name: "email redirect", status: http.StatusFound, location: "/email-verification", wantCode: "email_otp_required", wantState: LifecycleInteractionRequired},
		{name: "passkey required", body: `{"page":{"type":"passkey_challenge"}}`, wantCode: "passkey_required", wantState: LifecycleInteractionRequired},
		{name: "account deleted", body: `{"error":{"code":"account_deleted"}}`, wantCode: "account_deleted", wantState: LifecycleDead},
		{name: "untrusted redirect", status: http.StatusFound, location: "https://untrusted.invalid/collect", wantCode: "oauth_redirect_untrusted", wantState: LifecycleInteractionRequired},
		{name: "untrusted continuation", body: `{"page":{"type":"login_password"},"continue_url":"https://untrusted.invalid/collect"}`, wantCode: "oauth_redirect_untrusted", wantState: LifecycleInteractionRequired},
		{name: "unknown page", body: `{"page":{"type":"unknown_verification"},"continue_url":"/unknown-checkpoint"}`, wantCode: "authorization_completion_required", wantState: LifecycleInteractionRequired},
		{name: "server failure", status: http.StatusServiceUnavailable, body: `{"error":{"code":"server_error"}}`, wantCode: "server_error", wantState: LifecycleReloginPending},
		{name: "cloudflare challenge", status: http.StatusForbidden, body: `<html><title>Just a moment...</title><script src="/cdn-cgi/challenge-platform/h/g/orchestrate/chl_page/v1"></script></html>`, wantCode: "cloudflare_challenge", wantState: LifecycleReloginPending, wantStage: "oauth_redirect"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newLoginFixture(t, http.StatusOK, "")
			fixture.authorizePath = "/email-verification"
			fixture.passwordPageHandler = func(w http.ResponseWriter, r *http.Request) {
				if test.location != "" {
					http.Redirect(w, r, test.location, test.status)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if test.status != 0 {
					w.WriteHeader(test.status)
				}
				_, _ = io.WriteString(w, test.body)
			}
			credential, errLogin := NewService(fixture.options(time.Now())).Login(t.Context(), LoginInput{
				Relogin:    true,
				Credential: &Credential{Email: "person@example.com", Password: "correct-password", LoginMethod: LoginMethodPasswordTOTP},
			})
			authError, ok := AsAuthError(errLogin)
			if !ok || authError.Code != test.wantCode || credential.LifecycleState != test.wantState {
				t.Fatalf("selection error = %v, want %s/%s", errLogin, test.wantCode, test.wantState)
			}
			wantStage := test.wantStage
			if wantStage == "" {
				wantStage = "authorize_redirect"
			}
			if authError.FailureStage != wantStage {
				t.Fatalf("selection failure stage = %q", authError.FailureStage)
			}
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			if fixture.passwordPageCalls != 1 || fixture.passwordCalls != 0 || fixture.emailOTPCalls != 0 || fixture.tokenCalls != 0 {
				t.Fatalf("selection failure continued login: page=%d password=%d email=%d token=%d", fixture.passwordPageCalls, fixture.passwordCalls, fixture.emailOTPCalls, fixture.tokenCalls)
			}
		})
	}
}

func TestServicePasswordSelectionCallbackChecksState(t *testing.T) {
	for _, validState := range []bool{false, true} {
		for _, redirect := range []bool{false, true} {
			t.Run(fmt.Sprintf("state=%t/redirect=%t", validState, redirect), func(t *testing.T) {
				fixture := newLoginFixture(t, http.StatusOK, "")
				fixture.authorizePath = "/email-verification"
				fixture.passwordPageHandler = func(w http.ResponseWriter, r *http.Request) {
					callback := fixture.callbackURL()
					if !validState {
						callback += "-wrong-state"
					}
					if redirect {
						http.Redirect(w, r, callback, http.StatusFound)
						return
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = fmt.Fprintf(w, `{"page":{"type":"external_url"},"continue_url":%q}`, callback)
				}
				credential, errLogin := NewService(fixture.options(time.Now())).Login(t.Context(), LoginInput{
					Credential: &Credential{Email: "person@example.com", Password: "correct-password", LoginMethod: LoginMethodPasswordTOTP},
				})
				if validState {
					if errLogin != nil || credential.LifecycleState != LifecycleActive {
						t.Fatalf("callback error = %v", errLogin)
					}
				} else if authError, ok := AsAuthError(errLogin); !ok || authError.Code != "invalid_state" {
					t.Fatalf("callback error = %v, want invalid_state", errLogin)
				}
				fixture.mu.Lock()
				defer fixture.mu.Unlock()
				wantTokenCalls := 0
				if validState {
					wantTokenCalls = 1
				}
				if fixture.tokenCalls != wantTokenCalls || fixture.passwordCalls != 0 {
					t.Fatalf("callback token/password calls = %d/%d", fixture.tokenCalls, fixture.passwordCalls)
				}
			})
		}
	}
}

func TestServicePasswordSelectionCancellationClosesRequest(t *testing.T) {
	fixture := newLoginFixture(t, http.StatusOK, "")
	fixture.authorizePath = "/email-verification"
	started := make(chan struct{})
	closed := make(chan struct{})
	fixture.passwordPageHandler = func(w http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-r.Context().Done():
		case <-t.Context().Done():
		}
		close(closed)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := NewService(fixture.options(time.Now())).Login(ctx, LoginInput{
			Credential: &Credential{Email: "person@example.com", Password: "correct-password", LoginMethod: LoginMethodPasswordTOTP},
		})
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("password selection did not start")
	}
	cancel()
	select {
	case err := <-done:
		authError, ok := AsAuthError(err)
		if !ok || authError.Code != "acquisition_canceled" {
			t.Fatalf("cancel error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("canceled login did not return")
	}
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("canceled password request was not closed")
	}
}
