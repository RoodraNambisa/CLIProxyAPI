package chatgptweb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

type loginFixture struct {
	t                      *testing.T
	passwordStatus         int
	passwordBody           string
	passwordCalls          int
	authorizePath          string
	authorizeBody          string
	authorizeContinueCalls int
	authorizeRedirect      bool
	passwordPageRedirect   bool
	passwordRedirect       bool
	tokenCalls             int
	mfaVerifyCalls         int
	mfaStatus              int
	mfaBody                string
	wantTOTP               string
	state                  string
	codeChallenge          string
	server                 *httptest.Server
	mu                     sync.Mutex
}

func newLoginFixture(t *testing.T, passwordStatus int, passwordBody string) *loginFixture {
	t.Helper()
	fixture := &loginFixture{t: t, passwordStatus: passwordStatus, passwordBody: passwordBody, authorizePath: "/log-in"}
	fixture.server = httptest.NewServer(http.HandlerFunc(fixture.serveHTTP))
	t.Cleanup(fixture.server.Close)
	return fixture
}

func (fixture *loginFixture) serveHTTP(response http.ResponseWriter, request *http.Request) {
	switch request.URL.Path {
	case "/api/accounts/authorize":
		fixture.handleAuthorize(response, request)
	case "/backend-api/sentinel/req":
		fixture.handleSentinel(response, request)
	case "/api/accounts/authorize/continue":
		fixture.handleAuthorizeContinue(response, request)
	case "/log-in", "/log-in/password", "/email-verification":
		response.Header().Set("Content-Type", "text/html")
		_, _ = io.WriteString(response, "login")
	case "/password-page":
		if fixture.passwordPageRedirect {
			http.Redirect(response, request, fixture.callbackURL(), http.StatusFound)
			return
		}
		response.Header().Set("Content-Type", "text/html")
		_, _ = io.WriteString(response, "password")
	case "/auth/callback":
		_, _ = io.WriteString(response, "callback")
	case "/api/accounts/password/verify":
		fixture.handlePassword(response, request)
	case "/api/accounts/mfa/verify":
		fixture.handleMFAVerify(response, request)
	case "/api/accounts/oauth/token":
		fixture.handleTokenExchange(response, request)
	default:
		fixture.t.Errorf("unexpected request: %s %s", request.Method, request.URL.String())
		http.NotFound(response, request)
	}
}

func (fixture *loginFixture) handleAuthorize(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		fixture.t.Errorf("authorize method = %s", request.Method)
	}
	query := request.URL.Query()
	for key, want := range map[string]string{
		"client_id":             OAuthClientID,
		"audience":              AudienceURL,
		"response_type":         "code",
		"response_mode":         "query",
		"code_challenge_method": "S256",
	} {
		if got := query.Get(key); got != want {
			fixture.t.Errorf("authorize %s = %q, want %q", key, got, want)
		}
	}
	if query.Get("nonce") == "" || query.Get("device_id") == "" || query.Get("login_hint") != "person@example.com" {
		fixture.t.Errorf("authorize query = %s", request.URL.RawQuery)
	}
	fixture.mu.Lock()
	fixture.state = query.Get("state")
	fixture.codeChallenge = query.Get("code_challenge")
	fixture.mu.Unlock()
	http.SetCookie(response, &http.Cookie{Name: "login_session", Value: "session", Path: "/", HttpOnly: true})
	http.Redirect(response, request, fixture.authorizePath, http.StatusFound)
}

func (fixture *loginFixture) handleSentinel(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		fixture.t.Errorf("sentinel method = %s", request.Method)
	}
	var body map[string]any
	if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
		fixture.t.Errorf("decode sentinel request: %v", err)
	}
	if body["flow"] != "authorize_continue" && body["flow"] != "password_verify" {
		fixture.t.Errorf("sentinel flow = %#v", body["flow"])
	}
	response.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(response, `{"token":"fixture-challenge","proofofwork":{"required":false}}`)
}

func (fixture *loginFixture) handleAuthorizeContinue(response http.ResponseWriter, request *http.Request) {
	fixture.mu.Lock()
	fixture.authorizeContinueCalls++
	fixture.mu.Unlock()
	if request.Header.Get("OpenAI-Sentinel-Token") == "" || request.Header.Get("Oai-Device-Id") == "" {
		fixture.t.Errorf("authorize/continue headers missing: %#v", request.Header)
	}
	var body struct {
		Username struct {
			Value string `json:"value"`
			Kind  string `json:"kind"`
		} `json:"username"`
	}
	if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
		fixture.t.Errorf("decode authorize/continue: %v", err)
	}
	if body.Username.Value != "person@example.com" || body.Username.Kind != "email" {
		fixture.t.Errorf("authorize/continue username = %#v", body.Username)
	}
	if fixture.authorizeRedirect {
		http.Redirect(response, request, fixture.callbackURL(), http.StatusFound)
		return
	}
	response.Header().Set("Content-Type", "application/json")
	if fixture.authorizeBody != "" {
		_, _ = io.WriteString(response, fixture.authorizeBody)
		return
	}
	_, _ = io.WriteString(response, `{"continue_url":"/password-page","page":{"type":"password"}}`)
}

func (fixture *loginFixture) handlePassword(response http.ResponseWriter, request *http.Request) {
	fixture.mu.Lock()
	fixture.passwordCalls++
	fixture.mu.Unlock()
	if request.Header.Get("OpenAI-Sentinel-Token") == "" {
		fixture.t.Error("password request did not include a sentinel token")
	}
	var body map[string]string
	if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
		fixture.t.Errorf("decode password request: %v", err)
	}
	if body["password"] != "correct-password" {
		fixture.t.Errorf("password request value did not match fixture input")
	}
	if fixture.passwordRedirect {
		http.Redirect(response, request, fixture.callbackURL(), http.StatusFound)
		return
	}
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(fixture.passwordStatus)
	if fixture.passwordBody != "" {
		_, _ = io.WriteString(response, fixture.passwordBody)
		return
	}
	_, _ = fmt.Fprintf(response, `{"continue_url":%q,"page":{"type":"authorized"}}`, fixture.callbackURL())
}

func (fixture *loginFixture) handleMFAVerify(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		fixture.t.Errorf("MFA verify method = %s", request.Method)
	}
	if request.Header.Get("Oai-Device-Id") == "" || request.Header.Get("Origin") != fixture.server.URL {
		fixture.t.Errorf("MFA verify headers = %#v", request.Header)
	}
	var body map[string]string
	if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
		fixture.t.Errorf("decode MFA verify request: %v", err)
	}
	fixture.mu.Lock()
	fixture.mfaVerifyCalls++
	wantTOTP := fixture.wantTOTP
	fixture.mu.Unlock()
	if body["id"] != "totp-factor" || body["type"] != "totp" || body["mfa_request_id"] != "mfa-request" || body["code"] != wantTOTP {
		fixture.t.Errorf("MFA verify body = %#v", body)
	}
	response.Header().Set("Content-Type", "application/json")
	if fixture.mfaStatus != 0 {
		response.WriteHeader(fixture.mfaStatus)
	}
	if fixture.mfaBody != "" {
		_, _ = io.WriteString(response, fixture.mfaBody)
		return
	}
	_, _ = fmt.Fprintf(response, `{"continue_url":%q,"page":{"type":"authorized"}}`, fixture.callbackURL())
}

func (fixture *loginFixture) callbackURL() string {
	fixture.mu.Lock()
	state := fixture.state
	fixture.mu.Unlock()
	return fixture.server.URL + "/auth/callback?code=fixture-code&state=" + url.QueryEscape(state)
}

func (fixture *loginFixture) handleTokenExchange(response http.ResponseWriter, request *http.Request) {
	fixture.mu.Lock()
	fixture.tokenCalls++
	wantChallenge := fixture.codeChallenge
	fixture.mu.Unlock()
	var body struct {
		ClientID     string `json:"client_id"`
		CodeVerifier string `json:"code_verifier"`
		GrantType    string `json:"grant_type"`
		Code         string `json:"code"`
		RedirectURI  string `json:"redirect_uri"`
	}
	if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
		fixture.t.Errorf("decode token exchange: %v", err)
	}
	digest := sha256.Sum256([]byte(body.CodeVerifier))
	if got := base64.RawURLEncoding.EncodeToString(digest[:]); got != wantChallenge {
		fixture.t.Errorf("code verifier challenge = %q, want %q", got, wantChallenge)
	}
	if body.ClientID != OAuthClientID || body.GrantType != "authorization_code" || body.Code != "fixture-code" {
		fixture.t.Errorf("token exchange body = %#v", body)
	}
	expiresAt := time.Date(2026, time.July, 17, 12, 0, 0, 0, time.UTC).Unix()
	response.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprintf(response, `{"access_token":%q,"refresh_token":"refresh-token","id_token":"id-token"}`, testJWT(expiresAt))
}

func (fixture *loginFixture) options(now time.Time) Options {
	return Options{
		AuthBaseURL:     fixture.server.URL,
		SentinelBaseURL: fixture.server.URL,
		RedirectURL:     fixture.server.URL + "/auth/callback",
		Rand:            zeroReader{},
		Now:             func() time.Time { return now },
	}
}

func TestServiceLogin(t *testing.T) {
	fixedNow := time.Date(2026, time.July, 16, 12, 0, 0, 0, time.UTC)
	fixture := newLoginFixture(t, http.StatusOK, "")
	service := NewService(fixture.options(fixedNow))
	credential, err := service.Login(t.Context(), LoginInput{
		Email:      "person@example.com",
		Password:   "correct-password",
		TOTPSecret: "JBSWY3DPEHPK3PXP",
	})
	if err != nil {
		t.Fatal(err)
	}
	if credential.Type != Provider || credential.LifecycleState != LifecycleActive {
		t.Fatalf("credential lifecycle = %q/%q", credential.Type, credential.LifecycleState)
	}
	if credential.RefreshToken != "refresh-token" || credential.TOTPSecret != "JBSWY3DPEHPK3PXP" {
		t.Fatal("credential tokens or retained TOTP secret did not match")
	}
	if credential.Expired != "2026-07-17T12:00:00Z" || credential.LastLoginAt != "2026-07-16T12:00:00Z" {
		t.Fatalf("credential scheduling timestamps = expired %q, login %q", credential.Expired, credential.LastLoginAt)
	}
	fixture.mu.Lock()
	tokenCalls := fixture.tokenCalls
	fixture.mu.Unlock()
	if tokenCalls != 1 {
		t.Fatalf("token exchange calls = %d, want 1", tokenCalls)
	}
	if len(credential.Cookies) == 0 || credential.Persona.Profile != "chrome_146" {
		t.Fatal("login did not persist cookies and persona")
	}
}

func TestServiceReloginRestoresCookieBrowserIdentity(t *testing.T) {
	fixedNow := time.Date(2026, time.July, 16, 12, 0, 0, 0, time.UTC)
	fixture := newLoginFixture(t, http.StatusOK, "")
	service := NewService(fixture.options(fixedNow))
	authURL, err := url.Parse(fixture.server.URL)
	if err != nil {
		t.Fatal(err)
	}
	credential, err := service.Login(t.Context(), LoginInput{
		Credential: &Credential{
			Email:    "person@example.com",
			Password: "correct-password",
			Cookies: []Cookie{{
				Name: "oai-did", Value: "existing-device", Host: authURL.Hostname(), Path: "/",
			}},
		},
		Relogin: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if credential.DeviceID != "existing-device" {
		t.Fatalf("device ID = %q, want existing-device", credential.DeviceID)
	}
	if strings.TrimSpace(credential.SessionID) == "" {
		t.Fatal("re-login did not initialize a stable session ID")
	}
}

func TestServiceLoginCapturesFollowedOAuthCallbacks(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*loginFixture)
	}{
		{
			name: "authorize continue",
			configure: func(fixture *loginFixture) {
				fixture.authorizeRedirect = true
			},
		},
		{
			name: "authorize continuation page",
			configure: func(fixture *loginFixture) {
				fixture.passwordPageRedirect = true
			},
		},
		{
			name: "password verify",
			configure: func(fixture *loginFixture) {
				fixture.passwordRedirect = true
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newLoginFixture(t, http.StatusOK, "")
			test.configure(fixture)
			service := NewService(fixture.options(time.Date(2026, time.July, 16, 12, 0, 0, 0, time.UTC)))
			credential, err := service.Login(t.Context(), LoginInput{
				Email:    "person@example.com",
				Password: "correct-password",
			})
			if err != nil {
				t.Fatal(err)
			}
			if credential.LifecycleState != LifecycleActive || credential.RefreshToken != "refresh-token" {
				t.Fatalf("credential = state %q refresh token %q", credential.LifecycleState, credential.RefreshToken)
			}
		})
	}
}

func TestServiceLoginSkipsAuthorizeContinueOnPasswordPage(t *testing.T) {
	fixedNow := time.Date(2026, time.July, 16, 12, 0, 0, 0, time.UTC)
	fixture := newLoginFixture(t, http.StatusOK, "")
	fixture.authorizePath = "/log-in/password"
	service := NewService(fixture.options(fixedNow))
	credential, err := service.Login(t.Context(), LoginInput{
		Email:    "person@example.com",
		Password: "correct-password",
	})
	if err != nil {
		t.Fatal(err)
	}
	if credential.LifecycleState != LifecycleActive || credential.RefreshToken != "refresh-token" {
		t.Fatalf("credential = state %q refresh token %q", credential.LifecycleState, credential.RefreshToken)
	}
	fixture.mu.Lock()
	authorizeContinueCalls := fixture.authorizeContinueCalls
	passwordCalls := fixture.passwordCalls
	fixture.mu.Unlock()
	if authorizeContinueCalls != 0 || passwordCalls != 1 {
		t.Fatalf("authorize continue/password calls = %d/%d, want 0/1", authorizeContinueCalls, passwordCalls)
	}
}

func TestServiceLoginRequiresInteractionForAuthorizeVerificationPage(t *testing.T) {
	fixture := newLoginFixture(t, http.StatusOK, "")
	fixture.authorizePath = "/email-verification"
	service := NewService(fixture.options(time.Date(2026, time.July, 16, 12, 0, 0, 0, time.UTC)))
	credential, err := service.Login(t.Context(), LoginInput{
		Email:    "person@example.com",
		Password: "correct-password",
	})
	if err == nil {
		t.Fatal("Login() succeeded, want interaction requirement")
	}
	authError, ok := AsAuthError(err)
	if !ok || authError.Code != "email_otp_required" || authError.State != LifecycleInteractionRequired {
		t.Fatalf("Login() error = %#v", err)
	}
	if credential.LifecycleState != LifecycleInteractionRequired {
		t.Fatalf("credential lifecycle = %q", credential.LifecycleState)
	}
	fixture.mu.Lock()
	authorizeContinueCalls := fixture.authorizeContinueCalls
	passwordCalls := fixture.passwordCalls
	fixture.mu.Unlock()
	if authorizeContinueCalls != 0 || passwordCalls != 0 {
		t.Fatalf("authorize continue/password calls = %d/%d, want 0/0", authorizeContinueCalls, passwordCalls)
	}
}

func TestServiceLoginWithTOTPChallenge(t *testing.T) {
	fixedNow := time.Date(2026, time.July, 16, 12, 0, 0, 0, time.UTC)
	const secret = "JBSWY3DPEHPK3PXP"
	wantTOTP, errTOTP := GenerateTOTP(secret, fixedNow)
	if errTOTP != nil {
		t.Fatal(errTOTP)
	}
	fixture := newLoginFixture(t, http.StatusOK, `{
		"continue_url":"/mfa-challenge/totp-factor",
		"page":{"type":"mfa_challenge","payload":{
			"mfa_request_id":"top-level-request",
			"factors":[{"factor_type":"totp","id":"totp-factor","metadata":{"mfa_request_id":"mfa-request"}}]
		}}
	}`)
	fixture.wantTOTP = wantTOTP
	service := NewService(fixture.options(fixedNow))
	credential, errLogin := service.Login(t.Context(), LoginInput{
		Email:      "person@example.com",
		Password:   "correct-password",
		TOTPSecret: secret,
	})
	if errLogin != nil {
		t.Fatal(errLogin)
	}
	if credential.LifecycleState != LifecycleActive || credential.RefreshToken != "refresh-token" {
		t.Fatalf("credential = state %q refresh token %q", credential.LifecycleState, credential.RefreshToken)
	}
	fixture.mu.Lock()
	mfaVerifyCalls := fixture.mfaVerifyCalls
	fixture.mu.Unlock()
	if mfaVerifyCalls != 1 {
		t.Fatalf("MFA verify calls = %d, want 1", mfaVerifyCalls)
	}
}

func TestServiceLoginWithDirectTOTPChallenge(t *testing.T) {
	fixedNow := time.Date(2026, time.July, 16, 12, 0, 0, 0, time.UTC)
	const secret = "JBSWY3DPEHPK3PXP"
	wantTOTP, errTOTP := GenerateTOTP(secret, fixedNow)
	if errTOTP != nil {
		t.Fatal(errTOTP)
	}
	fixture := newLoginFixture(t, http.StatusOK, "")
	fixture.authorizeBody = `{
		"continue_url":"/mfa-challenge/totp-factor",
		"page":{"type":"mfa_challenge","payload":{
			"factors":[{"factor_type":"totp","id":"totp-factor","mfa_request_id":"mfa-request"}]
		}}
	}`
	fixture.wantTOTP = wantTOTP
	service := NewService(fixture.options(fixedNow))
	credential, errLogin := service.Login(t.Context(), LoginInput{
		Email:      "person@example.com",
		Password:   "correct-password",
		TOTPSecret: secret,
	})
	if errLogin != nil {
		t.Fatal(errLogin)
	}
	if credential.LifecycleState != LifecycleActive {
		t.Fatalf("credential lifecycle = %q", credential.LifecycleState)
	}
	fixture.mu.Lock()
	passwordCalls := fixture.passwordCalls
	mfaVerifyCalls := fixture.mfaVerifyCalls
	fixture.mu.Unlock()
	if passwordCalls != 0 || mfaVerifyCalls != 1 {
		t.Fatalf("password/MFA calls = %d/%d, want 0/1", passwordCalls, mfaVerifyCalls)
	}
}

func TestServiceLoginTOTPFailureClassifications(t *testing.T) {
	const challenge = `{
		"continue_url":"/mfa-challenge/totp-factor",
		"page":{"type":"mfa_challenge","payload":{
			"factors":[{"factor_type":"totp","id":"totp-factor","metadata":{"mfa_request_id":"mfa-request"}}]
		}}
	}`
	tests := []struct {
		name      string
		secret    string
		mfaStatus int
		mfaBody   string
		wantCode  string
		wantState LifecycleState
		wantCalls int
	}{
		{name: "missing secret", wantCode: "totp_required", wantState: LifecycleInteractionRequired},
		{name: "invalid local secret", secret: "not-base32!", wantCode: "invalid_totp_secret", wantState: LifecycleReauthRequired},
		{name: "rejected code", secret: "JBSWY3DPEHPK3PXP", mfaStatus: http.StatusBadRequest, mfaBody: `{"error":{"code":"invalid_code"}}`, wantCode: "invalid_totp", wantState: LifecycleReauthRequired, wantCalls: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixedNow := time.Date(2026, time.July, 16, 12, 0, 0, 0, time.UTC)
			fixture := newLoginFixture(t, http.StatusOK, challenge)
			fixture.mfaStatus = test.mfaStatus
			fixture.mfaBody = test.mfaBody
			if test.secret != "" {
				fixture.wantTOTP, _ = GenerateTOTP(test.secret, fixedNow)
			}
			service := NewService(fixture.options(fixedNow))
			credential, errLogin := service.Login(t.Context(), LoginInput{
				Email:      "person@example.com",
				Password:   "correct-password",
				TOTPSecret: test.secret,
			})
			if errLogin == nil {
				t.Fatal("Login() succeeded, want TOTP failure")
			}
			authError, ok := AsAuthError(errLogin)
			if !ok || authError.Code != test.wantCode || authError.State != test.wantState {
				t.Fatalf("Login() error = %#v", errLogin)
			}
			if credential.LifecycleState != test.wantState || credential.LifecycleReason != test.wantCode {
				t.Fatalf("credential lifecycle = %q/%q", credential.LifecycleState, credential.LifecycleReason)
			}
			fixture.mu.Lock()
			calls := fixture.mfaVerifyCalls
			fixture.mu.Unlock()
			if calls != test.wantCalls {
				t.Fatalf("MFA verify calls = %d, want %d", calls, test.wantCalls)
			}
		})
	}
}

func TestUnsupportedMFAFactorRequiresInteraction(t *testing.T) {
	authError := unsupportedMFAError(map[string]any{
		"factors": []any{map[string]any{"factor_type": "passkey", "id": "passkey-factor"}},
	})
	if authError.Code != "passkey_required" || authError.State != LifecycleInteractionRequired {
		t.Fatalf("unsupportedMFAError() = %#v", authError)
	}
}

func TestServiceLoginClassifications(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		body       string
		wantCode   string
		wantState  LifecycleState
		wantStatus int
	}{
		{
			name:       "invalid password",
			status:     http.StatusUnauthorized,
			body:       `{"error":{"code":"invalid_credentials","message":"Invalid credentials"}}`,
			wantCode:   "invalid_password",
			wantState:  LifecycleReauthRequired,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "deactivated account",
			status:     http.StatusForbidden,
			body:       `{"error":{"code":"account_deactivated","message":"Account deactivated"}}`,
			wantCode:   "account_deactivated",
			wantState:  LifecycleDead,
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "email interaction",
			status:     http.StatusOK,
			body:       `{"page":{"type":"email_otp_verification"},"continue_url":"/email-verification"}`,
			wantCode:   "email_otp_required",
			wantState:  LifecycleInteractionRequired,
			wantStatus: 0,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newLoginFixture(t, test.status, test.body)
			service := NewService(fixture.options(time.Date(2026, time.July, 16, 12, 0, 0, 0, time.UTC)))
			credential, err := service.Login(t.Context(), LoginInput{Email: "person@example.com", Password: "correct-password"})
			if err == nil {
				t.Fatal("Login() succeeded, want classified error")
			}
			authError, ok := AsAuthError(err)
			if !ok {
				t.Fatalf("Login() error type = %T", err)
			}
			if authError.Code != test.wantCode || authError.State != test.wantState || authError.StatusCode != test.wantStatus {
				t.Fatalf("Login() error = %#v", authError)
			}
			if credential.LifecycleState != test.wantState || credential.LifecycleReason != test.wantCode {
				t.Fatalf("credential lifecycle = %q/%q", credential.LifecycleState, credential.LifecycleReason)
			}
			fixture.mu.Lock()
			tokenCalls := fixture.tokenCalls
			fixture.mu.Unlock()
			if tokenCalls != 0 {
				t.Fatalf("token exchange calls = %d, want 0", tokenCalls)
			}
		})
	}
}

func TestPageTypeClassifications(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		"totp_verification":      "totp_required",
		"email_otp_verification": "email_otp_required",
		"sms_code_verification":  "sms_otp_required",
		"passkey_verification":   "passkey_required",
		"browser_confirmation":   "browser_confirmation_required",
		"turnstile_verification": "turnstile_required",
		"arkose_challenge":       "arkose_required",
	}
	for pageType, wantCode := range tests {
		authError := classifyPageType(pageType)
		if authError == nil || authError.Code != wantCode || authError.State != LifecycleInteractionRequired {
			t.Errorf("classifyPageType(%q) = %#v", pageType, authError)
		}
	}
}

func TestParseOAuthCallbackRequiresConfiguredTargetAndState(t *testing.T) {
	const (
		redirectURL = "https://platform.openai.com/auth/callback"
		state       = "expected-state"
	)
	tests := []struct {
		name        string
		rawURL      string
		wantCode    string
		wantMatched bool
		wantError   string
	}{
		{
			name:        "valid code",
			rawURL:      redirectURL + "?code=oauth-code&state=" + state,
			wantCode:    "oauth-code",
			wantMatched: true,
		},
		{
			name:        "wrong state",
			rawURL:      redirectURL + "?code=oauth-code&state=wrong",
			wantMatched: true,
			wantError:   "invalid_state",
		},
		{
			name:        "error without state",
			rawURL:      redirectURL + "?error=access_denied",
			wantMatched: true,
			wantError:   "invalid_state",
		},
		{
			name:        "valid error",
			rawURL:      redirectURL + "?error=access_denied&state=" + state,
			wantMatched: true,
			wantError:   "access_denied",
		},
		{
			name:   "foreign host",
			rawURL: "https://attacker.example/auth/callback?code=oauth-code&state=" + state,
		},
		{
			name:   "foreign path",
			rawURL: "https://platform.openai.com/other?code=oauth-code&state=" + state,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			code, matched, authError := parseOAuthCallback(test.rawURL, redirectURL, state)
			if code != test.wantCode || matched != test.wantMatched {
				t.Fatalf("parseOAuthCallback() = %q, %v, %#v", code, matched, authError)
			}
			if test.wantError == "" && authError != nil {
				t.Fatalf("parseOAuthCallback() error = %#v", authError)
			}
			if test.wantError != "" && (authError == nil || authError.Code != test.wantError) {
				t.Fatalf("parseOAuthCallback() error = %#v, want %q", authError, test.wantError)
			}
		})
	}
}

func TestClassifyOAuthContinuationURL(t *testing.T) {
	authBaseURL := "https://auth.openai.com"
	if authError := classifyOAuthContinuationURL(authBaseURL+"/authorize/resume", authBaseURL); authError != nil {
		t.Fatalf("trusted continuation = %#v", authError)
	}
	if authError := classifyOAuthContinuationURL(authBaseURL+"/email-verification", authBaseURL); authError == nil || authError.Code != "email_otp_required" {
		t.Fatalf("interaction continuation = %#v", authError)
	}
	if authError := classifyOAuthContinuationURL("https://attacker.example/continue", authBaseURL); authError == nil || authError.Code != "oauth_redirect_untrusted" {
		t.Fatalf("untrusted continuation = %#v", authError)
	}
}

func TestServiceAcquisitionContextUsesShorterDeadline(t *testing.T) {
	service := NewService(Options{AcquisitionTimeout: 50 * time.Millisecond})
	parent, cancelParent := context.WithTimeout(context.Background(), time.Second)
	defer cancelParent()
	ctx, cancel := service.acquisitionContext(parent)
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("acquisition context has no deadline")
	}
	if remaining := time.Until(deadline); remaining <= 0 || remaining > 100*time.Millisecond {
		t.Fatalf("acquisition deadline remaining = %v, want configured timeout", remaining)
	}
}

func TestDeletedOrDeactivatedTextIsPermanent(t *testing.T) {
	for _, body := range []string{
		`<html>This account was deleted or deactivated.</html>`,
		`You do not have an account because it has been deleted.`,
	} {
		authError := classifyHTTPResponse("password_verify", http.StatusForbidden, []byte(body), LifecycleLoginPending)
		if authError == nil || authError.State != LifecycleDead || !authError.Terminal {
			t.Fatalf("classifyHTTPResponse(%q) = %#v, want permanent dead", body, authError)
		}
	}
	authError := classifyPermanentAccountPayload([]byte(`{"page":{"type":"error","payload":{"message":"account_deleted"}}}`))
	if authError == nil || authError.Code != "account_deleted" || authError.State != LifecycleDead {
		t.Fatalf("classifyPermanentAccountPayload() = %#v, want account_deleted", authError)
	}
}

func TestServiceRefreshRotation(t *testing.T) {
	fixedNow := time.Date(2026, time.July, 16, 14, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		response    string
		wantRefresh string
	}{
		{name: "rotated", response: `{"access_token":"ACCESS","refresh_token":"new-refresh","id_token":"new-id"}`, wantRefresh: "new-refresh"},
		{name: "not rotated", response: `{"access_token":"ACCESS"}`, wantRefresh: "old-refresh"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expiresAt := time.Date(2026, time.July, 18, 0, 0, 0, 0, time.UTC).Unix()
			responseBody := strings.Replace(test.response, "ACCESS", testJWT(expiresAt), 1)
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				if request.URL.Path != "/oauth/token" {
					http.NotFound(response, request)
					return
				}
				if err := request.ParseForm(); err != nil {
					t.Errorf("ParseForm: %v", err)
				}
				if request.Form.Get("grant_type") != "refresh_token" || request.Form.Get("refresh_token") != "old-refresh" || request.Form.Get("client_id") != OAuthClientID {
					t.Errorf("refresh form = %#v", request.Form)
				}
				response.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(response, responseBody)
			}))
			defer server.Close()
			service := NewService(Options{AuthBaseURL: server.URL, Rand: zeroReader{}, Now: func() time.Time { return fixedNow }})
			credential, err := service.Refresh(t.Context(), Credential{
				Type:         Provider,
				AccessToken:  "old-access",
				RefreshToken: "old-refresh",
				IDToken:      "old-id",
				Persona:      DefaultPersona(),
				Cookies:      []Cookie{},
			}, "")
			if err != nil {
				t.Fatal(err)
			}
			if credential.RefreshToken != test.wantRefresh || credential.LifecycleState != LifecycleActive {
				t.Fatalf("refresh result = token %q state %q", credential.RefreshToken, credential.LifecycleState)
			}
			if credential.Expired != "2026-07-18T00:00:00Z" || credential.LastRefreshAt != "2026-07-16T14:00:00Z" {
				t.Fatalf("refresh timestamps = %q/%q", credential.Expired, credential.LastRefreshAt)
			}
			if test.wantRefresh == "old-refresh" && credential.IDToken != "old-id" {
				t.Fatalf("missing ID token response did not preserve old value: %q", credential.IDToken)
			}
		})
	}
}

func TestServiceRefreshInvalidGrant(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(response, `{"error":"invalid_grant","error_description":"session ended"}`)
	}))
	defer server.Close()
	service := NewService(Options{AuthBaseURL: server.URL})
	credential, err := service.Refresh(t.Context(), Credential{RefreshToken: "refresh", Persona: DefaultPersona()}, "")
	if err == nil {
		t.Fatal("Refresh() succeeded, want invalid_grant")
	}
	authError, ok := AsAuthError(err)
	if !ok || authError.Code != "invalid_grant" || authError.State != LifecycleReauthRequired || !authError.Terminal {
		t.Fatalf("Refresh() error = %#v", err)
	}
	if credential.LifecycleState != LifecycleReauthRequired {
		t.Fatalf("credential state = %q", credential.LifecycleState)
	}
}

func TestServiceRefreshMigratesBrowserIdentity(t *testing.T) {
	expiresAt := time.Date(2026, time.July, 18, 0, 0, 0, 0, time.UTC).Unix()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/oauth/token" {
			http.NotFound(response, request)
			return
		}
		_, _ = io.WriteString(response, `{"access_token":"`+testJWT(expiresAt)+`"}`)
	}))
	defer server.Close()
	authURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(Options{AuthBaseURL: server.URL, Rand: zeroReader{}})
	credential, err := service.Refresh(t.Context(), Credential{
		RefreshToken: "refresh",
		Persona:      DefaultPersona(),
		Cookies: []Cookie{
			{Name: "oai-did", Value: "unrelated-device", Host: "evil.example", Domain: "evil.example", Path: "/"},
			{Name: "OAI-DID", Value: "case-confused-device", Host: authURL.Hostname(), Path: "/"},
			{Name: "oai-did", Value: "cookie-device", Host: authURL.Hostname(), Path: "/"},
		},
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	if credential.DeviceID != "cookie-device" {
		t.Fatalf("device ID = %q", credential.DeviceID)
	}
	if strings.TrimSpace(credential.SessionID) == "" {
		t.Fatal("session ID was not generated")
	}
	if got, cookieErr := credentialCookieValueForURL(credential.Cookies, server.URL, "oai-did"); cookieErr != nil || got != "cookie-device" {
		t.Fatalf("persisted oai-did = %q", got)
	}
}

func TestCredentialCookieValueForURLRejectsExpiredCookie(t *testing.T) {
	for _, cookie := range []Cookie{
		{
			Name: "oai-did", Value: "expired-device", Host: "auth.openai.com", Domain: "auth.openai.com",
			Path: "/", Expires: time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano), Secure: true,
		},
		{
			Name: "oai-did", Value: "raw-expired-device", Host: "auth.openai.com", Domain: "auth.openai.com",
			Path: "/", RawExpires: time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat), Secure: true,
		},
	} {
		got, err := credentialCookieValueForURL([]Cookie{cookie}, AuthBaseURL, "oai-did")
		if err != nil {
			t.Fatal(err)
		}
		if got != "" {
			t.Fatalf("expired oai-did = %q, want empty", got)
		}
	}
}

func TestCredentialCookieValueForURLIgnoresInvalidRawExpiry(t *testing.T) {
	got, err := credentialCookieValueForURL([]Cookie{{
		Name: "oai-did", Value: "session-device", Host: "auth.openai.com", Domain: "auth.openai.com",
		Path: "/", RawExpires: "invalid", Secure: true,
	}}, AuthBaseURL, "oai-did")
	if err != nil {
		t.Fatal(err)
	}
	if got != "session-device" {
		t.Fatalf("session oai-did = %q, want session-device", got)
	}
}

func TestOAuthValuesDoNotGenerateUnusedDeviceID(t *testing.T) {
	service := NewService(Options{Rand: bytes.NewReader(make([]byte, 96))})
	pkce, state, nonce, err := service.oauthValues()
	if err != nil {
		t.Fatal(err)
	}
	if pkce.CodeVerifier == "" || state == "" || nonce == "" {
		t.Fatalf("oauth values = %#v, %q, %q", pkce, state, nonce)
	}
}

func testJWT(expiresAt int64) string {
	payload, _ := json.Marshal(map[string]int64{"exp": expiresAt})
	return "e30." + base64.RawURLEncoding.EncodeToString(payload) + ".signature"
}
