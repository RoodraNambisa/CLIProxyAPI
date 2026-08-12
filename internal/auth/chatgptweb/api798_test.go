package chatgptweb

import (
	"context"
	"errors"
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

const api798TestURL = "http://api798.com/get_code?email=person%40example.com&auth_code=opaque%2Bvalue%252F"

type api798RewriteTransport struct {
	target    *url.URL
	transport http.RoundTripper
	inspect   func(*http.Request)
}

type api798LeakingTransport struct{}

func (api798LeakingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("request failed for %s", request.URL.String())
}

func (transport *api798RewriteTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if transport.inspect != nil {
		transport.inspect(request)
	}
	cloned := request.Clone(request.Context())
	cloned.URL.Scheme = transport.target.Scheme
	cloned.URL.Host = transport.target.Host
	cloned.Host = transport.target.Host
	return transport.transport.RoundTrip(cloned)
}

func newAPI798TestClient(t *testing.T, server *httptest.Server, inspect func(*http.Request)) *http.Client {
	t.Helper()
	target, errParse := url.Parse(server.URL)
	if errParse != nil {
		t.Fatal(errParse)
	}
	base := http.DefaultTransport.(*http.Transport).Clone()
	base.Proxy = nil
	return &http.Client{
		Transport: &api798RewriteTransport{target: target, transport: base, inspect: inspect},
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func TestAPI798MailboxSessionAcceptsResentSameCode(t *testing.T) {
	issuedAt := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	baseline := api798Message{
		Available: true,
		Code:      "123456",
		Subject:   "Old code",
		Body:      "123456",
		DateRaw:   issuedAt.Add(-time.Minute).Format(time.RFC3339),
	}
	baseline.Date, baseline.HasReliableDate = parseAPI798Date(baseline.DateRaw)
	baseline.Fingerprint = api798MessageFingerprint(baseline)
	resent := baseline
	resent.Subject = "Resent code"
	resent.DateRaw = issuedAt.Add(time.Second).Format(time.RFC3339)
	resent.Date, resent.HasReliableDate = parseAPI798Date(resent.DateRaw)
	resent.Fingerprint = api798MessageFingerprint(resent)

	session := &api798MailboxSession{baselineFingerprint: baseline.Fingerprint, service: &Service{options: Options{Now: func() time.Time { return issuedAt.Add(time.Second) }}}}
	if !session.isFresh(resent, issuedAt, issuedAt.Add(time.Second)) {
		t.Fatal("resent message with the same code and a new mail fingerprint was rejected")
	}
	if session.isFresh(baseline, issuedAt, issuedAt.Add(time.Minute)) {
		t.Fatal("baseline message was accepted as a fresh OTP")
	}
}

func TestDecodeAPI798PendingResponse(t *testing.T) {
	message, errDecode := decodeAPI798Message([]byte(`{"success":false,"message":"waiting"}`))
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	if message.Available || message.Code != "" {
		t.Fatalf("pending message = %#v", message)
	}
}

func TestAPI798MailboxResponseClassification(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		body        string
		contentType string
		wantCode    string
	}{
		{name: "unauthorized", status: http.StatusUnauthorized, body: `{}`, contentType: "application/json", wantCode: "api798_authorization_failed"},
		{name: "forbidden html", status: http.StatusForbidden, body: `<html>forbidden</html>`, contentType: "text/html", wantCode: "api798_authorization_failed"},
		{name: "malformed", status: http.StatusOK, body: `{`, contentType: "application/json", wantCode: "api798_response_invalid"},
		{name: "trailing garbage", status: http.StatusOK, body: `{"success":false} trailing`, contentType: "application/json", wantCode: "api798_response_invalid"},
		{name: "multiple values", status: http.StatusOK, body: `{"success":false}{"success":true}`, contentType: "application/json", wantCode: "api798_response_invalid"},
		{name: "non json", status: http.StatusOK, body: `waiting`, contentType: "text/plain", wantCode: "api798_response_invalid"},
		{name: "rate limited", status: http.StatusTooManyRequests, body: `{}`, contentType: "application/json", wantCode: "api798_unavailable"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				response.Header().Set("Content-Type", test.contentType)
				response.WriteHeader(test.status)
				_, _ = io.WriteString(response, test.body)
			}))
			defer server.Close()
			service := NewService(Options{API798HTTPClient: newAPI798TestClient(t, server, nil)})
			mailbox, authError := service.newAPI798MailboxSession(&Credential{Email: "person@example.com", API798URL: api798TestURL})
			if authError != nil {
				t.Fatal(authError)
			}
			_, authError = mailbox.fetch(t.Context())
			if authError == nil || authError.Code != test.wantCode {
				t.Fatalf("error = %#v, want %q", authError, test.wantCode)
			}
		})
	}
}

func TestAPI798MailboxPrepareRetriesTransientResponse(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		requests++
		response.Header().Set("Content-Type", "application/json")
		if requests == 1 {
			response.Header().Set("Retry-After", "0")
			response.WriteHeader(http.StatusTooManyRequests)
			_, _ = io.WriteString(response, `{}`)
			return
		}
		_, _ = io.WriteString(response, `{"success":false}`)
	}))
	defer server.Close()
	service := NewService(Options{
		API798HTTPClient:   newAPI798TestClient(t, server, nil),
		API798PollInterval: time.Millisecond,
	})
	mailbox, authError := service.newAPI798MailboxSession(&Credential{Email: "person@example.com", API798URL: api798TestURL})
	if authError != nil {
		t.Fatal(authError)
	}
	if authError = mailbox.prepare(t.Context()); authError != nil {
		t.Fatal(authError)
	}
	if requests != 2 {
		t.Fatalf("requests = %d, want 2", requests)
	}
}

func TestAPI798NetworkErrorDoesNotRetainSecretURL(t *testing.T) {
	service := NewService(Options{API798HTTPClient: &http.Client{Transport: api798LeakingTransport{}}})
	mailbox, authError := service.newAPI798MailboxSession(&Credential{Email: "person@example.com", API798URL: api798TestURL})
	if authError != nil {
		t.Fatal(authError)
	}
	_, authError = mailbox.fetch(t.Context())
	if authError == nil || authError.Code != "api798_network_error" {
		t.Fatalf("error = %#v", authError)
	}
	for current := error(authError); current != nil; current = errors.Unwrap(current) {
		text := current.Error()
		if strings.Contains(text, "auth_code") || strings.Contains(text, "opaque") || strings.Contains(text, api798TestURL) {
			t.Fatalf("API798 secret URL leaked through error chain: %q", text)
		}
	}
}

func TestDecodeAPI798ExtractsCodeFromMessage(t *testing.T) {
	message, errDecode := decodeAPI798Message([]byte(`{
		"success": true,
		"subject": "Your verification code is 654321",
		"body": "Use 654321 to continue.",
		"date": "2026-08-12T12:00:00Z"
	}`))
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	if !message.Available || message.Code != "654321" {
		t.Fatalf("message = %#v", message)
	}

	message, errDecode = decodeAPI798Message([]byte(`{"success":true,"subject":"No code yet"}`))
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	if !message.Available || message.Code != "" {
		t.Fatalf("message without code = %#v", message)
	}
}

func TestAPI798MailboxResponseLimitAndTimeout(t *testing.T) {
	t.Run("response limit", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
			response.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(response, `{"success":true,"body":"`+strings.Repeat("x", int(api798ResponseLimit))+`"}`)
		}))
		defer server.Close()
		service := NewService(Options{API798HTTPClient: newAPI798TestClient(t, server, nil)})
		mailbox, _ := service.newAPI798MailboxSession(&Credential{Email: "person@example.com", API798URL: api798TestURL})
		_, authError := mailbox.fetch(t.Context())
		if authError == nil || authError.Code != "api798_response_too_large" {
			t.Fatalf("error = %#v", authError)
		}
	})

	t.Run("request timeout", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
			<-request.Context().Done()
		}))
		defer server.Close()
		service := NewService(Options{
			API798HTTPClient:     newAPI798TestClient(t, server, nil),
			API798RequestTimeout: 10 * time.Millisecond,
		})
		mailbox, _ := service.newAPI798MailboxSession(&Credential{Email: "person@example.com", API798URL: api798TestURL})
		_, authError := mailbox.fetch(t.Context())
		if authError == nil || authError.Code != "api798_request_timeout" {
			t.Fatalf("error = %#v", authError)
		}
	})
}

func TestServiceLoginWithExplicitAPI798(t *testing.T) {
	fixedNow := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	fixture := newLoginFixture(t, http.StatusOK, "")
	fixture.authorizeBody = `{"continue_url":"/email-verification","page":{"type":"email_otp"}}`
	fixture.wantEmailOTP = "123456"
	fixture.sentinelObserver = true

	var mailboxMu sync.Mutex
	mailboxCalls := 0
	rawQueries := make([]string, 0, 3)
	mailboxServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		mailboxMu.Lock()
		mailboxCalls++
		call := mailboxCalls
		mailboxMu.Unlock()
		response.Header().Set("Content-Type", "application/json")
		switch call {
		case 1:
			_, _ = io.WriteString(response, `{"success":true,"code":"123456","subject":"Old code","body":"123456","date":"2026-08-12T11:59:00Z"}`)
		case 2:
			_, _ = io.WriteString(response, `{"success":false}`)
		default:
			_, _ = io.WriteString(response, `{"success":true,"code":"123456","subject":"Resent code","body":"123456","date":"2026-08-12T12:00:01Z"}`)
		}
	}))
	defer mailboxServer.Close()

	options := fixture.options(fixedNow)
	options.API798PollInterval = time.Millisecond
	options.API798HTTPClient = newAPI798TestClient(t, mailboxServer, func(request *http.Request) {
		if request.URL.Scheme != "https" || request.URL.Host != "api798.com" || request.URL.Path != "/get_code" {
			t.Errorf("API798 request URL = %s", request.URL.String())
		}
		mailboxMu.Lock()
		rawQueries = append(rawQueries, request.URL.RawQuery)
		mailboxMu.Unlock()
	})
	service := NewService(options)
	observer := &sentinelTestObserver{token: `{"so":"fixture"}`}
	credential, errLogin := service.Login(t.Context(), LoginInput{
		BeginSentinelObserver: func(_ context.Context, request SentinelSDKRequest) (SentinelObserverHandle, error) {
			if request.Flow != "email_otp_validate" || !sentinelObserverRequired(request.Challenge) {
				t.Fatalf("observer request = %#v", request)
			}
			return observer, nil
		},
		Credential: &Credential{
			Type:        Provider,
			Email:       "person@example.com",
			LoginMethod: LoginMethodAPI798,
			API798URL:   api798TestURL,
		}})
	if errLogin != nil {
		t.Fatal(errLogin)
	}
	if credential.LifecycleState != LifecycleActive || credential.RefreshToken != "refresh-token" {
		t.Fatalf("credential = %#v", credential)
	}
	if credential.LoginMethod != LoginMethodAPI798 || credential.API798URL != api798TestURL {
		t.Fatal("login result did not preserve API798 settings")
	}
	if !observer.closed {
		t.Fatal("API798 login did not close the Session Observer")
	}
	fixture.mu.Lock()
	emailOTPCalls := fixture.emailOTPCalls
	passwordCalls := fixture.passwordCalls
	fixture.mu.Unlock()
	if emailOTPCalls != 1 || passwordCalls != 0 {
		t.Fatalf("email OTP calls = %d, password calls = %d", emailOTPCalls, passwordCalls)
	}
	mailboxMu.Lock()
	defer mailboxMu.Unlock()
	if mailboxCalls < 3 {
		t.Fatalf("mailbox calls = %d, want at least 3", mailboxCalls)
	}
	for _, rawQuery := range rawQueries {
		if !strings.Contains(rawQuery, "auth_code=opaque%2Bvalue%252F") {
			t.Fatalf("API798 auth_code encoding changed: %q", rawQuery)
		}
	}
}

func TestServiceLoginWithExplicitAPI798SelectsMFAEmailFactor(t *testing.T) {
	fixedNow := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	fixture := newLoginFixture(t, http.StatusOK, "")
	fixture.authorizeResponseBody = `{
		"continue_url":"/mfa-challenge/email-factor?factor_type=email&mfa_request_id=mfa-request",
		"page":{"type":"mfa_challenge","payload":{"mfa_request_id":"mfa-request","factors":[{"id":"email-factor","type":"email"}]}}
	}`
	fixture.wantMFAEmailOTP = "654321"
	fixture.mfaRedirectURL = fixture.server.URL + "/mfa-verify-follow"
	fixture.mfaRedirectStatus = http.StatusTemporaryRedirect

	var mailboxCalls int
	mailboxServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		mailboxCalls++
		response.Header().Set("Content-Type", "application/json")
		if mailboxCalls < 3 {
			_, _ = io.WriteString(response, `{"success":true,"code":"111111","subject":"Old code","body":"111111","date":"2026-08-12T11:59:00Z"}`)
			return
		}
		_, _ = io.WriteString(response, `{"success":true,"code":"654321","subject":"MFA code","body":"654321","date":"2026-08-12T12:00:01Z"}`)
	}))
	defer mailboxServer.Close()

	options := fixture.options(fixedNow)
	options.API798PollInterval = time.Millisecond
	options.API798HTTPClient = newAPI798TestClient(t, mailboxServer, nil)
	credential, errLogin := NewService(options).Login(t.Context(), LoginInput{Credential: &Credential{
		Type:        Provider,
		Email:       "person@example.com",
		LoginMethod: LoginMethodAPI798,
		API798URL:   api798TestURL,
	}})
	if errLogin != nil {
		t.Fatal(errLogin)
	}
	if credential.LifecycleState != LifecycleActive {
		t.Fatalf("credential = %#v", credential)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.mfaIssueCalls != 1 || fixture.mfaEmailVerifyCalls != 2 || fixture.passwordCalls != 0 {
		t.Fatalf("calls = issue:%d email_verify:%d password:%d", fixture.mfaIssueCalls, fixture.mfaEmailVerifyCalls, fixture.passwordCalls)
	}
}

func TestServiceLoginWithExplicitAPI798RetriesRejectedMFAEmailOTP(t *testing.T) {
	fixedNow := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	fixture := newLoginFixture(t, http.StatusOK, "")
	fixture.authorizeResponseBody = `{
		"continue_url":"/mfa-challenge/email-factor?factor_type=email&mfa_request_id=mfa-request",
		"page":{"type":"mfa_challenge","payload":{"mfa_request_id":"mfa-request","factors":[{"id":"email-factor","type":"email"}]}}
	}`
	fixture.wantMFAEmailOTP = "654321"
	fixture.mfaEmailOTPRejectFirst = true

	var mailboxCalls int
	mailboxServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		mailboxCalls++
		response.Header().Set("Content-Type", "application/json")
		switch mailboxCalls {
		case 1, 2:
			_, _ = io.WriteString(response, `{"success":true,"code":"111111","subject":"Old code","body":"111111","date":"2026-08-12T11:59:00Z"}`)
		case 3, 4:
			_, _ = io.WriteString(response, `{"success":true,"code":"654321","subject":"First code","body":"654321","date":"2026-08-12T12:00:01Z"}`)
		default:
			_, _ = io.WriteString(response, `{"success":true,"code":"654321","subject":"Fresh code","body":"654321","date":"2026-08-12T12:00:02Z"}`)
		}
	}))
	defer mailboxServer.Close()

	options := fixture.options(fixedNow)
	options.API798PollInterval = time.Millisecond
	options.API798HTTPClient = newAPI798TestClient(t, mailboxServer, nil)
	credential, errLogin := NewService(options).Login(t.Context(), LoginInput{Credential: &Credential{
		Type:        Provider,
		Email:       "person@example.com",
		LoginMethod: LoginMethodAPI798,
		API798URL:   api798TestURL,
	}})
	if errLogin != nil {
		t.Fatal(errLogin)
	}
	if credential.LifecycleState != LifecycleActive {
		t.Fatalf("credential = %#v", credential)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.mfaIssueCalls != 2 || fixture.mfaEmailVerifyCalls != 2 {
		t.Fatalf("calls = issue:%d email_verify:%d", fixture.mfaIssueCalls, fixture.mfaEmailVerifyCalls)
	}
}

func TestServiceLoginWithExplicitAPI798KeepsExhaustedMFAEmailOTPRetryable(t *testing.T) {
	fixedNow := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	fixture := newLoginFixture(t, http.StatusOK, "")
	fixture.authorizeResponseBody = `{
		"continue_url":"/mfa-challenge/email-factor?factor_type=email&mfa_request_id=mfa-request",
		"page":{"type":"mfa_challenge","payload":{"mfa_request_id":"mfa-request","factors":[{"id":"email-factor","type":"email"}]}}
	}`
	fixture.wantMFAEmailOTP = "654321"
	fixture.mfaEmailOTPRejectAll = true

	var mailboxCalls int
	mailboxServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		mailboxCalls++
		response.Header().Set("Content-Type", "application/json")
		switch mailboxCalls {
		case 1, 2:
			_, _ = io.WriteString(response, `{"success":true,"code":"111111","subject":"Old code","body":"111111","date":"2026-08-12T11:59:00Z"}`)
		case 3, 4:
			_, _ = io.WriteString(response, `{"success":true,"code":"654321","subject":"First code","body":"654321","date":"2026-08-12T12:00:01Z"}`)
		default:
			_, _ = io.WriteString(response, `{"success":true,"code":"654321","subject":"Fresh code","body":"654321","date":"2026-08-12T12:00:02Z"}`)
		}
	}))
	defer mailboxServer.Close()

	options := fixture.options(fixedNow)
	options.API798PollInterval = time.Millisecond
	options.API798HTTPClient = newAPI798TestClient(t, mailboxServer, nil)
	credential, errLogin := NewService(options).Login(t.Context(), LoginInput{Credential: &Credential{
		Type:        Provider,
		Email:       "person@example.com",
		LoginMethod: LoginMethodAPI798,
		API798URL:   api798TestURL,
	}})
	authError, ok := AsAuthError(errLogin)
	if !ok || authError.Code != "api798_email_otp_rejected" || !authError.Retryable || authError.Terminal {
		t.Fatalf("error = %#v, want retryable non-terminal rejection", errLogin)
	}
	if credential == nil || credential.LifecycleState != LifecycleLoginPending {
		t.Fatalf("credential lifecycle = %#v, want login_pending", credential)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.mfaIssueCalls != DefaultAPI798OTPAttempts || fixture.mfaEmailVerifyCalls != DefaultAPI798OTPAttempts {
		t.Fatalf("calls = issue:%d email_verify:%d", fixture.mfaIssueCalls, fixture.mfaEmailVerifyCalls)
	}
}

func TestServiceLoginWithExplicitAPI798SwitchesPasswordPageToEmailOTP(t *testing.T) {
	fixedNow := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	fixture := newLoginFixture(t, http.StatusOK, "")
	fixture.wantEmailOTP = "123456"
	mailboxCalls := 0
	mailboxServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		mailboxCalls++
		response.Header().Set("Content-Type", "application/json")
		if mailboxCalls <= 2 {
			_, _ = io.WriteString(response, `{"success":true,"code":"000000","subject":"Old code","body":"000000","date":"2026-08-12T11:59:00Z"}`)
			return
		}
		_, _ = io.WriteString(response, `{"success":true,"code":"123456","subject":"Fresh code","body":"123456","date":"2026-08-12T12:00:01Z"}`)
	}))
	defer mailboxServer.Close()

	options := fixture.options(fixedNow)
	options.API798PollInterval = time.Millisecond
	options.API798HTTPClient = newAPI798TestClient(t, mailboxServer, nil)
	credential, errLogin := NewService(options).Login(t.Context(), LoginInput{Credential: &Credential{
		Type:        Provider,
		Email:       "person@example.com",
		Password:    "correct-password",
		LoginMethod: LoginMethodAPI798,
		API798URL:   api798TestURL,
	}})
	if errLogin != nil {
		t.Fatal(errLogin)
	}
	if credential.LifecycleState != LifecycleActive {
		t.Fatalf("credential = %#v", credential)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.passwordCalls != 0 || fixture.emailOTPSendCalls != 1 || fixture.emailOTPPageCalls != 1 || fixture.emailOTPCalls != 1 {
		t.Fatalf("calls = password:%d send:%d page:%d validate:%d", fixture.passwordCalls, fixture.emailOTPSendCalls, fixture.emailOTPPageCalls, fixture.emailOTPCalls)
	}
}

func TestServiceLoginWithExplicitAPI798SwitchesPasskeyPageToEmailOTP(t *testing.T) {
	fixedNow := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	fixture := newLoginFixture(t, http.StatusOK, "")
	fixture.authorizeResponseBody = `{"continue_url":"/passkey","page":{"type":"passkey_challenge"}}`
	fixture.wantEmailOTP = "123456"
	mailboxCalls := 0
	mailboxServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		mailboxCalls++
		response.Header().Set("Content-Type", "application/json")
		if mailboxCalls <= 2 {
			_, _ = io.WriteString(response, `{"success":true,"code":"000000","subject":"Old code","body":"000000","date":"2026-08-12T11:59:00Z"}`)
			return
		}
		_, _ = io.WriteString(response, `{"success":true,"code":"123456","subject":"Fresh code","body":"123456","date":"2026-08-12T12:00:01Z"}`)
	}))
	defer mailboxServer.Close()

	options := fixture.options(fixedNow)
	options.API798PollInterval = time.Millisecond
	options.API798HTTPClient = newAPI798TestClient(t, mailboxServer, nil)
	credential, errLogin := NewService(options).Login(t.Context(), LoginInput{Credential: &Credential{
		Type:        Provider,
		Email:       "person@example.com",
		LoginMethod: LoginMethodAPI798,
		API798URL:   api798TestURL,
	}})
	if errLogin != nil {
		t.Fatal(errLogin)
	}
	if credential.LifecycleState != LifecycleActive {
		t.Fatalf("credential = %#v", credential)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.passwordCalls != 0 || fixture.emailOTPSendCalls != 1 || fixture.emailOTPPageCalls != 1 || fixture.emailOTPCalls != 1 {
		t.Fatalf("calls = password:%d send:%d page:%d validate:%d", fixture.passwordCalls, fixture.emailOTPSendCalls, fixture.emailOTPPageCalls, fixture.emailOTPCalls)
	}
}

func TestServiceLoginWithExplicitAPI798ReportsUnavailableEmailOTPEndpoint(t *testing.T) {
	fixture := newLoginFixture(t, http.StatusOK, "")
	fixture.emailOTPSendStatus = http.StatusBadRequest
	fixture.emailOTPSendBody = "unavailable"
	mailboxServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(response, `{"success":false}`)
	}))
	defer mailboxServer.Close()

	options := fixture.options(time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC))
	options.API798HTTPClient = newAPI798TestClient(t, mailboxServer, nil)
	_, errLogin := NewService(options).Login(t.Context(), LoginInput{Credential: &Credential{
		Type:        Provider,
		Email:       "person@example.com",
		LoginMethod: LoginMethodAPI798,
		API798URL:   api798TestURL,
	}})
	authError, ok := AsAuthError(errLogin)
	if !ok || authError.Code != "api798_email_otp_unavailable" || authError.FailureStage != "email_otp_send" {
		t.Fatalf("error = %#v", errLogin)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.emailOTPSendCalls != 1 || fixture.emailOTPPageCalls != 0 || fixture.passwordCalls != 0 {
		t.Fatalf("calls = send:%d page:%d password:%d", fixture.emailOTPSendCalls, fixture.emailOTPPageCalls, fixture.passwordCalls)
	}
}

func TestAPI798EmailOTPActivationResponseClassification(t *testing.T) {
	tests := []struct {
		name          string
		status        int
		payload       string
		wantCode      string
		wantState     LifecycleState
		wantRetryable bool
		wantTerminal  bool
	}{
		{
			name:          "request timeout",
			status:        http.StatusRequestTimeout,
			payload:       `{}`,
			wantCode:      "email_otp_send_failed",
			wantState:     LifecycleReloginPending,
			wantRetryable: true,
		},
		{
			name:          "too early",
			status:        http.StatusTooEarly,
			payload:       `{}`,
			wantCode:      "email_otp_send_failed",
			wantState:     LifecycleReloginPending,
			wantRetryable: true,
		},
		{
			name:          "expired oauth session",
			status:        http.StatusUnauthorized,
			payload:       `{}`,
			wantCode:      "email_otp_send_failed",
			wantState:     LifecycleReloginPending,
			wantRetryable: true,
		},
		{
			name:          "challenge response",
			status:        http.StatusForbidden,
			payload:       `<html>challenge</html>`,
			wantCode:      "email_otp_send_failed",
			wantState:     LifecycleReloginPending,
			wantRetryable: true,
		},
		{
			name:         "explicitly unavailable",
			status:       http.StatusBadRequest,
			payload:      `{"code":"email_otp_not_available"}`,
			wantCode:     "api798_email_otp_unavailable",
			wantState:    LifecycleReauthRequired,
			wantTerminal: true,
		},
		{
			name:         "missing endpoint",
			status:       http.StatusNotFound,
			payload:      `{}`,
			wantCode:     "api798_email_otp_unavailable",
			wantState:    LifecycleReauthRequired,
			wantTerminal: true,
		},
		{
			name:         "permanent account failure",
			status:       http.StatusNotFound,
			payload:      `{"error":"account_deactivated"}`,
			wantCode:     "account_deactivated",
			wantState:    LifecycleDead,
			wantTerminal: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authError := classifyAPI798EmailOTPActivationResponse(
				"email_otp_send",
				test.status,
				[]byte(test.payload),
				LifecycleReloginPending,
			)
			if authError == nil {
				t.Fatal("classification returned nil")
			}
			if authError.Code != test.wantCode || authError.State != test.wantState ||
				authError.Retryable != test.wantRetryable || authError.Terminal != test.wantTerminal ||
				authError.FailureStage != "email_otp_send" {
				t.Fatalf("classification = %#v", authError)
			}
		})
	}
}

func TestServiceLoginWithExplicitAPI798KeepsTemporaryActivationResponsesRetryable(t *testing.T) {
	tests := []struct {
		name       string
		stage      string
		configure  func(*loginFixture)
		wantStatus int
	}{
		{
			name:  "send request timeout",
			stage: "email_otp_send",
			configure: func(fixture *loginFixture) {
				fixture.emailOTPSendStatus = http.StatusRequestTimeout
			},
			wantStatus: http.StatusRequestTimeout,
		},
		{
			name:  "verification page too early",
			stage: "email_otp_page",
			configure: func(fixture *loginFixture) {
				fixture.emailOTPPageStatus = http.StatusTooEarly
			},
			wantStatus: http.StatusTooEarly,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newLoginFixture(t, http.StatusOK, "")
			test.configure(fixture)
			mailboxServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				response.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(response, `{"success":false}`)
			}))
			defer mailboxServer.Close()

			options := fixture.options(time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC))
			options.API798HTTPClient = newAPI798TestClient(t, mailboxServer, nil)
			_, errLogin := NewService(options).Login(t.Context(), LoginInput{Relogin: true, Credential: &Credential{
				Type:        Provider,
				Email:       "person@example.com",
				LoginMethod: LoginMethodAPI798,
				API798URL:   api798TestURL,
			}})
			authError, ok := AsAuthError(errLogin)
			if !ok || authError.StatusCode != test.wantStatus || authError.FailureStage != test.stage ||
				!authError.Retryable || authError.Terminal || authError.State != LifecycleReloginPending {
				t.Fatalf("error = %#v", errLogin)
			}
		})
	}
}

func TestAPI798ChallengeRefererPrefersTrustedContinuation(t *testing.T) {
	service := NewService(Options{AuthBaseURL: "https://auth.openai.com"})

	if got := service.api798ChallengeReferer(apiEnvelope{ContinueURL: "/passkey"}, "https://auth.openai.com/api/accounts/authorize/continue"); got != "https://auth.openai.com/passkey" {
		t.Fatalf("continuation referer = %q", got)
	}
	if got := service.api798ChallengeReferer(apiEnvelope{ContinueURL: "https://attacker.example/passkey"}, "https://auth.openai.com/log-in/password"); got != "https://auth.openai.com/log-in/password" {
		t.Fatalf("fallback referer = %q", got)
	}
	if got := service.api798ChallengeReferer(apiEnvelope{}, "https://auth.openai.com/log-in/password"); got != "https://auth.openai.com/log-in/password" {
		t.Fatalf("empty continuation referer = %q", got)
	}
	if got := service.api798ChallengeReferer(apiEnvelope{ContinueURL: "https://attacker.example/passkey"}, "https://attacker.example/fallback"); got != "https://auth.openai.com/log-in" {
		t.Fatalf("safe default referer = %q", got)
	}
}

func TestRejectedAPI798OTPStatusClassification(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		statusCode int
		payload    string
		want       bool
	}{
		{name: "known wrong code", statusCode: http.StatusBadRequest, payload: `{"code":"wrong_email_otp_code"}`, want: true},
		{name: "known expired code", statusCode: http.StatusBadRequest, payload: `{"code":"otp_expired"}`, want: true},
		{name: "unauthorized", statusCode: http.StatusUnauthorized, payload: `{}`, want: true},
		{name: "unrelated bad request", statusCode: http.StatusBadRequest, payload: `{"code":"account_problem"}`, want: false},
		{name: "server failure", statusCode: http.StatusInternalServerError, payload: `{"code":"wrong_email_otp_code"}`, want: false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := isRejectedAPI798OTPStatus(testCase.statusCode, []byte(testCase.payload)); got != testCase.want {
				t.Fatalf("isRejectedAPI798OTPStatus(%d, %s) = %v, want %v", testCase.statusCode, testCase.payload, got, testCase.want)
			}
		})
	}
}

func TestAPI798ChallengePageIsNotMistakenForRetryableUpstreamFailure(t *testing.T) {
	directChallenge := apiEnvelope{PageType: "email_otp", ContinueURL: "/email-verification"}
	mfaChallenge := apiEnvelope{PageType: "mfa_challenge", ContinueURL: "/mfa-challenge/email-factor"}
	for _, testCase := range []struct {
		name       string
		statusCode int
		want       bool
	}{
		{name: "success", statusCode: http.StatusOK, want: true},
		{name: "wrong code", statusCode: http.StatusBadRequest, want: true},
		{name: "rate limited", statusCode: http.StatusTooManyRequests, want: false},
		{name: "server failure", statusCode: http.StatusInternalServerError, want: false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := isRejectedAPI798EmailOTP(testCase.statusCode, nil, directChallenge); got != testCase.want {
				t.Fatalf("direct challenge result = %v, want %v", got, testCase.want)
			}
			if got := isRejectedAPI798MFAEmailOTP(testCase.statusCode, nil, mfaChallenge); got != testCase.want {
				t.Fatalf("MFA challenge result = %v, want %v", got, testCase.want)
			}
		})
	}
}

func TestServiceLoginWithExplicitAPI798ResendsRejectedEmailOTP(t *testing.T) {
	fixedNow := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	fixture := newLoginFixture(t, http.StatusOK, "")
	fixture.authorizeResponseBody = `{"continue_url":"/email-verification","page":{"type":"email_otp"}}`
	fixture.wantEmailOTP = "123456"
	fixture.emailOTPRejectFirst = true

	var mailboxCalls int
	mailboxServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		mailboxCalls++
		response.Header().Set("Content-Type", "application/json")
		switch mailboxCalls {
		case 1:
			_, _ = io.WriteString(response, `{"success":true,"code":"000000","subject":"Old code","body":"000000","date":"2026-08-12T11:59:00Z"}`)
		case 2, 3:
			_, _ = io.WriteString(response, `{"success":true,"code":"123456","subject":"First code","body":"123456","date":"2026-08-12T12:00:01Z"}`)
		default:
			_, _ = io.WriteString(response, `{"success":true,"code":"123456","subject":"Resent code","body":"123456","date":"2026-08-12T12:00:02Z"}`)
		}
	}))
	defer mailboxServer.Close()

	options := fixture.options(fixedNow)
	options.API798PollInterval = time.Millisecond
	options.API798HTTPClient = newAPI798TestClient(t, mailboxServer, nil)
	credential, errLogin := NewService(options).Login(t.Context(), LoginInput{Credential: &Credential{
		Type:        Provider,
		Email:       "person@example.com",
		LoginMethod: LoginMethodAPI798,
		API798URL:   api798TestURL,
	}})
	if errLogin != nil {
		t.Fatal(errLogin)
	}
	if credential.LifecycleState != LifecycleActive {
		t.Fatalf("credential = %#v", credential)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.emailOTPCalls != 2 || fixture.emailOTPResendCalls != 1 {
		t.Fatalf("calls = validate:%d resend:%d", fixture.emailOTPCalls, fixture.emailOTPResendCalls)
	}
}

func TestServiceLoginWithExplicitAPI798KeepsExhaustedEmailOTPRetryable(t *testing.T) {
	fixedNow := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	fixture := newLoginFixture(t, http.StatusOK, "")
	fixture.authorizeResponseBody = `{"continue_url":"/email-verification","page":{"type":"email_otp"}}`
	fixture.wantEmailOTP = "123456"
	fixture.emailOTPRejectAll = true

	var mailboxCalls int
	mailboxServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		mailboxCalls++
		response.Header().Set("Content-Type", "application/json")
		switch mailboxCalls {
		case 1:
			_, _ = io.WriteString(response, `{"success":true,"code":"000000","subject":"Old code","body":"000000","date":"2026-08-12T11:59:00Z"}`)
		case 2, 3:
			_, _ = io.WriteString(response, `{"success":true,"code":"123456","subject":"First code","body":"123456","date":"2026-08-12T12:00:01Z"}`)
		default:
			_, _ = io.WriteString(response, `{"success":true,"code":"123456","subject":"Resent code","body":"123456","date":"2026-08-12T12:00:02Z"}`)
		}
	}))
	defer mailboxServer.Close()

	options := fixture.options(fixedNow)
	options.API798PollInterval = time.Millisecond
	options.API798HTTPClient = newAPI798TestClient(t, mailboxServer, nil)
	credential, errLogin := NewService(options).Login(t.Context(), LoginInput{Credential: &Credential{
		Type:        Provider,
		Email:       "person@example.com",
		LoginMethod: LoginMethodAPI798,
		API798URL:   api798TestURL,
	}})
	authError, ok := AsAuthError(errLogin)
	if !ok || authError.Code != "api798_email_otp_rejected" || !authError.Retryable || authError.Terminal {
		t.Fatalf("error = %#v, want retryable non-terminal rejection", errLogin)
	}
	if credential == nil || credential.LifecycleState != LifecycleLoginPending {
		t.Fatalf("credential lifecycle = %#v, want login_pending", credential)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.emailOTPCalls != DefaultAPI798OTPAttempts || fixture.emailOTPResendCalls != DefaultAPI798OTPAttempts-1 {
		t.Fatalf("calls = validate:%d resend:%d", fixture.emailOTPCalls, fixture.emailOTPResendCalls)
	}
}

func TestServiceLoginWithExplicitAPI798DoesNotResendOnUpstreamFailure(t *testing.T) {
	fixedNow := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	fixture := newLoginFixture(t, http.StatusOK, "")
	fixture.authorizeResponseBody = `{"continue_url":"/email-verification","page":{"type":"email_otp"}}`
	fixture.wantEmailOTP = "123456"
	fixture.emailOTPStatus = http.StatusInternalServerError
	fixture.emailOTPBody = `{"error":{"code":"temporarily_unavailable"}}`

	mailboxCalls := 0
	mailboxServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		mailboxCalls++
		response.Header().Set("Content-Type", "application/json")
		if mailboxCalls == 1 {
			_, _ = io.WriteString(response, `{"success":true,"code":"000000","subject":"Old code","body":"000000","date":"2026-08-12T11:59:00Z"}`)
			return
		}
		_, _ = io.WriteString(response, `{"success":true,"code":"123456","subject":"Fresh code","body":"123456","date":"2026-08-12T12:00:01Z"}`)
	}))
	defer mailboxServer.Close()

	options := fixture.options(fixedNow)
	options.API798PollInterval = time.Millisecond
	options.API798HTTPClient = newAPI798TestClient(t, mailboxServer, nil)
	_, errLogin := NewService(options).Login(t.Context(), LoginInput{Credential: &Credential{
		Type:        Provider,
		Email:       "person@example.com",
		LoginMethod: LoginMethodAPI798,
		API798URL:   api798TestURL,
	}})
	authError, ok := AsAuthError(errLogin)
	if !ok || !authError.Retryable || authError.StatusCode != http.StatusInternalServerError || authError.FailureStage != "email_otp_validate" {
		t.Fatalf("error = %#v", errLogin)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.emailOTPCalls != 1 || fixture.emailOTPResendCalls != 0 {
		t.Fatalf("calls = validate:%d resend:%d", fixture.emailOTPCalls, fixture.emailOTPResendCalls)
	}
}

func TestAPI798LoginAcquisitionTimeoutHasIndependentMinimum(t *testing.T) {
	service := NewService(Options{
		AcquisitionTimeout:       5 * time.Second,
		API798AcquisitionTimeout: 10 * time.Second,
	})
	input := LoginInput{Credential: &Credential{
		Email:       "person@example.com",
		LoginMethod: LoginMethodAPI798,
		API798URL:   api798TestURL,
	}}
	if got := service.LoginAcquisitionTimeout(input); got != DefaultAPI798AcquisitionTimeout {
		t.Fatalf("API798 acquisition timeout = %s, want %s", got, DefaultAPI798AcquisitionTimeout)
	}
	input.LoginProxy = LoginProxyConfig{Enabled: true, AcquisitionTimeout: 3 * time.Minute}
	if got := service.LoginAcquisitionTimeout(input); got != 3*time.Minute {
		t.Fatalf("longer configured acquisition timeout = %s", got)
	}
	input.Credential.LoginMethod = LoginMethodPasswordTOTP
	input.Credential.Password = "secret"
	if got := service.LoginAcquisitionTimeout(input); got != 3*time.Minute {
		t.Fatalf("non-API798 acquisition timeout = %s", got)
	}
}

func TestAPI798RetryDelay(t *testing.T) {
	if got := api798RetryDelay(2*time.Second, 3, 0); got != 8*time.Second {
		t.Fatalf("exponential delay = %s", got)
	}
	if got := api798RetryDelay(2*time.Second, 10, 0); got != DefaultAPI798RetryMaxDelay {
		t.Fatalf("capped delay = %s", got)
	}
	if got := api798RetryDelay(2*time.Second, 3, 17*time.Second); got != 17*time.Second {
		t.Fatalf("Retry-After delay = %s", got)
	}
	if got := api798RetryDelay(2*time.Second, 3, 3*time.Minute); got != DefaultAPI798AcquisitionTimeout {
		t.Fatalf("capped Retry-After delay = %s", got)
	}
	if got := api798RetryDelay(time.Minute, 1, 0); got != DefaultAPI798RetryMaxDelay {
		t.Fatalf("capped base delay = %s", got)
	}
}

func TestParseAPI798RetryAfter(t *testing.T) {
	now := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	if got := parseAPI798RetryAfter(now.Add(7*time.Second).Format(http.TimeFormat), now); got != 7*time.Second {
		t.Fatalf("HTTP-date Retry-After = %s", got)
	}
	if got := parseAPI798RetryAfter("9223372036854775807", now); got != DefaultAPI798AcquisitionTimeout {
		t.Fatalf("large Retry-After = %s", got)
	}
}

func TestAPI798FetchPreservesRetryAfter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		response.Header().Set("Retry-After", "7")
		response.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(response, `{}`)
	}))
	defer server.Close()
	service := NewService(Options{API798HTTPClient: newAPI798TestClient(t, server, nil)})
	mailbox, _ := service.newAPI798MailboxSession(&Credential{Email: "person@example.com", API798URL: api798TestURL})
	_, authError := mailbox.fetch(t.Context())
	if authError == nil || authError.Code != "api798_unavailable" || authError.RetryAfter != 7*time.Second {
		t.Fatalf("error = %#v", authError)
	}
}

func TestNewAPI798HTTPClientDoesNotUseProxy(t *testing.T) {
	client := newAPI798HTTPClient()
	transport, ok := client.Transport.(*http.Transport)
	if !ok || transport.Proxy != nil {
		t.Fatalf("API798 transport = %#v", client.Transport)
	}
	request, errRequest := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://api798.com/get_code", nil)
	if errRequest != nil {
		t.Fatal(errRequest)
	}
	if request.URL.Hostname() != "api798.com" {
		t.Fatalf("request host = %q", request.URL.Hostname())
	}
}
