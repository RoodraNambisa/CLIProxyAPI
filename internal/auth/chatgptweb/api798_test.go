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
