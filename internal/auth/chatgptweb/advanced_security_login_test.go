package chatgptweb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type advancedSecurityLoginFixture struct {
	t                  *testing.T
	server             *httptest.Server
	allowedID          string
	finalize           bool
	persisted          bool
	issueCalls         int
	dumpCalls          int
	verifyCalls        int
	finalizeCalls      int
	ordinaryCalls      int
	verifiedKeyID      string
	verifyStatus       int
	verifyCode         string
	dropVerifyResponse bool
	finalizeStatus     int
	sessionEmail       string
	sessionUserID      string
	mu                 sync.Mutex
}

func newAdvancedSecurityLoginFixture(t *testing.T, allowedID string, finalize bool) *advancedSecurityLoginFixture {
	t.Helper()
	fixture := &advancedSecurityLoginFixture{
		t:              t,
		allowedID:      allowedID,
		finalize:       finalize,
		verifyStatus:   http.StatusOK,
		finalizeStatus: http.StatusOK,
		sessionEmail:   "person@example.com",
		sessionUserID:  "user-1",
	}
	fixture.server = httptest.NewServer(http.HandlerFunc(fixture.serveHTTP))
	t.Cleanup(fixture.server.Close)
	return fixture
}

func (fixture *advancedSecurityLoginFixture) serveHTTP(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Content-Type", "application/json")
	switch request.URL.Path {
	case "/api/auth/csrf":
		http.SetCookie(response, &http.Cookie{Name: "next-auth.csrf-token", Value: "csrf-token%7Chash", Path: "/"})
		_, _ = io.WriteString(response, `{"csrfToken":"csrf-token"}`)
	case "/api/auth/signin/openai":
		_, _ = fmt.Fprintf(response, `{"url":%q}`, fixture.server.URL+"/oauth/authorize")
	case "/oauth/authorize":
		response.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(response, `{"page":{"type":"advanced_account_security"},"continue_url":"/auth-challenge"}`)
	case advancedSecurityChallengeIssuePath:
		fixture.mu.Lock()
		fixture.issueCalls++
		fixture.mu.Unlock()
		var body map[string]any
		_ = json.NewDecoder(request.Body).Decode(&body)
		if body["method_id"] != "passkey" {
			fixture.t.Errorf("issue body = %#v", body)
		}
		_, _ = io.WriteString(response, `{}`)
	case advancedSecuritySessionDumpPath:
		fixture.mu.Lock()
		fixture.dumpCalls++
		fixture.mu.Unlock()
		challenge := base64.RawURLEncoding.EncodeToString([]byte("advanced-security-challenge"))
		_, _ = fmt.Fprintf(response, `{"client_auth_session":{"aas_enabled":true,"mfa_request_id":"aas-request-1","passkey_request_options":{"challenge":%q,"rpId":"openai.com","allowCredentials":[{"type":"public-key","id":%q}]}}}`, challenge, fixture.allowedID)
	case advancedSecurityChallengeVerifyPath:
		fixture.mu.Lock()
		fixture.verifyCalls++
		persisted := fixture.persisted
		fixture.mu.Unlock()
		if !persisted {
			fixture.t.Error("advanced security verify ran before sign_count was persisted")
		}
		var body struct {
			MethodID string `json:"method_id"`
			Response struct {
				RequestID string `json:"mfa_request_id"`
				Assertion struct {
					ID string `json:"id"`
				} `json:"signed_passkey_response"`
			} `json:"passkey_challenge_response"`
		}
		if errDecode := json.NewDecoder(request.Body).Decode(&body); errDecode != nil {
			fixture.t.Errorf("decode verify body: %v", errDecode)
		}
		fixture.verifiedKeyID = body.Response.Assertion.ID
		if body.MethodID != "passkey" || body.Response.RequestID != "aas-request-1" {
			fixture.t.Errorf("verify body = %#v", body)
		}
		if fixture.dropVerifyResponse {
			hijacker, ok := response.(http.Hijacker)
			if !ok {
				fixture.t.Error("response writer does not support connection hijacking")
				return
			}
			connection, _, errHijack := hijacker.Hijack()
			if errHijack != nil {
				fixture.t.Errorf("hijack verify response: %v", errHijack)
				return
			}
			_ = connection.Close()
			return
		}
		if fixture.verifyStatus != http.StatusOK {
			response.WriteHeader(fixture.verifyStatus)
			_, _ = fmt.Fprintf(response, `{"error":{"code":%q,"message":"verification failed"}}`, fixture.verifyCode)
			return
		}
		if fixture.finalize {
			_, _ = io.WriteString(response, `{"page":{"type":"advanced_account_security"},"continue_url":"/advanced-account-security/enrolled"}`)
			return
		}
		_, _ = fmt.Fprintf(response, `{"page":{"type":"callback"},"continue_url":%q}`, fixture.server.URL+"/api/auth/callback/openai?code=web-code&state=web-state")
	case advancedSecurityPostAuthFinalizePath:
		fixture.mu.Lock()
		fixture.finalizeCalls++
		fixture.mu.Unlock()
		if fixture.finalizeStatus != http.StatusOK {
			response.WriteHeader(fixture.finalizeStatus)
			_, _ = io.WriteString(response, `{"error":{"code":"temporarily_unavailable"}}`)
			return
		}
		_, _ = fmt.Fprintf(response, `{"page":{"type":"callback"},"continue_url":%q}`, fixture.server.URL+"/api/auth/callback/openai?code=web-code&state=web-state")
	case passkeyVerifyPath, "/api/accounts/password/verify":
		fixture.mu.Lock()
		fixture.ordinaryCalls++
		fixture.mu.Unlock()
		response.WriteHeader(http.StatusInternalServerError)
	case "/api/auth/callback/openai":
		http.SetCookie(response, &http.Cookie{Name: "next-auth.session-token", Value: "session-cookie", Path: "/", HttpOnly: true})
		http.Redirect(response, request, "/", http.StatusFound)
	case "/":
		response.Header().Set("Content-Type", "text/html")
		_, _ = io.WriteString(response, "<html>home</html>")
	case "/api/auth/session":
		expiresAt := time.Date(2026, time.August, 6, 0, 0, 0, 0, time.UTC).Unix()
		_, _ = fmt.Fprintf(response, `{"accessToken":%q,"user":{"id":%q,"email":%q},"account":{"id":"account-1","planType":"free"}}`, testJWT(expiresAt), fixture.sessionUserID, fixture.sessionEmail)
	default:
		fixture.t.Errorf("unexpected advanced security request: %s %s", request.Method, request.URL.String())
		http.NotFound(response, request)
	}
}

func TestServiceAdvancedSecurityLoginDoesNotRollBackCounterOrFallback(t *testing.T) {
	fixedNow := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	aas := testAdvancedAccountSecurityCredential(t)
	allowed := aas.Passkeys[0].Credential.CredentialID
	initialCount := aas.Passkeys[0].Credential.SignCount
	fixture := newAdvancedSecurityLoginFixture(t, allowed, false)
	fixture.verifyStatus = http.StatusServiceUnavailable
	fixture.verifyCode = "temporarily_unavailable"
	fixture.finalizeStatus = http.StatusServiceUnavailable
	service := NewService(fixture.options(fixedNow))
	var lastPersisted AdvancedAccountSecurityCredential
	credential, errLogin := service.Login(t.Context(), LoginInput{
		Credential: &Credential{
			CredentialSchemaVersion: CredentialSchemaVersionAdvancedAccountSecurity,
			Type:                    Provider,
			Email:                   "person@example.com",
			AccountID:               "account-1",
			UserID:                  "user-1",
			Password:                "must-not-be-used",
			WebAuthn:                testWebAuthnCredential(t),
			API798URL:               "https://api798.com/get_code?email=person%40example.com&auth_code=opaque",
			AdvancedAccountSecurity: aas,
		},
		Relogin: true,
		PersistAdvancedAccountSecurity: func(_ context.Context, updated AdvancedAccountSecurityCredential) (AdvancedAccountSecurityCredential, error) {
			lastPersisted = *CloneAdvancedAccountSecurityCredential(&updated)
			fixture.mu.Lock()
			fixture.persisted = true
			fixture.mu.Unlock()
			return *CloneAdvancedAccountSecurityCredential(&updated), nil
		},
	})
	if errLogin == nil {
		t.Fatal("Login() error = nil")
	}
	authError, ok := AsAuthError(errLogin)
	if !ok || !authError.Retryable || authError.Terminal || authError.FailureStage != "advanced_security_verify" {
		t.Fatalf("Login() error = %#v", errLogin)
	}
	fixture.mu.Lock()
	ordinaryCalls := fixture.ordinaryCalls
	fixture.mu.Unlock()
	if ordinaryCalls != 0 {
		t.Fatalf("ordinary fallback calls = %d", ordinaryCalls)
	}
	if lastPersisted.Passkeys[0].Credential.SignCount != initialCount+1 ||
		credential.AdvancedAccountSecurity.Passkeys[0].Credential.SignCount != initialCount+1 {
		t.Fatalf("counter persisted=%d credential=%d want=%d", lastPersisted.Passkeys[0].Credential.SignCount, credential.AdvancedAccountSecurity.Passkeys[0].Credential.SignCount, initialCount+1)
	}
}

func TestServiceAdvancedSecurityLoginRecoversAmbiguousVerifyResponse(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		verifyStatus int
		dropResponse bool
	}{
		{name: "server error", verifyStatus: http.StatusServiceUnavailable},
		{name: "response lost", verifyStatus: http.StatusOK, dropResponse: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			fixedNow := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
			aas := testAdvancedAccountSecurityCredential(t)
			allowed := aas.Passkeys[0].Credential.CredentialID
			initialCount := aas.Passkeys[0].Credential.SignCount
			fixture := newAdvancedSecurityLoginFixture(t, allowed, false)
			fixture.verifyStatus = testCase.verifyStatus
			fixture.verifyCode = "temporarily_unavailable"
			fixture.dropVerifyResponse = testCase.dropResponse
			service := NewService(fixture.options(fixedNow))
			persistCalls := 0

			credential, errLogin := service.Login(t.Context(), LoginInput{
				Credential: &Credential{
					CredentialSchemaVersion: CredentialSchemaVersionAdvancedAccountSecurity,
					Type:                    Provider,
					Email:                   "person@example.com",
					AccountID:               "account-1",
					UserID:                  "user-1",
					AdvancedAccountSecurity: aas,
				},
				Relogin: true,
				PersistAdvancedAccountSecurity: func(_ context.Context, updated AdvancedAccountSecurityCredential) (AdvancedAccountSecurityCredential, error) {
					persistCalls++
					fixture.mu.Lock()
					fixture.persisted = true
					fixture.mu.Unlock()
					return *CloneAdvancedAccountSecurityCredential(&updated), nil
				},
			})
			if errLogin != nil {
				t.Fatalf("Login() error = %v", errLogin)
			}
			fixture.mu.Lock()
			verifyCalls, finalizeCalls, ordinaryCalls := fixture.verifyCalls, fixture.finalizeCalls, fixture.ordinaryCalls
			fixture.mu.Unlock()
			if verifyCalls != 1 || finalizeCalls != 1 || ordinaryCalls != 0 {
				t.Fatalf("calls verify=%d finalize=%d ordinary=%d", verifyCalls, finalizeCalls, ordinaryCalls)
			}
			if persistCalls != 2 || credential.AdvancedAccountSecurity.Passkeys[0].Credential.SignCount != initialCount+1 ||
				credential.AdvancedAccountSecurity.Passkeys[0].Credential.LastUsedAt != fixedNow.Format(time.RFC3339) {
				t.Fatalf("persist calls=%d credential=%#v", persistCalls, credential.AdvancedAccountSecurity.Passkeys[0].Credential)
			}
		})
	}
}

func TestServiceAdvancedSecurityLoginRejectsSessionIdentityMismatch(t *testing.T) {
	fixedNow := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
	aas := testAdvancedAccountSecurityCredential(t)
	fixture := newAdvancedSecurityLoginFixture(t, aas.Passkeys[0].Credential.CredentialID, false)
	fixture.sessionEmail = "other@example.com"
	service := NewService(fixture.options(fixedNow))
	persistCalls := 0
	_, errLogin := service.Login(t.Context(), LoginInput{
		Credential: &Credential{
			CredentialSchemaVersion: CredentialSchemaVersionAdvancedAccountSecurity,
			Type:                    Provider,
			Email:                   "person@example.com",
			AccountID:               "account-1",
			UserID:                  "user-1",
			AdvancedAccountSecurity: aas,
		},
		Relogin: true,
		PersistAdvancedAccountSecurity: func(_ context.Context, updated AdvancedAccountSecurityCredential) (AdvancedAccountSecurityCredential, error) {
			persistCalls++
			fixture.mu.Lock()
			fixture.persisted = true
			fixture.mu.Unlock()
			return *CloneAdvancedAccountSecurityCredential(&updated), nil
		},
	})
	if errLogin == nil {
		t.Fatal("Login() error = nil")
	}
	authError, ok := AsAuthError(errLogin)
	if !ok || authError.Code != "passkey_session_invalid" || authError.State != LifecycleReauthRequired {
		t.Fatalf("Login() error = %#v", errLogin)
	}
	if persistCalls != 1 {
		t.Fatalf("persist calls = %d, want counter-only persistence", persistCalls)
	}
}

func (fixture *advancedSecurityLoginFixture) options(now time.Time) Options {
	return Options{
		AuthBaseURL:     fixture.server.URL,
		SessionBaseURL:  fixture.server.URL,
		SentinelBaseURL: fixture.server.URL,
		RedirectURL:     fixture.server.URL + "/auth/callback",
		Rand:            zeroReader{},
		Now:             func() time.Time { return now },
	}
}

func TestServiceAdvancedSecurityLoginSelectsAllowedCredentialAndFinalizes(t *testing.T) {
	for _, finalize := range []bool{false, true} {
		t.Run(fmt.Sprintf("finalize=%t", finalize), func(t *testing.T) {
			fixedNow := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
			aas := testAdvancedAccountSecurityCredential(t)
			allowed := aas.Passkeys[1].Credential.CredentialID
			fixture := newAdvancedSecurityLoginFixture(t, allowed, finalize)
			service := NewService(fixture.options(fixedNow))
			persistCalls := 0
			credential, errLogin := service.Login(t.Context(), LoginInput{
				Credential: &Credential{
					CredentialSchemaVersion: CredentialSchemaVersionAdvancedAccountSecurity,
					Type:                    Provider,
					Email:                   "person@example.com",
					AccountID:               "account-1",
					UserID:                  "user-1",
					Password:                "must-not-be-used",
					WebAuthn:                testWebAuthnCredential(t),
					API798URL:               "https://api798.com/get_code?email=person%40example.com&auth_code=opaque",
					AdvancedAccountSecurity: aas,
				},
				Relogin: true,
				PersistAdvancedAccountSecurity: func(_ context.Context, updated AdvancedAccountSecurityCredential) (AdvancedAccountSecurityCredential, error) {
					persistCalls++
					fixture.mu.Lock()
					fixture.persisted = true
					fixture.mu.Unlock()
					return *CloneAdvancedAccountSecurityCredential(&updated), nil
				},
			})
			if errLogin != nil {
				t.Fatalf("Login() error = %v", errLogin)
			}
			fixture.mu.Lock()
			issueCalls, dumpCalls, verifyCalls := fixture.issueCalls, fixture.dumpCalls, fixture.verifyCalls
			finalizeCalls, ordinaryCalls, verifiedKeyID := fixture.finalizeCalls, fixture.ordinaryCalls, fixture.verifiedKeyID
			fixture.mu.Unlock()
			wantFinalizeCalls := 0
			if finalize {
				wantFinalizeCalls = 1
			}
			if issueCalls != 1 || dumpCalls != 1 || verifyCalls != 1 || finalizeCalls != wantFinalizeCalls || ordinaryCalls != 0 {
				t.Fatalf("calls issue=%d dump=%d verify=%d finalize=%d ordinary=%d", issueCalls, dumpCalls, verifyCalls, finalizeCalls, ordinaryCalls)
			}
			if verifiedKeyID != allowed || persistCalls != 2 {
				t.Fatalf("verified key=%q persist calls=%d", verifiedKeyID, persistCalls)
			}
			if credential.LifecycleState != LifecycleActive || credential.AdvancedAccountSecurity.Passkeys[1].Credential.SignCount != 5 ||
				credential.AdvancedAccountSecurity.Passkeys[1].Credential.LastUsedAt != fixedNow.Format(time.RFC3339) {
				t.Fatalf("credential = %#v", credential)
			}
		})
	}
}
