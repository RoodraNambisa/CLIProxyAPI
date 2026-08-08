package chatgptweb

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type passkeyLoginFixture struct {
	t              *testing.T
	server         *httptest.Server
	credentialID   string
	direct         bool
	verifyStatus   int
	verifyCode     string
	verifyFailures int
	intermediate   bool
	callbackURL    string
	persisted      bool
	challengeCalls int
	verifyCalls    int
	passwordCalls  int
	sessionEmail   string
	sessionAccount string
	sessionUser    string
	sessionToken   string
	mu             sync.Mutex
}

func newPasskeyLoginFixture(t *testing.T, credentialID string, direct bool) *passkeyLoginFixture {
	t.Helper()
	fixture := &passkeyLoginFixture{
		t:              t,
		credentialID:   credentialID,
		direct:         direct,
		sessionEmail:   "person@example.com",
		sessionAccount: "account-1",
		sessionUser:    "user-1",
	}
	fixture.server = httptest.NewServer(http.HandlerFunc(fixture.serveHTTP))
	t.Cleanup(fixture.server.Close)
	return fixture
}

func (fixture *passkeyLoginFixture) serveHTTP(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Content-Type", "application/json")
	switch request.URL.Path {
	case "/api/auth/csrf":
		http.SetCookie(response, &http.Cookie{Name: "next-auth.csrf-token", Value: "csrf-token%7Chash", Path: "/"})
		_, _ = io.WriteString(response, `{"csrfToken":"csrf-token"}`)
	case "/api/auth/signin/openai":
		if request.URL.Query().Get("ext-passkey-client-capabilities") != "11111" || request.URL.Query().Get("login_hint") != "person@example.com" {
			fixture.t.Errorf("signin query = %s", request.URL.RawQuery)
		}
		_, _ = fmt.Fprintf(response, `{"url":%q}`, fixture.server.URL+"/oauth/authorize")
	case "/oauth/authorize":
		http.SetCookie(response, &http.Cookie{Name: "oai-did", Value: "rotated-device", Path: "/"})
		if fixture.direct {
			fixture.writeChallenge(response)
			return
		}
		_, _ = io.WriteString(response, `{"page":{"type":"login_start"},"continue_url":"/sign-in"}`)
	case "/backend-api/sentinel/req":
		_, _ = io.WriteString(response, `{"token":"fixture-token","proofofwork":{"required":false}}`)
	case "/api/accounts/authorize/continue":
		fixture.writeChallenge(response)
	case passkeyVerifyPath:
		fixture.mu.Lock()
		fixture.verifyCalls++
		persisted := fixture.persisted
		status := fixture.verifyStatus
		if fixture.verifyFailures > 0 {
			fixture.verifyFailures--
			status = http.StatusServiceUnavailable
		}
		fixture.mu.Unlock()
		if !persisted {
			fixture.t.Error("Passkey verify ran before sign_count was persisted")
		}
		var body map[string]any
		if errDecode := json.NewDecoder(request.Body).Decode(&body); errDecode != nil {
			fixture.t.Errorf("decode Passkey verify body: %v", errDecode)
		}
		if body["mfa_request_id"] != "request-1" || body["using_conditional_ui"] != false {
			fixture.t.Errorf("Passkey verify body = %#v", body)
		}
		if status != 0 {
			code := fixture.verifyCode
			if code == "" {
				code = "temporarily_unavailable"
			}
			response.Header().Set("Content-Type", "application/json")
			response.Header().Set("Cf-Ray", "safe-ray-SJC")
			response.WriteHeader(status)
			_, _ = fmt.Fprintf(response, `{"error":{"code":%q}}`, code)
			return
		}
		callbackURL := fixture.callbackURL
		if callbackURL == "" {
			callbackURL = fixture.server.URL + "/api/auth/callback/openai?code=web-code&state=web-state"
			if fixture.intermediate {
				callbackURL = fixture.server.URL + "/oauth/passkey/resume"
			}
		}
		_, _ = fmt.Fprintf(response, `{"page":{"type":"callback"},"continue_url":%q}`, callbackURL)
	case "/oauth/passkey/resume":
		http.Redirect(response, request, "/api/auth/callback/openai?code=web-code&state=web-state", http.StatusFound)
	case "/api/auth/callback/openai":
		http.SetCookie(response, &http.Cookie{Name: "next-auth.session-token", Value: "session-cookie", Path: "/", HttpOnly: true})
		http.Redirect(response, request, "/", http.StatusFound)
	case "/":
		response.Header().Set("Content-Type", "text/html")
		_, _ = io.WriteString(response, "<html>home</html>")
	case "/api/auth/session":
		expiresAt := time.Date(2026, time.August, 6, 0, 0, 0, 0, time.UTC).Unix()
		accessToken := fixture.sessionToken
		if accessToken == "" {
			accessToken = testJWT(expiresAt)
		}
		_, _ = fmt.Fprintf(response, `{"accessToken":%q,"user":{"id":%q,"email":%q},"account":{"id":%q,"planType":"free"}}`, accessToken, fixture.sessionUser, fixture.sessionEmail, fixture.sessionAccount)
	case "/api/accounts/password/verify":
		fixture.mu.Lock()
		fixture.passwordCalls++
		fixture.mu.Unlock()
		response.WriteHeader(http.StatusInternalServerError)
	default:
		fixture.t.Errorf("unexpected Passkey request: %s %s", request.Method, request.URL.String())
		http.NotFound(response, request)
	}
}

func (fixture *passkeyLoginFixture) writeChallenge(response http.ResponseWriter) {
	fixture.mu.Lock()
	fixture.challengeCalls++
	challengeCall := fixture.challengeCalls
	fixture.mu.Unlock()
	challenge := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("passkey-challenge-%d", challengeCall)))
	_, _ = fmt.Fprintf(response, `{"page":{"type":"login_password","payload":{"passkey_challenge_option":{"mfa_request_id":"request-1","passkey_request_options":{"challenge":%q,"rpId":"openai.com","allowCredentials":[{"type":"public-key","id":%q}]}}}}}`, challenge, fixture.credentialID)
}

func (fixture *passkeyLoginFixture) options(now time.Time) Options {
	return Options{
		AuthBaseURL:     fixture.server.URL,
		SessionBaseURL:  fixture.server.URL,
		SentinelBaseURL: fixture.server.URL,
		RedirectURL:     fixture.server.URL + "/auth/callback",
		Rand:            zeroReader{},
		Now:             func() time.Time { return now },
	}
}

func TestServicePasskeyLoginCompletesDirectAndEmailChallengeFlows(t *testing.T) {
	for _, test := range []struct {
		direct       bool
		intermediate bool
	}{
		{direct: true},
		{direct: false},
		{direct: true, intermediate: true},
	} {
		t.Run(fmt.Sprintf("direct=%t/intermediate=%t", test.direct, test.intermediate), func(t *testing.T) {
			fixedNow := time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)
			webAuthn := testWebAuthnCredential(t)
			fixture := newPasskeyLoginFixture(t, webAuthn.CredentialID, test.direct)
			fixture.intermediate = test.intermediate
			service := NewService(fixture.options(fixedNow))
			persistCalls := 0
			credential, errLogin := service.Login(t.Context(), LoginInput{
				Credential: &Credential{
					CredentialSchemaVersion: 2,
					Type:                    Provider,
					Email:                   "person@example.com",
					Password:                "must-not-be-used",
					WebAuthn:                webAuthn,
				},
				Relogin: true,
				PersistWebAuthn: func(_ context.Context, updated WebAuthnCredential) (WebAuthnCredential, error) {
					persistCalls++
					if persistCalls == 1 && updated.SignCount != 1 {
						t.Fatalf("prewritten sign_count = %d", updated.SignCount)
					}
					fixture.mu.Lock()
					fixture.persisted = true
					fixture.mu.Unlock()
					return updated, nil
				},
			})
			if errLogin != nil {
				t.Fatalf("Login() error = %v", errLogin)
			}
			fixture.mu.Lock()
			verifyCalls := fixture.verifyCalls
			passwordCalls := fixture.passwordCalls
			fixture.mu.Unlock()
			if verifyCalls != 1 || passwordCalls != 0 || persistCalls != 2 {
				t.Fatalf("calls = verify:%d password:%d persist:%d", verifyCalls, passwordCalls, persistCalls)
			}
			if credential.LifecycleState != LifecycleActive || credential.RefreshStrategy != RefreshStrategyChatGPTSession ||
				credential.RefreshToken != "" || credential.WebAuthn.SignCount != 1 || credential.WebAuthn.LastUsedAt != fixedNow.Format(time.RFC3339) {
				t.Fatalf("credential = %#v", credential)
			}
			if credential.AccessToken == "" || !HasSessionCookieForURL(credential.Cookies, fixture.server.URL) {
				t.Fatal("Passkey login did not retain the access token and session cookie")
			}
			if credential.DeviceID != "rotated-device" {
				t.Fatalf("device_id = %q, want rotated-device", credential.DeviceID)
			}
		})
	}
}

func TestServicePasskeyLoginDoesNotRegressLastUsedAt(t *testing.T) {
	webAuthn := testWebAuthnCredential(t)
	webAuthn.LastUsedAt = "2026-08-05T12:00:00Z"
	fixture := newPasskeyLoginFixture(t, webAuthn.CredentialID, true)
	service := NewService(fixture.options(time.Date(2026, time.August, 5, 11, 0, 0, 0, time.UTC)))
	credential, errLogin := service.Login(t.Context(), LoginInput{
		Credential: &Credential{
			CredentialSchemaVersion: CredentialSchemaVersionWebAuthn,
			Type:                    Provider,
			Email:                   "person@example.com",
			WebAuthn:                webAuthn,
		},
		PersistWebAuthn: func(_ context.Context, updated WebAuthnCredential) (WebAuthnCredential, error) {
			fixture.mu.Lock()
			fixture.persisted = true
			fixture.mu.Unlock()
			return updated, nil
		},
	})
	if errLogin != nil {
		t.Fatalf("Login() error = %v", errLogin)
	}
	if credential.WebAuthn == nil || credential.WebAuthn.LastUsedAt != "2026-08-05T12:00:00Z" {
		t.Fatalf("last_used_at = %#v", credential.WebAuthn)
	}
}

func TestCreateWebAuthnAssertionRollsBackOnlyPersistenceFailure(t *testing.T) {
	credential := testWebAuthnCredential(t)
	options := map[string]any{
		"challenge": base64.RawURLEncoding.EncodeToString([]byte("challenge")),
		"rpId":      WebAuthnRPID,
		"allowCredentials": []map[string]any{{
			"type": "public-key",
			"id":   credential.CredentialID,
		}},
	}
	if _, errAssertion := createWebAuthnAssertion(credential, options, func(WebAuthnCredential) (WebAuthnCredential, error) {
		return WebAuthnCredential{}, fmt.Errorf("disk full")
	}); !errors.Is(errAssertion, errWebAuthnStatePersistence) || credential.SignCount != 0 {
		t.Fatalf("assertion error=%v sign_count=%d", errAssertion, credential.SignCount)
	}
	if _, errAssertion := createWebAuthnAssertion(credential, options, func(updated WebAuthnCredential) (WebAuthnCredential, error) {
		return updated, nil
	}); errAssertion != nil || credential.SignCount != 1 {
		t.Fatalf("assertion error=%v sign_count=%d", errAssertion, credential.SignCount)
	}
}

func TestServicePasskeyLoginTreatsCounterPersistenceFailureAsTransient(t *testing.T) {
	webAuthn := testWebAuthnCredential(t)
	fixture := newPasskeyLoginFixture(t, webAuthn.CredentialID, true)
	service := NewService(fixture.options(time.Now()))
	credential, errLogin := service.Login(t.Context(), LoginInput{
		Credential: &Credential{
			CredentialSchemaVersion: CredentialSchemaVersionWebAuthn,
			Type:                    Provider,
			Email:                   "person@example.com",
			WebAuthn:                webAuthn,
		},
		Relogin: true,
		PersistWebAuthn: func(context.Context, WebAuthnCredential) (WebAuthnCredential, error) {
			return WebAuthnCredential{}, errors.New("temporary storage failure")
		},
	})
	authError, ok := AsAuthError(errLogin)
	if !ok || authError.Code != "passkey_state_persist_failed" || authError.State != LifecycleReloginPending || !authError.Retryable || authError.Terminal || authError.FailureStage != "passkey_verify" {
		t.Fatalf("Login() error = %#v", errLogin)
	}
	fixture.mu.Lock()
	verifyCalls := fixture.verifyCalls
	fixture.mu.Unlock()
	if verifyCalls != 0 || credential == nil || credential.WebAuthn == nil || credential.WebAuthn.SignCount != 0 {
		t.Fatalf("verify calls=%d credential=%#v", verifyCalls, credential)
	}
}

func TestCreateWebAuthnAssertionProducesVerifiableES256Payload(t *testing.T) {
	credential := testWebAuthnCredential(t)
	challenge := base64.RawURLEncoding.EncodeToString([]byte("challenge"))
	assertion, errAssertion := createWebAuthnAssertion(credential, map[string]any{
		"challenge": challenge,
		"rpId":      WebAuthnRPID,
		"allowCredentials": []map[string]any{{
			"type": "public-key",
			"id":   credential.CredentialID,
		}},
	}, func(updated WebAuthnCredential) (WebAuthnCredential, error) {
		return updated, nil
	})
	if errAssertion != nil {
		t.Fatal(errAssertion)
	}
	responsePayload, ok := assertion["response"].(map[string]any)
	if !ok {
		t.Fatalf("assertion response = %#v", assertion["response"])
	}
	if _, exists := responsePayload["userHandle"]; exists {
		t.Fatalf("allow-list assertion unexpectedly included userHandle: %#v", responsePayload["userHandle"])
	}
	rawAssertion, errMarshal := json.Marshal(assertion)
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	var decoded webAuthnAssertionResponse
	if errUnmarshal := json.Unmarshal(rawAssertion, &decoded); errUnmarshal != nil {
		t.Fatal(errUnmarshal)
	}
	clientData, errClientData := base64.RawURLEncoding.DecodeString(decoded.Response.ClientDataJSON)
	authenticatorData, errAuthenticator := base64.RawURLEncoding.DecodeString(decoded.Response.AuthenticatorData)
	signature, errSignature := base64.RawURLEncoding.DecodeString(decoded.Response.Signature)
	if errClientData != nil || errAuthenticator != nil || errSignature != nil {
		t.Fatalf("decode assertion: client=%v authenticator=%v signature=%v", errClientData, errAuthenticator, errSignature)
	}
	var clientDataPayload map[string]any
	if errDecode := json.Unmarshal(clientData, &clientDataPayload); errDecode != nil {
		t.Fatal(errDecode)
	}
	if clientDataPayload["challenge"] != challenge || clientDataPayload["origin"] != WebAuthnOrigin || clientDataPayload["type"] != "webauthn.get" {
		t.Fatalf("client data = %#v", clientDataPayload)
	}
	if len(authenticatorData) != 37 || authenticatorData[32] != 0x05 || binary.BigEndian.Uint32(authenticatorData[33:]) != 1 {
		t.Fatalf("authenticator data = %x", authenticatorData)
	}
	rpHash := sha256.Sum256([]byte(WebAuthnRPID))
	if string(authenticatorData[:32]) != string(rpHash[:]) {
		t.Fatalf("rp hash = %x, want %x", authenticatorData[:32], rpHash)
	}
	privateDER, errDecodeKey := base64.StdEncoding.DecodeString(credential.PrivateKeyPKCS8)
	if errDecodeKey != nil {
		t.Fatal(errDecodeKey)
	}
	parsed, errParseKey := x509.ParsePKCS8PrivateKey(privateDER)
	if errParseKey != nil {
		t.Fatal(errParseKey)
	}
	privateKey := parsed.(*ecdsa.PrivateKey)
	clientHash := sha256.Sum256(clientData)
	signedPayload := append(append([]byte(nil), authenticatorData...), clientHash[:]...)
	signedHash := sha256.Sum256(signedPayload)
	if !ecdsa.VerifyASN1(&privateKey.PublicKey, signedHash[:], signature) {
		t.Fatal("WebAuthn assertion signature did not verify")
	}
}

func TestCreateWebAuthnAssertionIncludesUserHandleForDiscoverableRequest(t *testing.T) {
	credential := testWebAuthnCredential(t)
	assertion, errAssertion := createWebAuthnAssertion(credential, map[string]any{
		"challenge": base64.RawURLEncoding.EncodeToString([]byte("challenge")),
		"rpId":      WebAuthnRPID,
	}, func(updated WebAuthnCredential) (WebAuthnCredential, error) {
		return updated, nil
	})
	if errAssertion != nil {
		t.Fatal(errAssertion)
	}
	responsePayload, ok := assertion["response"].(map[string]any)
	if !ok {
		t.Fatalf("assertion response = %#v", assertion["response"])
	}
	if responsePayload["userHandle"] != credential.UserHandle {
		t.Fatalf("discoverable assertion userHandle = %#v, want %q", responsePayload["userHandle"], credential.UserHandle)
	}
}

func TestCreateWebAuthnAssertionRejectsNoncanonicalChallenge(t *testing.T) {
	credential := testWebAuthnCredential(t)
	_, errAssertion := createWebAuthnAssertion(credential, map[string]any{
		"challenge": " " + base64.RawURLEncoding.EncodeToString([]byte("challenge")),
		"rpId":      WebAuthnRPID,
	}, func(updated WebAuthnCredential) (WebAuthnCredential, error) {
		return updated, nil
	})
	if !errors.Is(errAssertion, errWebAuthnRequestOptionsInvalid) || credential.SignCount != 0 {
		t.Fatalf("assertion error=%v sign_count=%d", errAssertion, credential.SignCount)
	}
}

func TestPasskeyCSRFTokenUsesOnlyCookiesScopedToSessionOrigin(t *testing.T) {
	cookies := []Cookie{
		{Name: "next-auth.csrf-token", Value: "stale%7Chash", Domain: "auth.openai.com", Path: "/", Secure: true},
		{Name: "next-auth.csrf-token", Value: "current%7Chash", Domain: "chatgpt.com", Path: "/", Secure: true},
	}
	if got := passkeyCSRFToken([]byte(`{"csrfToken":"body-token"}`), cookies, "https://chatgpt.com"); got != "current" {
		t.Fatalf("passkeyCSRFToken() = %q, want current", got)
	}
	if got := passkeyCSRFToken([]byte(`{"csrfToken":"body-token"}`), cookies, "https://other.example"); got != "body-token" {
		t.Fatalf("passkeyCSRFToken() fallback = %q, want body-token", got)
	}
}

func TestClassifyPasskeyChallengeResponseUsesSafeLifecycleStates(t *testing.T) {
	rejected := classifyPasskeyChallengeResponse(http.StatusBadRequest, []byte(`{"error":{"code":"invalid_request"}}`), LifecycleReloginPending)
	if rejected == nil || rejected.Code != "passkey_challenge_unavailable" || rejected.State != LifecycleReauthRequired || !rejected.Terminal || rejected.Retryable {
		t.Fatalf("rejected challenge = %#v", rejected)
	}
	temporary := classifyPasskeyChallengeResponse(http.StatusServiceUnavailable, []byte(`{"error":{"code":"temporarily_unavailable"}}`), LifecycleReloginPending)
	if temporary == nil || temporary.Code != "passkey_challenge_unavailable" || temporary.State != LifecycleReloginPending || temporary.Terminal || !temporary.Retryable {
		t.Fatalf("temporary challenge = %#v", temporary)
	}
	dead := classifyPasskeyChallengeResponse(http.StatusForbidden, []byte(`{"error":{"code":"account_deactivated"}}`), LifecycleReloginPending)
	if dead == nil || dead.Code != "account_deactivated" || dead.State != LifecycleDead {
		t.Fatalf("dead challenge = %#v", dead)
	}
	deadVerify := classifyPasskeyVerificationResponse(http.StatusForbidden, []byte(`{"error":{"code":"account_deactivated"}}`), LifecycleReloginPending)
	if deadVerify == nil || deadVerify.Code != "account_deactivated" || deadVerify.State != LifecycleDead {
		t.Fatalf("dead verification = %#v", deadVerify)
	}
	redirectVerify := classifyPasskeyVerificationResponse(http.StatusFound, nil, LifecycleReloginPending)
	if redirectVerify == nil || redirectVerify.Code != "passkey_verification_failed" || redirectVerify.State != LifecycleReauthRequired || redirectVerify.FailureStage != "passkey_verify" {
		t.Fatalf("redirect verification = %#v", redirectVerify)
	}
	callback := classifyPasskeyProtocolResponse("passkey_callback", "passkey_verification_failed", "callback failed", http.StatusBadRequest, nil, LifecycleReloginPending)
	if callback == nil || callback.Code != "passkey_verification_failed" || callback.State != LifecycleReauthRequired || callback.FailureStage != "passkey_callback" {
		t.Fatalf("callback response = %#v", callback)
	}
	session := classifyPasskeyProtocolResponse("passkey_session", "passkey_session_invalid", "session failed", http.StatusServiceUnavailable, nil, LifecycleReloginPending)
	if session == nil || session.Code != "passkey_session_invalid" || session.State != LifecycleReloginPending || !session.Retryable || session.FailureStage != "passkey_session" {
		t.Fatalf("session response = %#v", session)
	}
	redirectSession := classifyPasskeyProtocolResponse("passkey_session", "passkey_session_invalid", "session failed", http.StatusFound, nil, LifecycleReauthRequired)
	if redirectSession == nil || redirectSession.Code != "passkey_session_invalid" || redirectSession.State != LifecycleReauthRequired || redirectSession.FailureStage != "passkey_session" {
		t.Fatalf("redirect session response = %#v", redirectSession)
	}
	emptySession := classifyPasskeyProtocolResponse("passkey_session", "passkey_session_invalid", "session failed", http.StatusNoContent, nil, LifecycleReauthRequired)
	if emptySession == nil || emptySession.Code != "passkey_session_invalid" || emptySession.State != LifecycleReauthRequired || emptySession.FailureStage != "passkey_session" {
		t.Fatalf("empty session response = %#v", emptySession)
	}
}

func TestServicePasskeyLoginNeverFallsBackToPassword(t *testing.T) {
	webAuthn := testWebAuthnCredential(t)
	fixture := newPasskeyLoginFixture(t, webAuthn.CredentialID, true)
	fixture.verifyStatus = http.StatusBadRequest
	fixture.verifyCode = "invalid_passkey_response"
	service := NewService(fixture.options(time.Now()))
	credential, errLogin := service.Login(t.Context(), LoginInput{
		Credential: &Credential{
			CredentialSchemaVersion: 2,
			Type:                    Provider,
			Email:                   "person@example.com",
			Password:                "correct-password",
			WebAuthn:                webAuthn,
		},
		PersistWebAuthn: func(_ context.Context, updated WebAuthnCredential) (WebAuthnCredential, error) {
			fixture.mu.Lock()
			fixture.persisted = true
			fixture.mu.Unlock()
			return updated, nil
		},
	})
	authError, ok := AsAuthError(errLogin)
	fixture.mu.Lock()
	passwordCalls := fixture.passwordCalls
	fixture.mu.Unlock()
	if !ok || authError.Code != "passkey_verification_failed" || authError.DiagnosticCode != "invalid_passkey_response" ||
		authError.FailureStage != "passkey_verify" || authError.StatusCode != http.StatusBadRequest || authError.Attempts != 1 ||
		authError.ResponseType != "json" || authError.ContentType != "application/json" || authError.CFRay != "safe-ray-SJC" ||
		!strings.Contains(authError.ResponseBody, "invalid_passkey_response") || authError.ResponseBodyTruncated || passwordCalls != 0 {
		t.Fatalf("credential=%#v error=%#v password_calls=%d", credential, errLogin, passwordCalls)
	}
}

func TestServicePasskeyLoginRequiresMatchingSessionIdentity(t *testing.T) {
	for _, test := range []struct {
		name            string
		sessionEmail    string
		sessionAccount  string
		expectedAccount string
		expectedUser    string
		existingToken   string
		sessionToken    string
	}{
		{name: "email mismatch", sessionEmail: "other@example.com", sessionAccount: "account-1"},
		{name: "missing known account", sessionEmail: "person@example.com", expectedAccount: "account-1"},
		{name: "missing known user", sessionEmail: "person@example.com", sessionAccount: "account-1", expectedUser: "user-1"},
		{
			name:          "missing identity known from access token",
			sessionEmail:  "person@example.com",
			existingToken: testIdentityJWT(time.Now().Add(time.Hour).Unix(), "account-1", "user-1", "person@example.com"),
		},
		{
			name:         "session identity evidence conflicts internally",
			sessionEmail: "person@example.com",
			sessionToken: testIdentityJWT(time.Now().Add(time.Hour).Unix(), "account-1", "user-1", "other@example.com"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			webAuthn := testWebAuthnCredential(t)
			fixture := newPasskeyLoginFixture(t, webAuthn.CredentialID, true)
			fixture.sessionEmail = test.sessionEmail
			fixture.sessionAccount = test.sessionAccount
			fixture.sessionToken = test.sessionToken
			if test.expectedUser != "" {
				fixture.sessionUser = ""
			}
			if test.existingToken != "" {
				fixture.sessionUser = ""
			}
			service := NewService(fixture.options(time.Now()))
			_, errLogin := service.Login(t.Context(), LoginInput{
				Credential: &Credential{
					CredentialSchemaVersion: 2,
					Type:                    Provider,
					Email:                   "person@example.com",
					AccountID:               test.expectedAccount,
					UserID:                  test.expectedUser,
					AccessToken:             test.existingToken,
					WebAuthn:                webAuthn,
				},
				PersistWebAuthn: func(_ context.Context, updated WebAuthnCredential) (WebAuthnCredential, error) {
					fixture.mu.Lock()
					fixture.persisted = true
					fixture.mu.Unlock()
					return updated, nil
				},
			})
			authError, ok := AsAuthError(errLogin)
			if !ok || authError.Code != "passkey_session_invalid" || authError.State != LifecycleReauthRequired {
				t.Fatalf("Login() error = %#v", errLogin)
			}
		})
	}
}

func TestServicePasskeyLoginRejectsUntrustedCallback(t *testing.T) {
	webAuthn := testWebAuthnCredential(t)
	fixture := newPasskeyLoginFixture(t, webAuthn.CredentialID, true)
	fixture.callbackURL = "https://example.com/api/auth/callback/openai"
	service := NewService(fixture.options(time.Now()))
	_, errLogin := service.Login(t.Context(), LoginInput{
		Credential: &Credential{
			CredentialSchemaVersion: 2,
			Type:                    Provider,
			Email:                   "person@example.com",
			WebAuthn:                webAuthn,
		},
		PersistWebAuthn: func(_ context.Context, updated WebAuthnCredential) (WebAuthnCredential, error) {
			fixture.mu.Lock()
			fixture.persisted = true
			fixture.mu.Unlock()
			return updated, nil
		},
	})
	authError, ok := AsAuthError(errLogin)
	if !ok || authError.Code != "passkey_verification_failed" || authError.FailureStage != "passkey_callback" {
		t.Fatalf("Login() error = %#v", errLogin)
	}
}

func TestConsumePasskeyCallbackDoesNotTrustPreexistingSessionCookie(t *testing.T) {
	var callbackCalls int
	var sessionBase string
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/oauth/passkey/resume":
			response.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(response, `{"continue_url":%q}`, sessionBase+"/api/auth/callback/openai?code=web-code&state=web-state")
		case "/oauth/passkey/body":
			response.Header().Set("Content-Type", "text/html")
			_, _ = fmt.Fprintf(response, `<script>location.href=%q</script>`, sessionBase+"/api/auth/callback/openai?code=body-code&amp;state=body-state")
		case "/api/auth/callback/openai":
			callbackCalls++
			http.SetCookie(response, &http.Cookie{Name: "next-auth.session-token", Value: "new-session", Path: "/", HttpOnly: true})
			http.Redirect(response, request, "/", http.StatusFound)
		case "/":
			response.Header().Set("Content-Type", "text/html")
			_, _ = io.WriteString(response, "<html>home</html>")
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()
	sessionBase = server.URL
	authBase := strings.Replace(server.URL, "127.0.0.1", "localhost", 1)
	client, errClient := NewClient(DefaultPersona(), "", nil)
	if errClient != nil {
		t.Fatal(errClient)
	}
	defer client.CloseIdleConnections()
	if errCookie := client.SetCookie(sessionBase, "next-auth.session-token", "old-session"); errCookie != nil {
		t.Fatal(errCookie)
	}
	service := NewService(Options{AuthBaseURL: authBase, SessionBaseURL: sessionBase})
	if errCallback := service.consumePasskeyCallback(t.Context(), client, authBase+"/oauth/passkey/resume", LifecycleReloginPending); errCallback != nil {
		t.Fatal(errCallback)
	}
	if errCallback := service.consumePasskeyCallback(t.Context(), client, authBase+"/oauth/passkey/body", LifecycleReloginPending); errCallback != nil {
		t.Fatal(errCallback)
	}
	if callbackCalls != 2 {
		t.Fatalf("callback calls = %d, want 2", callbackCalls)
	}
	value, errCookie := credentialCookieValueForURL(client.ExportCookies(), sessionBase, "next-auth.session-token")
	if errCookie != nil || value != "new-session" {
		t.Fatalf("session cookie = %q, error = %v", value, errCookie)
	}
}

func TestServicePasskeyLoginFreshFlowRetryAdvancesCounterAgain(t *testing.T) {
	webAuthn := testWebAuthnCredential(t)
	fixture := newPasskeyLoginFixture(t, webAuthn.CredentialID, true)
	fixture.verifyFailures = 1
	proxy := newLoginConnectProxy(t, nil)
	service := NewService(fixture.options(time.Now()))
	persisted := cloneWebAuthnCredential(webAuthn)
	credential, errLogin := service.Login(t.Context(), LoginInput{
		Credential: &Credential{
			CredentialSchemaVersion: 2,
			Type:                    Provider,
			Email:                   "person@example.com",
			WebAuthn:                webAuthn,
		},
		LoginProxy: LoginProxyConfig{
			Enabled:         true,
			URLTemplate:     proxy.URL,
			RequestAttempts: 1,
			FlowAttempts:    2,
		},
		PersistWebAuthn: func(_ context.Context, updated WebAuthnCredential) (WebAuthnCredential, error) {
			if updated.SignCount < persisted.SignCount {
				t.Fatalf("sign_count moved backwards: %d -> %d", persisted.SignCount, updated.SignCount)
			}
			persisted = cloneWebAuthnCredential(&updated)
			fixture.mu.Lock()
			fixture.persisted = true
			fixture.mu.Unlock()
			return updated, nil
		},
	})
	if errLogin != nil {
		t.Fatalf("Login() error = %v", errLogin)
	}
	fixture.mu.Lock()
	verifyCalls := fixture.verifyCalls
	fixture.mu.Unlock()
	if verifyCalls != 2 || persisted.SignCount != 2 || credential.WebAuthn.SignCount != 2 {
		t.Fatalf("verify_calls=%d persisted=%#v credential=%#v", verifyCalls, persisted, credential.WebAuthn)
	}
}

func TestServicePasskeyLoginCanConfirmInvalidResponseAcrossFreshFlows(t *testing.T) {
	webAuthn := testWebAuthnCredential(t)
	fixture := newPasskeyLoginFixture(t, webAuthn.CredentialID, true)
	fixture.verifyStatus = http.StatusBadRequest
	fixture.verifyCode = "invalid_passkey_response"
	proxy := newLoginConnectProxy(t, nil)
	service := NewService(fixture.options(time.Now()))
	persisted := cloneWebAuthnCredential(webAuthn)
	credential, errLogin := service.Login(t.Context(), LoginInput{
		Credential: &Credential{
			CredentialSchemaVersion: 2,
			Type:                    Provider,
			Email:                   "person@example.com",
			WebAuthn:                webAuthn,
		},
		LoginProxy: LoginProxyConfig{
			Enabled:         true,
			URLTemplate:     proxy.URL,
			RequestAttempts: 1,
			FlowAttempts:    2,
		},
		RetryInvalidPasskeyResponse: true,
		PersistWebAuthn: func(_ context.Context, updated WebAuthnCredential) (WebAuthnCredential, error) {
			persisted = cloneWebAuthnCredential(&updated)
			fixture.mu.Lock()
			fixture.persisted = true
			fixture.mu.Unlock()
			return updated, nil
		},
	})
	authError, ok := AsAuthError(errLogin)
	fixture.mu.Lock()
	challengeCalls := fixture.challengeCalls
	verifyCalls := fixture.verifyCalls
	fixture.mu.Unlock()
	if !ok || authError.DiagnosticCode != "invalid_passkey_response" || authError.Attempts != 2 ||
		credential == nil || credential.WebAuthn.SignCount != 2 || persisted.SignCount != 2 ||
		challengeCalls != 2 || verifyCalls != 2 {
		t.Fatalf("credential=%#v error=%#v persisted=%#v challenge_calls=%d verify_calls=%d", credential, errLogin, persisted, challengeCalls, verifyCalls)
	}
}
