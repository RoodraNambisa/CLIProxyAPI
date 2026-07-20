package chatgptweb

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

func TestDecodeImportCredentialCompatibilityFields(t *testing.T) {
	tests := []struct {
		name         string
		payload      string
		strategy     RefreshStrategy
		sessionValue string
	}{
		{name: "oauth aliases", payload: `{"accessToken":"access","refreshToken":"refresh","idToken":"id"}`, strategy: RefreshStrategyWebOAuthRT},
		{name: "cookie header", payload: `{"access_token":"access","cookie_header":"__Secure-next-auth.session-token=session"}`, strategy: RefreshStrategyChatGPTSession},
		{name: "explicit session value", payload: `{"access_token":"access","session_cookie":"session"}`, strategy: RefreshStrategyChatGPTSession, sessionValue: "session"},
		{name: "padded session value", payload: `{"access_token":"access","session_cookie":"session=="}`, strategy: RefreshStrategyChatGPTSession, sessionValue: "session=="},
		{name: "token only", payload: `{"access_token":"access","session_token":"must-not-be-cookie"}`, strategy: RefreshStrategyTokenOnly},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			credential, err := DecodeImportCredential([]byte(test.payload))
			if err != nil {
				t.Fatal(err)
			}
			if credential.RefreshStrategy != test.strategy {
				t.Fatalf("strategy = %q, want %q", credential.RefreshStrategy, test.strategy)
			}
			if test.name == "token only" && HasSessionCookie(credential.Cookies) {
				t.Fatal("session_token was interpreted as a browser cookie")
			}
			if test.sessionValue != "" {
				value := ""
				for _, cookie := range credential.Cookies {
					if cookie.Name == "__Secure-next-auth.session-token" {
						value = cookie.Value
					}
				}
				if value != test.sessionValue {
					t.Fatalf("session cookie value = %q, want %q", value, test.sessionValue)
				}
			}
		})
	}
}

func TestDecodeImportCredentialAcceptsBrowserCookieExports(t *testing.T) {
	credential, errDecode := DecodeImportCredential([]byte(`{
		"access_token":"access",
		"cookies":[
			{"name":"__Secure-next-auth.session-token","value":"session","domain":"chatgpt.com","path":"/","secure":true,"httpOnly":true,"sameSite":"lax"},
			{"name":"other","value":"value","domain":"chatgpt.com","path":"/","same_site":"no_restriction"}
		]
	}`))
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	if credential.RefreshStrategy != RefreshStrategyChatGPTSession || len(credential.Cookies) != 2 {
		t.Fatalf("credential = %+v", credential)
	}
	if !credential.Cookies[0].HTTPOnly || credential.Cookies[0].SameSite != int(http.SameSiteLaxMode) {
		t.Fatalf("first cookie = %+v", credential.Cookies[0])
	}
	if credential.Cookies[1].SameSite != int(http.SameSiteNoneMode) {
		t.Fatalf("second cookie = %+v", credential.Cookies[1])
	}
}

func TestDecodeImportCredentialAcceptsNumericBrowserCookieExpiry(t *testing.T) {
	expiresAt := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	payload, errMarshal := json.Marshal(map[string]any{
		"access_token": "access",
		"cookies": []map[string]any{{
			"name": "__Secure-next-auth.session-token", "value": "session", "domain": "chatgpt.com", "path": "/", "expires": expiresAt.Unix(),
		}},
	})
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	credential, errDecode := DecodeImportCredential(payload)
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	if credential.RefreshStrategy != RefreshStrategyChatGPTSession || len(credential.Cookies) != 1 || credential.Cookies[0].Expires != expiresAt.Format(time.RFC3339Nano) {
		t.Fatalf("credential = %+v", credential)
	}
}

func TestDecodeImportCredentialDistinguishesExpiredAndSessionCookieSentinels(t *testing.T) {
	tests := []struct {
		name         string
		expires      int
		wantExpires  string
		wantStrategy RefreshStrategy
	}{
		{name: "zero is unix epoch", expires: 0, wantExpires: time.Unix(0, 0).UTC().Format(time.RFC3339Nano), wantStrategy: RefreshStrategyTokenOnly},
		{name: "negative is browser session sentinel", expires: -1, wantExpires: "", wantStrategy: RefreshStrategyChatGPTSession},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload, errMarshal := json.Marshal(map[string]any{
				"access_token": "access",
				"cookies": []map[string]any{{
					"name": "__Secure-next-auth.session-token", "value": "session", "domain": "chatgpt.com", "path": "/", "expires": test.expires,
				}},
			})
			if errMarshal != nil {
				t.Fatal(errMarshal)
			}
			credential, errDecode := DecodeImportCredential(payload)
			if errDecode != nil {
				t.Fatal(errDecode)
			}
			if credential.RefreshStrategy != test.wantStrategy || len(credential.Cookies) != 1 || credential.Cookies[0].Expires != test.wantExpires {
				t.Fatalf("credential = %+v", credential)
			}
		})
	}
}

func TestDecodeImportCredentialFreezesPositiveCookieMaxAge(t *testing.T) {
	before := time.Now().UTC()
	credential, errDecode := DecodeImportCredential([]byte(`{
		"access_token":"access",
		"cookies":[{"name":"__Secure-next-auth.session-token","value":"session","domain":"chatgpt.com","path":"/","maxAge":60}]
	}`))
	after := time.Now().UTC()
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	if credential.RefreshStrategy != RefreshStrategyChatGPTSession || len(credential.Cookies) != 1 || credential.Cookies[0].MaxAge != 0 {
		t.Fatalf("credential = %+v", credential)
	}
	expiresAt, errParse := time.Parse(time.RFC3339Nano, credential.Cookies[0].Expires)
	if errParse != nil {
		t.Fatalf("cookie expiry = %q: %v", credential.Cookies[0].Expires, errParse)
	}
	if expiresAt.Before(before.Add(time.Minute)) || expiresAt.After(after.Add(time.Minute)) {
		t.Fatalf("cookie expiry = %v, want between %v and %v", expiresAt, before.Add(time.Minute), after.Add(time.Minute))
	}
}

func TestDecodeImportCredentialRejectsLoginOnlyInput(t *testing.T) {
	_, err := DecodeImportCredential([]byte(`{"email":"person@example.com","password":"secret","totp_secret":"JBSWY3DPEHPK3PXP"}`))
	if err == nil {
		t.Fatal("DecodeImportCredential() accepted password-only input")
	}
}

func TestDecodeImportCredentialRejectsForeignProvider(t *testing.T) {
	_, err := DecodeImportCredential([]byte(`{"type":"codex","access_token":"access"}`))
	if err == nil {
		t.Fatal("DecodeImportCredential() accepted codex credential")
	}
}

func TestDecodeImportCredentialRejectsCodexSource(t *testing.T) {
	_, err := DecodeImportCredential([]byte(`{"type":"chatgpt-web","refresh_strategy":"codex_source","source_auth_id":"codex.json","source_credential_uid":"uid","access_token":"access","refresh_token":"must-not-be-imported"}`))
	if err == nil {
		t.Fatal("DecodeImportCredential() accepted a linked Codex source")
	}
}

func TestDecodeImportCredentialRejectsConflictingExplicitAndTokenIdentity(t *testing.T) {
	token := testIdentityJWT(time.Now().Add(time.Hour).Unix(), "account-token", "user-token", "token@example.com")
	payload, errMarshal := json.Marshal(map[string]any{
		"access_token": token,
		"account_id":   "account-upload",
		"email":        "upload@example.com",
	})
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	if _, errDecode := DecodeImportCredential(payload); errDecode == nil {
		t.Fatal("DecodeImportCredential() accepted conflicting explicit and JWT identities")
	}
}
