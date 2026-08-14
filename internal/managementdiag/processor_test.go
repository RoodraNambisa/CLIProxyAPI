package managementdiag

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestProcessResponseBodyDetailLevelsAndPermanentSecrets(t *testing.T) {
	raw := `{"email":"person@example.com","message":"upstream detail","url":"https://example.com/path?view=full&code=oauth-secret","access_token":"token-secret","api798_url":"https://api798.com/get_code?email=person@example.com&auth_code=api-secret","recovery_keys":["recover-secret"]}`
	safe, _ := ProcessResponseBody(raw, DetailLevelSafe, 4096)
	full, _ := ProcessResponseBody(raw, DetailLevelFull, 4096)
	if strings.Contains(safe, "person@example.com") || strings.Contains(safe, "view=full") {
		t.Fatalf("safe response retained full details: %s", safe)
	}
	for _, want := range []string{"person@example.com", "upstream detail", "view=full"} {
		if !strings.Contains(full, want) {
			t.Fatalf("full response missing %q: %s", want, full)
		}
	}
	for _, secret := range []string{"oauth-secret", "token-secret", "api-secret", "recover-secret"} {
		if strings.Contains(safe, secret) || strings.Contains(full, secret) {
			t.Fatalf("diagnostic leaked %q: safe=%s full=%s", secret, safe, full)
		}
	}
}

func TestProcessTextSupportsHTMLAndUTF8Truncation(t *testing.T) {
	raw := `<html><body>person@example.com Bearer token-secret https://example.com/error?trace=abc</body></html>` + strings.Repeat("界", 2000)
	full, truncated := ProcessText(raw, DetailLevelFull, 4096)
	if !truncated || !strings.Contains(full, "person@example.com") || !strings.Contains(full, "trace=abc") {
		t.Fatalf("full=%q truncated=%v", full, truncated)
	}
	if strings.Contains(full, "token-secret") {
		t.Fatalf("full leaked bearer token: %s", full)
	}
}

func TestProcessURLAlwaysRedactsSignedAndAuthorizationParameters(t *testing.T) {
	ordinary := ProcessURL("https://example.com/path?view=full&lang=zh", DetailLevelFull)
	if !strings.Contains(ordinary, "view=full") || !strings.Contains(ordinary, "lang=zh") {
		t.Fatalf("ordinary URL = %s", ordinary)
	}
	for _, raw := range []string{
		"https://api798.com/get_code?email=person@example.com&auth_code=secret",
		"https://bucket.blob.core.windows.net/file?sv=1&sig=secret",
		"https://cdn.example.com/file?Expires=123&Signature=secret&Key-Pair-Id=key-id",
		"https://bucket.example.com/file?X-Amz-Credential=credential-scope&X-Amz-Date=20260814T000000Z",
	} {
		processed := ProcessURL(raw, DetailLevelFull)
		if strings.Contains(processed, "secret") || strings.Contains(processed, "person@example.com") || strings.Contains(processed, "key-id") || strings.Contains(processed, "credential-scope") {
			t.Fatalf("sensitive URL leaked: %s", processed)
		}
	}
}

func TestProcessResponseBodyAlwaysHidesCredentialVariants(t *testing.T) {
	raw := `{"client_secret":"client-secret","otp_code":"123456","oauth_code":"oauth-secret","password_hash":"password-secret","cookies":"cookie-secret","nested":{"credentialId":"credential-secret","userHandle":"handle-secret","signedPasskeyResponse":{"response":{"signature":"passkey-secret"}}},"message":"ordinary detail"}`
	for _, level := range []string{DetailLevelSafe, DetailLevelFull} {
		processed, _ := ProcessResponseBody(raw, level, 4096)
		if !strings.Contains(processed, "ordinary detail") {
			t.Fatalf("%s response lost ordinary detail: %s", level, processed)
		}
		for _, secret := range []string{"client-secret", "123456", "oauth-secret", "password-secret", "cookie-secret", "credential-secret", "handle-secret", "passkey-secret"} {
			if strings.Contains(processed, secret) {
				t.Fatalf("%s response leaked %q: %s", level, secret, processed)
			}
		}
	}
}

func TestProcessTextAlwaysHidesRecoveryCollections(t *testing.T) {
	for _, input := range []string{
		`recovery_keys=["recover-one","recover-two"]`,
		`recovery-codes: recover-three`,
		`credential_id=credential-secret user_handle=handle-secret password_hash=password-secret`,
		`token=generic-token code_verifier=oauth-verifier client_data_json=passkey-client signature=passkey-signature`,
	} {
		processed, _ := ProcessText(input, DetailLevelFull, 4096)
		for _, secret := range []string{"recover-", "credential-secret", "handle-secret", "password-secret", "generic-token", "oauth-verifier", "passkey-client", "passkey-signature"} {
			if strings.Contains(processed, secret) {
				t.Fatalf("full diagnostics leaked credential material: %s", processed)
			}
		}
	}
}

func TestManagementOnlyValueDoesNotExposeContentToLogFormatters(t *testing.T) {
	const secret = "management diagnostic body"
	wrapped := NewManagementOnlyValue(secret)
	if got := wrapped.Value(); got != secret {
		t.Fatalf("management value = %q", got)
	}
	if got := fmt.Sprint(wrapped); strings.Contains(got, secret) {
		t.Fatalf("text formatter exposed management content: %s", got)
	}
	encoded, errMarshal := json.Marshal(map[string]any{"response_body": wrapped})
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	if strings.Contains(string(encoded), secret) {
		t.Fatalf("JSON formatter exposed management content: %s", encoded)
	}

	withFallback := NewManagementOnlyValueWithFallback("/path?trace=full", "/path")
	if got := fmt.Sprint(withFallback); got != "/path" {
		t.Fatalf("safe fallback = %q", got)
	}
}
