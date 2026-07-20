package chatgptweb

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

func TestHasSessionCookieValidatesScopeExpiryAndChunks(t *testing.T) {
	now := time.Date(2026, time.July, 19, 12, 0, 0, 0, time.UTC)
	base := Cookie{Name: "__Secure-next-auth.session-token", Value: "session", Domain: "chatgpt.com", Path: "/", Secure: true}
	if !hasSessionCookieAt([]Cookie{base}, now) {
		t.Fatal("valid session cookie was rejected")
	}
	legacy := base
	legacy.Domain = ""
	legacy.Path = ""
	if hasSessionCookieAt([]Cookie{legacy}, now) {
		t.Fatal("unscoped session cookie was accepted without normalization")
	}
	wrongDomain := base
	wrongDomain.Domain = "example.com"
	if hasSessionCookieAt([]Cookie{wrongDomain}, now) {
		t.Fatal("foreign-domain session cookie was accepted")
	}
	wrongPath := base
	wrongPath.Path = "/other"
	if hasSessionCookieAt([]Cookie{wrongPath}, now) {
		t.Fatal("wrong-path session cookie was accepted")
	}
	expired := base
	expired.RawExpires = now.Add(-time.Minute).Format(http.TimeFormat)
	if hasSessionCookieAt([]Cookie{expired}, now) {
		t.Fatal("expired session cookie was accepted")
	}
	unanchoredMaxAge := base
	unanchoredMaxAge.MaxAge = 60
	if hasSessionCookieAt([]Cookie{unanchoredMaxAge}, now) {
		t.Fatal("persisted positive Max-Age without an absolute expiry was accepted")
	}
	firstChunk := base
	firstChunk.Name += ".0"
	if hasSessionCookieAt([]Cookie{firstChunk}, now) {
		t.Fatal("incomplete chunked session cookie was accepted")
	}
	secondChunk := base
	secondChunk.Name += ".1"
	if !hasSessionCookieAt([]Cookie{firstChunk, secondChunk}, now) {
		t.Fatal("complete chunked session cookie was rejected")
	}
}

func TestDecodeCredentialScopesLegacySessionCookie(t *testing.T) {
	credential, errDecode := DecodeCredential([]byte(`{"type":"chatgpt-web","cookies":[{"name":"next-auth.session-token","value":"session"}]}`))
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	if credential.RefreshStrategy != RefreshStrategyChatGPTSession || len(credential.Cookies) != 1 {
		t.Fatalf("credential = %+v", credential)
	}
	cookie := credential.Cookies[0]
	if cookie.Domain != "chatgpt.com" || cookie.Host != "chatgpt.com" || cookie.Path != "/" || !cookie.Secure {
		t.Fatalf("scoped cookie = %+v", cookie)
	}
}

func TestCredentialMetadataRoundTrip(t *testing.T) {
	t.Parallel()
	original := &Credential{
		Type:               Provider,
		Email:              "person@example.com",
		Password:           "secret-password",
		TOTPSecret:         "JBSWY3DPEHPK3PXP",
		AccessToken:        "access",
		RefreshToken:       "refresh",
		IDToken:            "id",
		Expired:            "2026-07-16T12:00:00Z",
		Cookies:            []Cookie{{Name: "session", Value: "cookie", Host: "auth.openai.com", Path: "/", Secure: true}},
		Persona:            DefaultPersona(),
		LifecycleState:     LifecycleActive,
		LifecycleReason:    "",
		LifecycleUpdatedAt: "2026-07-16T10:00:00Z",
		LastLoginAt:        "2026-07-16T10:00:00Z",
		LastRefreshAt:      "2026-07-16T11:00:00Z",
		LastReloginAt:      "2026-07-16T09:00:00Z",
	}
	metadata := map[string]any{"unrelated": "preserved"}
	original.ApplyToMetadata(metadata)
	if metadata["unrelated"] != "preserved" {
		t.Fatal("ApplyToMetadata removed unrelated metadata")
	}
	decoded, err := ParseCredential(metadata)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{
		"type", "email", "password", "totp_secret", "access_token", "refresh_token", "id_token", "expired",
		"cookies", "persona", "lifecycle_state", "lifecycle_reason", "lifecycle_updated_at",
		"last_login_at", "last_refresh_at", "last_relogin_at",
	} {
		if _, ok := fields[field]; !ok {
			t.Errorf("credential JSON is missing %q", field)
		}
	}
	if decoded.Password != original.Password || decoded.TOTPSecret != original.TOTPSecret {
		t.Fatal("credential secrets did not round-trip")
	}
}

func TestCredentialMetadataSanitizesUnknownLifecycleReason(t *testing.T) {
	credential := &Credential{
		Type:            Provider,
		LifecycleState:  LifecycleReauthRequired,
		LifecycleReason: "tokenLikeABC123",
	}
	metadata := make(map[string]any)
	credential.ApplyToMetadata(metadata)
	if metadata["lifecycle_reason"] != "authentication_failed" {
		t.Fatalf("persisted lifecycle reason = %v", metadata["lifecycle_reason"])
	}
	metadata["lifecycle_reason"] = "another-secret-shaped-code"
	decoded, errDecode := ParseCredential(metadata)
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	if decoded.LifecycleReason != "authentication_failed" {
		t.Fatalf("decoded lifecycle reason = %q", decoded.LifecycleReason)
	}
	if got := SafeLifecycleReason("account_deleted"); got != "account_deleted" {
		t.Fatalf("known lifecycle reason = %q", got)
	}
	if got := SafeLifecycleReason("access_denied"); got != "access_denied" {
		t.Fatalf("access denied lifecycle reason = %q", got)
	}
	if got := SafeLifecycleReason("reauth_required"); got != "reauth_required" {
		t.Fatalf("reauth required lifecycle reason = %q", got)
	}
	for _, reason := range []string{
		"authorize_network_error",
		"authorize_continue_network_error",
		"authorize_redirect_network_error",
		"password_verify_network_error",
		"mfa_verify_network_error",
		"token_refresh_network_error",
		"token_exchange_network_error",
		"oauth_redirect_network_error",
	} {
		if got := SafeLifecycleReason(reason); got != reason {
			t.Errorf("network lifecycle reason %q = %q", reason, got)
		}
	}
	credential.LifecycleState = LifecycleState("tokenLikeStateABC123")
	credential.ApplyToMetadata(metadata)
	if metadata["lifecycle_state"] != string(LifecycleReauthRequired) {
		t.Fatalf("persisted lifecycle state = %v", metadata["lifecycle_state"])
	}
	metadata["lifecycle_state"] = "another-secret-shaped-state"
	decoded, errDecode = ParseCredential(metadata)
	if errDecode != nil {
		t.Fatal(errDecode)
	}
	if decoded.LifecycleState != LifecycleReauthRequired {
		t.Fatalf("decoded lifecycle state = %q", decoded.LifecycleState)
	}
}

func TestCredentialMetadataInfersLegacyLifecycleState(t *testing.T) {
	tests := []struct {
		name        string
		accessToken string
		want        LifecycleState
	}{
		{name: "active token", accessToken: "access-token", want: LifecycleActive},
		{name: "pending without token", want: LifecycleLoginPending},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metadata := map[string]any{
				"type":         Provider,
				"email":        "legacy@example.com",
				"access_token": test.accessToken,
			}
			credential, errParse := ParseCredential(metadata)
			if errParse != nil {
				t.Fatal(errParse)
			}
			if credential.LifecycleState != test.want {
				t.Fatalf("inferred lifecycle state = %q, want %q", credential.LifecycleState, test.want)
			}

			credential.LifecycleState = ""
			credential.ApplyToMetadata(metadata)
			if metadata["lifecycle_state"] != string(test.want) {
				t.Fatalf("persisted lifecycle state = %v, want %q", metadata["lifecycle_state"], test.want)
			}
		})
	}
}

func TestCredentialMetadataInfersLegacyRefreshStrategy(t *testing.T) {
	tests := []struct {
		name     string
		metadata map[string]any
		want     RefreshStrategy
		mode     string
	}{
		{name: "oauth refresh token", metadata: map[string]any{"refresh_token": "refresh"}, want: RefreshStrategyWebOAuthRT, mode: CredentialModeNative},
		{name: "session cookie", metadata: map[string]any{"cookies": []map[string]any{{"name": "__Secure-next-auth.session-token", "value": "session"}}}, want: RefreshStrategyChatGPTSession, mode: CredentialModeNative},
		{name: "legacy password login", metadata: map[string]any{"access_token": "access", "email": "person@example.com", "password": "secret"}, want: RefreshStrategyWebOAuthRT, mode: CredentialModeNative},
		{name: "codex source", metadata: map[string]any{"source_auth_id": "codex-a.json", "source_credential_uid": "source-uid"}, want: RefreshStrategyCodexSource, mode: CredentialModeLinkedCodex},
		{name: "partial codex source with oauth refresh token", metadata: map[string]any{"source_auth_id": "stale-codex.json", "refresh_token": "refresh"}, want: RefreshStrategyWebOAuthRT, mode: CredentialModeNative},
		{name: "partial codex source without refresh material", metadata: map[string]any{"source_credential_uid": "stale-uid", "access_token": "access"}, want: RefreshStrategyTokenOnly, mode: CredentialModeTokenOnly},
		{name: "access token only", metadata: map[string]any{"access_token": "access"}, want: RefreshStrategyTokenOnly, mode: CredentialModeTokenOnly},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.metadata["type"] = Provider
			credential, err := ParseCredential(test.metadata)
			if err != nil {
				t.Fatal(err)
			}
			if credential.RefreshStrategy != test.want || credential.CredentialMode != test.mode {
				t.Fatalf("strategy/mode = %q/%q, want %q/%q", credential.RefreshStrategy, credential.CredentialMode, test.want, test.mode)
			}
		})
	}
}

func TestCredentialMetadataRejectsUnknownRefreshStrategy(t *testing.T) {
	_, err := ParseCredential(map[string]any{"type": Provider, "refresh_strategy": "unknown"})
	if err == nil {
		t.Fatal("ParseCredential() succeeded with unknown refresh strategy")
	}
}
