package chatgptweb

import (
	"encoding/json"
	"testing"
)

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
