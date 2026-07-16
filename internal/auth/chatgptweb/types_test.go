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
