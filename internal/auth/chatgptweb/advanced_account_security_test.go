package chatgptweb

import (
	"encoding/base64"
	"encoding/json"
	"reflect"
	"testing"
)

func TestCredentialAdvancedAccountSecurityRoundTrip(t *testing.T) {
	aas := testAdvancedAccountSecurityCredential(t)
	payload, errMarshal := json.Marshal(map[string]any{
		"type":                      Provider,
		"credential_schema_version": CredentialSchemaVersionAdvancedAccountSecurity,
		"email":                     "person@example.com",
		"access_token":              "access-token",
		"advanced_account_security": aas,
		"login_method":              LoginMethodAdvancedSecurityPasskey,
	})
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	decoded, errDecode := DecodeCredential(payload)
	if errDecode != nil {
		t.Fatalf("DecodeCredential() error = %v", errDecode)
	}
	if decoded.CredentialSchemaVersion != CredentialSchemaVersionAdvancedAccountSecurity ||
		decoded.AdvancedAccountSecurity == nil || !reflect.DeepEqual(decoded.AdvancedAccountSecurity, aas) {
		t.Fatalf("decoded advanced account security = %#v", decoded.AdvancedAccountSecurity)
	}

	metadata := map[string]any{"unrelated": "preserved"}
	decoded.ApplyToMetadata(metadata)
	reparsed, errParse := ParseCredential(metadata)
	if errParse != nil {
		t.Fatalf("ParseCredential() error = %v", errParse)
	}
	if !reflect.DeepEqual(reparsed.AdvancedAccountSecurity, aas) {
		t.Fatal("advanced account security material did not survive metadata round trip")
	}
	if metadata["unrelated"] != "preserved" {
		t.Fatal("ApplyToMetadata() removed unrelated metadata")
	}
}

func TestCloneCredentialDeepCopiesAdvancedAccountSecurity(t *testing.T) {
	original := &Credential{AdvancedAccountSecurity: testAdvancedAccountSecurityCredential(t)}
	cloned := cloneCredential(original)
	cloned.AdvancedAccountSecurity.Passkeys[0].Credential.SignCount++
	cloned.AdvancedAccountSecurity.RecoveryKeys[0].RecoveryKey = "changed"
	if original.AdvancedAccountSecurity.Passkeys[0].Credential.SignCount == cloned.AdvancedAccountSecurity.Passkeys[0].Credential.SignCount ||
		original.AdvancedAccountSecurity.RecoveryKeys[0].RecoveryKey == "changed" {
		t.Fatal("cloneCredential() shared advanced account security state")
	}
}

func TestResolveLoginMethodAlwaysPrefersAdvancedAccountSecurity(t *testing.T) {
	credential := &Credential{
		CredentialSchemaVersion: CredentialSchemaVersionAdvancedAccountSecurity,
		AdvancedAccountSecurity: testAdvancedAccountSecurityCredential(t),
		WebAuthn:                testWebAuthnCredential(t),
		Password:                "secret",
		API798URL:               "https://api798.com/get_code?email=person%40example.com&auth_code=opaque",
		Email:                   "person@example.com",
		LoginMethod:             LoginMethodAPI798,
	}
	method, errResolve := ResolveLoginMethod(credential, true)
	if errResolve != nil || method != LoginMethodAdvancedSecurityPasskey {
		t.Fatalf("ResolveLoginMethod() = %q, %v", method, errResolve)
	}
}

func testAdvancedAccountSecurityCredential(t *testing.T) *AdvancedAccountSecurityCredential {
	t.Helper()
	passkey := testWebAuthnCredential(t)
	passkey.SignCount = 6
	passkey.BackupEligible = true
	passkey.BackupState = true
	passkey.Transports = []string{"hybrid"}

	securityKey := testWebAuthnCredential(t)
	securityKey.CredentialID = base64.RawURLEncoding.EncodeToString([]byte("credential-id-2"))
	securityKey.UserHandle = base64.RawURLEncoding.EncodeToString([]byte("user-handle-2"))
	securityKey.MFAFactorID = "factor-2"
	securityKey.SignCount = 4
	securityKey.Transports = []string{"usb"}

	recoveryKeys := make([]AdvancedAccountRecoveryKey, 5)
	for index := range recoveryKeys {
		recoveryKeys[index] = AdvancedAccountRecoveryKey{
			RecoveryKey:                "display-" + string(rune('a'+index)),
			AccountRecoveryCode:        "account-" + string(rune('a'+index)),
			XWingPublicKeyBase64:       base64.StdEncoding.EncodeToString([]byte{byte(index + 1), 1}),
			AuthenticationSecretBase64: base64.StdEncoding.EncodeToString([]byte{byte(index + 1), 2}),
		}
	}
	return &AdvancedAccountSecurityCredential{
		Version: 1,
		Enabled: true,
		Passkeys: []AdvancedAccountSecurityPasskeyCredential{
			{Kind: "passkey", IsNonDeviceBound: true, Credential: *passkey},
			{Kind: "security-key", IsSecurityKey: true, Credential: *securityKey},
		},
		RecoveryKeys: recoveryKeys,
		EnrolledAt:   "2026-08-05T00:00:00Z",
		VerifiedAt:   "2026-08-05T01:00:00Z",
		LoginMethod:  "passkey",
	}
}
