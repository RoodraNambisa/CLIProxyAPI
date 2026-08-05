package chatgptweb

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"testing"
)

func TestDecodeCredentialAcceptsWebAuthnV1(t *testing.T) {
	credential := testWebAuthnCredential(t)
	payload, errMarshal := json.Marshal(map[string]any{
		"type":                      Provider,
		"credential_schema_version": 2,
		"email":                     "person@example.com",
		"access_token":              "access-token",
		"webauthn":                  credential,
	})
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	decoded, errDecode := DecodeCredential(payload)
	if errDecode != nil {
		t.Fatalf("DecodeCredential() error = %v", errDecode)
	}
	if decoded.CredentialSchemaVersion != 2 || decoded.WebAuthn == nil {
		t.Fatalf("decoded credential = %#v", decoded)
	}
	metadata := make(map[string]any)
	decoded.ApplyToMetadata(metadata)
	reparsed, errParse := ParseCredential(metadata)
	if errParse != nil {
		t.Fatalf("ParseCredential() error = %v", errParse)
	}
	if reparsed.WebAuthn == nil || reparsed.WebAuthn.PrivateKeyPKCS8 != credential.PrivateKeyPKCS8 {
		t.Fatal("WebAuthn private key did not survive metadata round trip")
	}
}

func TestDecodeCredentialRejectsInvalidWebAuthnSchemas(t *testing.T) {
	valid := testWebAuthnCredential(t)
	tests := []struct {
		name   string
		schema int
		key    *WebAuthnCredential
		mutate func(*WebAuthnCredential)
	}{
		{name: "key_without_schema", key: valid},
		{name: "schema_without_key", schema: 2},
		{name: "unknown_schema", schema: 3},
		{name: "wrong_version", schema: 2, key: valid, mutate: func(key *WebAuthnCredential) { key.Version = 2 }},
		{name: "wrong_rp", schema: 2, key: valid, mutate: func(key *WebAuthnCredential) { key.RPID = "example.com" }},
		{name: "wrong_origin", schema: 2, key: valid, mutate: func(key *WebAuthnCredential) { key.Origin = "https://example.com" }},
		{name: "noncanonical_origin", schema: 2, key: valid, mutate: func(key *WebAuthnCredential) { key.Origin += "/" }},
		{name: "noncanonical_credential_id", schema: 2, key: valid, mutate: func(key *WebAuthnCredential) { key.CredentialID += " " }},
		{name: "noncanonical_user_handle", schema: 2, key: valid, mutate: func(key *WebAuthnCredential) { key.UserHandle += " " }},
		{name: "wrong_algorithm", schema: 2, key: valid, mutate: func(key *WebAuthnCredential) { key.Algorithm = -257 }},
		{name: "bad_user_handle", schema: 2, key: valid, mutate: func(key *WebAuthnCredential) { key.UserHandle = "not base64url" }},
		{name: "bad_private_key", schema: 2, key: valid, mutate: func(key *WebAuthnCredential) { key.PrivateKeyPKCS8 = "not-base64" }},
		{name: "missing_factor", schema: 2, key: valid, mutate: func(key *WebAuthnCredential) { key.MFAFactorID = "" }},
		{name: "bad_created_at", schema: 2, key: valid, mutate: func(key *WebAuthnCredential) { key.CreatedAt = "yesterday" }},
		{name: "bad_last_used_at", schema: 2, key: valid, mutate: func(key *WebAuthnCredential) { key.LastUsedAt = "tomorrow" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var key *WebAuthnCredential
			if test.key != nil {
				key = cloneWebAuthnCredential(test.key)
			}
			if test.mutate != nil {
				test.mutate(key)
			}
			payload, errMarshal := json.Marshal(map[string]any{
				"type":                      Provider,
				"credential_schema_version": test.schema,
				"email":                     "person@example.com",
				"access_token":              "access-token",
				"webauthn":                  key,
			})
			if errMarshal != nil {
				t.Fatal(errMarshal)
			}
			if _, errDecode := DecodeCredential(payload); errDecode == nil {
				t.Fatalf("DecodeCredential() error = %v", errDecode)
			}
		})
	}
}

func TestDecodeCredentialRejectsIncompleteWebAuthnV1(t *testing.T) {
	keyPayload, errMarshal := json.Marshal(testWebAuthnCredential(t))
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	var key map[string]any
	if errUnmarshal := json.Unmarshal(keyPayload, &key); errUnmarshal != nil {
		t.Fatal(errUnmarshal)
	}
	for _, missing := range []string{"sign_count", "backup_eligible", "backup_state", "last_used_at"} {
		t.Run(missing, func(t *testing.T) {
			incomplete := make(map[string]any, len(key))
			for field, value := range key {
				incomplete[field] = value
			}
			delete(incomplete, missing)
			payload, errEncode := json.Marshal(map[string]any{
				"type":                      Provider,
				"credential_schema_version": CredentialSchemaVersionWebAuthn,
				"email":                     "person@example.com",
				"access_token":              "access-token",
				"webauthn":                  incomplete,
			})
			if errEncode != nil {
				t.Fatal(errEncode)
			}
			if _, errDecode := DecodeCredential(payload); errDecode == nil {
				t.Fatalf("DecodeCredential() accepted WebAuthn without %s", missing)
			}
		})
	}
}

func TestCompareWebAuthnLastUsedAtUsesTimeSemantics(t *testing.T) {
	earlierWithOffset := "2026-08-05T10:00:00+08:00"
	laterUTC := "2026-08-05T03:00:00Z"
	if CompareWebAuthnLastUsedAt(laterUTC, earlierWithOffset) <= 0 {
		t.Fatal("later UTC timestamp was not ordered after an earlier offset timestamp")
	}
	if CompareWebAuthnLastUsedAt("", earlierWithOffset) >= 0 {
		t.Fatal("empty timestamp was not ordered before a valid timestamp")
	}
}

func testWebAuthnCredential(t *testing.T) *WebAuthnCredential {
	t.Helper()
	privateKey, errGenerate := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if errGenerate != nil {
		t.Fatal(errGenerate)
	}
	privateDER, errMarshal := x509.MarshalPKCS8PrivateKey(privateKey)
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	return &WebAuthnCredential{
		Version:         1,
		CredentialID:    base64.RawURLEncoding.EncodeToString([]byte("credential-id-1")),
		UserHandle:      base64.RawURLEncoding.EncodeToString([]byte("user-handle-1")),
		RPID:            WebAuthnRPID,
		Origin:          WebAuthnOrigin,
		Algorithm:       WebAuthnES256Algorithm,
		PrivateKeyPKCS8: base64.StdEncoding.EncodeToString(privateDER),
		MFAFactorID:     "factor-1",
		Transports:      []string{"internal"},
		UserPresent:     true,
		UserVerified:    true,
		CreatedAt:       "2026-08-05T00:00:00Z",
		LastUsedAt:      "2026-08-05T00:00:00Z",
	}
}
