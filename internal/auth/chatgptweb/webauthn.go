package chatgptweb

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

const (
	CredentialSchemaVersionWebAuthn = 2
	WebAuthnCredentialVersion       = 1
	WebAuthnES256Algorithm          = -7
	WebAuthnRPID                    = "openai.com"
	WebAuthnOrigin                  = "https://auth.openai.com"
)

// WebAuthnCredential is the persisted software authenticator produced by
// go_registrar2.0. PrivateKeyPKCS8 is sensitive and must never be exposed by
// status, task, or diagnostic APIs.
type WebAuthnCredential struct {
	Version         int      `json:"version"`
	CredentialID    string   `json:"credential_id"`
	UserHandle      string   `json:"user_handle"`
	RPID            string   `json:"rp_id"`
	Origin          string   `json:"origin"`
	Algorithm       int      `json:"algorithm"`
	PrivateKeyPKCS8 string   `json:"private_key_pkcs8"`
	SignCount       uint32   `json:"sign_count"`
	MFAFactorID     string   `json:"mfa_factor_id,omitempty"`
	Transports      []string `json:"transports"`
	UserPresent     bool     `json:"user_present"`
	UserVerified    bool     `json:"user_verified"`
	BackupEligible  bool     `json:"backup_eligible"`
	BackupState     bool     `json:"backup_state"`
	CreatedAt       string   `json:"created_at"`
	LastUsedAt      string   `json:"last_used_at"`
}

// ValidateCredentialWebAuthn validates the versioned persisted credential
// without normalizing malformed input into a legacy password credential.
func ValidateCredentialWebAuthn(credential *Credential) error {
	if credential == nil {
		return errors.New("chatgpt web credential is nil")
	}
	schemaVersion := credential.CredentialSchemaVersion
	switch schemaVersion {
	case 0, 1:
		if credential.WebAuthn != nil {
			return errors.New("chatgpt web WebAuthn credential requires credential_schema_version 2")
		}
		return nil
	case CredentialSchemaVersionWebAuthn:
		if credential.WebAuthn == nil {
			return errors.New("chatgpt web credential schema 2 requires webauthn")
		}
	default:
		return fmt.Errorf("unsupported chatgpt web credential schema version %d", schemaVersion)
	}
	return ValidateWebAuthnCredential(credential.WebAuthn)
}

// ValidateWebAuthnCredential verifies the software key and the fixed OpenAI
// relying-party contract used by WebAuthn v1.
func ValidateWebAuthnCredential(credential *WebAuthnCredential) error {
	if credential == nil {
		return errors.New("chatgpt web WebAuthn credential is nil")
	}
	if credential.Version != WebAuthnCredentialVersion {
		return fmt.Errorf("unsupported chatgpt web WebAuthn version %d", credential.Version)
	}
	credentialID := strings.TrimSpace(credential.CredentialID)
	if credentialID == "" {
		return errors.New("chatgpt web WebAuthn credential_id is required")
	}
	if credentialID != credential.CredentialID {
		return errors.New("chatgpt web WebAuthn credential_id is invalid")
	}
	if _, err := decodeWebAuthnBase64URL(credentialID); err != nil {
		return fmt.Errorf("chatgpt web WebAuthn credential_id is invalid: %w", err)
	}
	userHandle := strings.TrimSpace(credential.UserHandle)
	if userHandle == "" {
		return errors.New("chatgpt web WebAuthn user_handle is required")
	}
	if userHandle != credential.UserHandle {
		return errors.New("chatgpt web WebAuthn user_handle is invalid")
	}
	if _, err := decodeWebAuthnBase64URL(userHandle); err != nil {
		return fmt.Errorf("chatgpt web WebAuthn user_handle is invalid: %w", err)
	}
	factorID := strings.TrimSpace(credential.MFAFactorID)
	if factorID == "" {
		return errors.New("chatgpt web WebAuthn mfa_factor_id is required")
	}
	if factorID != credential.MFAFactorID {
		return errors.New("chatgpt web WebAuthn mfa_factor_id is invalid")
	}
	if credential.RPID != WebAuthnRPID {
		return errors.New("chatgpt web WebAuthn rp_id is invalid")
	}
	if credential.Origin != WebAuthnOrigin {
		return errors.New("chatgpt web WebAuthn origin is invalid")
	}
	if credential.Algorithm != WebAuthnES256Algorithm {
		return errors.New("chatgpt web WebAuthn algorithm must be ES256")
	}
	if !credential.UserPresent || !credential.UserVerified {
		return errors.New("chatgpt web WebAuthn credential must require user presence and verification")
	}
	if credential.BackupState && !credential.BackupEligible {
		return errors.New("chatgpt web WebAuthn backup state is inconsistent")
	}
	if strings.TrimSpace(credential.CreatedAt) == "" {
		return errors.New("chatgpt web WebAuthn created_at is required")
	}
	if len(credential.Transports) == 0 || len(credential.Transports) > 8 {
		return errors.New("chatgpt web WebAuthn transports are invalid")
	}
	for _, transport := range credential.Transports {
		if strings.TrimSpace(transport) == "" {
			return errors.New("chatgpt web WebAuthn transport is empty")
		}
	}
	privateDER, errDecode := base64.StdEncoding.DecodeString(strings.TrimSpace(credential.PrivateKeyPKCS8))
	if errDecode != nil {
		return errors.New("chatgpt web WebAuthn private key is invalid")
	}
	parsed, errParse := x509.ParsePKCS8PrivateKey(privateDER)
	if errParse != nil {
		return errors.New("chatgpt web WebAuthn private key is invalid")
	}
	privateKey, ok := parsed.(*ecdsa.PrivateKey)
	if !ok || privateKey.Curve != elliptic.P256() {
		return errors.New("chatgpt web WebAuthn private key must be P-256")
	}
	return nil
}

func cloneWebAuthnCredential(source *WebAuthnCredential) *WebAuthnCredential {
	if source == nil {
		return nil
	}
	clone := *source
	clone.Transports = append([]string(nil), source.Transports...)
	return &clone
}

func decodeWebAuthnBase64URL(value string) ([]byte, error) {
	value = strings.TrimSpace(value)
	if decoded, err := base64.RawURLEncoding.DecodeString(value); err == nil && len(decoded) > 0 {
		return decoded, nil
	}
	decoded, err := base64.URLEncoding.DecodeString(value)
	if err != nil || len(decoded) == 0 {
		return nil, errors.New("invalid base64url value")
	}
	return decoded, nil
}
