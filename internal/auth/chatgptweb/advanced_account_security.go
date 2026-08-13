package chatgptweb

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	CredentialSchemaVersionAdvancedAccountSecurity = 3
	AdvancedAccountSecurityCredentialVersion       = 1
	AdvancedAccountSecurityFeature                 = "advanced_account_security_v1"
	advancedAccountRecoveryKeyCount                = 5
)

// AdvancedAccountRecoveryKey is sensitive one-time recovery material. It may
// be persisted and exported with the credential, but must never be consumed by
// automatic login or exposed by status APIs.
type AdvancedAccountRecoveryKey struct {
	RecoveryKey                string `json:"recovery_key"`
	AccountRecoveryCode        string `json:"account_recovery_code"`
	XWingPublicKeyBase64       string `json:"xwing_public_key_base64"`
	AuthenticationSecretBase64 string `json:"authentication_secret_base64"`
}

type AdvancedAccountSecurityCredential struct {
	Version      int                                        `json:"version"`
	Enabled      bool                                       `json:"enabled"`
	Passkeys     []AdvancedAccountSecurityPasskeyCredential `json:"passkeys"`
	RecoveryKeys []AdvancedAccountRecoveryKey               `json:"recovery_keys"`
	EnrolledAt   string                                     `json:"enrolled_at"`
	VerifiedAt   string                                     `json:"verified_at"`
	LoginMethod  string                                     `json:"login_method"`
}

type AdvancedAccountSecurityPasskeyCredential struct {
	Kind             string             `json:"kind"`
	IsNonDeviceBound bool               `json:"is_non_device_bound"`
	IsSecurityKey    bool               `json:"is_security_key"`
	Credential       WebAuthnCredential `json:"credential"`
}

func ValidateAdvancedAccountSecurityCredential(credential *AdvancedAccountSecurityCredential) error {
	if credential == nil {
		return errors.New("chatgpt web advanced account security credential is nil")
	}
	if credential.Version != AdvancedAccountSecurityCredentialVersion || !credential.Enabled {
		return errors.New("chatgpt web advanced account security version or enabled state is invalid")
	}
	if strings.TrimSpace(credential.LoginMethod) != "passkey" {
		return errors.New("chatgpt web advanced account security login_method is invalid")
	}
	enrolledAt, errEnrolled := time.Parse(time.RFC3339Nano, strings.TrimSpace(credential.EnrolledAt))
	if errEnrolled != nil {
		return errors.New("chatgpt web advanced account security enrolled_at is invalid")
	}
	verifiedAt, errVerified := time.Parse(time.RFC3339Nano, strings.TrimSpace(credential.VerifiedAt))
	if errVerified != nil || verifiedAt.Before(enrolledAt) {
		return errors.New("chatgpt web advanced account security verified_at is invalid")
	}
	if len(credential.Passkeys) != 2 {
		return fmt.Errorf("chatgpt web advanced account security requires 2 passkeys, got %d", len(credential.Passkeys))
	}
	if len(credential.RecoveryKeys) != advancedAccountRecoveryKeyCount {
		return fmt.Errorf("chatgpt web advanced account security requires %d recovery keys", advancedAccountRecoveryKeyCount)
	}

	seenKinds := make(map[string]struct{}, len(credential.Passkeys))
	seenCredentialIDs := make(map[string]struct{}, len(credential.Passkeys))
	seenFactorIDs := make(map[string]struct{}, len(credential.Passkeys))
	seenPrivateKeys := make(map[string]struct{}, len(credential.Passkeys))
	for index := range credential.Passkeys {
		passkey := &credential.Passkeys[index]
		kind := strings.ToLower(strings.TrimSpace(passkey.Kind))
		if _, duplicate := seenKinds[kind]; duplicate {
			return errors.New("chatgpt web advanced account security passkey kind is duplicated")
		}
		seenKinds[kind] = struct{}{}
		if errValidate := ValidateWebAuthnCredential(&passkey.Credential); errValidate != nil {
			return fmt.Errorf("chatgpt web advanced account security %s credential is invalid: %w", kind, errValidate)
		}
		if passkey.Credential.SignCount == 0 || strings.TrimSpace(passkey.Credential.LastUsedAt) == "" {
			return fmt.Errorf("chatgpt web advanced account security %s credential has not completed validation", kind)
		}
		createdAt, _ := time.Parse(time.RFC3339Nano, passkey.Credential.CreatedAt)
		lastUsedAt, _ := time.Parse(time.RFC3339Nano, passkey.Credential.LastUsedAt)
		if lastUsedAt.Before(createdAt) {
			return fmt.Errorf("chatgpt web advanced account security %s last_used_at is invalid", kind)
		}
		if _, duplicate := seenCredentialIDs[passkey.Credential.CredentialID]; duplicate {
			return errors.New("chatgpt web advanced account security credential_id is duplicated")
		}
		seenCredentialIDs[passkey.Credential.CredentialID] = struct{}{}
		if _, duplicate := seenFactorIDs[passkey.Credential.MFAFactorID]; duplicate {
			return errors.New("chatgpt web advanced account security mfa_factor_id is duplicated")
		}
		seenFactorIDs[passkey.Credential.MFAFactorID] = struct{}{}
		if _, duplicate := seenPrivateKeys[passkey.Credential.PrivateKeyPKCS8]; duplicate {
			return errors.New("chatgpt web advanced account security private key is duplicated")
		}
		seenPrivateKeys[passkey.Credential.PrivateKeyPKCS8] = struct{}{}

		switch kind {
		case "passkey":
			if !passkey.IsNonDeviceBound || passkey.IsSecurityKey ||
				!passkey.Credential.BackupEligible || !passkey.Credential.BackupState ||
				!containsFold(passkey.Credential.Transports, "hybrid") {
				return errors.New("chatgpt web advanced account security passkey classification is inconsistent")
			}
		case "security-key":
			if !passkey.IsSecurityKey || passkey.Credential.BackupEligible || passkey.Credential.BackupState ||
				!containsFold(passkey.Credential.Transports, "usb") {
				return errors.New("chatgpt web advanced account security security-key classification is inconsistent")
			}
		default:
			return fmt.Errorf("chatgpt web advanced account security passkey kind %q is unsupported", kind)
		}
	}
	if _, ok := seenKinds["passkey"]; !ok {
		return errors.New("chatgpt web advanced account security passkey is missing")
	}
	if _, ok := seenKinds["security-key"]; !ok {
		return errors.New("chatgpt web advanced account security security-key is missing")
	}
	return validateAdvancedAccountRecoveryKeys(credential.RecoveryKeys)
}

func validateAdvancedAccountRecoveryKeys(recoveryKeys []AdvancedAccountRecoveryKey) error {
	seenKeys := make(map[string]struct{}, len(recoveryKeys))
	seenCodes := make(map[string]struct{}, len(recoveryKeys))
	for _, recovery := range recoveryKeys {
		displayKey := strings.TrimSpace(recovery.RecoveryKey)
		accountCode := strings.TrimSpace(recovery.AccountRecoveryCode)
		if displayKey == "" || accountCode == "" || strings.TrimSpace(recovery.XWingPublicKeyBase64) == "" ||
			strings.TrimSpace(recovery.AuthenticationSecretBase64) == "" {
			return errors.New("chatgpt web advanced account security recovery material is incomplete")
		}
		if _, errDecode := decodeFlexibleBase64(recovery.XWingPublicKeyBase64); errDecode != nil {
			return errors.New("chatgpt web advanced account security X-Wing public key is invalid")
		}
		if _, errDecode := decodeFlexibleBase64(recovery.AuthenticationSecretBase64); errDecode != nil {
			return errors.New("chatgpt web advanced account security authentication secret is invalid")
		}
		if _, duplicate := seenKeys[displayKey]; duplicate {
			return errors.New("chatgpt web advanced account security recovery key is duplicated")
		}
		seenKeys[displayKey] = struct{}{}
		if _, duplicate := seenCodes[accountCode]; duplicate {
			return errors.New("chatgpt web advanced account security account recovery code is duplicated")
		}
		seenCodes[accountCode] = struct{}{}
	}
	return nil
}

func CloneAdvancedAccountSecurityCredential(source *AdvancedAccountSecurityCredential) *AdvancedAccountSecurityCredential {
	if source == nil {
		return nil
	}
	clone := *source
	clone.Passkeys = make([]AdvancedAccountSecurityPasskeyCredential, len(source.Passkeys))
	for index := range source.Passkeys {
		clone.Passkeys[index] = source.Passkeys[index]
		clone.Passkeys[index].Credential = *cloneWebAuthnCredential(&source.Passkeys[index].Credential)
	}
	clone.RecoveryKeys = append([]AdvancedAccountRecoveryKey(nil), source.RecoveryKeys...)
	return &clone
}

// MergeAdvancedAccountSecurityRuntimeState preserves monotonic authenticator
// state while leaving all imported private and recovery material unchanged.
func MergeAdvancedAccountSecurityRuntimeState(imported, current *AdvancedAccountSecurityCredential) {
	if imported == nil || current == nil {
		return
	}
	for importedIndex := range imported.Passkeys {
		for currentIndex := range current.Passkeys {
			importedKey := &imported.Passkeys[importedIndex].Credential
			currentKey := &current.Passkeys[currentIndex].Credential
			if !WebAuthnAuthenticatorMatches(importedKey, currentKey) {
				continue
			}
			if currentKey.SignCount > importedKey.SignCount {
				importedKey.SignCount = currentKey.SignCount
			}
			if CompareWebAuthnLastUsedAt(currentKey.LastUsedAt, importedKey.LastUsedAt) > 0 {
				importedKey.LastUsedAt = currentKey.LastUsedAt
			}
			break
		}
	}
}

func containsFold(values []string, target string) bool {
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), target) {
			return true
		}
	}
	return false
}

func decodeFlexibleBase64(value string) ([]byte, error) {
	value = strings.TrimSpace(value)
	for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if decoded, errDecode := encoding.DecodeString(value); errDecode == nil && len(decoded) > 0 {
			return decoded, nil
		}
	}
	return nil, errors.New("invalid base64 value")
}
