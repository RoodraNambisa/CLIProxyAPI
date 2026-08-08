package chatgptweb

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
)

var (
	errWebAuthnRequestOptionsInvalid = errors.New("WebAuthn request options are invalid")
	errWebAuthnCredentialNotAllowed  = errors.New("WebAuthn request does not allow the persisted credential")
	errWebAuthnStatePersistence      = errors.New("WebAuthn state persistence failed")
)

type webAuthnRequestOptions struct {
	Challenge        string                         `json:"challenge"`
	RPID             string                         `json:"rpId"`
	AllowCredentials []webAuthnCredentialDescriptor `json:"allowCredentials"`
	UserVerification string                         `json:"userVerification"`
}

type webAuthnCredentialDescriptor struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

type webAuthnAssertionResponse struct {
	ID                      string                        `json:"id"`
	RawID                   string                        `json:"rawId"`
	Type                    string                        `json:"type"`
	AuthenticatorAttachment string                        `json:"authenticatorAttachment"`
	ClientExtensionResults  map[string]any                `json:"clientExtensionResults"`
	Response                webAuthnAuthenticatorResponse `json:"response"`
}

type webAuthnAuthenticatorResponse struct {
	AuthenticatorData string `json:"authenticatorData"`
	ClientDataJSON    string `json:"clientDataJSON"`
	Signature         string `json:"signature"`
	UserHandle        string `json:"userHandle,omitempty"`
}

func createWebAuthnAssertion(
	credential *WebAuthnCredential,
	rawOptions any,
	persist func(WebAuthnCredential) (WebAuthnCredential, error),
) (map[string]any, error) {
	if credential == nil {
		return nil, errors.New("WebAuthn credential is unavailable")
	}
	if errValidate := ValidateWebAuthnCredential(credential); errValidate != nil {
		return nil, errValidate
	}
	options, errOptions := decodeWebAuthnRequestOptions(rawOptions)
	if errOptions != nil {
		return nil, errOptions
	}
	rpID := strings.TrimSpace(options.RPID)
	if rpID == "" {
		rpID = credential.RPID
	}
	if (options.RPID != "" && rpID != options.RPID) || rpID != credential.RPID || rpID != WebAuthnRPID {
		return nil, fmt.Errorf("%w: unexpected relying party", errWebAuthnRequestOptionsInvalid)
	}
	if !webAuthnRequestAllowsCredential(options.AllowCredentials, credential.CredentialID) {
		return nil, errWebAuthnCredentialNotAllowed
	}
	privateDER, errDecode := base64.StdEncoding.DecodeString(strings.TrimSpace(credential.PrivateKeyPKCS8))
	if errDecode != nil {
		return nil, errors.New("WebAuthn private key is invalid")
	}
	parsed, errParse := x509.ParsePKCS8PrivateKey(privateDER)
	if errParse != nil {
		return nil, errors.New("WebAuthn private key is invalid")
	}
	privateKey, ok := parsed.(*ecdsa.PrivateKey)
	if !ok || privateKey.Curve != elliptic.P256() {
		return nil, errors.New("WebAuthn private key must be P-256")
	}
	if credential.SignCount == math.MaxUint32 {
		return nil, errors.New("WebAuthn signature counter is exhausted")
	}
	previousCount := credential.SignCount
	credential.SignCount++
	if persist == nil {
		credential.SignCount = previousCount
		return nil, errors.New("WebAuthn signature counter persistence is unavailable")
	}
	persisted, errPersist := persist(*cloneWebAuthnCredential(credential))
	if errPersist != nil {
		credential.SignCount = previousCount
		return nil, fmt.Errorf("%w: %w", errWebAuthnStatePersistence, errPersist)
	}
	if errValidate := ValidateWebAuthnCredential(&persisted); errValidate != nil ||
		persisted.SignCount != credential.SignCount || !WebAuthnAuthenticatorMatches(&persisted, credential) {
		return nil, errors.New("persisted WebAuthn signature counter did not match the credential")
	}
	*credential = *cloneWebAuthnCredential(&persisted)

	clientDataJSON, errMarshal := json.Marshal(map[string]any{
		"type":        "webauthn.get",
		"challenge":   options.Challenge,
		"origin":      credential.Origin,
		"crossOrigin": false,
	})
	if errMarshal != nil {
		return nil, fmt.Errorf("encode WebAuthn client data: %w", errMarshal)
	}
	authenticatorData := buildWebAuthnAuthenticatorData(credential.RPID, credential.SignCount)
	clientHash := sha256.Sum256(clientDataJSON)
	signedPayload := append(append([]byte(nil), authenticatorData...), clientHash[:]...)
	signedHash := sha256.Sum256(signedPayload)
	signature, errSign := ecdsa.SignASN1(rand.Reader, privateKey, signedHash[:])
	if errSign != nil {
		return nil, fmt.Errorf("sign WebAuthn assertion: %w", errSign)
	}
	// Browser AuthenticatorAssertionResponse serialization omits userHandle
	// when the relying party supplied an allow-list. A discoverable credential
	// request has no allowCredentials and must return the resident user handle.
	userHandle := ""
	if len(options.AllowCredentials) == 0 {
		userHandle = credential.UserHandle
	}
	assertion := webAuthnAssertionResponse{
		ID:                      credential.CredentialID,
		RawID:                   credential.CredentialID,
		Type:                    "public-key",
		AuthenticatorAttachment: "platform",
		ClientExtensionResults:  map[string]any{},
		Response: webAuthnAuthenticatorResponse{
			AuthenticatorData: base64.RawURLEncoding.EncodeToString(authenticatorData),
			ClientDataJSON:    base64.RawURLEncoding.EncodeToString(clientDataJSON),
			Signature:         base64.RawURLEncoding.EncodeToString(signature),
			UserHandle:        userHandle,
		},
	}
	rawAssertion, errEncode := json.Marshal(assertion)
	if errEncode != nil {
		return nil, fmt.Errorf("encode WebAuthn assertion: %w", errEncode)
	}
	var result map[string]any
	if errDecodeAssertion := json.Unmarshal(rawAssertion, &result); errDecodeAssertion != nil {
		return nil, fmt.Errorf("decode WebAuthn assertion: %w", errDecodeAssertion)
	}
	return result, nil
}

// WebAuthnAuthenticatorMatches compares immutable authenticator material while
// ignoring the monotonic counter and last-used timestamp.
func WebAuthnAuthenticatorMatches(left, right *WebAuthnCredential) bool {
	if left == nil || right == nil {
		return left == right
	}
	leftClone := cloneWebAuthnCredential(left)
	rightClone := cloneWebAuthnCredential(right)
	leftClone.SignCount = 0
	rightClone.SignCount = 0
	leftClone.LastUsedAt = ""
	rightClone.LastUsedAt = ""
	return reflect.DeepEqual(leftClone, rightClone)
}

func decodeWebAuthnRequestOptions(raw any) (webAuthnRequestOptions, error) {
	var options webAuthnRequestOptions
	if current, ok := raw.(map[string]any); ok {
		if nested, exists := current["passkey_request_options"]; exists {
			raw = nested
		} else if nested, exists := current["publicKey"]; exists {
			raw = nested
		}
	}
	encoded, errEncode := json.Marshal(raw)
	if errEncode != nil {
		return options, errWebAuthnRequestOptionsInvalid
	}
	if errDecode := json.Unmarshal(encoded, &options); errDecode != nil {
		return options, errWebAuthnRequestOptionsInvalid
	}
	if strings.TrimSpace(options.Challenge) != options.Challenge {
		return options, fmt.Errorf("%w: challenge is not canonical", errWebAuthnRequestOptionsInvalid)
	}
	if _, errChallenge := decodeWebAuthnBase64URL(options.Challenge); errChallenge != nil {
		return options, fmt.Errorf("%w: challenge is invalid", errWebAuthnRequestOptionsInvalid)
	}
	return options, nil
}

func webAuthnRequestAllowsCredential(descriptors []webAuthnCredentialDescriptor, credentialID string) bool {
	if len(descriptors) == 0 {
		return true
	}
	for _, descriptor := range descriptors {
		if descriptor.ID == credentialID && (strings.TrimSpace(descriptor.Type) == "" || descriptor.Type == "public-key") {
			return true
		}
	}
	return false
}

func buildWebAuthnAuthenticatorData(rpID string, signCount uint32) []byte {
	rpHash := sha256.Sum256([]byte(rpID))
	result := make([]byte, 37)
	copy(result, rpHash[:])
	result[32] = 0x05
	binary.BigEndian.PutUint32(result[33:], signCount)
	return result
}
