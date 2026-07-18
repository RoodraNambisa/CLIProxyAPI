package chatgptweb

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// NormalizeEmail returns the canonical identity used for ChatGPT Web accounts.
func NormalizeEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}

// CredentialFileName returns the stable storage key for a ChatGPT Web account.
func CredentialFileName(email string) string {
	sum := sha256.Sum256([]byte(NormalizeEmail(email)))
	return "chatgpt-web-" + hex.EncodeToString(sum[:8]) + ".json"
}
