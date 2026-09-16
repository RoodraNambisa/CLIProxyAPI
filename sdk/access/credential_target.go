package access

import (
	"context"
	"fmt"
	"strings"
)

// CredentialTargetAuthID reads the trusted selection installed by authentication.
// Incoming headers and request bodies cannot set this value.
func CredentialTargetAuthID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if carrier, ok := ctx.Value("gin").(interface{ GetString(string) string }); ok {
		return carrier.GetString(CredentialTargetAuthIDContextKey)
	}
	return ""
}

const (
	CredentialTargetSuffix   = "-auth-"
	MetadataCredentialTarget = "credential_target"
	// CredentialTargetAuthIDContextKey is set only after authenticated resolution.
	CredentialTargetAuthIDContextKey = "clientCredentialTargetAuthID"
)

// NormalizeCredentialTarget accepts IDs and aliases that remain safe in API keys.
func NormalizeCredentialTarget(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if len(value) == 0 || len(value) > 64 {
		return "", fmt.Errorf("credential target must contain 1 to 64 ASCII letters, digits, hyphens or underscores")
	}
	for _, ch := range value {
		if ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '_' {
			continue
		}
		return "", fmt.Errorf("credential target must contain 1 to 64 ASCII letters, digits, hyphens or underscores")
	}
	return value, nil
}
