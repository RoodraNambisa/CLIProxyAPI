package chatgptweb

import (
	"crypto/sha256"
	"fmt"
	"strconv"
	"strings"
)

const (
	browserEnvironmentCatalogVersion     = "v3"
	browserEnvironmentV2Variants         = 32
	browserEnvironmentVariantsPerPersona = 64
)

// BrowserEnvironmentIdentity selects the stable application-layer browser
// environment used by Sentinel without changing the credential's transport
// Persona.
type BrowserEnvironmentIdentity struct {
	CatalogVersion string `json:"catalog_version"`
	CatalogID      string `json:"catalog_id"`
}

func browserEnvironmentIdentityForSlot(persona Persona, slot int) BrowserEnvironmentIdentity {
	persona = canonicalPersona(persona)
	variantCount := browserEnvironmentVariantCount(persona)
	slot %= variantCount
	if slot < 0 {
		slot += variantCount
	}
	return BrowserEnvironmentIdentity{
		CatalogVersion: browserEnvironmentCatalogVersion,
		CatalogID:      fmt.Sprintf("%s-e%02d", persona.CatalogID, slot),
	}
}

func browserEnvironmentSlot(persona Persona, identity BrowserEnvironmentIdentity) (int, bool) {
	persona = canonicalPersona(persona)
	if identity.CatalogVersion != browserEnvironmentCatalogVersion {
		return 0, false
	}
	prefix := persona.CatalogID + "-e"
	if !strings.HasPrefix(identity.CatalogID, prefix) {
		return 0, false
	}
	suffix := strings.TrimPrefix(identity.CatalogID, prefix)
	if len(suffix) != 2 {
		return 0, false
	}
	slot, err := strconv.Atoi(suffix)
	if err != nil || slot < 0 || slot >= browserEnvironmentVariantCount(persona) {
		return 0, false
	}
	return slot, true
}

func browserEnvironmentIdentityForSeed(persona Persona, seed string) BrowserEnvironmentIdentity {
	seed = strings.TrimSpace(seed)
	if seed == "" {
		return BrowserEnvironmentIdentity{}
	}
	digest := sha256.Sum256([]byte("chatgpt-web-browser-environment-" + browserEnvironmentCatalogVersion + "\x00" + seed))
	return browserEnvironmentIdentityForSlot(persona, int(digest[len(digest)-1])%browserEnvironmentVariantCount(persona))
}

func browserEnvironmentVariantCount(persona Persona) int {
	persona = canonicalPersona(persona)
	if strings.EqualFold(persona.CatalogVersion, personaCatalogV2Version) {
		return browserEnvironmentV2Variants
	}
	return browserEnvironmentVariantsPerPersona
}

func resolveCredentialBrowserEnvironment(credential *Credential, fallbackSeed string) {
	if credential == nil {
		return
	}
	if credential.BrowserEnvironment != nil {
		if _, ok := browserEnvironmentSlot(credential.Persona, *credential.BrowserEnvironment); ok {
			return
		}
	}
	seed := strings.TrimSpace(credential.DeviceID)
	if seed == "" {
		seed = strings.TrimSpace(credential.CredentialUID)
	}
	if seed == "" {
		seed = strings.TrimSpace(fallbackSeed)
	}
	identity := browserEnvironmentIdentityForSeed(credential.Persona, seed)
	if identity.CatalogID == "" {
		credential.BrowserEnvironment = nil
		return
	}
	credential.BrowserEnvironment = &identity
}

// ResolveCredentialBrowserEnvironment resolves and stores the credential's
// stable Sentinel browser environment identity.
func ResolveCredentialBrowserEnvironment(credential *Credential, fallbackSeed string) BrowserEnvironmentIdentity {
	resolveCredentialBrowserEnvironment(credential, fallbackSeed)
	if credential == nil || credential.BrowserEnvironment == nil {
		return BrowserEnvironmentIdentity{}
	}
	return *credential.BrowserEnvironment
}
