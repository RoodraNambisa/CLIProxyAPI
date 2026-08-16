package chatgptweb

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
)

func TestBrowserEnvironmentCatalogHasStablePerPersonaSlots(t *testing.T) {
	for _, catalog := range [][]personaCatalogEntry{personaCatalogV3, personaCatalogV2[:]} {
		for _, entry := range catalog {
			variantCount := browserEnvironmentVariantCount(entry.persona)
			seen := make(map[string]struct{}, variantCount)
			for slot := 0; slot < variantCount; slot++ {
				identity := browserEnvironmentIdentityForSlot(entry.persona, slot)
				gotSlot, ok := browserEnvironmentSlot(entry.persona, identity)
				if !ok || gotSlot != slot {
					t.Fatalf("identity %q resolved to %d/%v, want %d", identity.CatalogID, gotSlot, ok, slot)
				}
				seen[identity.CatalogID] = struct{}{}
			}
			if len(seen) != variantCount {
				t.Fatalf("persona %q has %d unique environments", entry.persona.CatalogID, len(seen))
			}
		}
	}
}

func TestBrowserEnvironmentCatalogAcceptsCanonicalThreeDigitSlots(t *testing.T) {
	persona := personaCatalogV3[0].persona
	identity := browserEnvironmentIdentityForSlot(persona, 255)
	if !strings.HasSuffix(identity.CatalogID, "-e255") {
		t.Fatalf("slot 255 identity = %q", identity.CatalogID)
	}
	if slot, ok := browserEnvironmentSlot(persona, identity); !ok || slot != 255 {
		t.Fatalf("slot 255 resolved to %d/%v", slot, ok)
	}
	for _, suffix := range []string{"e000", "e064", "e256", "e1000"} {
		candidate := BrowserEnvironmentIdentity{
			CatalogVersion: browserEnvironmentCatalogVersion,
			CatalogID:      persona.CatalogID + "-" + suffix,
		}
		if slot, ok := browserEnvironmentSlot(persona, candidate); ok {
			t.Fatalf("non-canonical identity %q resolved to slot %d", candidate.CatalogID, slot)
		}
	}
}

func TestBrowserEnvironmentCatalogPreservesV2SeedMapping(t *testing.T) {
	persona := personaCatalogV2[4].persona
	seed := "persisted-v2-device"
	digest := sha256.Sum256([]byte("chatgpt-web-browser-environment-" + browserEnvironmentCatalogVersion + "\x00" + seed))
	wantSlot := int(digest[len(digest)-1]) % browserEnvironmentV2Variants
	identity := browserEnvironmentIdentityForSeed(persona, seed)
	gotSlot, ok := browserEnvironmentSlot(persona, identity)
	if !ok || gotSlot != wantSlot {
		t.Fatalf("v2 seed mapped to %d/%v, want %d", gotSlot, ok, wantSlot)
	}
	if wrapped := browserEnvironmentIdentityForSlot(persona, wantSlot+browserEnvironmentV2Variants); wrapped != identity {
		t.Fatalf("v2 slot expansion changed identity: %#v != %#v", wrapped, identity)
	}
}

func TestCredentialBrowserEnvironmentSelectionIsStable(t *testing.T) {
	credential := &Credential{
		CredentialUID:  "credential-uid",
		DeviceID:       "stable-device",
		Email:          "first@example.test",
		AccessToken:    "first-access-token",
		Cookies:        []Cookie{{Name: "session", Value: "first-session-cookie"}},
		SourceProxyURL: "http://first-proxy.example.test:8080",
		Persona:        personaCatalogV3[3].persona,
	}
	first := ResolveCredentialBrowserEnvironment(credential, "auth-id")
	for index := 0; index < 10; index++ {
		credential.Email = fmt.Sprintf("changed-%d@example.test", index)
		credential.AccessToken = fmt.Sprintf("changed-access-%d", index)
		credential.Cookies = []Cookie{{Name: "session", Value: fmt.Sprintf("changed-session-%d", index)}}
		credential.SourceProxyURL = fmt.Sprintf("http://changed-proxy-%d.example.test:8080", index)
		got := ResolveCredentialBrowserEnvironment(credential, fmt.Sprintf("different-auth-%d", index))
		if got != first {
			t.Fatalf("environment changed: %#v != %#v", got, first)
		}
	}
	if _, ok := browserEnvironmentSlot(credential.Persona, first); !ok {
		t.Fatalf("environment %#v is invalid for persona %q", first, credential.Persona.CatalogID)
	}
}

func TestBrowserEnvironmentSelectionDistribution(t *testing.T) {
	const credentialCount = 10_000
	counts := make([]int, browserEnvironmentVariantsPerPersona)
	persona := personaCatalogV3[1].persona
	for index := 0; index < credentialCount; index++ {
		identity := browserEnvironmentIdentityForSeed(persona, fmt.Sprintf("device-%05d", index))
		slot, ok := browserEnvironmentSlot(persona, identity)
		if !ok {
			t.Fatalf("environment %q is invalid", identity.CatalogID)
		}
		counts[slot]++
	}

	for slot, count := range counts {
		if count < 15 || count > 70 {
			t.Fatalf("slot %d count = %d, distribution = %#v", slot, count, counts)
		}
	}
}

func TestCredentialBrowserEnvironmentSeedPriority(t *testing.T) {
	persona := personaCatalogV3[6].persona
	credential := &Credential{
		DeviceID:      "device-seed",
		CredentialUID: "credential-seed",
		Persona:       persona,
	}
	if got, want := ResolveCredentialBrowserEnvironment(credential, "auth-seed"), browserEnvironmentIdentityForSeed(persona, "device-seed"); got != want {
		t.Fatalf("device seed environment = %#v, want %#v", got, want)
	}

	credential.DeviceID = ""
	credential.BrowserEnvironment = nil
	if got, want := ResolveCredentialBrowserEnvironment(credential, "auth-seed"), browserEnvironmentIdentityForSeed(persona, "credential-seed"); got != want {
		t.Fatalf("credential seed environment = %#v, want %#v", got, want)
	}

	credential.CredentialUID = ""
	credential.BrowserEnvironment = nil
	if got, want := ResolveCredentialBrowserEnvironment(credential, "auth-seed"), browserEnvironmentIdentityForSeed(persona, "auth-seed"); got != want {
		t.Fatalf("auth seed environment = %#v, want %#v", got, want)
	}
}

func TestCredentialBrowserEnvironmentRejectsCrossPersonaIdentity(t *testing.T) {
	identity := browserEnvironmentIdentityForSlot(personaCatalogV3[0].persona, 7)
	credential := &Credential{
		DeviceID:           "stable-device",
		Persona:            personaCatalogV3[4].persona,
		BrowserEnvironment: &identity,
	}
	got := ResolveCredentialBrowserEnvironment(credential, "")
	if _, ok := browserEnvironmentSlot(credential.Persona, got); !ok {
		t.Fatalf("environment %#v was not repaired for persona %q", got, credential.Persona.CatalogID)
	}
	if got == identity {
		t.Fatal("cross-persona environment was retained")
	}
}

func TestCredentialBrowserEnvironmentMetadataRoundTrip(t *testing.T) {
	credential := &Credential{
		Type:          Provider,
		CredentialUID: "credential-uid",
		DeviceID:      "stable-device",
		Persona:       personaCatalogV3[2].persona,
	}
	want := ResolveCredentialBrowserEnvironment(credential, "auth-id")
	metadata := map[string]any{}
	credential.ApplyToMetadata(metadata)
	decoded, err := ParseCredential(metadata)
	if err != nil {
		t.Fatalf("ParseCredential() error = %v", err)
	}
	if decoded.BrowserEnvironment == nil || *decoded.BrowserEnvironment != want {
		t.Fatalf("browser environment = %#v, want %#v", decoded.BrowserEnvironment, want)
	}
}
