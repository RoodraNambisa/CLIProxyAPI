package chatgptweb

import (
	"fmt"
	"testing"
)

func TestBrowserEnvironmentCatalogHasStablePerPersonaSlots(t *testing.T) {
	for _, entry := range personaCatalogV2 {
		seen := make(map[string]struct{}, browserEnvironmentVariantsPerPersona)
		for slot := 0; slot < browserEnvironmentVariantsPerPersona; slot++ {
			identity := browserEnvironmentIdentityForSlot(entry.persona, slot)
			gotSlot, ok := browserEnvironmentSlot(entry.persona, identity)
			if !ok || gotSlot != slot {
				t.Fatalf("identity %q resolved to %d/%v, want %d", identity.CatalogID, gotSlot, ok, slot)
			}
			seen[identity.CatalogID] = struct{}{}
		}
		if len(seen) != browserEnvironmentVariantsPerPersona {
			t.Fatalf("persona %q has %d unique environments", entry.persona.CatalogID, len(seen))
		}
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
		Persona:        personaCatalogV2[3].persona,
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
	persona := personaCatalogV2[1].persona
	for index := 0; index < credentialCount; index++ {
		identity := browserEnvironmentIdentityForSeed(persona, fmt.Sprintf("device-%05d", index))
		slot, ok := browserEnvironmentSlot(persona, identity)
		if !ok {
			t.Fatalf("environment %q is invalid", identity.CatalogID)
		}
		counts[slot]++
	}

	for slot, count := range counts {
		if count < 240 || count > 390 {
			t.Fatalf("slot %d count = %d, distribution = %#v", slot, count, counts)
		}
	}
}

func TestCredentialBrowserEnvironmentSeedPriority(t *testing.T) {
	persona := personaCatalogV2[6].persona
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
	identity := browserEnvironmentIdentityForSlot(personaCatalogV2[0].persona, 7)
	credential := &Credential{
		DeviceID:           "stable-device",
		Persona:            personaCatalogV2[4].persona,
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
		Persona:       personaCatalogV2[2].persona,
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
