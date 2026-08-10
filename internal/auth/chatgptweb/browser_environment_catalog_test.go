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
		CredentialUID: "credential-uid",
		DeviceID:      "stable-device",
		Persona:       personaCatalogV2[3].persona,
	}
	first := ResolveCredentialBrowserEnvironment(credential, "auth-id")
	for index := 0; index < 10; index++ {
		got := ResolveCredentialBrowserEnvironment(credential, fmt.Sprintf("different-auth-%d", index))
		if got != first {
			t.Fatalf("environment changed: %#v != %#v", got, first)
		}
	}
	if _, ok := browserEnvironmentSlot(credential.Persona, first); !ok {
		t.Fatalf("environment %#v is invalid for persona %q", first, credential.Persona.CatalogID)
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
