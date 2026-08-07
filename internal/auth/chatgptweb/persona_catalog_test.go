package chatgptweb

import (
	"fmt"
	"strings"
	"testing"
)

func TestPersonaCatalogV2IsCoherent(t *testing.T) {
	if len(personaCatalogV2) != 8 {
		t.Fatalf("catalog entries = %d, want 8", len(personaCatalogV2))
	}
	seen := make(map[string]struct{}, len(personaCatalogV2))
	for index := range personaCatalogV2 {
		entry := personaCatalogV2[index]
		persona := entry.persona
		if persona.CatalogVersion != personaCatalogVersion || persona.CatalogID == "" {
			t.Fatalf("entry %d identity = %q/%q", index, persona.CatalogVersion, persona.CatalogID)
		}
		if _, exists := seen[persona.CatalogID]; exists {
			t.Fatalf("duplicate catalog ID %q", persona.CatalogID)
		}
		seen[persona.CatalogID] = struct{}{}
		if persona.Profile != "chrome_146" || chromeMajor(persona.UserAgent) != "146" {
			t.Fatalf("entry %q transport/UA = %q/%q", persona.CatalogID, persona.Profile, persona.UserAgent)
		}
		switch entry.platform {
		case sentinelBrowserPlatformMac:
			if persona.Platform != "MacIntel" || !strings.Contains(persona.UserAgent, "Macintosh") ||
				!strings.Contains(entry.webGLRenderer, "Apple") {
				t.Fatalf("entry %q has incoherent macOS fields", persona.CatalogID)
			}
		case sentinelBrowserPlatformWindows:
			if persona.Platform != "Win32" || !strings.Contains(persona.UserAgent, "Windows NT") ||
				!strings.Contains(entry.webGLRenderer, "Direct3D11") {
				t.Fatalf("entry %q has incoherent Windows fields", persona.CatalogID)
			}
		default:
			t.Fatalf("entry %q has unsupported platform %d", persona.CatalogID, entry.platform)
		}
		if entry.deviceMemory > 8 || entry.devicePixelRatio <= 0 || entry.colorDepth <= 0 {
			t.Fatalf("entry %q has invalid browser capacity", persona.CatalogID)
		}
	}
}

func TestCredentialPersonaSelectionIsStable(t *testing.T) {
	credential := &Credential{
		CredentialUID: "credential-uid",
		DeviceID:      "11111111-2222-4333-8444-555555555555",
		Persona: Persona{
			Profile:   "chrome_144_psk",
			UserAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/144.0.0.0",
			Platform:  "MacIntel",
		},
	}
	resolveCredentialPersona(credential, "auth-id")
	first := credential.Persona
	for iteration := 0; iteration < 10; iteration++ {
		resolveCredentialPersona(credential, "different-auth-id")
		if credential.Persona != first {
			t.Fatalf("persona changed: %#v != %#v", credential.Persona, first)
		}
	}
	if first.Profile != "chrome_146" || first.CatalogVersion != personaCatalogVersion {
		t.Fatalf("legacy persona was not migrated: %#v", first)
	}
}

func TestPersonaCatalogDistribution(t *testing.T) {
	counts := make([]int, len(personaCatalogV2))
	for index := 0; index < 10_000; index++ {
		entry := personaCatalogEntryForSeed(fmt.Sprintf("device-%d", index))
		for catalogIndex := range personaCatalogV2 {
			if personaCatalogV2[catalogIndex].persona.CatalogID == entry.persona.CatalogID {
				counts[catalogIndex]++
				break
			}
		}
	}
	for index, count := range counts {
		if count < 1100 || count > 1400 {
			t.Fatalf("catalog entry %d count = %d", index, count)
		}
	}
}

func TestClientRejectsArbitraryPersonaByCanonicalizing(t *testing.T) {
	client, err := NewClient(Persona{
		Profile:   "firefox_120",
		UserAgent: "custom",
		Platform:  "Linux x86_64",
	}, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	persona := client.Persona()
	if persona.Profile != "chrome_146" || persona.CatalogVersion != personaCatalogVersion || persona.CatalogID == "" {
		t.Fatalf("client persona = %#v", persona)
	}
}
