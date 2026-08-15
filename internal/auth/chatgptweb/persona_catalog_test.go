package chatgptweb

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	fhttp "github.com/bogdanfinn/fhttp"
)

func TestPersonaCatalogV2IsCoherent(t *testing.T) {
	if len(personaCatalogV2) != 8 {
		t.Fatalf("catalog entries = %d, want 8", len(personaCatalogV2))
	}
	seen := make(map[string]struct{}, len(personaCatalogV2))
	for index := range personaCatalogV2 {
		entry := personaCatalogV2[index]
		persona := entry.persona
		if persona.CatalogVersion != personaCatalogV2Version || persona.CatalogID == "" {
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

func TestPersonaCatalogV3LocalePackMatchesHeadersAndNavigator(t *testing.T) {
	seen := make(map[string]struct{})
	for _, entry := range personaCatalogV3 {
		persona := entry.persona
		if _, exists := seen[persona.Language]; exists {
			continue
		}
		seen[persona.Language] = struct{}{}
		client, errClient := NewClient(persona, "", nil)
		if errClient != nil {
			t.Fatalf("NewClient(%q) error = %v", persona.Language, errClient)
		}
		request, errRequest := fhttp.NewRequest("GET", "https://chatgpt.com/", nil)
		if errRequest != nil {
			client.CloseIdleConnections()
			t.Fatalf("NewRequest(%q) error = %v", persona.Language, errRequest)
		}
		client.applyHeaders(request, nil)
		headerValue := func(name string) string {
			values := request.Header[name]
			if len(values) == 0 {
				return ""
			}
			return values[0]
		}
		if headerValue("accept-language") != persona.AcceptLanguage ||
			headerValue("user-agent") != persona.UserAgent ||
			!strings.Contains(headerValue("sec-ch-ua"), `v="146"`) {
			client.CloseIdleConnections()
			t.Fatalf("locale %q headers = %#v", persona.Language, request.Header)
		}
		client.CloseIdleConnections()

		values, _, _ := normalizeConversationTurnstileEnvironment(
			ConversationTurnstileEnvironment{Persona: persona},
			testTime(),
		)
		languages, ok := values["window.navigator.languages"].(*conversationTurnstileArray)
		wantLanguages := personaNavigatorLanguages(persona)
		wantValues := make([]any, len(wantLanguages))
		for index := range wantLanguages {
			wantValues[index] = wantLanguages[index]
		}
		if values["window.navigator.language"] != persona.Language || !ok ||
			!reflect.DeepEqual(languages.items, wantValues) {
			t.Fatalf("locale %q navigator = language:%#v languages:%#v", persona.Language,
				values["window.navigator.language"], languages)
		}
	}
	if len(seen) != 1 {
		t.Fatalf("tested locale packs = %d, want 1", len(seen))
	}
}

func TestPersonaCatalogV3HasCoherentChrome146Personas(t *testing.T) {
	if len(personaCatalogV3) != 8 {
		t.Fatalf("catalog entries = %d, want 8", len(personaCatalogV3))
	}
	seen := make(map[string]struct{}, len(personaCatalogV3))
	languageCounts := make(map[string]int)
	wantLanguages := map[string]struct {
		acceptLanguage string
		navigator      []string
	}{
		"en-US": {acceptLanguage: "en-US,en;q=0.9", navigator: []string{"en-US", "en"}},
	}
	for index, entry := range personaCatalogV3 {
		persona := entry.persona
		if persona.CatalogVersion != personaCatalogVersion || persona.CatalogID == "" {
			t.Fatalf("entry %d identity = %q/%q", index, persona.CatalogVersion, persona.CatalogID)
		}
		if _, exists := seen[persona.CatalogID]; exists {
			t.Fatalf("duplicate catalog ID %q", persona.CatalogID)
		}
		seen[persona.CatalogID] = struct{}{}
		languageCounts[persona.Language]++
		locale, supportedLocale := wantLanguages[persona.Language]
		if persona.Profile != "chrome_146" || chromeMajor(persona.UserAgent) != "146" {
			t.Fatalf("entry %q transport/UA = %q/%q", persona.CatalogID, persona.Profile, persona.UserAgent)
		}
		if !supportedLocale || persona.AcceptLanguage != locale.acceptLanguage ||
			!slices.Equal(personaNavigatorLanguages(persona), locale.navigator) {
			t.Fatalf("entry %q locale = %q/%q/%v", persona.CatalogID, persona.Language,
				persona.AcceptLanguage, personaNavigatorLanguages(persona))
		}
		if _, ok := findTLSProfile(persona.Profile); !ok {
			t.Fatalf("entry %q has unsupported TLS profile %q", persona.CatalogID, persona.Profile)
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
	}
	for language := range wantLanguages {
		if languageCounts[language] != len(personaCatalogV2) {
			t.Fatalf("language %q entries = %d, want %d", language, languageCounts[language], len(personaCatalogV2))
		}
	}
}

func TestPersonaCatalogV2UsesCoherentCPUAndDisplayProfiles(t *testing.T) {
	tests := map[string]struct {
		width               int
		height              int
		hardwareConcurrency int
		devicePixelRatio    float64
	}{
		"c146-mac-m2-1470":      {width: 1470, height: 956, hardwareConcurrency: 8, devicePixelRatio: 2},
		"c146-mac-m4p-1512":     {width: 1512, height: 982, hardwareConcurrency: 14, devicePixelRatio: 2},
		"c146-win-rtx4060-2560": {width: 2560, height: 1440, hardwareConcurrency: 16, devicePixelRatio: 1},
	}
	for index := range personaCatalogV2 {
		entry := personaCatalogV2[index]
		want, ok := tests[entry.persona.CatalogID]
		if !ok {
			continue
		}
		if entry.persona.ScreenWidth != want.width || entry.persona.ScreenHeight != want.height ||
			entry.persona.HardwareConcurrency != want.hardwareConcurrency || entry.devicePixelRatio != want.devicePixelRatio {
			t.Fatalf("entry %q = %dx%d HC%d DPR%g", entry.persona.CatalogID, entry.persona.ScreenWidth,
				entry.persona.ScreenHeight, entry.persona.HardwareConcurrency, entry.devicePixelRatio)
		}
		delete(tests, entry.persona.CatalogID)
	}
	if len(tests) != 0 {
		t.Fatalf("missing corrected catalog entries: %v", tests)
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

func TestCredentialPersonaPreservesPersistedV2Identity(t *testing.T) {
	want := personaCatalogV2[3].persona
	credential := &Credential{DeviceID: "stable-device", Persona: want}
	resolveCredentialPersona(credential, "different-seed")
	if credential.Persona != want {
		t.Fatalf("persisted v2 persona changed: %#v != %#v", credential.Persona, want)
	}
	if languages := personaNavigatorLanguages(credential.Persona); !slices.Equal(languages, []string{"en-US"}) {
		t.Fatalf("persisted v2 navigator languages changed: %#v", languages)
	}
}

func TestPersonaCatalogDistribution(t *testing.T) {
	counts := make([]int, len(personaCatalogV3))
	for index := 0; index < 10_000; index++ {
		entry := personaCatalogEntryForSeed(fmt.Sprintf("device-%d", index))
		for catalogIndex := range personaCatalogV3 {
			if personaCatalogV3[catalogIndex].persona.CatalogID == entry.persona.CatalogID {
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

func TestFindTLSProfileRestrictsProductionCatalogToChrome146(t *testing.T) {
	if _, ok := findTLSProfile("CHROME_146"); !ok {
		t.Fatal("Chrome 146 TLS profile is unsupported")
	}
	for _, name := range []string{"chrome_144", "chrome_144_psk", "chrome_145", "chrome_146_psk"} {
		if _, ok := findTLSProfile(name); ok {
			t.Fatalf("unvalidated TLS profile %q was accepted", name)
		}
	}
}
