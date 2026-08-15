package chatgptweb

import (
	"crypto/sha256"
	"strings"
)

const (
	personaCatalogV2Version = "v2"
	personaCatalogVersion   = "v3"
)

type personaCatalogEntry struct {
	persona          Persona
	platform         sentinelBrowserPlatform
	webGLVendor      string
	webGLRenderer    string
	availLeft        int
	availTop         int
	availWidth       int
	availHeight      int
	innerWidth       int
	innerHeight      int
	devicePixelRatio float64
	deviceMemory     float64
	jsHeapSizeLimit  float64
	colorDepth       int
	maxTouchPoints   int
}

const (
	chrome146MacUserAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
	chrome146WinUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)

// personaCatalogV2 is immutable. Its order is part of the stable credential
// mapping and must not be changed after release.
var personaCatalogV2 = [...]personaCatalogEntry{
	newPersonaCatalogEntry(
		"c146-mac-m1-1440", chrome146MacUserAgent, "MacIntel", 1440, 900, 8,
		sentinelBrowserPlatformMac,
		"Google Inc. (Apple)", "ANGLE (Apple, ANGLE Metal Renderer: Apple M1, Unspecified Version)",
		0, 25, 1440, 847, 1440, 775, 2, 8, 4_294_967_296, 30,
	),
	newPersonaCatalogEntry(
		"c146-mac-m2-1470", chrome146MacUserAgent, "MacIntel", 1470, 956, 8,
		sentinelBrowserPlatformMac,
		"Google Inc. (Apple)", "ANGLE (Apple, ANGLE Metal Renderer: Apple M2, Unspecified Version)",
		0, 25, 1470, 843, 1470, 771, 2, 8, 4_294_967_296, 30,
	),
	newPersonaCatalogEntry(
		"c146-mac-m3p-1728", chrome146MacUserAgent, "MacIntel", 1728, 1117, 12,
		sentinelBrowserPlatformMac,
		"Google Inc. (Apple)", "ANGLE (Apple, ANGLE Metal Renderer: Apple M3 Pro, Unspecified Version)",
		0, 25, 1728, 1004, 1728, 932, 2, 8, 4_294_967_296, 30,
	),
	newPersonaCatalogEntry(
		"c146-mac-m4p-1512", chrome146MacUserAgent, "MacIntel", 1512, 982, 14,
		sentinelBrowserPlatformMac,
		"Google Inc. (Apple)", "ANGLE (Apple, ANGLE Metal Renderer: Apple M4 Pro, Unspecified Version)",
		0, 25, 1512, 929, 1512, 857, 2, 8, 4_294_967_296, 30,
	),
	newPersonaCatalogEntry(
		"c146-win-uhd-1366", chrome146WinUserAgent, "Win32", 1366, 768, 8,
		sentinelBrowserPlatformWindows,
		"Google Inc. (Intel)", "ANGLE (Intel, Intel(R) UHD Graphics 620 Direct3D11 vs_5_0 ps_5_0, D3D11)",
		0, 0, 1366, 728, 1366, 656, 1, 8, 4_294_967_296, 24,
	),
	newPersonaCatalogEntry(
		"c146-win-iris-1536", chrome146WinUserAgent, "Win32", 1536, 864, 12,
		sentinelBrowserPlatformWindows,
		"Google Inc. (Intel)", "ANGLE (Intel, Intel(R) Iris(R) Xe Graphics Direct3D11 vs_5_0 ps_5_0, D3D11)",
		0, 0, 1536, 824, 1536, 752, 1.25, 8, 4_294_967_296, 24,
	),
	newPersonaCatalogEntry(
		"c146-win-rtx3060-1920", chrome146WinUserAgent, "Win32", 1920, 1080, 16,
		sentinelBrowserPlatformWindows,
		"Google Inc. (NVIDIA)", "ANGLE (NVIDIA, NVIDIA GeForce RTX 3060 Direct3D11 vs_5_0 ps_5_0, D3D11)",
		0, 0, 1920, 1040, 1920, 968, 1, 8, 4_294_967_296, 24,
	),
	newPersonaCatalogEntry(
		"c146-win-rtx4060-2560", chrome146WinUserAgent, "Win32", 2560, 1440, 16,
		sentinelBrowserPlatformWindows,
		"Google Inc. (NVIDIA)", "ANGLE (NVIDIA, NVIDIA GeForce RTX 4060 Direct3D11 vs_5_0 ps_5_0, D3D11)",
		0, 0, 2560, 1400, 2560, 1328, 1, 8, 4_294_967_296, 24,
	),
}

func newPersonaCatalogEntry(
	id, userAgent, platform string,
	width, height, hardwareConcurrency int,
	browserPlatform sentinelBrowserPlatform,
	webGLVendor, webGLRenderer string,
	availLeft, availTop, availWidth, availHeight, innerWidth, innerHeight int,
	devicePixelRatio, deviceMemory, jsHeapSizeLimit float64,
	colorDepth int,
) personaCatalogEntry {
	return personaCatalogEntry{
		persona: Persona{
			CatalogVersion:      personaCatalogV2Version,
			CatalogID:           id,
			Profile:             "chrome_146",
			UserAgent:           userAgent,
			AcceptLanguage:      "en-US,en;q=0.9",
			Language:            "en-US",
			Platform:            platform,
			ScreenWidth:         width,
			ScreenHeight:        height,
			HardwareConcurrency: hardwareConcurrency,
		},
		platform:         browserPlatform,
		webGLVendor:      webGLVendor,
		webGLRenderer:    webGLRenderer,
		availLeft:        availLeft,
		availTop:         availTop,
		availWidth:       availWidth,
		availHeight:      availHeight,
		innerWidth:       innerWidth,
		innerHeight:      innerHeight,
		devicePixelRatio: devicePixelRatio,
		deviceMemory:     deviceMemory,
		jsHeapSizeLimit:  jsHeapSizeLimit,
		colorDepth:       colorDepth,
		maxTouchPoints:   0,
	}
}

type personaLocalePack struct {
	id             string
	acceptLanguage string
	language       string
}

var personaCatalogV3 = buildPersonaCatalogV3()

func buildPersonaCatalogV3() []personaCatalogEntry {
	locales := [...]personaLocalePack{
		{id: "en-us", acceptLanguage: "en-US,en;q=0.9", language: "en-US"},
	}
	entries := make([]personaCatalogEntry, 0, len(locales)*len(personaCatalogV2))
	for _, locale := range locales {
		for _, base := range personaCatalogV2 {
			entry := base
			entry.persona.CatalogVersion = personaCatalogVersion
			entry.persona.CatalogID = "c146-" + locale.id + "-" + strings.TrimPrefix(base.persona.CatalogID, "c146-")
			entry.persona.Profile = "chrome_146"
			entry.persona.AcceptLanguage = locale.acceptLanguage
			entry.persona.Language = locale.language
			entries = append(entries, entry)
		}
	}
	return entries
}

func personaNavigatorLanguages(persona Persona) []string {
	persona = canonicalPersona(persona)
	if strings.EqualFold(persona.CatalogVersion, personaCatalogV2Version) {
		if language := strings.TrimSpace(persona.Language); language != "" {
			return []string{language}
		}
		return []string{DefaultPersona().Language}
	}
	languages := make([]string, 0, 3)
	appendLanguage := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		for _, existing := range languages {
			if strings.EqualFold(existing, value) {
				return
			}
		}
		languages = append(languages, value)
	}
	appendLanguage(persona.Language)
	for _, part := range strings.Split(persona.AcceptLanguage, ",") {
		appendLanguage(strings.SplitN(part, ";", 2)[0])
	}
	if len(languages) == 0 {
		appendLanguage(DefaultPersona().Language)
	}
	return languages
}

func personaCatalogEntries(version string) []personaCatalogEntry {
	switch strings.ToLower(strings.TrimSpace(version)) {
	case personaCatalogV2Version:
		return personaCatalogV2[:]
	case personaCatalogVersion:
		return personaCatalogV3
	default:
		return nil
	}
}

func personaCatalogEntryByID(version, id string) (personaCatalogEntry, bool) {
	id = strings.TrimSpace(id)
	entries := personaCatalogEntries(version)
	for index := range entries {
		if entries[index].persona.CatalogID == id {
			return entries[index], true
		}
	}
	return personaCatalogEntry{}, false
}

func personaCatalogEntryForPersona(persona Persona) (personaCatalogEntry, bool) {
	if entry, ok := personaCatalogEntryByID(persona.CatalogVersion, persona.CatalogID); ok {
		return entry, true
	}
	for _, entries := range [][]personaCatalogEntry{personaCatalogV3, personaCatalogV2[:]} {
		for index := range entries {
			candidate := entries[index].persona
			if strings.EqualFold(strings.TrimSpace(persona.Profile), candidate.Profile) &&
				strings.TrimSpace(persona.UserAgent) == candidate.UserAgent &&
				strings.TrimSpace(persona.AcceptLanguage) == candidate.AcceptLanguage &&
				strings.TrimSpace(persona.Language) == candidate.Language &&
				strings.EqualFold(strings.TrimSpace(persona.Platform), candidate.Platform) &&
				persona.ScreenWidth == candidate.ScreenWidth &&
				persona.ScreenHeight == candidate.ScreenHeight &&
				persona.HardwareConcurrency == candidate.HardwareConcurrency {
				return entries[index], true
			}
		}
	}
	return personaCatalogEntry{}, false
}

func personaCatalogEntryForSeed(seed string) personaCatalogEntry {
	seed = strings.TrimSpace(seed)
	if seed == "" {
		return personaCatalogV3[0]
	}
	digest := sha256.Sum256([]byte("chatgpt-web-persona-" + personaCatalogVersion + "\x00" + seed))
	index := uint64(digest[len(digest)-1]) % uint64(len(personaCatalogV3))
	return personaCatalogV3[index]
}

func canonicalPersona(persona Persona) Persona {
	entry, _ := personaCatalogRuntimeEntry(persona)
	return entry.persona
}

func personaCatalogRuntimeEntry(persona Persona) (personaCatalogEntry, int) {
	if entry, ok := personaCatalogEntryForPersona(persona); ok {
		entries := personaCatalogEntries(entry.persona.CatalogVersion)
		for index := range entries {
			if entries[index].persona.CatalogID == entry.persona.CatalogID {
				return entry, index
			}
		}
	}
	return personaCatalogV3[0], 0
}

func resolveCredentialPersona(credential *Credential, fallbackSeed string) {
	if credential == nil {
		return
	}
	if entry, ok := personaCatalogEntryByID(credential.Persona.CatalogVersion, credential.Persona.CatalogID); ok {
		credential.Persona = entry.persona
		return
	}
	seed := strings.TrimSpace(credential.DeviceID)
	if seed == "" {
		seed = strings.TrimSpace(credential.CredentialUID)
	}
	if seed == "" {
		seed = strings.TrimSpace(fallbackSeed)
	}
	if seed == "" {
		if entry, ok := personaCatalogEntryForPersona(credential.Persona); ok {
			credential.Persona = entry.persona
		} else {
			credential.Persona = normalizeLegacyPersona(credential.Persona)
		}
		return
	}
	credential.Persona = personaCatalogEntryForSeed(seed).persona
}

// ResolveCredentialPersona maps a credential to the current immutable catalog.
// The fallback seed is used only when the credential has no device or stable UID.
func ResolveCredentialPersona(credential *Credential, fallbackSeed string) Persona {
	resolveCredentialPersona(credential, fallbackSeed)
	if credential == nil {
		return Persona{}
	}
	resolveCredentialBrowserEnvironment(credential, fallbackSeed)
	return credential.Persona
}
