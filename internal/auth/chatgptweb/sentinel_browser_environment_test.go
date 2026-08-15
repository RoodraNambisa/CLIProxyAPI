package chatgptweb

import (
	"encoding/json"
	"io"
	"reflect"
	"testing"
	"time"
)

type sentinelSequenceReader struct {
	next byte
}

func (reader *sentinelSequenceReader) Read(buffer []byte) (int, error) {
	value := reader.next
	for index := range buffer {
		buffer[index] = value
	}
	reader.next++
	return len(buffer), nil
}

func TestSentinelBrowserProfilesMatchEnvironmentCatalog(t *testing.T) {
	if sentinelBrowserFingerprintSlots != browserEnvironmentVariantsPerPersona {
		t.Fatalf("fingerprint slots = %d, environment slots = %d", sentinelBrowserFingerprintSlots, browserEnvironmentVariantsPerPersona)
	}
	seen := make(map[string]struct{}, len(personaCatalogV3)*browserEnvironmentVariantsPerPersona)
	for _, entry := range personaCatalogV3 {
		for slot := 0; slot < browserEnvironmentVariantsPerPersona; slot++ {
			profile := sentinelBrowserProfileForSlot(entry.persona, slot)
			if profile.version != browserEnvironmentCatalogVersion || profile.slot != slot {
				t.Fatalf("profile %q/%d identity = %s/%s/%d", entry.persona.CatalogID, slot, profile.version, profile.catalogID, profile.slot)
			}
			if _, ok := browserEnvironmentSlot(entry.persona, BrowserEnvironmentIdentity{CatalogVersion: profile.version, CatalogID: profile.catalogID}); !ok {
				t.Fatalf("profile %q is not valid for %q", profile.catalogID, entry.persona.CatalogID)
			}
			if _, exists := seen[profile.catalogID]; exists {
				t.Fatalf("duplicate environment profile %q", profile.catalogID)
			}
			seen[profile.catalogID] = struct{}{}
			if profile.platform != entry.platform || profile.webGLVendor != entry.webGLVendor || profile.webGLRenderer != entry.webGLRenderer {
				t.Fatalf("profile %q crossed hardware families: %#v", profile.catalogID, profile)
			}
			if profile.screenWidth != entry.persona.ScreenWidth || profile.screenHeight != entry.persona.ScreenHeight ||
				profile.hardwareConcurrency != entry.persona.HardwareConcurrency || profile.devicePixelRatio != entry.devicePixelRatio ||
				profile.deviceMemory != entry.deviceMemory || profile.colorDepth != entry.colorDepth {
				t.Fatalf("profile %q changed fixed family attributes: %#v", profile.catalogID, profile)
			}
			if profile.innerWidth < 1 || profile.innerWidth > profile.availWidth || profile.innerHeight < 1 || profile.innerHeight > profile.availHeight {
				t.Fatalf("profile %q has invalid viewport: %#v", profile.catalogID, profile)
			}
			if profile.outerWidth < profile.innerWidth || profile.outerWidth > profile.availWidth ||
				profile.outerHeight < profile.innerHeight || profile.outerHeight > profile.availHeight {
				t.Fatalf("profile %q has invalid outer window: %#v", profile.catalogID, profile)
			}
		}
	}
	if len(seen) != 512 {
		t.Fatalf("environment catalog has %d entries, want 512", len(seen))
	}
}

func TestSentinelBrowserProfilePreservesOriginalSlots(t *testing.T) {
	entry := personaCatalogV2[0]
	profile := sentinelBrowserProfileForSlot(entry.persona, 17)
	if profile.innerWidth != scaleBrowserViewport(entry.innerWidth, 92, 640, entry.availWidth) ||
		profile.innerHeight != scaleBrowserViewport(entry.innerHeight, 96, 480, entry.availHeight) ||
		profile.jsHeapSizeLimit != sentinelBrowserHeapProfiles[2] {
		t.Fatalf("persisted e17 profile changed: %#v", profile)
	}
	identity := BrowserEnvironmentIdentity{CatalogVersion: browserEnvironmentCatalogVersion, CatalogID: entry.persona.CatalogID + "-e17"}
	if slot, ok := browserEnvironmentSlot(entry.persona, identity); !ok || slot != 17 {
		t.Fatalf("persisted v2 environment resolved to %d/%v", slot, ok)
	}
}

func TestSentinelBrowserProfileUsesPersistedEnvironmentIdentity(t *testing.T) {
	for _, entry := range personaCatalogV3 {
		identity := browserEnvironmentIdentityForSlot(entry.persona, 17)
		environment := ConversationTurnstileEnvironment{
			Persona:            entry.persona,
			BrowserEnvironment: identity,
			DeviceID:           "11111111-2222-4333-8444-555555555555",
		}
		first := resolveSentinelBrowserProfile(environment)
		environment.DeviceID = "11111111-2222-4333-8444-666666666666"
		second := resolveSentinelBrowserProfile(environment)
		if first != second {
			t.Fatalf("environment %q changed with device ID: %#v != %#v", identity.CatalogID, first, second)
		}
		if first.catalogID != identity.CatalogID || first.slot != 17 {
			t.Fatalf("environment %q resolved to %q/%d", identity.CatalogID, first.catalogID, first.slot)
		}
	}
}

func TestSentinelBrowserEnvironmentUsesCanonicalPersona(t *testing.T) {
	legacy := Persona{
		Profile:             "chrome_144",
		UserAgent:           "Mozilla/5.0 (X11; Linux x86_64) Chrome/144.0.0.0",
		AcceptLanguage:      "de-DE",
		Language:            "de-DE",
		Platform:            "Linux x86_64",
		ScreenWidth:         800,
		ScreenHeight:        600,
		HardwareConcurrency: 2,
	}
	environment := ConversationTurnstileEnvironment{Persona: legacy, DeviceID: "legacy-device"}
	profile := resolveSentinelBrowserProfile(environment)
	if _, ok := browserEnvironmentSlot(personaCatalogV3[0].persona, BrowserEnvironmentIdentity{CatalogVersion: profile.version, CatalogID: profile.catalogID}); !ok {
		t.Fatalf("legacy runtime profile = %q", profile.catalogID)
	}
	values, _, _ := normalizeConversationTurnstileEnvironment(environment, testTime())
	want := personaCatalogV3[0].persona
	if values["window.navigator.userAgent"] != want.UserAgent ||
		values["window.navigator.platform"] != want.Platform ||
		values["window.screen.width"] != want.ScreenWidth ||
		values["window.screen.height"] != want.ScreenHeight ||
		values["window.navigator.hardwareConcurrency"] != want.HardwareConcurrency {
		t.Fatalf("legacy environment was not canonicalized: %#v", values)
	}
}

func TestSentinelRuntimeBootstrapSharesCatalogSnapshot(t *testing.T) {
	startedAt := time.Unix(1_700_000_000, 123_000_000)
	random := &sentinelSequenceReader{}
	manager := &SentinelRuntimeManager{
		random: random,
		now:    func() time.Time { return startedAt },
	}
	entry := personaCatalogV3[6]
	identity := browserEnvironmentIdentityForSlot(entry.persona, 23)
	environment := ConversationTurnstileEnvironment{
		Persona:            entry.persona,
		BrowserEnvironment: identity,
		DeviceID:           "11111111-2222-4333-8444-555555555555",
		PageStartedAt:      startedAt,
	}
	request := SentinelSDKRequest{Environment: environment}
	source := &sentinelSourceCacheEntry{url: "https://sentinel.openai.com/sentinel/test/sdk.js"}

	first := decodeSentinelRuntimeBootstrap(t, manager, request, source)
	second := decodeSentinelRuntimeBootstrap(t, manager, request, source)
	profile := resolveSentinelBrowserProfile(environment)
	for key, want := range map[string]any{
		"device_id":           environment.DeviceID,
		"user_agent":          entry.persona.UserAgent,
		"language":            entry.persona.Language,
		"platform":            entry.persona.Platform,
		"fingerprint_version": browserEnvironmentCatalogVersion,
		"fingerprint_catalog": identity.CatalogID,
		"fingerprint_slot":    float64(profile.slot),
		"webgl_vendor":        profile.webGLVendor,
		"webgl_renderer":      profile.webGLRenderer,
		"screen_avail_left":   float64(profile.availLeft),
		"screen_avail_top":    float64(profile.availTop),
		"device_memory":       profile.deviceMemory,
		"page_started_at_ms":  float64(startedAt.UnixNano()) / float64(time.Millisecond),
	} {
		if first[key] != want || second[key] != want {
			t.Fatalf("bootstrap %q changed: first=%#v second=%#v want=%#v", key, first[key], second[key], want)
		}
	}
	wantLanguages := make([]any, len(personaNavigatorLanguages(entry.persona)))
	for index, language := range personaNavigatorLanguages(entry.persona) {
		wantLanguages[index] = language
	}
	if !reflect.DeepEqual(first["languages"], wantLanguages) || !reflect.DeepEqual(second["languages"], wantLanguages) {
		t.Fatalf("bootstrap languages = first:%#v second:%#v want:%#v", first["languages"], second["languages"], wantLanguages)
	}
	if first["random_b64"] == second["random_b64"] {
		t.Fatalf("page random pool did not change: %q", first["random_b64"])
	}
}

func decodeSentinelRuntimeBootstrap(t *testing.T, manager *SentinelRuntimeManager, request SentinelSDKRequest, source *sentinelSourceCacheEntry) map[string]any {
	t.Helper()
	payload, err := manager.runtimeBootstrap(request, source)
	if err != nil {
		t.Fatalf("runtimeBootstrap() error = %v", err)
	}
	var decoded map[string]any
	if err = json.Unmarshal([]byte(payload), &decoded); err != nil {
		t.Fatalf("decode runtime bootstrap: %v", err)
	}
	return decoded
}

var _ io.Reader = (*sentinelSequenceReader)(nil)

func testTime() time.Time {
	return time.Unix(1_700_000_000, 0)
}
