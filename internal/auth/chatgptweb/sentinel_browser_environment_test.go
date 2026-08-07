package chatgptweb

import (
	"encoding/json"
	"io"
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

func TestSentinelBrowserProfilesMatchPersonaCatalog(t *testing.T) {
	if sentinelBrowserFingerprintSlots != len(personaCatalogV2) {
		t.Fatalf("fingerprint slots = %d, catalog entries = %d", sentinelBrowserFingerprintSlots, len(personaCatalogV2))
	}
	seen := make(map[string]struct{}, len(personaCatalogV2))
	for index, entry := range personaCatalogV2 {
		profile := sentinelBrowserProfileForSlot(Persona{}, index)
		want := sentinelBrowserProfileForCatalogEntry(entry, index)
		if profile != want {
			t.Fatalf("catalog profile %d mismatch:\n got: %#v\nwant: %#v", index, profile, want)
		}
		if profile.version != personaCatalogVersion || profile.catalogID != entry.persona.CatalogID || profile.slot != index {
			t.Fatalf("catalog profile %d identity = %s/%s/%d", index, profile.version, profile.catalogID, profile.slot)
		}
		if _, exists := seen[profile.catalogID]; exists {
			t.Fatalf("duplicate catalog profile %q", profile.catalogID)
		}
		seen[profile.catalogID] = struct{}{}
	}
}

func TestSentinelBrowserProfileFollowsCredentialPersona(t *testing.T) {
	for index, entry := range personaCatalogV2 {
		environment := ConversationTurnstileEnvironment{
			Persona:  entry.persona,
			DeviceID: "11111111-2222-4333-8444-555555555555",
		}
		first := resolveSentinelBrowserProfile(environment)
		environment.DeviceID = "11111111-2222-4333-8444-666666666666"
		second := resolveSentinelBrowserProfile(environment)
		if first != second {
			t.Fatalf("catalog %q changed with device ID: %#v != %#v", entry.persona.CatalogID, first, second)
		}
		if first.catalogID != entry.persona.CatalogID || first.slot != index {
			t.Fatalf("catalog %q resolved to %q/%d", entry.persona.CatalogID, first.catalogID, first.slot)
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
	if profile.catalogID != personaCatalogV2[0].persona.CatalogID {
		t.Fatalf("legacy runtime profile = %q", profile.catalogID)
	}
	values, _, _ := normalizeConversationTurnstileEnvironment(environment, testTime())
	want := personaCatalogV2[0].persona
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
	entry := personaCatalogV2[5]
	environment := ConversationTurnstileEnvironment{
		Persona:       entry.persona,
		DeviceID:      "11111111-2222-4333-8444-555555555555",
		PageStartedAt: startedAt,
	}
	request := SentinelSDKRequest{Environment: environment}
	source := &sentinelSourceCacheEntry{url: "https://sentinel.openai.com/sentinel/test/sdk.js"}

	first := decodeSentinelRuntimeBootstrap(t, manager, request, source)
	second := decodeSentinelRuntimeBootstrap(t, manager, request, source)
	profile := resolveSentinelBrowserProfile(environment)
	for key, want := range map[string]any{
		"device_id":           environment.DeviceID,
		"user_agent":          entry.persona.UserAgent,
		"platform":            entry.persona.Platform,
		"fingerprint_version": personaCatalogVersion,
		"fingerprint_catalog": entry.persona.CatalogID,
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
