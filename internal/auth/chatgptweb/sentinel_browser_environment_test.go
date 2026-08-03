package chatgptweb

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
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

func TestSentinelBrowserFingerprintSpaceIsStableAndComplete(t *testing.T) {
	persona := DefaultPersona()
	seen := make(map[string]int, sentinelBrowserFingerprintSlots)
	for slot := 0; slot < sentinelBrowserFingerprintSlots; slot++ {
		profile := sentinelBrowserProfileForSlot(persona, slot)
		key := fmt.Sprintf(
			"%s\x00%s\x00%d\x00%d\x00%d\x00%d\x00%d\x00%d\x00%g\x00%g\x00%g\x00%d",
			profile.webGLVendor,
			profile.webGLRenderer,
			profile.availLeft,
			profile.availTop,
			profile.availWidth,
			profile.availHeight,
			profile.innerWidth,
			profile.innerHeight,
			profile.devicePixelRatio,
			profile.deviceMemory,
			profile.jsHeapSizeLimit,
			profile.colorDepth,
		)
		if previous, exists := seen[key]; exists {
			t.Fatalf("fingerprint slots %d and %d resolve to the same values", previous, slot)
		}
		seen[key] = slot
		if profile.slot != slot || profile.version != sentinelBrowserFingerprintVersion {
			t.Fatalf("profile %d identity = %s/%d", slot, profile.version, profile.slot)
		}
	}
	if len(seen) != sentinelBrowserFingerprintSlots {
		t.Fatalf("unique fingerprint profiles = %d", len(seen))
	}
}

func TestSentinelBrowserFingerprintV1GoldenProfiles(t *testing.T) {
	tests := []struct {
		slot int
		want sentinelBrowserProfile
	}{
		{
			slot: 0x000,
			want: sentinelBrowserProfile{
				version: sentinelBrowserFingerprintVersion, slot: 0x000, platform: sentinelBrowserPlatformMac,
				webGLVendor: "Google Inc. (Apple)", webGLRenderer: "ANGLE (Apple, ANGLE Metal Renderer: Apple M1, Unspecified Version)",
				availWidth: 1920, availHeight: 1080, innerWidth: 1920, innerHeight: 1008,
				devicePixelRatio: 1, deviceMemory: 4, jsHeapSizeLimit: 2_147_352_576, colorDepth: 24,
			},
		},
		{
			slot: 0x123,
			want: sentinelBrowserProfile{
				version: sentinelBrowserFingerprintVersion, slot: 0x123, platform: sentinelBrowserPlatformMac,
				webGLVendor: "Google Inc. (Apple)", webGLRenderer: "ANGLE (Apple, ANGLE Metal Renderer: Apple M2, Unspecified Version)",
				availWidth: 1920, availHeight: 1080, innerWidth: 1888, innerHeight: 992,
				devicePixelRatio: 1.5, deviceMemory: 4, jsHeapSizeLimit: 2_199_023_255, colorDepth: 24,
			},
		},
		{
			slot: 0xfff,
			want: sentinelBrowserProfile{
				version: sentinelBrowserFingerprintVersion, slot: 0xfff, platform: sentinelBrowserPlatformMac,
				webGLVendor: "Google Inc. (ATI Technologies Inc.)", webGLRenderer: "ANGLE (ATI Technologies Inc., AMD Radeon Pro 5600M OpenGL Engine, OpenGL 4.1)",
				availLeft: 72, availTop: 24, availWidth: 1848, availHeight: 1056, innerWidth: 1800, innerHeight: 960,
				devicePixelRatio: 2, deviceMemory: 8, jsHeapSizeLimit: 4_647_288_832, colorDepth: 30,
			},
		},
	}
	for _, test := range tests {
		if got := sentinelBrowserProfileForSlot(DefaultPersona(), test.slot); got != test.want {
			t.Fatalf("slot %#03x profile changed:\n got: %#v\nwant: %#v", test.slot, got, test.want)
		}
	}
}

func TestSentinelBrowserFingerprintIsBoundToDeviceID(t *testing.T) {
	environment := ConversationTurnstileEnvironment{
		Persona:  DefaultPersona(),
		DeviceID: "11111111-2222-4333-8444-555555555555",
	}
	first := resolveSentinelBrowserProfile(environment)
	second := resolveSentinelBrowserProfile(environment)
	if first != second {
		t.Fatalf("same device profile changed: %#v != %#v", first, second)
	}
	environment.DeviceID = "11111111-2222-4333-8444-666666666666"
	third := resolveSentinelBrowserProfile(environment)
	if third.slot == first.slot {
		t.Fatalf("test device IDs unexpectedly share slot %d", first.slot)
	}
}

func TestSentinelBrowserFingerprintPreservesPersonaAndPlatform(t *testing.T) {
	tests := []struct {
		name     string
		persona  Persona
		platform sentinelBrowserPlatform
	}{
		{name: "mac", persona: DefaultPersona(), platform: sentinelBrowserPlatformMac},
		{name: "windows", persona: Persona{Profile: "custom", UserAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/146.0.0.0", AcceptLanguage: "fr-FR", Language: "fr-FR", Platform: "Win32", ScreenWidth: 1600, ScreenHeight: 900, HardwareConcurrency: 12}, platform: sentinelBrowserPlatformWindows},
		{name: "linux", persona: Persona{Profile: "custom", UserAgent: "Mozilla/5.0 (X11; Linux x86_64) Chrome/146.0.0.0", AcceptLanguage: "de-DE", Language: "de-DE", Platform: "Linux x86_64", ScreenWidth: 1366, ScreenHeight: 768, HardwareConcurrency: 4}, platform: sentinelBrowserPlatformLinux},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			environment := ConversationTurnstileEnvironment{Persona: test.persona, DeviceID: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"}
			profile := resolveSentinelBrowserProfile(environment)
			if profile.platform != test.platform {
				t.Fatalf("platform profile = %d/%q", profile.platform, profile.webGLRenderer)
			}
			switch test.platform {
			case sentinelBrowserPlatformMac:
				if strings.Contains(profile.webGLRenderer, "Direct3D") {
					t.Fatalf("mac profile uses Windows renderer %q", profile.webGLRenderer)
				}
			case sentinelBrowserPlatformWindows:
				if !strings.Contains(profile.webGLRenderer, "Direct3D11") {
					t.Fatalf("Windows profile renderer = %q", profile.webGLRenderer)
				}
			case sentinelBrowserPlatformLinux:
				if !strings.Contains(profile.webGLRenderer, "OpenGL") {
					t.Fatalf("Linux profile renderer = %q", profile.webGLRenderer)
				}
			}
			values, _, _ := normalizeConversationTurnstileEnvironment(environment, testTime())
			if values["window.navigator.userAgent"] != test.persona.UserAgent ||
				values["window.navigator.platform"] != test.persona.Platform ||
				values["window.screen.width"] != test.persona.ScreenWidth ||
				values["window.screen.height"] != test.persona.ScreenHeight ||
				values["window.navigator.hardwareConcurrency"] != test.persona.HardwareConcurrency {
				t.Fatalf("explicit persona was not preserved: %#v", values)
			}
		})
	}
}

func TestSentinelBrowserFingerprintDistributionForTenThousandDevices(t *testing.T) {
	counts := make([]int, sentinelBrowserFingerprintSlots)
	for index := 0; index < 10_000; index++ {
		environment := ConversationTurnstileEnvironment{
			Persona:  DefaultPersona(),
			DeviceID: fmt.Sprintf("00000000-0000-4000-8000-%012x", index),
		}
		counts[resolveSentinelBrowserProfile(environment).slot]++
	}
	used := 0
	maximum := 0
	for _, count := range counts {
		if count > 0 {
			used++
		}
		if count > maximum {
			maximum = count
		}
	}
	if used < 3500 || maximum > 12 {
		t.Fatalf("fingerprint distribution used=%d maximum=%d", used, maximum)
	}
}

func TestSentinelRuntimeBootstrapSharesStableFingerprintSnapshot(t *testing.T) {
	startedAt := time.Unix(1_700_000_000, 123_000_000)
	random := &sentinelSequenceReader{}
	manager := &SentinelRuntimeManager{
		random: random,
		now:    func() time.Time { return startedAt },
	}
	environment := ConversationTurnstileEnvironment{
		Persona:       DefaultPersona(),
		DeviceID:      "11111111-2222-4333-8444-555555555555",
		PageStartedAt: startedAt,
	}
	request := SentinelSDKRequest{
		Environment: environment,
	}
	source := &sentinelSourceCacheEntry{url: "https://sentinel.openai.com/sentinel/test/sdk.js"}

	first := decodeSentinelRuntimeBootstrap(t, manager, request, source)
	second := decodeSentinelRuntimeBootstrap(t, manager, request, source)
	profile := resolveSentinelBrowserProfile(environment)
	for key, want := range map[string]any{
		"device_id":           environment.DeviceID,
		"fingerprint_version": sentinelBrowserFingerprintVersion,
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
