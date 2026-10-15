package openai

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/tidwall/gjson"
)

func TestBuildXAISpeechPayloadMapsOpenAIVoice(t *testing.T) {
	payload, format, err := buildXAISpeechPayload([]byte(`{"model":"tts-1","input":" Hello ","voice":"Alloy","response_format":"mp3"}`))
	if err != nil {
		t.Fatalf("buildXAISpeechPayload() error = %v", err)
	}
	if format != "mp3" {
		t.Fatalf("format = %q, want mp3", format)
	}
	if gjson.GetBytes(payload, "text").String() != "Hello" {
		t.Fatalf("text = %s", payload)
	}
	if gjson.GetBytes(payload, "voice_id").String() != "ara" {
		t.Fatalf("voice_id = %s", payload)
	}
	if gjson.GetBytes(payload, "language").String() != "auto" {
		t.Fatalf("language = %s", payload)
	}
	if gjson.GetBytes(payload, "output_format").Exists() {
		t.Fatalf("mp3 should omit output_format: %s", payload)
	}
	if gjson.GetBytes(payload, "model").Exists() {
		t.Fatalf("upstream payload should omit model: %s", payload)
	}
}

func TestBuildXAISpeechPayloadWavAndSpeed(t *testing.T) {
	payload, format, err := buildXAISpeechPayload([]byte(`{"text":"你好","voice_id":"Luna","language":"zh","speed":1.25,"response_format":"wav"}`))
	if err != nil {
		t.Fatalf("buildXAISpeechPayload() error = %v", err)
	}
	if format != "wav" {
		t.Fatalf("format = %q, want wav", format)
	}
	if gjson.GetBytes(payload, "voice_id").String() != "luna" {
		t.Fatalf("voice_id = %s", payload)
	}
	if gjson.GetBytes(payload, "language").String() != "zh" {
		t.Fatalf("language = %s", payload)
	}
	if gjson.GetBytes(payload, "speed").Float() != 1.25 {
		t.Fatalf("speed = %s", payload)
	}
	if gjson.GetBytes(payload, "output_format.codec").String() != "wav" || gjson.GetBytes(payload, "output_format.sample_rate").Int() != 24000 {
		t.Fatalf("output_format = %s", payload)
	}
}

func TestBuildXAISpeechPayloadNativePCMKeepsSampleRate(t *testing.T) {
	payload, format, err := buildXAISpeechPayload([]byte(`{"input":"pcm","output_format":{"codec":"pcm","sample_rate":16000}}`))
	if err != nil {
		t.Fatalf("buildXAISpeechPayload() error = %v", err)
	}
	if format != "pcm" {
		t.Fatalf("format = %q, want pcm", format)
	}
	if gjson.GetBytes(payload, "voice_id").String() != "eve" {
		t.Fatalf("voice_id = %s", payload)
	}
	if gjson.GetBytes(payload, "output_format.sample_rate").Int() != 16000 {
		t.Fatalf("sample_rate = %s", payload)
	}
}

func TestBuildXAISpeechPayloadRejects(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{name: "missing input", body: `{"voice":"eve"}`, want: "input is required"},
		{name: "bad format", body: `{"input":"hi","response_format":"flac"}`, want: "response_format"},
		{name: "bad json", body: `{`, want: "valid JSON"},
		{name: "zero speed omitted", body: `{"input":"hi","speed":0}`, want: ""},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			payload, _, err := buildXAISpeechPayload([]byte(tt.body))
			if tt.want == "" {
				if err != nil {
					t.Fatalf("buildXAISpeechPayload() error = %v", err)
				}
				if gjson.GetBytes(payload, "speed").Exists() {
					t.Fatalf("speed should be omitted: %s", payload)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestBuildXAISpeechPayloadRejectsLongInput(t *testing.T) {
	body := `{"input":"` + strings.Repeat("a", maxXAISpeechRunes+1) + `"}`
	_, _, err := buildXAISpeechPayload([]byte(body))
	if err == nil || !strings.Contains(err.Error(), "60000") {
		t.Fatalf("error = %v", err)
	}
	if !utf8.ValidString(strings.Repeat("你", 2)) {
		t.Fatal("rune fixture is invalid")
	}
}

func TestSpeechRoutingModel(t *testing.T) {
	cases := []struct {
		in   string
		want string
		ok   bool
	}{
		{in: "", want: "grok-tts", ok: true},
		{in: "tts-1-hd", want: "grok-tts", ok: true},
		{in: "gpt-4o-mini-tts", want: "grok-tts", ok: true},
		{in: "xai/grok-tts", want: "grok-tts", ok: true},
		{in: "grok-voice-tts-1.0", want: "grok-voice-tts-1.0", ok: true},
		{in: "x-ai/grok-voice-tts-1.0(high)", want: "grok-voice-tts-1.0", ok: true},
		{in: "grok-4.7", ok: false},
		{in: "openai/tts-1", ok: false},
	}
	for _, tt := range cases {
		t.Run(tt.in, func(t *testing.T) {
			got, ok := speechRoutingModel(tt.in)
			if ok != tt.ok || got != tt.want {
				t.Fatalf("speechRoutingModel(%q) = %q, %v; want %q, %v", tt.in, got, ok, tt.want, tt.ok)
			}
		})
	}
}

func TestSpeechResponseContentType(t *testing.T) {
	if got := speechResponseContentType("mp3", "application/json", []byte(`{"audio":"SUQz","audio_timestamps":{},"duration":1}`)); got != "application/json" {
		t.Fatalf("timestamp envelope mislabeled as audio: %q", got)
	}
	if got := speechResponseContentType("mp3", ""); got != "audio/mpeg" {
		t.Fatalf("empty upstream = %q", got)
	}
	if got := speechResponseContentType("wav", "application/json"); got != "audio/wav" {
		t.Fatalf("json upstream = %q", got)
	}
	if got := speechResponseContentType("pcm", "audio/L16; rate=24000"); got != "audio/L16; rate=24000" {
		t.Fatalf("audio upstream = %q", got)
	}
}

func TestXAISpeechNativeOptionsArePreserved(t *testing.T) {
	body := []byte(`{"text":"hello","voice_id":"eve","output_format":{"codec":"mp3","sample_rate":44100,"bit_rate":192000},"with_timestamps":true,"text_normalization":false,"optimize_streaming_latency":1,"replace":{"CLI":"command line"}}`)
	payload, format, err := buildXAISpeechPayload(body)
	if err != nil || format != "mp3" {
		t.Fatalf("native speech: %s, %v", format, err)
	}
	for _, path := range []string{"output_format.codec", "output_format.sample_rate", "output_format.bit_rate", "with_timestamps", "text_normalization", "optimize_streaming_latency", "replace.CLI"} {
		if got, want := gjson.GetBytes(payload, path), gjson.GetBytes(body, path); got.Raw != want.Raw {
			t.Errorf("native option %s lost: got %s, want %s", path, got.Raw, want.Raw)
		}
	}
	for _, codec := range []string{"mulaw", "alaw"} {
		_, format, err := buildXAISpeechPayload([]byte(`{"text":"hello","output_format":{"codec":"` + codec + `","sample_rate":8000}}`))
		if err != nil || format != codec {
			t.Errorf("official native codec %s rejected: %v", codec, err)
		}
	}
}
