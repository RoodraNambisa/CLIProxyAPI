package util

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestPromptCacheLogStreamHandlesWholeScalarWithoutMaskingUnrelatedText(t *testing.T) {
	for _, key := range []string{"a", "abc", "cache-key-value"} {
		r := NewPromptCacheLogRedactor(key)
		for _, body := range []string{key, "banana", strings.Repeat("z", 2048) + key} {
			for split := 0; split <= len(body); split++ {
				stream := r.Stream()
				got := append([]byte(nil), stream.Write([]byte(body[:split]), false)...)
				got = append(got, stream.Write([]byte(body[split:]), true)...)
				if string(got) != r.Redact(body) {
					t.Fatalf("whole-scalar policy differed for key length %d, body length %d, split %d", len(key), len(body), split)
				}
			}
		}
	}
}

func TestPromptCacheLogRedactorPreservesJSONAndHandlesChunkBoundaries(t *testing.T) {
	for _, key := range []string{"a", "E", `x\"y`, "cache-key-value", " cache with spaces ", "缓存鍵"} {
		metadata, _ := json.Marshal(map[string]string{"prompt_cache_key": key, "unrelated": "server"})
		body, _ := json.Marshal(map[string]any{"prompt_cache_key": key, "session_id": key, "metadata": string(metadata), "enabled": true, "model": "astra"})
		original := string(body)
		redactor := PromptCacheLogRedactorForRequest(body)
		got := redactor.Redact(original)
		var decoded map[string]any
		if err := json.Unmarshal([]byte(got), &decoded); err != nil {
			t.Fatal("redaction corrupted JSON syntax")
		}
		if decoded["prompt_cache_key"] != PromptCacheLogMarker || decoded["session_id"] != PromptCacheLogMarker || decoded["enabled"] != true || decoded["model"] != "astra" {
			t.Fatal("short cache key changed syntax or unrelated words")
		}
		var mirror map[string]any
		if err := json.Unmarshal([]byte(decoded["metadata"].(string)), &mirror); err != nil || mirror["prompt_cache_key"] != PromptCacheLogMarker {
			t.Fatal("encoded metadata was not safely redacted")
		}
		for split := 0; split <= len(body); split++ {
			stream := redactor.Stream()
			out := append([]byte(nil), stream.Write(body[:split], false)...)
			out = append(out, stream.Write(body[split:], true)...)
			if string(out) != got {
				t.Fatalf("split %d changed redaction", split)
			}
		}
		if string(body) != original {
			t.Fatal("redaction changed original request bytes")
		}
	}
}

func TestPromptCacheLogRedactorHeadersAndResourceBound(t *testing.T) {
	r := NewPromptCacheLogRedactor("a")
	headers := map[string][]string{"Session-Id": {"a"}, "Content-Type": {"application/json"}}
	masked := r.Headers(headers)
	if masked["Session-Id"][0] != PromptCacheLogMarker || masked["Content-Type"][0] != "application/json" || headers["Session-Id"][0] != "a" {
		t.Fatal("header redaction changed unrelated content or wire headers")
	}
	r = NewPromptCacheLogRedactor(strings.Repeat("x", promptCacheLogPatternLimit+1))
	if r.Redact("diagnostic details") != PromptCacheLogMarker {
		t.Fatal("oversized log matcher was constructed")
	}
	stream := r.Stream()
	if string(stream.Write([]byte("first"), false)) != PromptCacheLogMarker || len(stream.Write([]byte("second"), true)) != 0 {
		t.Fatal("oversized key retained or repeated body details")
	}
	for _, body := range []string{`{}`, `{"prompt_cache_key":null}`, `{"prompt_cache_key":42}`, `{"prompt_cache_key":" "}`} {
		if PromptCacheLogRedactorForRequest([]byte(body)) != nil {
			t.Fatal("invalid key changed logging")
		}
	}
}

func TestPromptCacheLogRedactorAcceptsEquivalentJSONEscapes(t *testing.T) {
	for index, tc := range []struct{ key, encoded string }{
		{"a", `"\u0061"`},
		{"/", `"\/"`},
		{"<>\u2028", `"<>\u2028"`},
		{"ÿ", `"\u00Ff"`},
		{"😀", `"\uD83d\uDe00"`},
		{"a/key", `"\u0061\/ke\u0079"`},
	} {
		redactor := NewPromptCacheLogRedactor(tc.key)
		payload := []byte(`{"prompt_cache_key":` + tc.encoded + `}`)
		for depth := 0; depth < 3; depth++ {
			got := redactor.Redact(string(payload))
			decoded := []byte(got)
			for unwrap := 0; unwrap < depth; unwrap++ {
				var value string
				if err := json.Unmarshal(decoded, &value); err != nil {
					t.Fatal("escaped mirror lost JSON syntax")
				}
				decoded = []byte(value)
			}
			var object map[string]any
			if err := json.Unmarshal(decoded, &object); err != nil || object["prompt_cache_key"] != PromptCacheLogMarker {
				t.Fatalf("escaped key remained in case %d depth %d", index, depth)
			}
			for split := 0; split <= len(payload); split++ {
				stream := redactor.Stream()
				out := append([]byte(nil), stream.Write(payload[:split], false)...)
				out = append(out, stream.Write(payload[split:], true)...)
				if string(out) != got {
					t.Fatal("escaped key split across chunks changed redaction")
				}
			}
			payload, _ = json.Marshal(string(payload))
		}
	}
}

func TestPromptCacheLogRedactorEncodedMirrorQuoteEscapes(t *testing.T) {
	r := NewPromptCacheLogRedactor("a")
	body := []byte(`{"prompt_cache_key":"a","metadata":"{\u0022prompt_cache_key\u0022:\u0022\u0061\u0022}"}`)
	for depth := 0; depth < 2; depth++ {
		masked := r.Redact(string(body))
		decoded := []byte(masked)
		if depth == 1 {
			var text string
			if err := json.Unmarshal(decoded, &text); err != nil {
				t.Fatal(err)
			}
			decoded = []byte(text)
		}
		var outer map[string]string
		if err := json.Unmarshal(decoded, &outer); err != nil {
			t.Fatal("redaction corrupted encoded mirror")
		}
		var inner map[string]string
		if err := json.Unmarshal([]byte(outer["metadata"]), &inner); err != nil || inner["prompt_cache_key"] != PromptCacheLogMarker {
			t.Fatal("encoded mirror leaked cache key")
		}
		for split := 0; split <= len(body); split++ {
			stream := r.Stream()
			got := append([]byte(nil), stream.Write(body[:split], false)...)
			got = append(got, stream.Write(body[split:], true)...)
			if string(got) != masked {
				t.Fatal("mirror quote escape split changed redaction")
			}
		}
		body, _ = json.Marshal(string(body))
	}
}

func TestPromptCacheLogStreamReleasesLargeChunkStorage(t *testing.T) {
	stream := NewPromptCacheLogRedactor("cache-key-value").Stream()
	body := bytes.Repeat([]byte("x"), 2*1024*1024)
	first := stream.Write(body, false)
	if len(stream.pending) >= promptCacheLogPatternLimit || cap(stream.pending) > 64*1024 {
		t.Fatal("stream retained a large chunk for a small matching tail")
	}
	tail := stream.Write(nil, true)
	if len(first)+len(tail) != len(body) || !bytes.Equal(first, body[:len(first)]) || !bytes.Equal(tail, body[len(first):]) {
		t.Fatal("releasing storage changed unmatched log data")
	}
}
