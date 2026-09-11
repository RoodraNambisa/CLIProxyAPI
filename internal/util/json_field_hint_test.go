package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestJSONFieldHintKeepsLiteralAndEscapedCandidates(t *testing.T) {
	for _, field := range []string{"id", "tools", "encrypted_content", "summary", "generate_summary", "reasoning_effort"} {
		for _, encoded := range []string{fmt.Sprintf("%q", field), `"\u` + fmt.Sprintf("%04x", field[0]) + field[1:] + `"`} {
			for _, body := range []string{"{" + encoded + ":null}", `{"input":[{` + encoded + `:false}]}`, `{"unrelated":"text",` + encoded + `:0}`} {
				if !JSONMayContainAnyField([]byte(body), "other", field) {
					t.Fatalf("missed a candidate for %s: %s", field, body)
				}
			}
		}
	}
	for _, body := range [][]byte{nil, {}, []byte(`{}`), []byte(`{"input":[{"content":"ordinary text"}]}`), []byte(`{"tool_choice":"auto","parallel_tool_calls":true}`), []byte(`{"content":"tools"}`), []byte(`{"content":"\u0061"}`)} {
		if JSONMayContainAnyField(body, "id", "tools", "summary") {
			t.Fatalf("unexpected literal candidate in %s", body)
		}
	}
	// A hint must not be used as evidence that a protocol field exists.
	for _, body := range []string{`{"metadata":{"tools":[]}}`, `{"\u0061":false}`, `{"content":"unterminated`} {
		if !JSONMayContainAnyField([]byte(body), "tools") {
			t.Fatal("conservative nested-field, escaped-key or incomplete-string handling changed")
		}
	}
}

func TestJSONFieldHintHandlesLargeEscapedContentWithoutMutation(t *testing.T) {
	content, err := json.Marshal(strings.Repeat("quoted \"tools\" and \"id\"\n\\", 32768))
	if err != nil {
		t.Fatal(err)
	}
	body := append([]byte(`{"content":`), content...)
	body = append(body, '}')
	if JSONMayContainAnyField(body, "tools", "id") {
		t.Fatal("escaped values were mistaken for field names")
	}
	body = append(body[:len(body)-1], `,"tools":[]}`...)
	original := bytes.Clone(body)
	if !JSONMayContainAnyField(body, "tools") || !bytes.Equal(body, original) {
		t.Fatal("escaped content hid a later key or changed caller bytes")
	}
}

func BenchmarkJSONFieldHint(b *testing.B) {
	for _, size := range []int{128, 1 << 20, 10 << 20} {
		b.Run(fmt.Sprintf("bytes=%d", size), func(b *testing.B) {
			body := []byte(`{"input":[{"role":"user","content":"` + strings.Repeat("x", size) + `"}]}`)
			b.SetBytes(int64(len(body)))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				JSONMayContainAnyField(body, "tools", "summary", "generate_summary")
			}
		})
	}
}

func FuzzJSONFieldHintDoesNotMissDecodedKeys(f *testing.F) {
	for _, body := range []string{
		`{"tools":[]}`, `{"in\u0070ut":[{"\u0069d":"value"}]}`,
		`{"content":"\\\"tools\\\"","reasoning":{"summary":null}}`,
		`[{"metadata":{"generate_summary":false}},{"content":"\\"}]`,
		`{"extra_body":{"google":{"thinking_config":{"include_thoughts":true}}}}`,
	} {
		f.Add([]byte(body))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		if len(body) > 1<<20 || !json.Valid(body) {
			return
		}
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.UseNumber()
		var value any
		if err := decoder.Decode(&value); err != nil {
			t.Fatal(err)
		}
		var hasKey func(any, string) bool
		hasKey = func(value any, field string) bool {
			switch value := value.(type) {
			case map[string]any:
				for key, child := range value {
					if key == field || hasKey(child, field) {
						return true
					}
				}
			case []any:
				for _, child := range value {
					if hasKey(child, field) {
						return true
					}
				}
			}
			return false
		}
		for _, field := range []string{"id", "tools", "encrypted_content", "summary", "generate_summary", "extra_body", "reasoning_effort"} {
			if hasKey(value, field) && !JSONMayContainAnyField(body, field) {
				t.Fatalf("decoded field %q was missed in %s", field, body)
			}
		}
	})
}
