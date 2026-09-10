package gemini

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexGeminiStreamPartArrays(t *testing.T) {
	for _, tc := range []struct{ name, event, parts string }{
		{"partial-image", `{"type":"response.image_generation_call.partial_image","item_id":"i","partial_image_b64":"ZmFrZQ==","output_format":"webp"}`, `[{"inlineData":{"data":"ZmFrZQ==","mimeType":"image/webp"}}]`},
		{"image", `{"type":"response.output_item.done","item":{"type":"image_generation_call","id":"i","result":"ZmFrZQ=="}}`, `[{"inlineData":{"data":"ZmFrZQ==","mimeType":"image/png"}}]`},
		{"function", `{"type":"response.output_item.done","item":{"type":"function_call","name":"tool","arguments":"{\"n\":9007199254740993}"}}`, `[{"functionCall":{"name":"tool","args":{"n":9007199254740993}}}]`},
		{"reasoning", `{"type":"response.reasoning_text.delta","delta":"thought"}`, `[{"thought":true,"text":"thought"}]`},
		{"summary", `{"type":"response.reasoning_summary_text.delta","delta":"summary"}`, `[{"thought":true,"text":"summary"}]`},
		{"text", `{"type":"response.output_text.delta","delta":"answer"}`, `[{"text":"answer"}]`},
		{"fallback", `{"type":"response.output_item.done","item":{"type":"message","content":[{"type":"output_text","text":"one"},{"type":"output_text","text":""},{"type":"refusal","text":"ignored"},{"type":"output_text","text":"two"}]}}`, `[{"text":"one"},{"text":"two"}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := []byte("data: " + tc.event)
			before := bytes.Clone(raw)
			var state any
			chunks := ConvertCodexResponseToGemini(t.Context(), "fixture", nil, nil, raw, &state)
			if len(chunks) != 1 {
				t.Fatalf("got %d chunks", len(chunks))
			}
			decode := func(text string) any {
				decoder := json.NewDecoder(strings.NewReader(text))
				decoder.UseNumber()
				var value any
				if err := decoder.Decode(&value); err != nil {
					t.Fatal(err)
				}
				return value
			}
			if got := gjson.GetBytes(chunks[0], "candidates.0.content.parts").Raw; !reflect.DeepEqual(decode(got), decode(tc.parts)) {
				t.Fatalf("parts = %s, want %s", got, tc.parts)
			}
			if !bytes.Equal(raw, before) {
				t.Fatal("input event changed")
			}
		})
	}
}

func BenchmarkCodexGeminiStreamParts(b *testing.B) {
	for _, size := range []int{32, 1 << 20, 10 << 20} {
		b.Run(fmt.Sprintf("text%d", size), func(b *testing.B) {
			raw := []byte(`data: {"type":"response.output_text.delta","delta":` + strconv.Quote(strings.Repeat("x", size)) + `}`)
			benchmarkCodexGeminiStreamParts(b, raw)
		})
	}
	b.Run("fallback1000", func(b *testing.B) {
		part := `{"type":"output_text","text":"answer"}`
		raw := []byte(`data: {"type":"response.output_item.done","item":{"type":"message","content":[` + strings.TrimSuffix(strings.Repeat(part+",", 1000), ",") + `]}}`)
		benchmarkCodexGeminiStreamParts(b, raw)
	})
}

func benchmarkCodexGeminiStreamParts(b *testing.B, raw []byte) {
	b.Helper()
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var state any
		if got := ConvertCodexResponseToGemini(context.Background(), "fixture", nil, nil, raw, &state); len(got) != 1 {
			b.Fatal("missing event")
		}
	}
}
