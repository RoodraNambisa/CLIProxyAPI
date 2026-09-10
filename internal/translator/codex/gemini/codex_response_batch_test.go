package gemini

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexGeminiNonStreamPartOrder(t *testing.T) {
	raw := []byte(`{"type":"response.incomplete","response":{"id":"fixture","created_at":1,"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"usage":{"input_tokens":7,"output_tokens":3},"output":[{"type":"function_call","name":"first","arguments":"{\"n\":9007199254740993}"},{"type":"function_call","name":"second","arguments":"{}"},{"type":"reasoning","summary":[{"type":"summary_text","text":"summary"}],"content":[{"type":"reasoning_text","text":"reasoning"},{"type":"text","text":"tail"},{"type":"text","text":""}]},{"type":"message","content":[{"type":"output_text","text":""},{"type":"output_text","text":"answer"},{"type":"refusal","refusal":"ignored"}]},{"type":"function_call","name":"broken","arguments":"{"},{"type":"image_generation_call","result":"ZmFrZQ==","output_format":"webp"},{"type":"function_call","name":"last","arguments":"{}"}]}}`)
	before := bytes.Clone(raw)
	got := ConvertCodexResponseToGeminiNonStream(context.Background(), "fixture-model", nil, nil, raw, nil)
	parts := gjson.GetBytes(got, "candidates.0.content.parts").Array()
	if len(parts) != 9 {
		t.Fatalf("got %d parts: %s", len(parts), got)
	}
	for i, name := range map[int]string{0: "first", 1: "second", 8: "last"} {
		if parts[i].Get("functionCall.name").String() != name {
			t.Fatalf("part %d: %s", i, parts[i].Raw)
		}
	}
	if parts[0].Get("functionCall.args.n").Raw != "9007199254740993" {
		t.Fatal("tool integer lost precision")
	}
	for i, text := range []string{"summary", "reasoning", "tail", "", "answer"} {
		part := parts[i+2]
		if part.Get("text").String() != text || !part.Get("text").Exists() || part.Get("thought").Bool() != (i < 3) {
			t.Fatalf("text part %d: %s", i, part.Raw)
		}
	}
	if parts[7].Get("inlineData.mimeType").String() != "image/webp" || parts[7].Get("inlineData.data").String() != "ZmFrZQ==" {
		t.Fatal("image part changed")
	}
	if gjson.GetBytes(got, "candidates.0.finishReason").String() != "MAX_TOKENS" || gjson.GetBytes(got, "usageMetadata.totalTokenCount").Int() != 10 || gjson.GetBytes(got, "responseId").String() != "fixture" {
		t.Fatalf("metadata changed: %s", got)
	}
	if !bytes.Equal(raw, before) {
		t.Fatal("source response mutated")
	}
}

func TestCodexGeminiNonStreamEmptyPartArrays(t *testing.T) {
	for _, output := range []string{"", `,"output":[]`, `,"output":null`, `,"output":[{"type":"unknown"},{"type":"reasoning","summary":""},{"type":"image_generation_call","result":""}]`} {
		got := ConvertCodexResponseToGeminiNonStream(context.Background(), "fixture", nil, nil, []byte(`{"type":"response.completed","response":{"id":"empty"`+output+`}}`), nil)
		if parts := gjson.GetBytes(got, "candidates.0.content.parts"); !parts.IsArray() || len(parts.Array()) != 0 {
			t.Fatalf("empty output did not preserve array: %s", got)
		}
	}
}

func BenchmarkCodexGeminiNonStreamParts(b *testing.B) {
	for _, count := range []int{1, 100, 1000} {
		b.Run(fmt.Sprintf("groups%d", count), func(b *testing.B) {
			var payload strings.Builder
			payload.WriteString(`{"type":"response.completed","response":{"id":"bench","output":[`)
			for i := 0; i < count; i++ {
				if i > 0 {
					payload.WriteByte(',')
				}
				payload.WriteString(`{"type":"reasoning","summary":"reasoning"},{"type":"message","content":[{"type":"output_text","text":"answer"}]},{"type":"function_call","name":"tool","arguments":"{}"},{"type":"image_generation_call","result":"ZmFrZQ=="}`)
			}
			payload.WriteString(`]}}`)
			benchmarkCodexGeminiParts(b, []byte(payload.String()))
		})
	}
	b.Run("text10MiB", func(b *testing.B) {
		raw := []byte(`{"type":"response.completed","response":{"output":[{"type":"message","content":[{"type":"output_text","text":` + strconv.Quote(strings.Repeat("x", 10<<20)) + `}]}]}}`)
		benchmarkCodexGeminiParts(b, raw)
	})
}

func benchmarkCodexGeminiParts(b *testing.B, raw []byte) {
	b.Helper()
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := ConvertCodexResponseToGeminiNonStream(context.Background(), "fixture", nil, nil, raw, nil); len(got) == 0 {
			b.Fatal("missing response")
		}
	}
}
