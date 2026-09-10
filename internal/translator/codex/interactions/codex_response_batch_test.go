package interactions

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexInteractionsNonStreamStepOrder(t *testing.T) {
	response := `{"id":"fixture","model":"actual","status":"incomplete","usage":{"input_tokens":7,"output_tokens":3,"output_tokens_details":{"reasoning_tokens":1},"input_tokens_details":{"cached_tokens":2}},"output":[{"type":"message","content":[{"type":"output_text","text":"one"},{"type":"output_text","text":""},{"type":"output_text","content":"two"}]},{"type":"reasoning","content":[{"text":"reason"},{"summary_text":"tail"}]},{"type":"function_call","id":"item","call_id":"call","name":"tool","arguments":"{\"n\":9007199254740993}"},{"type":"function_call","name":"broken","arguments":"{"},{"type":"image_generation_call","result":"ZmFrZQ==","output_format":"jpeg"},{"type":"message","content":[]},{"type":"reasoning","content":""},{"type":"image_generation_call","result":""},{"type":"tool_call","id":"fallback","name":"last","arguments":{}}]}`
	for _, raw := range [][]byte{[]byte(response), []byte(`{"type":"response.incomplete","response":` + response + `}`)} {
		before := bytes.Clone(raw)
		got := ConvertCodexResponseToInteractionsNonStream(context.Background(), "fallback", nil, nil, raw, nil)
		steps := gjson.GetBytes(got, "steps").Array()
		if len(steps) != 5 {
			t.Fatalf("got %d steps: %s", len(steps), got)
		}
		if steps[0].Get("content.#").Int() != 2 || steps[0].Get("content.0.text").String() != "one" || steps[0].Get("content.1.text").String() != "two" || steps[1].Get("content.0.text").String() != "reason\ntail" {
			t.Fatal("text order or filtering changed")
		}
		if steps[2].Get("call_id").String() != "call" || steps[2].Get("arguments.n").Raw != "9007199254740993" || steps[4].Get("call_id").String() != "fallback" {
			t.Fatal("tool identity or argument precision changed")
		}
		if steps[3].Get("content.0.mime_type").String() != "image/jpeg" || steps[3].Get("content.0.data").String() != "ZmFrZQ==" {
			t.Fatal("image result changed")
		}
		if gjson.GetBytes(got, "model").String() != "actual" || gjson.GetBytes(got, "status").String() != "incomplete" || gjson.GetBytes(got, "usage.output_tokens").Int() != 2 || gjson.GetBytes(got, "usage.cached_tokens").Int() != 2 || gjson.GetBytes(got, "usage.total_tokens").Int() != 10 {
			t.Fatalf("status, model or usage changed: %s", got)
		}
		if !bytes.Equal(raw, before) {
			t.Fatal("source response mutated")
		}
	}
}

func TestCodexInteractionsNonStreamEmptySteps(t *testing.T) {
	for _, output := range []string{"", `,"output":[]`, `,"output":null`, `,"output":[{"type":"unknown"},{"type":"message","content":[{"text":""}]},{"type":"reasoning","summary":""}]`} {
		got := ConvertCodexResponseToInteractionsNonStream(context.Background(), "fixture", nil, nil, []byte(`{"id":"empty"`+output+`}`), nil)
		if steps := gjson.GetBytes(got, "steps"); !steps.IsArray() || len(steps.Array()) != 0 {
			t.Fatalf("empty output did not preserve array: %s", got)
		}
	}
}

func BenchmarkCodexInteractionsNonStreamSteps(b *testing.B) {
	for _, count := range []int{1, 100, 1000} {
		b.Run(fmt.Sprintf("groups%d", count), func(b *testing.B) {
			var payload strings.Builder
			payload.WriteString(`{"type":"response.completed","response":{"id":"bench","output":[`)
			for i := 0; i < count; i++ {
				if i > 0 {
					payload.WriteByte(',')
				}
				payload.WriteString(`{"type":"reasoning","summary":"reasoning"},{"type":"message","content":[{"type":"output_text","text":"one"},{"type":"output_text","text":"two"}]},{"type":"function_call","name":"tool","arguments":"{}"},{"type":"image_generation_call","result":"ZmFrZQ=="}`)
			}
			payload.WriteString(`]}}`)
			benchmarkCodexInteractionsSteps(b, []byte(payload.String()))
		})
	}
	b.Run("text10MiB", func(b *testing.B) {
		raw := []byte(`{"type":"response.completed","response":{"id":"bench","output":[{"type":"message","content":[{"type":"output_text","text":` + strconv.Quote(strings.Repeat("x", 10<<20)) + `}]}]}}`)
		benchmarkCodexInteractionsSteps(b, raw)
	})
}

func benchmarkCodexInteractionsSteps(b *testing.B, raw []byte) {
	b.Helper()
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := ConvertCodexResponseToInteractionsNonStream(context.Background(), "fixture", nil, nil, raw, nil); len(got) == 0 {
			b.Fatal("missing response")
		}
	}
}
