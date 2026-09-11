package responses

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexResponsesDefaultControlsNeedOnlyOwnedOutputBuffer(t *testing.T) {
	for _, body := range [][]byte{
		[]byte(`{"input":[{"role":"user","content":"` + strings.Repeat("x", 1<<20) + `"}]}`),
		[]byte(`{"input":"` + strings.Repeat("x", 1<<20) + `"}`),
	} {
		var out []byte
		if allocations := testing.AllocsPerRun(5, func() {
			out = ConvertOpenAIResponsesRequestToCodex("fixture", body, true)
		}); allocations > 1 {
			t.Fatalf("plain Responses conversion allocated %g times; one owned output buffer is sufficient", allocations)
		}
		if !gjson.GetBytes(out, "stream").Bool() || gjson.GetBytes(out, "store").Bool() || !gjson.GetBytes(out, "parallel_tool_calls").Bool() || gjson.GetBytes(out, "include.0").Str != "reasoning.encrypted_content" {
			t.Fatal("allocation optimization changed required controls")
		}
	}
}

func TestCodexResponsesFastPathOwnsResultAfterSourceRelease(t *testing.T) {
	for _, fixture := range []string{
		`{"input":"text \"quoted\" \\path\n","prompt_cache_key":" keep "}`,
		`{"input":[{"type":"message","id":"msg_1","role":"user","content":"text"}],"metadata":{"opaque":{"value":1}}}`,
		`{"in\u0070ut":[{"role":"system","content":[{"type":"input_text","text":"keep","prompt_cache_breakpoint":true}]}],"reasoning":{"effort":"high","summary":"auto"}}`,
		`{"input":[],"tools":[{"type":"web_search_preview"}],"tool_choice":"auto"}`,
	} {
		body := []byte(fixture)
		out := ConvertOpenAIResponsesRequestToCodex("fixture", body, true)
		want := bytes.Clone(out)
		if !gjson.ValidBytes(out) || string(body) != fixture {
			t.Fatal("conversion corrupted output or source bytes")
		}
		clear(body)
		if !bytes.Equal(out, want) {
			t.Fatal("converted output retained the released mutable source")
		}
	}
}

func TestCodexResponsesControlsPrecedeHistoryWithoutChangingValues(t *testing.T) {
	body := []byte(`{"input":[{"type":"message","id":"msg_first","content":"text"}],"reasoning":{"summary":"auto","effort":"high"},"input":[{"type":"function_call_output","call_id":"pair","output":{"id":"business"}}],"prompt_cache_key":" keep ","reasoning":{"summary":"none"}}`)
	out := ConvertOpenAIResponsesRequestToCodex("fixture", body, true)
	var fields []string
	var inputs, reasoning []string
	gjson.ParseBytes(out).ForEach(func(key, value gjson.Result) bool {
		fields = append(fields, key.Str)
		switch key.Str {
		case "input":
			inputs = append(inputs, value.Raw)
		case "reasoning":
			reasoning = append(reasoning, value.Raw)
		}
		return true
	})
	if len(inputs) != 2 || inputs[0] != `[{"type":"message","id":"msg_first","content":"text"}]` || inputs[1] != `[{"type":"function_call_output","call_id":"pair","output":{"id":"business"}}]` {
		t.Fatal("moving controls changed input values, order, or pairing")
	}
	if len(reasoning) != 2 || reasoning[0] != `{"summary":"auto","effort":"high"}` || reasoning[1] != `{"summary":"none"}` || gjson.GetBytes(out, "prompt_cache_key").Str != " keep " {
		t.Fatal("moving controls changed duplicate precedence or cache identity")
	}
	if fields[len(fields)-2] != "input" || fields[len(fields)-1] != "input" {
		t.Fatal("request controls require traversing input history")
	}
}

func BenchmarkCodexResponsesOwnedRequest(b *testing.B) {
	for _, size := range []int{1 << 20, 10 << 20} {
		b.Run(fmt.Sprintf("bytes=%d", size), func(b *testing.B) {
			body := []byte(`{"input":[{"role":"user","content":"` + strings.Repeat("x", size) + `"}]}`)
			b.SetBytes(int64(len(body)))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				ConvertOpenAIResponsesRequestToCodex("fixture", body, true)
			}
		})
	}
}
