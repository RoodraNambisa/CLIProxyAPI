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
