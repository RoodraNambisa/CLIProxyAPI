package claude

import (
	"fmt"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func BenchmarkCodexClaudeHistory(b *testing.B) {
	for _, turns := range []int{1, 100, 1000} {
		b.Run(fmt.Sprint(turns), func(b *testing.B) {
			var body strings.Builder
			body.WriteString(`{"system":"system","tools":[{"name":"read","input_schema":{"type":"object"}}],"messages":[`)
			for i := 0; i < turns; i++ {
				if i > 0 {
					body.WriteByte(',')
				}
				fmt.Fprintf(&body, `{"role":"assistant","content":[{"type":"text","text":%q},{"type":"tool_use","id":"call-%d","name":"read","input":{"value":%d}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"call-%d","content":[{"type":"text","text":%q}]}]}`, strings.Repeat("a", 128), i, i, i, strings.Repeat("b", 128))
			}
			body.WriteString(`]}`)
			request := []byte(body.String())
			if output := ConvertClaudeRequestToCodex("gpt-5", request, false); !gjson.ValidBytes(output) || gjson.GetBytes(output, "input.#").Int() != int64(3*turns+1) {
				b.Fatal("invalid benchmark translation")
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(request)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ConvertClaudeRequestToCodex("gpt-5", request, false)
			}
		})
	}
}
