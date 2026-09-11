package helps

import (
	"fmt"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexInputHeaderMatchesOriginalLookup(t *testing.T) {
	for _, raw := range []string{
		`{}`, `null`, `[]`, `"text"`,
		`{"id":"msg_first","type":"message"}`,
		`{"content":{"id":"nested","type":"reasoning"},"id":"fc_one","type":"function_call"}`,
		`{"id":null,"id":"msg_later","type":false,"type":"message"}`,
		`{"i\u0064":"msg_first","id":"msg_later","ty\u0070e":"message"}`,
		`{"id":"msg_\"quoted\\id","type":"me\u0073sage"}`,
		`{"content":null,"id":17,"type":{}}`,
		`{"type":"message","content":"` + strings.Repeat(`escaped \" content `, 1<<15) + `","id":"msg_late"}`,
	} {
		item := gjson.Parse(raw)
		id, kind := readCodexInputItemHeader(item)
		if id != item.Get("id").Str || kind != item.Get("type").Str {
			t.Fatalf("header differs from original lookup: id=%q type=%q", id, kind)
		}
	}
}

func FuzzCodexInputHeaderMatchesOriginalLookup(f *testing.F) {
	for _, raw := range []string{`{}`, `{"id":null,"id":"msg_later","type":"message"}`, `{"content":[{"type":"input_text","text":"large"}],"id":"fc_test","type":"function_call"}`, `{"i\u0064":"msg_test","type":"message"}`} {
		f.Add(raw)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		if len(raw) > 1<<20 || !gjson.Valid(raw) {
			return
		}
		item := gjson.Parse(raw)
		id, kind := readCodexInputItemHeader(item)
		if id != item.Get("id").Str || kind != item.Get("type").Str {
			t.Fatalf("header lookup changed for %q", raw)
		}
	})
}

func BenchmarkCodexInputHeaderLayout(b *testing.B) {
	for _, first := range []bool{false, true} {
		b.Run(fmt.Sprintf("header-first=%t", first), func(b *testing.B) {
			fields := []string{`"content":"` + strings.Repeat("x", 10<<20) + `"`, `"id":"msg_test","type":"message"`}
			if first {
				fields[0], fields[1] = fields[1], fields[0]
			}
			item := gjson.Parse(`{` + strings.Join(fields, ",") + `}`)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				readCodexInputItemHeader(item)
			}
		})
	}
}
