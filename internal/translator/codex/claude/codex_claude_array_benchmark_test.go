package claude

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func codexClaudeArrayResponse(count, textBytes int) []byte {
	var out strings.Builder
	out.WriteString(`{"type":"response.completed","response":{"id":"resp_fixture","model":"fixture","status":"completed","usage":{"input_tokens":12,"output_tokens":5,"input_tokens_details":{"cached_tokens":3}},"output":[`)
	for index := range count {
		if index > 0 {
			out.WriteByte(',')
		}
		fmt.Fprintf(&out, `{"type":"message","content":[{"type":"output_text","text":%q}]},{"type":"reasoning","summary":[{"text":"first"},{"text":"second"}],"encrypted_content":"opaque"},{"type":"web_search_call","id":"search_%d","action":{"query":"query"},"results":[{"url":"https://fixture.test","title":"source"}]},{"type":"function_call","call_id":"call_%d","name":"normal","arguments":"{\"n\":9007199254740993}"},{"type":"web_search_call","id":"search_%d","action":{"query":"duplicate"}}`, strings.Repeat("x", textBytes), index, index, index)
	}
	out.WriteString(`]}}`)
	return []byte(out.String())
}

func TestCodexClaudeResponseArrayPreservesMixedContentAndDeduplication(t *testing.T) {
	for _, count := range []int{0, 1, 3} {
		body := codexClaudeArrayResponse(count, 4)
		original := bytes.Clone(body)
		out := ConvertCodexResponseToClaudeNonStream(t.Context(), "fixture", nil, nil, body, nil)
		if !bytes.Equal(body, original) || !gjson.ValidBytes(out) {
			t.Fatal("response conversion changed input or returned invalid JSON")
		}
		content := gjson.GetBytes(out, "content").Array()
		if len(content) != 5*count || gjson.GetBytes(out, "usage.cache_read_input_tokens").Int() != 3 || gjson.GetBytes(out, "usage.input_tokens").Int() != 9 {
			t.Fatal("content count or cache usage changed")
		}
		if count == 0 {
			if gjson.GetBytes(out, "content").Raw != "[]" || gjson.GetBytes(out, "stop_reason").String() != "end_turn" {
				t.Fatal("empty response contract changed")
			}
			continue
		}
		for index := range count {
			parts := content[index*5 : index*5+5]
			if parts[0].Get("text").String() != "xxxx" || parts[1].Get("thinking").String() != "first\n\nsecond" || parts[1].Get("signature").String() != "opaque" {
				t.Fatal("text, summary paragraphs or opaque signature changed")
			}
			if parts[2].Get("id").String() != fmt.Sprintf("search_%d", index) || parts[2].Get("input.query").String() != "query" || parts[3].Get("tool_use_id").String() != parts[2].Get("id").String() || parts[3].Get("content.0.url").String() != "https://fixture.test" {
				t.Fatal("search pairing, order or duplicate suppression changed")
			}
			if parts[4].Get("id").String() != fmt.Sprintf("call_%d", index) || parts[4].Get("input.n").Raw != "9007199254740993" || gjson.GetBytes(out, "stop_reason").String() != "tool_use" {
				t.Fatal("tool payload or stop reason changed")
			}
		}
	}
}

func TestCodexSearchResultArrayPreservesEmptyAndFallback(t *testing.T) {
	for _, tc := range []struct {
		root, item string
		wantNil    bool
		wantCount  int
	}{
		{`{}`, `{}`, true, 0},
		{`{}`, `{"results":[]}`, false, 0},
		{`{}`, `{"results":[{"title":"no URL"}]}`, false, 0},
		{`{"results":[{"url":"https://fixture.test/a"},{"title":"no URL"},{"url":"https://fixture.test/b","title":"second"}]}`, `{}`, false, 2},
		{`{"results":[{"url":"ignored"}]}`, `{"results":[]}`, false, 0},
	} {
		out := codexWebSearchResultContent(gjson.Parse(tc.root), gjson.Parse(tc.item))
		if (out == nil) != tc.wantNil || len(gjson.ParseBytes(out).Array()) != tc.wantCount {
			t.Fatal("search results changed absent/empty or filtered-item semantics")
		}
		if tc.wantCount == 2 && (gjson.GetBytes(out, "0.title").String() != "https://fixture.test/a" || gjson.GetBytes(out, "1.url").String() != "https://fixture.test/b") {
			t.Fatal("search result ordering or fallback title changed")
		}
	}
}

func BenchmarkCodexClaudeResponseArrays(b *testing.B) {
	for _, tc := range []struct{ count, textBytes int }{{0, 0}, {1, 128}, {100, 128}, {1000, 128}, {1, 1 << 20}, {1, 10 << 20}} {
		b.Run(fmt.Sprintf("items=%d/textBytes=%d", tc.count, tc.textBytes), func(b *testing.B) {
			body := codexClaudeArrayResponse(tc.count, tc.textBytes)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				ConvertCodexResponseToClaudeNonStream(context.Background(), "fixture", nil, nil, body, nil)
			}
		})
	}
}
