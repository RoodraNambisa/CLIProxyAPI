package chat_completions

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexChatRequestOwnsHistoryAfterSourceRelease(t *testing.T) {
	for _, fixture := range []string{
		`{"messages":[{"role":"user","content":"text \"quoted\" \\path\n"}],"reasoning_effort":"high"}`,
		`{"messages":[{"role":"user","content":[{"type":"text","text":"text"},{"type":"image_url","image_url":{"url":"data:image/png;base64,AAAA"}},{"type":"file","file":{"filename":"fixture.pdf","file_data":"data:application/pdf;base64,AAAA"}}]}]}`,
		`{"messages":[{"role":"assistant","tool_calls":[{"id":"call_one","type":"function","function":{"name":"read","arguments":"{}"}}]},{"role":"tool","tool_call_id":"call_one","content":"result"}],"tools":[{"type":"function","function":{"name":"read","parameters":{"type":"object"}}}]}`,
		`{"messages":null,"messages":[{"role":"user","content":"later"}]}`,
		`{"messag\u0065s":[{"role":"user","content":"escaped key"}]}`,
	} {
		body := []byte(fixture)
		out := ConvertOpenAIRequestToCodex("fixture", body, true)
		want := bytes.Clone(out)
		if !gjson.ValidBytes(out) || string(body) != fixture {
			t.Fatal("conversion corrupted output or source")
		}
		clear(body)
		if !bytes.Equal(out, want) {
			t.Fatal("converted history retained the released source")
		}
	}
}

func BenchmarkCodexChatOwnedRequest(b *testing.B) {
	for _, size := range []int{1 << 20, 10 << 20} {
		b.Run(fmt.Sprintf("bytes=%d", size), func(b *testing.B) {
			body := []byte(`{"messages":[{"role":"user","content":"` + strings.Repeat("x", size) + `"}]}`)
			b.SetBytes(int64(len(body)))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				ConvertOpenAIRequestToCodex("fixture", body, true)
			}
		})
	}
}

func TestCodexChatControlEditsKeepHistoryAndSchema(t *testing.T) {
	body := []byte(`{"messages":[{"role":"user","content":"keep"}],"tools":[{"type":"function","function":{"name":"read","strict":true,"parameters":{"type":"object","properties":{"input":{"type":"string"}},"required":["input"]}}}],"tool_choice":{"type":"function","function":{"name":"read"}},"response_format":{"type":"json_schema","json_schema":{"name":"answer","strict":true,"schema":{"type":"object","properties":{"input":{"type":"string"}},"required":["input"],"additionalProperties":false}}},"text":{"verbosity":"low"},"store":true}`)
	out := ConvertOpenAIRequestToCodex("fixture", body, true)
	if gjson.GetBytes(out, "input.0.content.0.text").Str != "keep" || gjson.GetBytes(out, "store").Bool() || gjson.GetBytes(out, "tool_choice.name").Str != "read" || !gjson.GetBytes(out, "tools.0.strict").Bool() {
		t.Fatal("control edits changed input, storage or tool selection")
	}
	if gjson.GetBytes(out, "text.format.schema.properties.input.type").Str != "string" || !gjson.GetBytes(out, "text.format.strict").Bool() || gjson.GetBytes(out, "text.verbosity").Str != "low" || gjson.GetBytes(out, "tools.0.parameters.required.0").Str != "input" {
		t.Fatal("history fill changed similarly named fields in schemas")
	}
}
