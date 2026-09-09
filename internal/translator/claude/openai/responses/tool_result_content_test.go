package responses

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/tidwall/gjson"
)

func TestClaudeResponsesToolResultContentKeepsMediaAndOrder(t *testing.T) {
	for _, kind := range []string{"function_call_output", "custom_tool_call_output"} {
		raw := []byte(`{"input":[{"type":"` + kind + `","call_id":"original-pair","output":[{"type":"input_text","text":"before"},{"type":"input_image","image_url":"data:image/png;base64,aW1hZ2U="},{"type":"input_file","file_data":"data:application/pdf;base64,cGRm"},{"type":"output_text","text":"after"}]}]}`)
		before := bytes.Clone(raw)
		out := ConvertOpenAIResponsesRequestToClaude("claude-fixture", raw, false)
		result := gjson.GetBytes(out, "messages.0.content.0")
		parts := result.Get("content").Array()
		if !result.Get("content").IsArray() || len(parts) != 4 || parts[0].Get("text").String() != "before" ||
			parts[1].Get("type").String() != "image" || parts[1].Get("source.media_type").String() != "image/png" || parts[1].Get("source.data").String() != "aW1hZ2U=" ||
			parts[2].Get("type").String() != "document" || parts[2].Get("source.media_type").String() != "application/pdf" || parts[2].Get("source.data").String() != "cGRm" ||
			parts[3].Get("text").String() != "after" || result.Get("tool_use_id").String() != "original-pair" || !bytes.Equal(raw, before) {
			t.Fatal("structured tool output lost media, order, pairing or source immutability")
		}
		clear(raw)
		if gjson.GetBytes(out, "messages.0.content.0.content.2.source.data").String() != "cGRm" {
			t.Fatal("structured output retained a released request buffer")
		}
	}
}

func TestClaudeResponsesToolResultContentPreservesOpaqueAndInvalidArrays(t *testing.T) {
	for _, output := range []string{
		`[]`, `[{"type":"input_text","text":"known"},{"type":"future","value":9007199254740993}]`,
		`[{"type":"input_text","text":3}]`, `[{"type":"input_image","image_url":"data:image/png,raw"}]`,
		`[{"type":"input_file","file_data":"data:application/pdf;base64,"}]`,
		`[{"type":"input_file","file_id":"unsupported"}]`, `["business","JSON"]`,
		`{"type":"input_image","image_url":"business-value"}`, `"[{\"type\":\"input_text\",\"text\":\"business\"}]"`,
	} {
		raw := []byte(`{"input":[{"type":"custom_tool_call_output","call_id":"paired","output":` + output + `}]}`)
		out := ConvertOpenAIResponsesRequestToClaude("claude-fixture", raw, false)
		result := gjson.GetBytes(out, "messages.0.content.0.content")
		if result.Type != gjson.String || result.String() != gjson.Parse(output).String() {
			t.Fatal("unsupported content or business JSON was partially reinterpreted")
		}
	}
	for _, tc := range []struct{ part, path, want string }{
		{`{"type":"input_text","text":"one"}`, "content", "one"},
		{`{"type":"input_image","url":"https://example.invalid/fixture.png"}`, "content.0.source.url", "https://example.invalid/fixture.png"},
		{`{"type":"input_file","file_data":"cGRm"}`, "content.0.source.media_type", "application/octet-stream"},
		{`{"type":"input_image","image_url":"data:;base64,aW1hZ2U="}`, "content.0.source.media_type", "application/octet-stream"},
	} {
		raw := []byte(fmt.Sprintf(`{"input":[{"type":"function_call_output","call_id":"paired","output":[%s]}]}`, tc.part))
		out := ConvertOpenAIResponsesRequestToClaude("claude-fixture", raw, false)
		if gjson.GetBytes(out, "messages.0.content.0."+tc.path).String() != tc.want {
			t.Fatal("valid structured content did not retain its supported representation")
		}
	}
}
