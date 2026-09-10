package responses

import (
	"testing"

	"github.com/tidwall/gjson"
)

func TestClaudeResponsesPreservesRefusalHistoryInOrder(t *testing.T) {
	for _, raw := range []string{`"cannot help with that"`, `""`, `null`, `42`, `true`, `{}`} {
		for _, stream := range []bool{false, true} {
			body := []byte(`{"input":[{"role":"user","content":"first"},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"before"},{"type":"refusal","refusal":` + raw + `,"cache_control":{"type":"ephemeral"}},{"type":"output_text","text":"after"}]},{"role":"user","content":"next"}]}`)
			out := ConvertOpenAIResponsesRequestToClaude("claude-sonnet-4-5", body, stream)
			parts := gjson.GetBytes(out, "messages.1.content").Array()
			if gjson.GetBytes(out, "messages.1.role").String() != "assistant" || len(gjson.GetBytes(out, "messages").Array()) != 3 {
				t.Fatal("refusal history changed the surrounding turns")
			}
			if raw == `"cannot help with that"` {
				if len(parts) != 3 || parts[1].Get("text").String() != "cannot help with that" || parts[1].Get("cache_control.type").String() != "ephemeral" {
					t.Error("valid refusal or its cache annotation was dropped")
				}
			} else if len(parts) != 2 {
				t.Errorf("empty or non-string refusal %s invented text", raw)
			}
			if len(parts) < 2 || parts[0].Get("text").String() != "before" || parts[len(parts)-1].Get("text").String() != "after" {
				t.Error("refusal reordered adjacent content")
			}
		}
	}
}
