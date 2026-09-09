package claude

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexClaudeIncompleteReasonOverridesToolFinish(t *testing.T) {
	for _, tc := range []struct{ reason, finish string }{{"max_tokens", "max_tokens"}, {"max_output_tokens", "max_tokens"}, {"content_filter", "refusal"}, {"", "tool_use"}} {
		t.Run(tc.reason+tc.finish, func(t *testing.T) {
			event, status := "response.incomplete", "incomplete"
			if tc.reason == "" {
				event, status = "response.completed", "completed"
			}
			item := `{"type":"function_call","id":"fc_first","call_id":"first","name":"run","arguments":"{}"}`
			terminal := []byte(fmt.Sprintf(`{"type":%q,"response":{"status":%q,"incomplete_details":{"reason":%q},"output":[%s],"usage":{"input_tokens":2,"output_tokens":3}}}`, event, status, tc.reason, item))
			out := ConvertCodexResponseToClaudeNonStream(t.Context(), "model", nil, nil, terminal, nil)
			if gjson.GetBytes(out, "stop_reason").String() != tc.finish || gjson.GetBytes(out, "content.0.name").String() != "run" {
				t.Fatal("non-stream tool masked the terminal reason")
			}
			var state any
			ConvertCodexResponseToClaude(t.Context(), "model", nil, nil, []byte(`data: {"type":"response.output_item.added","item":`+item+`}`), &state)
			chunks := ConvertCodexResponseToClaude(t.Context(), "model", nil, nil, append([]byte("data: "), terminal...), &state)
			finishes := 0
			for _, line := range bytes.Split(bytes.Join(chunks, nil), []byte("\n")) {
				if !bytes.HasPrefix(line, []byte("data: ")) {
					continue
				}
				if r := gjson.GetBytes(line[6:], "delta.stop_reason"); r.Exists() {
					finishes++
					if r.String() != tc.finish {
						t.Fatal("stream tool masked the terminal reason")
					}
				}
			}
			if finishes != 1 {
				t.Fatalf("got %d terminal reasons", finishes)
			}
			if late := ConvertCodexResponseToClaude(t.Context(), "model", nil, nil, append([]byte("data: "), terminal...), &state); len(late) != 0 {
				t.Fatal("duplicate terminal was delivered")
			}
		})
	}
}
