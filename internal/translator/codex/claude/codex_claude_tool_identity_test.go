package claude

import (
	"fmt"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestClaudeToolIdentityIsRequiredAcrossResponseModes(t *testing.T) {
	for _, fields := range []string{`"name":"run","call_id":"call_a"`, `"name":"run"`, `"call_id":"call_a"`, `"name":"","call_id":""`} {
		for _, status := range []string{"completed", "incomplete"} {
			t.Run(fields+"/"+status, func(t *testing.T) {
				item := `{"type":"function_call","arguments":"{}",` + fields + `}`
				terminal := []byte(fmt.Sprintf(`{"type":"response.%s","response":{"status":%q,"output":[%s]}}`, status, status, item))
				valid := gjson.Get(item, "name").String() != "" && gjson.Get(item, "call_id").String() != ""
				count, reason := 0, "end_turn"
				if valid {
					count, reason = 1, "tool_use"
				}
				out := ConvertCodexResponseToClaudeNonStream(t.Context(), "", nil, nil, terminal, nil)
				if gjson.GetBytes(out, "content.#").Int() != int64(count) || gjson.GetBytes(out, "stop_reason").String() != reason {
					t.Fatalf("invalid non-stream tool: %s", out)
				}
				var state any
				chunks := ConvertCodexResponseToClaude(t.Context(), "", nil, nil, []byte(`data: {"type":"response.output_item.done","output_index":0,"item":`+item+`}`), &state)
				chunks = append(chunks, ConvertCodexResponseToClaude(t.Context(), "", nil, nil, append([]byte("data: "), terminal...), &state)...)
				blocks := assertCodexClaudeBlocks(t, chunks)
				if len(blocks) != count || !strings.Contains(stringJoinCodexClaudeChunks(chunks), `"stop_reason":"`+reason+`"`) {
					t.Fatal("response modes disagree on unresolved tool identity")
				}
			})
		}
	}
}
