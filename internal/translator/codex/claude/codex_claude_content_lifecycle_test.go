package claude

import (
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

// assertCodexClaudeBlocks checks the client-visible block lifecycle, including terminal ordering.
func assertCodexClaudeBlocks(t *testing.T, outputs [][]byte) []gjson.Result {
	t.Helper()
	var blocks []gjson.Result
	open, terminal := -1, false
	for _, chunk := range outputs {
		for _, line := range strings.Split(string(chunk), "\n") {
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			event := gjson.Parse(strings.TrimPrefix(line, "data: "))
			index := int(event.Get("index").Int())
			switch event.Get("type").String() {
			case "content_block_start":
				if terminal || open != -1 || index != len(blocks) {
					t.Fatalf("invalid block start: %s", event.Raw)
				}
				open = index
				blocks = append(blocks, event.Get("content_block"))
			case "content_block_delta":
				if terminal || open != index {
					t.Fatalf("delta outside its block: %s", event.Raw)
				}
			case "content_block_stop":
				if terminal || open != index {
					t.Fatalf("stop outside its block: %s", event.Raw)
				}
				open = -1
			case "message_delta", "message_stop":
				if open != -1 {
					t.Fatal("terminal with an open content block")
				}
				terminal = true
			}
		}
	}
	if open != -1 {
		t.Fatal("content block was never closed")
	}
	return blocks
}

func TestCodexClaudeTextBlockLifecycle(t *testing.T) {
	const text = `{"type":"response.output_text.delta","delta":"answer"}`
	for _, tc := range []struct {
		name   string
		events []string
		blocks int
	}{
		{"missing start and done", []string{text}, 1},
		{"nontext part", []string{`{"type":"response.content_part.added","part":{"type":"refusal"}}`, `{"type":"response.content_part.done","part":{"type":"refusal"}}`}, 0},
		{"duplicate part boundaries", []string{`{"type":"response.content_part.added","part":{"type":"output_text"}}`, `{"type":"response.content_part.added","part":{"type":"output_text"}}`, text, `{"type":"response.content_part.done","part":{"type":"output_text"}}`, `{"type":"response.content_part.done","part":{"type":"output_text"}}`}, 1},
		{"item done without part done", []string{text, `{"type":"response.output_item.done","item":{"type":"message","content":[{"type":"output_text","text":"answer"}]}}`}, 1},
		{"text then reasoning", []string{text, `{"type":"response.reasoning_summary_text.delta","delta":"thought"}`}, 2},
		{"reasoning then text", []string{`{"type":"response.reasoning_summary_text.delta","delta":"thought"}`, text}, 2},
		{"text then tool", []string{text, `{"type":"response.output_item.added","item":{"type":"function_call","name":"run","call_id":"call_a"}}`}, 2},
		{"text then web search", []string{text, `{"type":"response.output_item.done","item":{"type":"web_search_call","id":"ws_test","action":{"type":"search","query":"q"}}}`}, 3},
	} {
		for _, status := range []string{"completed", "incomplete"} {
			t.Run(tc.name+"/"+status, func(t *testing.T) {
				var state any
				var output [][]byte
				for _, event := range append(append([]string(nil), tc.events...), `{"type":"response.`+status+`","response":{"status":"`+status+`"}}`) {
					output = append(output, ConvertCodexResponseToClaude(t.Context(), "", nil, nil, []byte("data: "+event), &state)...)
				}
				if blocks := assertCodexClaudeBlocks(t, output); len(blocks) != tc.blocks {
					t.Fatalf("got %d blocks, want %d", len(blocks), tc.blocks)
				}
			})
		}
	}
}
