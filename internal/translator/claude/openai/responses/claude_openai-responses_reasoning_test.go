package responses

import (
	"fmt"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestClaudeResponsesReasoningBlocksKeepIdentityAndOrder(t *testing.T) {
	events := []string{
		`{"type":"message_start","message":{"id":"result"}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"thinking"}}`,
		`{"type":"content_block_start","index":1,"content_block":{"type":"text"}}`,
		`{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"answer"}}`,
		`{"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"pair","name":"lookup"}}`,
		`{"type":"content_block_start","index":3,"content_block":{"type":"thinking"}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"first"}}`,
		`{"type":"content_block_delta","index":3,"delta":{"type":"thinking_delta","thinking":"second"}}`,
		`{"type":"content_block_delta","index":9,"delta":{"type":"thinking_delta","thinking":"unknown"}}`,
		`{"type":"content_block_delta","index":1,"delta":{"type":"thinking_delta","thinking":"wrong type"}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"late"}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"text"}}`,
		`{"type":"content_block_stop","index":1}`,
		`{"type":"content_block_stop","index":2}`,
		`{"type":"content_block_stop","index":3}`,
		`{"type":"content_block_start","index":4,"content_block":{"type":"thinking"}}`,
		`{"type":"content_block_stop","index":4}`, `{"type":"message_stop"}`,
	}
	var state any
	counts := make(map[string]int)
	var completed gjson.Result
	for _, event := range events {
		for _, chunk := range ConvertClaudeResponseToOpenAIResponses(t.Context(), "", nil, nil, []byte("data: "+event), &state) {
			for _, line := range strings.Split(string(chunk), "\n") {
				if !strings.HasPrefix(line, "data:") {
					continue
				}
				data := gjson.Parse(strings.TrimPrefix(line, "data:"))
				kind := data.Get("type").String()
				counts[kind]++
				index := data.Get("output_index").Int()
				if strings.HasPrefix(kind, "response.reasoning_summary_") {
					if index != 0 && index != 3 && index != 4 || data.Get("item_id").String() != fmt.Sprintf("rs_result_%d", index) {
						t.Errorf("reasoning event was assigned another block: %s", data.Raw)
					}
					if kind == "response.reasoning_summary_text.delta" && (index == 0 && data.Get("delta").String() != "first" || index == 3 && data.Get("delta").String() != "second") {
						t.Errorf("reasoning delta was mixed: %s", data.Raw)
					}
				}
				if kind == "response.completed" {
					completed = data.Get("response")
				}
			}
		}
	}
	if counts["response.output_item.added"] != 5 || counts["response.output_item.done"] != 5 || counts["response.reasoning_summary_text.delta"] != 2 || counts["response.reasoning_summary_text.done"] != 3 || counts["response.reasoning_summary_part.done"] != 3 {
		t.Errorf("reasoning lifecycle mismatch: %v", counts)
	}
	raw := []byte("data: " + strings.Join(events, "\n\ndata: ") + "\n\n")
	nonstream := gjson.ParseBytes(ConvertClaudeResponseToOpenAIResponsesNonStream(t.Context(), "", nil, nil, raw, nil))
	for _, result := range []gjson.Result{completed, nonstream} {
		if result.Get("output.#").Int() != 5 || result.Get("output.0.id").String() != "rs_result_0" || result.Get("output.0.summary.0.text").String() != "first" || result.Get("output.1.content.0.text").String() != "answer" || result.Get("output.2.call_id").String() != "pair" || result.Get("output.3.summary.0.text").String() != "second" || result.Get("output.4.id").String() != "rs_result_4" || result.Get("output.4.summary.0.text").String() != "" {
			t.Errorf("reasoning order or summary lost: %s", result.Get("output").Raw)
		}
	}
}
