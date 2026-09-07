package responses

import (
	"fmt"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func claudeResponsesTextFixture() []string {
	return []string{
		`{"type":"message_start","message":{"id":"result"}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"pair","name":"lookup"}}`,
		`{"type":"content_block_start","index":1,"content_block":{"type":"text"}}`,
		`{"type":"content_block_start","index":2,"content_block":{"type":"text"}}`,
		`{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"first"}}`,
		`{"type":"content_block_delta","index":2,"delta":{"type":"text_delta","text":"second"}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"wrong"}}`,
		`{"type":"content_block_stop","index":1}`,
		`{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"late"}}`,
		`{"type":"content_block_start","index":1,"content_block":{"type":"text"}}`,
		`{"type":"content_block_stop","index":1}`,
		`{"type":"content_block_stop","index":2}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"message_stop"}`,
	}
}

func TestClaudeResponsesTextStreamsUseDistinctPositions(t *testing.T) {
	var state any
	counts := make(map[string]int)
	var completed gjson.Result
	for _, event := range claudeResponsesTextFixture() {
		for _, chunk := range ConvertClaudeResponseToOpenAIResponses(t.Context(), "", nil, nil, []byte("data: "+event), &state) {
			for _, line := range strings.Split(string(chunk), "\n") {
				if !strings.HasPrefix(line, "data:") {
					continue
				}
				data := gjson.Parse(strings.TrimPrefix(line, "data:"))
				kind := data.Get("type").String()
				counts[kind]++
				index := data.Get("output_index").Int()
				if strings.HasPrefix(kind, "response.output_text.") || strings.HasPrefix(kind, "response.content_part.") {
					if index < 1 || index > 2 || data.Get("item_id").String() != fmt.Sprintf("msg_result_%d", index) {
						t.Errorf("text event lost its index or identity: %s", data.Raw)
					}
					textPath := ""
					if kind == "response.output_text.done" {
						textPath = "text"
					} else if kind == "response.content_part.done" {
						textPath = "part.text"
					}
					if textPath != "" && (index == 1 && data.Get(textPath).String() != "first" || index == 2 && data.Get(textPath).String() != "second") {
						t.Fatal("text completion lost its full content")
					}
				}
				if kind == "response.output_item.done" && data.Get("item.type").String() == "message" && (index == 1 && data.Get("item.content.0.text").String() != "first" || index == 2 && data.Get("item.content.0.text").String() != "second") {
					t.Fatal("message completion lost text")
				}
				if kind == "response.completed" {
					completed = data.Get("response")
				}
			}
		}
	}
	if counts["response.output_item.added"] != 3 || counts["response.output_item.done"] != 3 || counts["response.output_text.delta"] != 2 || counts["response.output_text.done"] != 2 {
		t.Fatalf("text lifecycle mismatch: %v", counts)
	}
	assertClaudeResponsesTextOrder(t, completed)
}

func assertClaudeResponsesTextOrder(t *testing.T, response gjson.Result) {
	t.Helper()
	if response.Get("output.#").Int() != 3 || response.Get("output.0.call_id").String() != "pair" || response.Get("output.1.id").String() != "msg_result_1" || response.Get("output.1.content.0.text").String() != "first" || response.Get("output.2.id").String() != "msg_result_2" || response.Get("output.2.content.0.text").String() != "second" {
		t.Fatalf("text and tool order changed: %s", response.Get("output").Raw)
	}
}

func TestClaudeResponsesTextRejectsInvalidExplicitIndex(t *testing.T) {
	for _, index := range []string{`"0"`, `-1`, `0.5`, `18446744073709551616`} {
		var state any
		for _, event := range []string{
			`{"type":"content_block_start","index":%s,"content_block":{"type":"text"}}`,
			`{"type":"content_block_delta","index":%s,"delta":{"type":"text_delta","text":"wrong"}}`,
			`{"type":"content_block_stop","index":%s}`,
		} {
			if len(ConvertClaudeResponseToOpenAIResponses(t.Context(), "", nil, nil, []byte("data: "+fmt.Sprintf(event, index)), &state)) != 0 {
				t.Fatal("invalid explicit index reached output")
			}
		}
	}
}
