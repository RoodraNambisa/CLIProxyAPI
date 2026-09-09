package responses

import (
	"reflect"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestClaudeResponsesOutputIndexesMatchFinalArray(t *testing.T) {
	events := []string{
		`{"type":"message_start","message":{"id":"indexes"}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"unknown"}}`,
		`{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","name":"unidentified"}}`,
		`{"type":"content_block_start","index":11,"content_block":{"type":"thinking","thinking":"reason"}}`,
		`{"type":"content_block_start","index":4,"content_block":{"type":"tool_use","id":"paired","name":"tool"}}`,
		`{"type":"content_block_start","index":11,"content_block":{"type":"text"}}`,
		`{"type":"content_block_start","index":8,"content_block":{"type":"text"}}`,
		`{"type":"content_block_delta","index":8,"delta":{"type":"text_delta","text":"partial"}}`,
		`{"type":"content_block_delta","index":11,"delta":{"type":"signature_delta","signature":"opaque"}}`,
		`{"type":"content_block_delta","index":4,"delta":{"type":"input_json_delta","partial_json":"{\"input\":\"kept\"}"}}`,
		`{"type":"content_block_stop","index":8}`,
		`{"type":"content_block_stop","index":11}`,
		`{"type":"content_block_stop","index":4}`,
		`{"type":"message_delta","delta":{"stop_reason":"max_tokens"}}`,
		`{"type":"message_stop"}`,
	}
	for _, custom := range []bool{false, true} {
		request := []byte(`{"tools":[{"type":"function","name":"tool"}]}`)
		if custom {
			request = []byte(`{"tools":[{"type":"custom","name":"tool"}]}`)
		}
		var state any
		var result gjson.Result
		ids := []string{"rs_indexes_11", "fc_paired", "msg_indexes_8"}
		if custom {
			ids[1] = "ctc_paired"
		}
		added, done := make(map[int64]int), make(map[int64]gjson.Result)
		for _, source := range events {
			for _, event := range claudeResponsesTerminalEvents(t, &state, request, source) {
				if index := event.Get("output_index"); index.Exists() {
					i := index.Int()
					if i < 0 || i >= int64(len(ids)) {
						t.Fatal("source block index leaked into output_index")
					}
					id := event.Get("item.id").String()
					if id == "" {
						id = event.Get("item_id").String()
					}
					if id != ids[i] {
						t.Fatal("dense index borrowed another output identity")
					}
					if event.Get("type").String() == "response.output_item.added" {
						added[i]++
					}
					if event.Get("type").String() == "response.output_item.done" {
						done[i] = event.Get("item")
					}
				}
				if event.Get("type").String() == "response.incomplete" {
					result = event.Get("response")
				}
			}
		}
		for i, item := range result.Get("output").Array() {
			if added[int64(i)] != 1 || item.Get("id").String() != ids[i] || !reflect.DeepEqual(item.Value(), done[int64(i)].Value()) {
				t.Fatal("final array differs from stream positions or item completions")
			}
		}
		if result.Get("output.#").Int() != 3 || result.Get("output.0.encrypted_content").String() != "opaque" || result.Get("output.0.status").String() != "completed" || result.Get("output.2.status").String() != "incomplete" {
			t.Fatal("out-of-order block indexes changed output order or terminal state")
		}
		nonstream := gjson.ParseBytes(ConvertClaudeResponseToOpenAIResponsesNonStream(t.Context(), "", request, nil, []byte("data: "+strings.Join(events, "\ndata: ")+"\n"), nil))
		if !reflect.DeepEqual(nonstream.Get("output").Value(), result.Get("output").Value()) {
			t.Fatal("non-stream output order differs")
		}
		claudeResponsesTerminalEvents(t, &state, request, `{"type":"message_start","message":{"id":"reset"}}`)
		for _, event := range claudeResponsesTerminalEvents(t, &state, request, `{"type":"content_block_start","index":7,"content_block":{"type":"text"}}`) {
			if event.Get("output_index").Int() != 0 {
				t.Fatal("new response did not reset output numbering")
			}
		}
	}
}
