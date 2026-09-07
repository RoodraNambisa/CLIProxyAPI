package responses

import (
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestClaudeResponsesInterleavedToolsRetainIndexedIdentity(t *testing.T) {
	events := []string{
		`{"type":"message_start","message":{"id":"result"}}`,
		`{"type":"content_block_delta","index":9,"delta":{"type":"input_json_delta","partial_json":"orphan"}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"first","name":"first_tool","input":{}}}`,
		`{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"second","name":"second_tool","input":{}}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"wrong","name":"wrong_tool","input":{}}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"first\":"}}`,
		`{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"second\":2}"}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"1}"}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"late"}}`,
		`{"type":"content_block_stop","index":1}`,
		`{"type":"message_stop"}`,
		`{"type":"message_stop"}`,
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
				if strings.HasPrefix(kind, "response.function_call_arguments.") {
					want := "fc_first"
					if data.Get("output_index").Int() == 1 {
						want = "fc_second"
					}
					if data.Get("output_index").Int() > 1 || data.Get("item_id").String() != want {
						t.Fatal("tool delta/done was assigned another call's identity")
					}
				}
				if kind == "response.output_item.done" && data.Get("item.type").String() == "function_call" {
					want := "first"
					if data.Get("output_index").Int() == 1 {
						want = "second"
					}
					if data.Get("item.call_id").String() != want {
						t.Fatal("tool completion was assigned another call")
					}
				}
				if kind == "response.completed" {
					completed = data.Get("response")
				}
			}
		}
	}
	if counts["response.output_item.added"] != 2 || counts["response.output_item.done"] != 2 || counts["response.function_call_arguments.delta"] != 3 || counts["response.function_call_arguments.done"] != 2 || counts["response.completed"] != 1 {
		t.Fatalf("duplicate or missing tool events: %v", counts)
	}
	stream := "data: " + strings.Join(events, "\n\ndata: ") + "\n\n"
	nonstream := gjson.ParseBytes(ConvertClaudeResponseToOpenAIResponsesNonStream(t.Context(), "", nil, nil, []byte(stream), nil))
	for _, output := range []gjson.Result{completed, nonstream} {
		if output.Get("output.#").Int() != 2 || output.Get("output.0.call_id").String() != "first" || output.Get("output.0.name").String() != "first_tool" || output.Get("output.0.arguments").String() != `{"first":1}` || output.Get("output.1.call_id").String() != "second" || output.Get("output.1.arguments").String() != `{"second":2}` {
			t.Fatal("aggregated tools were overwritten or orphan fragments became a new call")
		}
	}
}

func TestClaudeResponsesUnindexedDeltaDoesNotAttachToToolZero(t *testing.T) {
	for _, index := range []string{"", `,"index":-1`, `,"index":"0"`, `,"index":0.5`, `,"index":18446744073709551616`} {
		var state any
		ConvertClaudeResponseToOpenAIResponses(t.Context(), "", nil, nil, []byte(`data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"first","name":"first_tool"}}`), &state)
		event := []byte(`data: {"type":"content_block_delta"` + index + `,"delta":{"type":"input_json_delta","partial_json":"wrong"}}`)
		if chunks := ConvertClaudeResponseToOpenAIResponses(t.Context(), "", nil, nil, event, &state); len(chunks) != 0 {
			t.Fatal("invalid or missing index was assigned to an active tool")
		}
	}
}

func TestClaudeResponsesMissingToolIDDoesNotBorrowAnotherCall(t *testing.T) {
	events := []string{
		`{"type":"message_start","message":{"id":"result"}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"first","name":"first_tool"}}`,
		`{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","name":"unidentified"}}`,
		`{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{}"}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"message_stop"}`,
	}
	var state any
	var completed gjson.Result
	for _, event := range events {
		for _, chunk := range ConvertClaudeResponseToOpenAIResponses(t.Context(), "", nil, nil, []byte("data: "+event), &state) {
			for _, line := range strings.Split(string(chunk), "\n") {
				if strings.HasPrefix(line, "data:") {
					data := gjson.Parse(strings.TrimPrefix(line, "data:"))
					if data.Get("type").String() == "response.completed" {
						completed = data.Get("response")
					}
				}
			}
		}
	}
	nonstream := gjson.ParseBytes(ConvertClaudeResponseToOpenAIResponsesNonStream(t.Context(), "", nil, nil, []byte("data: "+strings.Join(events, "\n\ndata: ")+"\n\n"), nil))
	for _, output := range []gjson.Result{completed, nonstream} {
		if output.Get("output.#").Int() != 1 || output.Get("output.0.call_id").String() != "first" || output.Get("output.0.arguments").String() != "{}" {
			t.Fatal("missing identity borrowed another call or empty function arguments lost their object value")
		}
	}
}

func TestClaudeResponsesLegacyTextWithoutToolIndexesIsPreserved(t *testing.T) {
	var state any
	var completed gjson.Result
	for _, event := range []string{`{"type":"message_start","message":{"id":"result"}}`, `{"type":"content_block_start","content_block":{"type":"text"}}`, `{"type":"content_block_delta","delta":{"type":"text_delta","text":"legacy"}}`, `{"type":"content_block_stop"}`, `{"type":"message_stop"}`} {
		for _, chunk := range ConvertClaudeResponseToOpenAIResponses(t.Context(), "", nil, nil, []byte("data: "+event), &state) {
			for _, line := range strings.Split(string(chunk), "\n") {
				if strings.HasPrefix(line, "data:") {
					data := gjson.Parse(strings.TrimPrefix(line, "data:"))
					if data.Get("type").String() == "response.completed" {
						completed = data.Get("response")
					}
				}
			}
		}
	}
	if completed.Get("output.0.content.0.text").String() != "legacy" {
		t.Fatal("tool index validation changed existing text fallback")
	}
}
