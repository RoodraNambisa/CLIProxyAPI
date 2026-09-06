package chat_completions

import (
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexToolStreamsKeepInterleavedArgumentsSeparate(t *testing.T) {
	var state any
	arguments := map[int64]string{}
	announcements := map[int64]string{}
	for _, event := range []string{
		`{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"fc_a","call_id":"call_a","name":"first"}}`,
		`{"type":"response.output_item.added","output_index":1,"item":{"type":"custom_tool_call","id":"ctc_b","call_id":"call_b","name":"second"}}`,
		`{"type":"response.function_call_arguments.delta","item_id":"wrong-id","output_index":0,"delta":"a1"}`,
		`{"type":"response.custom_tool_call_input.delta","item_id":"ctc_b","delta":"b1"}`,
		`{"type":"response.function_call_arguments.delta","item_id":"fc_a","delta":"a2"}`,
		`{"type":"response.function_call_arguments.done","output_index":0,"arguments":"a1a2"}`,
		`{"type":"response.custom_tool_call_input.done","item_id":"ctc_b","input":"b1"}`,
		`{"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","id":"fc_a","call_id":"call_a","name":"first","arguments":"a1a2"}}`,
		`{"type":"response.output_item.done","output_index":1,"item":{"type":"custom_tool_call","id":"ctc_b","call_id":"call_b","name":"second","input":"b1"}}`,
		`{"type":"response.output_item.done","output_index":1,"item":{"type":"custom_tool_call","id":"ctc_b","call_id":"call_b","name":"second","input":"b1"}}`,
	} {
		for _, chunk := range ConvertCodexResponseToOpenAI(t.Context(), "model", nil, nil, []byte("data: "+event), &state) {
			call := gjson.GetBytes(chunk, "choices.0.delta.tool_calls.0")
			index := call.Get("index").Int()
			arguments[index] += call.Get("function.arguments").String()
			if id := call.Get("id").String(); id != "" {
				announcements[index] += id
			}
		}
	}
	if arguments[0] != "a1a2" || arguments[1] != "b1" || announcements[0] != "call_a" || announcements[1] != "call_b" {
		t.Fatal("tool identities or arguments were mixed/duplicated")
	}
}

func TestCodexToolStreamsRejectAmbiguousOrConflictingEvents(t *testing.T) {
	var state any
	for _, event := range []string{
		`{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"fc_a","call_id":"call_a","name":"first"}}`,
		`{"type":"response.output_item.added","output_index":1,"item":{"type":"function_call","id":"fc_b","call_id":"call_b","name":"second"}}`,
	} {
		ConvertCodexResponseToOpenAI(t.Context(), "model", nil, nil, []byte("data: "+event), &state)
	}
	for _, event := range []string{
		`{"type":"response.function_call_arguments.delta","delta":"unknown"}`,
		`{"type":"response.function_call_arguments.delta","item_id":"fc_a","output_index":1,"delta":"conflict"}`,
		`{"type":"response.function_call_arguments.delta","item_id":"missing","delta":"unknown"}`,
		`{"type":"response.custom_tool_call_input.delta","item_id":"fc_a","delta":"wrong-kind"}`,
		`{"type":"response.function_call_arguments.delta","item_id":"fc_a","call_id":"new_call","delta":"conflicting-call"}`,
		`{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"fc_new","call_id":"call_new","name":"third"}}`,
		`{"type":"response.function_call_arguments.delta","item_id":"fc_new","delta":"stolen-index"}`,
	} {
		if out := ConvertCodexResponseToOpenAI(t.Context(), "model", nil, nil, []byte("data: "+event), &state); len(out) != 0 {
			t.Fatal("unidentified arguments were appended to a tool")
		}
	}
}

func TestCodexToolStreamsRecoverMissingStartAndIgnoreDuplicateTerminal(t *testing.T) {
	var state any
	delta := `data: {"type":"response.function_call_arguments.delta","item_id":"fc_a","delta":"partial"}`
	if out := ConvertCodexResponseToOpenAI(t.Context(), "model", nil, nil, []byte(delta), &state); len(out) != 0 {
		t.Fatal("unannounced tool emitted an invalid index")
	}
	done := `data: {"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","id":"fc_a","call_id":"call_a","name":"tool","arguments":"complete"}}`
	out := ConvertCodexResponseToOpenAI(t.Context(), "model", nil, nil, []byte(done), &state)
	if len(out) != 1 || gjson.GetBytes(out[0], "choices.0.delta.tool_calls.0.function.arguments").String() != "complete" {
		t.Fatal("missing start was not repaired by the complete item")
	}
	if out = ConvertCodexResponseToOpenAI(t.Context(), "model", nil, nil, []byte(done), &state); len(out) != 0 {
		t.Fatal("duplicate tool terminal emitted twice")
	}
	terminal := []byte(`data: {"type":"response.completed","response":{"status":"completed"}}`)
	if out = ConvertCodexResponseToOpenAI(t.Context(), "model", nil, nil, terminal, &state); len(out) != 1 {
		t.Fatal("completion was not emitted")
	}
	if out = ConvertCodexResponseToOpenAI(t.Context(), "model", nil, nil, terminal, &state); len(out) != 0 {
		t.Fatal("duplicate completion was emitted")
	}
}

func TestCodexReasoningTextStreamAndNonStream(t *testing.T) {
	var state any
	var reasoning strings.Builder
	for _, event := range []string{
		`{"type":"response.reasoning_text.delta","item_id":"rs_1","delta":"one"}`,
		`{"type":"response.reasoning_text.delta","item_id":"rs_1","delta":"two"}`,
		`{"type":"response.reasoning_text.done","item_id":"rs_1"}`,
		`{"type":"response.reasoning_text.done","item_id":"rs_1"}`,
	} {
		for _, chunk := range ConvertCodexResponseToOpenAI(t.Context(), "model", nil, nil, []byte("data: "+event), &state) {
			reasoning.WriteString(gjson.GetBytes(chunk, "choices.0.delta.reasoning_content").String())
		}
	}
	if reasoning.String() != "onetwo\n\n" {
		t.Fatal("reasoning text was dropped or terminal duplicated")
	}
	nonStream := ConvertCodexResponseToOpenAINonStream(t.Context(), "model", nil, nil, []byte(`{"type":"response.completed","response":{"status":"completed","output":[{"type":"reasoning","content":[{"type":"reasoning_text","text":"one"},{"type":"reasoning_text","text":"two"}]},{"type":"message","content":[{"type":"output_text","text":"a"},{"type":"output_text","text":"b"}]}]}}`), nil)
	if gjson.GetBytes(nonStream, "choices.0.message.reasoning_content").String() != "onetwo" || gjson.GetBytes(nonStream, "choices.0.message.content").String() != "ab" {
		t.Fatal("non-stream conversion lost content parts")
	}
}

func TestCodexNativeCustomToolStreamUsesCustomInput(t *testing.T) {
	var state any
	original := []byte(`{"tools":[{"type":"custom","custom":{"name":"code_exec"}}]}`)
	var input strings.Builder
	announcements := 0
	for _, event := range []string{
		`{"type":"response.output_item.added","output_index":0,"item":{"type":"custom_tool_call","id":"ctc_a","call_id":"call_a","name":"code_exec"}}`,
		`{"type":"response.custom_tool_call_input.delta","item_id":"ctc_a","delta":"print("}`,
		`{"type":"response.function_call_arguments.delta","item_id":"ctc_a","delta":"wrong-kind"}`,
		`{"type":"response.custom_tool_call_input.delta","item_id":"ctc_a","delta":"1)"}`,
		`{"type":"response.custom_tool_call_input.done","item_id":"ctc_a","input":"print(1)"}`,
		`{"type":"response.output_item.done","output_index":0,"item":{"type":"custom_tool_call","id":"ctc_a","call_id":"call_a","name":"code_exec","input":"print(1)"}}`,
	} {
		for _, chunk := range ConvertCodexResponseToOpenAI(t.Context(), "gpt-5.4", original, nil, []byte("data: "+event), &state) {
			call := gjson.GetBytes(chunk, "choices.0.delta.tool_calls.0")
			if call.Get("function").Exists() {
				t.Fatal("native custom input was placed in a function envelope")
			}
			if call.Get("id").Exists() {
				announcements++
				if call.Get("type").String() != "custom" || call.Get("custom.name").String() != "code_exec" {
					t.Fatal("native custom call lost its type or name")
				}
			}
			input.WriteString(call.Get("custom.input").String())
		}
	}
	if input.String() != "print(1)" || announcements != 1 {
		t.Fatal("native custom input was duplicated or mixed with another event kind")
	}
}
