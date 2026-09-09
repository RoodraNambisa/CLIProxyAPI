package responses

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestClaudeResponsesNonStreamTerminalStatus(t *testing.T) {
	for _, reason := range []string{"max_tokens", " MAX_TOKENS ", "end_turn", "tool_use", ""} {
		for _, kind := range []string{"text", "thinking", "redacted_thinking", "function", "custom"} {
			for _, partial := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/partial=%t", reason, kind, partial), func(t *testing.T) {
					start, delta := `{"type":"text"}`, `{"type":"text_delta","text":"partial"}`
					switch kind {
					case "thinking":
						start, delta = `{"type":"thinking","signature":"opaque"}`, `{"type":"thinking_delta","thinking":"partial"}`
					case "redacted_thinking":
						start, delta = `{"type":"redacted_thinking","data":"opaque"}`, `{"type":"text_delta","text":"ignored"}`
					case "function", "custom":
						start, delta = `{"type":"tool_use","id":"call","name":"tool"}`, `{"type":"input_json_delta","partial_json":"{\"input\":\"partial"}`
					}
					events := []string{
						`{"type":"message_start","message":{"id":"terminal"}}`,
						`{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"earlier","name":"function"}}`,
						`{"type":"content_block_stop","index":0}`,
						`{"type":"content_block_start","index":7,"content_block":` + start + `}`,
					}
					if partial {
						events = append(events, `{"type":"content_block_delta","index":7,"delta":`+delta+`}`)
					}
					events = append(events, `{"type":"content_block_stop","index":7}`, fmt.Sprintf(`{"type":"message_delta","delta":{"stop_reason":%q}}`, reason),
						`{"type":"message_delta","delta":{"stop_reason":null}}`, `{"type":"message_delta","delta":{"stop_reason":{}}}`, `{"type":"message_stop"}`,
						`{"type":"message_delta","delta":{"stop_reason":"max_tokens"}}`, `{"type":"message_stop"}`)
					request := []byte(`{"tools":[{"type":"` + kind + `","name":"tool"}]}`)
					result := gjson.ParseBytes(ConvertClaudeResponseToOpenAIResponsesNonStream(t.Context(), "", request, nil, []byte("data: "+strings.Join(events, "\n\ndata: ")+"\n\n"), nil))
					var state any
					var streamed gjson.Result
					sequence, terminals := int64(0), 0
					done := make(map[int64]int)
					for _, source := range events {
						for _, event := range claudeResponsesTerminalEvents(t, &state, request, source) {
							if event.Get("sequence_number").Int() != sequence+1 {
								t.Fatal("deferred events broke sequence ordering")
							}
							sequence++
							kind := event.Get("type").String()
							if kind == "response.output_item.done" {
								index := event.Get("output_index").Int()
								done[index]++
								if index == 1 && gjson.Get(source, "type").String() != "message_stop" {
									t.Fatal("last output was completed before the stop reason")
								}
								position := "0"
								if index == 1 {
									position = "1"
								}
								if !reflect.DeepEqual(event.Get("item").Value(), result.Get("output."+position).Value()) {
									t.Fatal("item.done differs from the non-stream output")
								}
							}
							if kind == "response.completed" || kind == "response.incomplete" {
								terminals++
								streamed = event.Get("response")
								if kind != "response."+streamed.Get("status").String() {
									t.Fatal("terminal event disagrees with response status")
								}
							}
						}
					}
					if terminals != 1 || done[0] != 1 || done[1] != 1 || streamed.Get("status").String() != result.Get("status").String() ||
						!reflect.DeepEqual(streamed.Get("output").Value(), result.Get("output").Value()) ||
						!reflect.DeepEqual(streamed.Get("incomplete_details").Value(), result.Get("incomplete_details").Value()) {
						t.Fatal("stream and non-stream terminal results differ or were duplicated")
					}
					want := "completed"
					if strings.EqualFold(strings.TrimSpace(reason), "max_tokens") {
						want = "incomplete"
						if result.Get("incomplete_details.reason").String() != "max_output_tokens" {
							t.Fatal("missing output limit reason")
						}
					} else if result.Get("incomplete_details").Type != gjson.Null {
						t.Fatal("unexpected incomplete details")
					}
					if result.Get("status").String() != want || result.Get("output.1.status").String() != want || result.Get("output.0.status").String() != "completed" || result.Get("output.0.arguments").String() != "{}" {
						t.Fatal("terminal or earlier item status is incorrect")
					}
					if kind == "function" || kind == "custom" {
						field, args := "arguments", ""
						if kind == "custom" {
							field = "input"
						} else if want == "completed" {
							args = "{}"
						}
						if partial {
							args = `{"input":"partial`
						}
						if result.Get("output.1."+field).String() != args {
							t.Fatal("partial arguments were completed or lost")
						}
					}
				})
			}
		}
	}
}

func claudeResponsesTerminalEvents(t *testing.T, state *any, request []byte, source string) []gjson.Result {
	t.Helper()
	var events []gjson.Result
	for _, chunk := range ConvertClaudeResponseToOpenAIResponses(t.Context(), "", request, nil, []byte("data: "+source), state) {
		for _, line := range strings.Split(string(chunk), "\n") {
			if strings.HasPrefix(line, "data:") {
				events = append(events, gjson.Parse(strings.TrimPrefix(line, "data:")))
			}
		}
	}
	return events
}

func TestClaudeResponsesTerminalInterleavedAndAbandonedBlocks(t *testing.T) {
	var state any
	send := func(source string) []gjson.Result { return claudeResponsesTerminalEvents(t, &state, nil, source) }
	noDone := func(events []gjson.Result) {
		t.Helper()
		for _, event := range events {
			if strings.HasSuffix(event.Get("type").String(), ".done") {
				t.Fatal("unfinished or last block was finalized early")
			}
		}
	}
	send(`{"type":"message_start","message":{"id":"interleaved"}}`)
	send(`{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"first","name":"first"}}`)
	noDone(send(`{"type":"content_block_start","index":1,"content_block":{"type":"text"}}`))
	noDone(send(`{"type":"content_block_stop","index":1}`))
	noDone(send(`{"type":"content_block_start","index":4,"content_block":{"type":"unknown"}}`))
	noDone(send(`{"type":"content_block_start","index":4,"content_block":{"type":"tool_use","name":"missing-id"}}`))
	noDone(send(`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"kept\":true}"}}`))
	completedFirst := false
	for _, event := range send(`{"type":"content_block_stop","index":0}`) {
		if event.Get("type").String() == "response.output_item.done" {
			completedFirst = event.Get("item.arguments").String() == `{"kept":true}` && event.Get("item.status").String() == "completed"
		}
	}
	if !completedFirst {
		t.Fatal("earlier closed tool was not finalized with its own arguments")
	}
	noDone(send(`{"type":"content_block_stop","index":0}`))
	noDone(send(`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"late"}}`))
	completedSecond := false
	for _, event := range send(`{"type":"content_block_delta","index":2,"delta":{"type":"text_delta","text":"partial"}}`) {
		if event.Get("type").String() == "response.output_item.done" {
			completedSecond = event.Get("output_index").Int() == 1 && event.Get("item.status").String() == "completed"
		}
	}
	if !completedSecond {
		t.Fatal("implicit text block did not release the preceding closed block")
	}
	send(`{"type":"message_delta","delta":{"stop_reason":"max_tokens"}}`)
	for _, event := range send(`{"type":"message_stop"}`) {
		if event.Get("type").String() == "response.output_item.done" &&
			(event.Get("output_index").Int() != 2 || event.Get("item.status").String() != "incomplete" || event.Get("item.content.0.text").String() != "partial") {
			t.Fatal("missing block stop lost the final partial item")
		}
	}
	send(`{"type":"message_start","message":{"id":"abandoned"}}`)
	send(`{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"abandoned","name":"first"}}`)
	noDone(send(`{"type":"content_block_stop","index":0}`))
	// EOF or cancellation supplies no message_stop; a new message must discard the pending item.
	noDone(send(`{"type":"message_start","message":{"id":"new"}}`))
	for _, event := range send(`{"type":"message_stop"}`) {
		if event.Get("type").String() != "response.completed" || event.Get("response.output").Raw != "[]" || event.Get("response.id").String() != "new" {
			t.Fatal("new message emitted abandoned tool state or inherited truncation")
		}
	}
}

func TestClaudeResponsesNonStreamTerminalResetAndEmpty(t *testing.T) {
	raw := `data: {"type":"message_start","message":{"id":"old"}}
data: {"type":"message_delta","delta":{"stop_reason":"max_tokens"}}
data: {"type":"message_stop"}
`
	result := gjson.ParseBytes(ConvertClaudeResponseToOpenAIResponsesNonStream(t.Context(), "", nil, nil, []byte(raw), nil))
	if result.Get("status").String() != "incomplete" || result.Get("output").Raw != "[]" {
		t.Fatal("empty incomplete response was lost")
	}
	raw += `data: {"type":"message_start","message":{"id":"new"}}
data: {"type":"message_stop"}
`
	result = gjson.ParseBytes(ConvertClaudeResponseToOpenAIResponsesNonStream(t.Context(), "", nil, nil, []byte(raw), nil))
	if result.Get("id").String() != "new" || result.Get("status").String() != "completed" || result.Get("incomplete_details").Type != gjson.Null {
		t.Fatal("new message inherited old stop reason")
	}
}
