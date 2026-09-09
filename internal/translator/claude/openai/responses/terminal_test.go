package responses

import (
	"fmt"
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
