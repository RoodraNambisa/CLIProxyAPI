package responses

import (
	"bytes"
	"testing"

	"github.com/tidwall/gjson"
)

func TestResponsesAssistantContentStaysWithToolCalls(t *testing.T) {
	for _, stream := range []bool{false, true} {
		body := []byte(`{"input":[{"type":"reasoning","summary":[{"type":"summary_text","text":"plan"}]},{"role":"assistant","content":[{"type":"output_text","text":"checking"}],"reasoning_content":"plan"},{"type":"function_call","call_id":"call_a","name":"lookup","arguments":"{}","reasoning_content":"lookup detail"},{"type":"custom_tool_call","call_id":"call_b","name":"patch","input":"edit"},{"role":"user","content":"next"},{"type":"custom_tool_call_output","call_id":"call_b","output":"patched"},{"type":"function_call_output","call_id":"call_a","output":"found"}]}`)
		before := bytes.Clone(body)
		got := ConvertOpenAIResponsesRequestToOpenAIChatCompletions("model", body, stream)
		messages := gjson.GetBytes(got, "messages").Array()
		if len(messages) != 4 || messages[0].Get("role").String() != "assistant" || messages[0].Get("content.0.text").String() != "checking" || messages[0].Get("tool_calls.#").Int() != 2 {
			t.Fatal("assistant text split from its tool calls")
		}
		if messages[0].Get("reasoning_content").String() != "plan\n\nlookup detail" {
			t.Fatal("assistant reasoning was lost or duplicated")
		}
		if messages[1].Get("tool_call_id").String() != "call_b" || messages[2].Get("tool_call_id").String() != "call_a" || messages[3].Get("role").String() != "user" {
			t.Fatal("tool results and deferred user message lost their order")
		}
		if !bytes.Equal(body, before) {
			t.Fatal("history conversion mutated the source")
		}
	}
}

func TestResponsesReasoningDoesNotCrossUserBoundary(t *testing.T) {
	for _, body := range []string{
		`{"input":[{"type":"reasoning","summary":[{"type":"summary_text","text":"earlier"}]},{"role":"user","content":"next"},{"type":"function_call","call_id":"later","name":"lookup","arguments":"{}"}]}`,
		`{"input":[{"type":"reasoning","content":[{"type":"reasoning_text","text":"earlier"}]},{"role":"user","content":"next"},{"type":"function_call","call_id":"later","name":"lookup","arguments":"{}"}]}`,
	} {
		got := ConvertOpenAIResponsesRequestToOpenAIChatCompletions("model", []byte(body), false)
		messages := gjson.GetBytes(got, "messages").Array()
		if len(messages) != 3 || messages[0].Get("reasoning_content").String() != "earlier" || messages[1].Get("role").String() != "user" || messages[2].Get("reasoning_content").Exists() {
			t.Fatal("reasoning crossed a user turn or disappeared")
		}
	}
}

func TestResponsesAssistantMergeStopsAtInterveningItems(t *testing.T) {
	for _, middle := range []string{`{"role":"user","content":"next"}`, `{"type":"unknown"}`} {
		body := []byte(`{"input":[{"role":"assistant","content":"earlier"},` + middle + `,{"type":"function_call","call_id":"later","name":"lookup","arguments":"{}"}]}`)
		got := ConvertOpenAIResponsesRequestToOpenAIChatCompletions("model", body, false)
		if gjson.GetBytes(got, "messages.0.tool_calls").Exists() {
			t.Fatal("tool calls were merged across an intervening item")
		}
	}
}

func TestResponsesReasoningKeepsOpaqueAndInvalidFieldsOutOfChatText(t *testing.T) {
	body := []byte(`{"input":[{"type":"reasoning","encrypted_content":"opaque-fixture","summary":[{"type":"summary_text","text":123}],"content":[{"type":"reasoning_text","text":{"business":"value"}}]},{"role":"assistant","content":"answer","reasoning_content":false}]}`)
	before := bytes.Clone(body)
	got := ConvertOpenAIResponsesRequestToOpenAIChatCompletions("model", body, false)
	if gjson.GetBytes(got, "messages.#").Int() != 1 || gjson.GetBytes(got, "messages.0.reasoning_content").Exists() || !bytes.Equal(body, before) {
		t.Fatal("opaque or invalid reasoning was interpreted as plaintext")
	}
}
