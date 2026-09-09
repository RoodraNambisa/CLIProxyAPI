package helps

import (
	"fmt"
	"testing"
)

func TestResponsesTokenClassifierRequiresActualContent(t *testing.T) {
	for _, body := range []string{
		`{"type":"response.output_text.delta","delta":"answer"}`,
		`{"type":"response.reasoning_text.delta","delta":" "}`,
		`{"type":"response.reasoning_summary_text.done","text":"summary"}`,
		`{"type":"response.function_call_arguments.delta","delta":"{"}`,
		`{"type":"response.custom_tool_call_input.done","input":"command"}`,
		`{"type":"response.audio.delta","data":"YQ=="}`,
		`{"type":"response.image_generation_call.partial_image","partial_image_b64":"YQ=="}`,
		`{"type":"response.shell_call_command.added","command":"pwd"}`,
		`{"type":"response.output_item.added","item":{"type":"message","content":[{"type":"output_text","text":"initial"}]}}`,
		`{"type":"response.output_item.done","item":{"type":"function_call","arguments":"{}"}}`,
		`{"type":"response.output_item.done","item":{"type":"reasoning","summary":[{"type":"summary_text","text":"summary"}]}}`,
		`{"type":"response.completed","response":{"output":[{"type":"message","content":[{"type":"output_text","text":"one chunk"}]}]}}`,
		`{"type":"response.incomplete","response":{"output":[{"type":"custom_tool_call","input":"partial input"}]}}`,
	} {
		for _, prefix := range []string{"", "data: "} {
			if !IsResponsesTokenEvent([]byte(prefix + body)) {
				t.Fatalf("actual model content was missed: %s", body)
			}
		}
	}
	for _, body := range []string{
		"", ": ping\n\n", "data: [DONE]", `{"type":"response.output_text.delta","delta":`,
		`{"type":"response.created","response":{"id":"fixture"}}`,
		`{"type":"response.in_progress"}`, `{"type":"rate_limits.updated"}`,
		`{"type":"response.output_text.delta","delta":""}`, `{"type":"response.output_text.delta","delta":42}`,
		`{"type":"response.output_text.delta","delta":{"text":"not a string"}}`,
		`{"type":"response.output_item.added","item":{"type":"function_call","name":"tool","arguments":""}}`,
		`{"type":"response.output_item.done","item":{"type":"function_call_output","output":"tool output"}}`,
		`{"type":"response.output_item.done","item":{"type":"message","role":"tool","content":[{"type":"text","text":"tool output"}]}}`,
		`{"type":"response.output_item.done","item":{"type":"message","summary":[{"type":"text","text":"metadata"}]}}`,
		`{"type":"response.output_item.done","item":{"type":"reasoning","encrypted_content":"opaque"}}`,
		`{"type":"response.output_item.done","item":{"type":"message","content":[{"type":"input_text","text":"input"}]}}`,
		`{"type":"response.web_search_call.completed"}`, `{"type":"response.code_interpreter_call_output.delta","delta":"tool output"}`,
		`{"type":"response.completed","response":{"output":[],"usage":{"output_tokens":3}}}`,
		`{"type":"response.failed","response":{"error":{"message":"failure"}}}`, `{"type":"error","message":"failure"}`,
	} {
		if IsResponsesTokenEvent([]byte(body)) {
			t.Fatalf("metadata, tool output or invalid content counted as a model token: %s", body)
		}
	}
	for _, event := range []string{"response.done", "response.incomplete", "response.failed"} {
		if IsResponsesTokenEvent([]byte(fmt.Sprintf(`{"type":%q,"response":{"output":[]}}`, event))) {
			t.Fatal("an empty terminal fabricated a first token")
		}
	}
}
