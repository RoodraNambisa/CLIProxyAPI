package helps

import (
	"bytes"

	"github.com/tidwall/gjson"
)

// IsResponsesTokenEvent identifies substantive model content in a decoded
// Responses event or one SSE data line. Lifecycle events alone are not tokens.
func IsResponsesTokenEvent(payload []byte) bool {
	payload = bytes.TrimSpace(payload)
	if data, ok := bytes.CutPrefix(payload, []byte("data:")); ok {
		payload = bytes.TrimSpace(data)
	}
	if !gjson.ValidBytes(payload) {
		return false
	}
	root := gjson.ParseBytes(payload)
	switch root.Get("type").String() {
	case "response.reasoning_summary_text.delta", "response.reasoning.delta", "response.reasoning_text.delta",
		"response.output_text.delta", "response.text.delta", "response.function_call_arguments.delta",
		"response.custom_tool_call_input.delta", "response.code_interpreter_call_code.delta", "response.mcp_call_arguments.delta",
		"response.shell_call_command.delta", "response.refusal.delta", "response.audio.transcript.delta":
		return responsesContentString(root.Get("delta"))
	case "response.audio.delta":
		return responsesContentString(root.Get("delta")) || responsesContentString(root.Get("data"))
	case "response.image_generation_call.partial_image":
		return responsesContentString(root.Get("partial_image_b64"))
	case "response.shell_call_command.added", "response.shell_call_command.done":
		return responsesContentString(root.Get("command"))
	case "response.reasoning_summary_text.done", "response.reasoning_text.done", "response.output_text.done":
		return responsesContentString(root.Get("text"))
	case "response.refusal.done":
		return responsesContentString(root.Get("refusal"))
	case "response.function_call_arguments.done", "response.mcp_call_arguments.done":
		return responsesContentString(root.Get("arguments"))
	case "response.custom_tool_call_input.done":
		return responsesContentString(root.Get("input"))
	case "response.code_interpreter_call_code.done":
		return responsesContentString(root.Get("code"))
	case "response.reasoning_summary_part.done", "response.content_part.added", "response.content_part.done":
		return responsesContentString(root.Get("part.text")) || responsesContentString(root.Get("part.refusal"))
	case "response.output_item.added", "response.output_item.done":
		return responsesItemHasContent(root.Get("item"))
	case "response.completed", "response.done", "response.incomplete", "response.failed":
		if output := root.Get("response.output"); output.IsArray() {
			for _, item := range output.Array() {
				if responsesItemHasContent(item) {
					return true
				}
			}
		}
	}
	return false
}

func responsesContentString(value gjson.Result) bool {
	return value.Type == gjson.String && value.Str != ""
}

func responsesItemHasContent(item gjson.Result) bool {
	itemType := item.Get("type").String()
	switch itemType {
	case "function_call", "mcp_call":
		return responsesContentString(item.Get("arguments"))
	case "custom_tool_call":
		return responsesContentString(item.Get("input"))
	case "code_interpreter_call":
		return responsesContentString(item.Get("code"))
	case "image_generation_call":
		return responsesContentString(item.Get("result"))
	case "message", "reasoning":
		if role := item.Get("role").String(); itemType == "message" && role != "" && role != "assistant" {
			return false
		}
		for _, field := range []string{"content", "summary"} {
			if itemType == "message" && field != "content" {
				continue
			}
			if parts := item.Get(field); parts.IsArray() {
				for _, part := range parts.Array() {
					switch part.Get("type").String() {
					case "output_text", "text", "summary_text", "reasoning_text", "refusal":
						if responsesContentString(part.Get("text")) || responsesContentString(part.Get("refusal")) {
							return true
						}
					}
				}
			}
		}
	}
	return false
}
