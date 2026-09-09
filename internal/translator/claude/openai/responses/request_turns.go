package responses

import (
	"bytes"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// claudeResponsesRequestTurns groups adjacent Responses items into Claude turns.
// Tool calls normally end an assistant turn, but separate signed reasoning
// blocks when a later reasoning item follows them.
type claudeResponsesRequestTurns struct {
	items [][]byte
	role  string
	parts [][]byte
	tools [][]byte
}

func claudeResponsesRawArray(items [][]byte) []byte {
	return append(append([]byte{'['}, bytes.Join(items, []byte{','})...), ']')
}

func claudeResponsesTextPart(text string) []byte {
	part, _ := sjson.SetBytes([]byte(`{"type":"text","text":""}`), "text", text)
	return part
}

func (b *claudeResponsesRequestTurns) selectRole(role string) {
	if b.role != "" && b.role != role {
		b.flush()
	}
	b.role = role
}

func (b *claudeResponsesRequestTurns) appendParts(role string, parts ...[]byte) {
	if len(parts) == 0 {
		return
	}
	b.selectRole(role)
	b.parts = append(b.parts, parts...)
}

func (b *claudeResponsesRequestTurns) appendToolUse(part []byte) {
	b.selectRole("assistant")
	b.tools = append(b.tools, part)
}

func (b *claudeResponsesRequestTurns) appendReasoning(part []byte) {
	if len(part) == 0 {
		return
	}
	b.selectRole("assistant")
	if len(b.tools) > 0 {
		b.parts = append(b.parts, b.tools...)
		b.tools = nil
	}
	last := len(b.parts) - 1
	if last >= 0 && gjson.GetBytes(part, "type").String() == "thinking" && gjson.GetBytes(b.parts[last], "type").String() == "thinking" {
		b.parts[last] = part
		return
	}
	b.parts = append(b.parts, part)
}

func (b *claudeResponsesRequestTurns) flush() {
	parts := append(b.parts, b.tools...)
	if len(parts) > 0 {
		msg, _ := sjson.SetBytes([]byte(`{"role":"","content":[]}`), "role", b.role)
		if len(parts) == 1 && gjson.GetBytes(parts[0], "type").String() == "text" &&
			!gjson.GetBytes(parts[0], "cache_control").Exists() && !gjson.GetBytes(parts[0], "citations").Exists() {
			msg, _ = sjson.SetBytes(msg, "content", gjson.GetBytes(parts[0], "text").String())
		} else {
			msg, _ = sjson.SetRawBytes(msg, "content", claudeResponsesRawArray(parts))
		}
		b.items = append(b.items, msg)
	}
	b.role, b.parts, b.tools = "", nil, nil
}

func (b *claudeResponsesRequestTurns) finish(isCompat bool) []byte {
	hadParts := len(b.parts) > 0 || len(b.tools) > 0
	// Native Claude rejects a final assistant block consisting of thinking.
	// Compatibility models explicitly retain that prefill, including empty blocks.
	if !isCompat && b.role == "assistant" && len(b.tools) == 0 {
		for len(b.parts) > 0 {
			kind := gjson.GetBytes(b.parts[len(b.parts)-1], "type").String()
			if kind != "thinking" && kind != "redacted_thinking" {
				break
			}
			b.parts = b.parts[:len(b.parts)-1]
		}
	}
	b.flush()
	if len(b.items) == 0 && hadParts {
		b.appendParts("user", claudeResponsesTextPart(""))
		b.flush()
	}
	return claudeResponsesRawArray(b.items)
}
