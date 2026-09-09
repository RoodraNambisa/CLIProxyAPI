package responses

import (
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/signature"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// ClaudeResponsesRedactedThinkingPrefix carries an opaque Claude redacted block
// through Responses without treating its data as readable reasoning text.
const ClaudeResponsesRedactedThinkingPrefix = "claude-redacted-thinking:"

func claudeResponsesReplayReasoning(item gjson.Result, isCompat bool) []byte {
	if role := item.Get("role"); role.Exists() && role.String() != "assistant" {
		return nil
	}
	encrypted := item.Get("encrypted_content")
	if encrypted.Exists() && encrypted.Type != gjson.String {
		return nil
	}
	opaque := encrypted.String()
	if data, redacted := strings.CutPrefix(opaque, ClaudeResponsesRedactedThinkingPrefix); redacted {
		if data == "" {
			return nil
		}
		part, _ := sjson.SetBytes([]byte(`{"type":"redacted_thinking","data":""}`), "data", data)
		return part
	}
	native, ok := signature.CompatibleSignatureForProvider(signature.SignatureProviderClaude, opaque)
	if !ok {
		if !isCompat {
			return nil
		}
		native = opaque
	}
	text := claudeResponsesReasoningPartsText(item.Get("summary"))
	if text == "" {
		text = claudeResponsesReasoningPartsText(item.Get("content"))
	}
	part, _ := sjson.SetBytes([]byte(`{"type":"thinking","thinking":"","signature":""}`), "thinking", text)
	part, _ = sjson.SetBytes(part, "signature", native)
	return part
}

func claudeResponsesReasoningPartsText(parts gjson.Result) string {
	if !parts.IsArray() {
		return ""
	}
	var text strings.Builder
	parts.ForEach(func(_, part gjson.Result) bool {
		if value := part.Get("text"); value.Type == gjson.String {
			text.WriteString(value.String())
		} else if part.Type == gjson.String {
			text.WriteString(part.String())
		}
		return true
	})
	return text.String()
}

func claudeResponsesRequiresBlockIndex(root gjson.Result, hasTools, reasoningAtZero bool) bool {
	event := root.Get("type").String()
	if !strings.HasPrefix(event, "content_block_") {
		return false
	}
	if root.Get("index").Exists() {
		return true
	}
	switch event {
	case "content_block_start":
		switch root.Get("content_block.type").String() {
		case "tool_use", "thinking", "redacted_thinking":
			return true
		}
	case "content_block_delta":
		switch root.Get("delta.type").String() {
		case "input_json_delta", "thinking_delta", "signature_delta":
			return true
		}
	case "content_block_stop":
		return hasTools || reasoningAtZero
	}
	// Preserve the existing unindexed text fallback without assigning an
	// unindexed signature or stop event to reasoning at the same position.
	return false
}

func newClaudeResponsesReasoningBlock(id string, content gjson.Result) *claudeResponsesTextBlock {
	block := &claudeResponsesTextBlock{ID: id, Redacted: content.Get("type").String() == "redacted_thinking"}
	if block.Redacted {
		if data := content.Get("data"); data.Type == gjson.String && data.String() != "" {
			block.Signature.WriteString(ClaudeResponsesRedactedThinkingPrefix)
			block.Signature.WriteString(data.String())
		}
		return block
	}
	if signature := content.Get("signature"); signature.Type == gjson.String {
		block.Signature.WriteString(signature.String())
	}
	if text := content.Get("thinking"); text.Type == gjson.String {
		block.Text.WriteString(text.String())
	}
	return block
}
