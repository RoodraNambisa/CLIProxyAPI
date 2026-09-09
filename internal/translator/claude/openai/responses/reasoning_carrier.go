package responses

import (
	"strings"

	"github.com/tidwall/gjson"
)

// ClaudeResponsesRedactedThinkingPrefix carries an opaque Claude redacted block
// through Responses without treating its data as readable reasoning text.
const ClaudeResponsesRedactedThinkingPrefix = "claude-redacted-thinking:"

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
