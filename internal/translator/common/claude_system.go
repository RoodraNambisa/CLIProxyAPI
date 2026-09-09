package common

import (
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// IsClaudeSystemInputRole identifies operator instructions in OpenAI inputs.
func IsClaudeSystemInputRole(role string) bool {
	switch strings.ToLower(strings.TrimSpace(role)) {
	case "system", "developer":
		return true
	default:
		return false
	}
}

// ClaudeSystemInputBlocks preserves separate instruction blocks and their cache
// metadata. Unsupported block payloads are replaced by typed markers so the
// executor can reject them without retaining image or document data.
func ClaudeSystemInputBlocks(content, item gjson.Result) [][]byte {
	var blocks [][]byte
	appendText := func(text string, source gjson.Result) {
		if text == "" {
			return
		}
		block, _ := sjson.SetBytes([]byte(`{"type":"text","text":""}`), "text", text)
		if cache := source.Get("cache_control"); cache.IsObject() {
			block, _ = sjson.SetRawBytes(block, "cache_control", []byte(cache.Raw))
		}
		blocks = append(blocks, block)
	}
	if content.Type == gjson.String {
		appendText(content.String(), gjson.Result{})
	} else if content.IsArray() {
		content.ForEach(func(_, part gjson.Result) bool {
			switch part.Get("type").String() {
			case "text", "input_text", "output_text":
				appendText(part.Get("text").String(), part)
			default:
				blocks = append(blocks, []byte(`{"type":"unsupported_system_content"}`))
			}
			return true
		})
	}
	if last := len(blocks) - 1; last >= 0 && !gjson.GetBytes(blocks[last], "cache_control").Exists() {
		if cache := item.Get("cache_control"); cache.IsObject() {
			blocks[last], _ = sjson.SetRawBytes(blocks[last], "cache_control", []byte(cache.Raw))
		}
	}
	return blocks
}

const (
	claudeSystemReminderStart = "<system-reminder>"
	claudeSystemReminderEnd   = "</system-reminder>"
)

// ClaudeMessageSystemReminderText converts a Claude message-level system value
// into ordinary user-visible reminder text for non-Claude upstream formats.
func ClaudeMessageSystemReminderText(content gjson.Result) (string, bool) {
	parts := claudeSystemTextParts(content)
	if len(parts) == 0 {
		return "", false
	}
	text := strings.Join(parts, "\n")
	if strings.TrimSpace(text) == "" {
		return "", false
	}
	return claudeSystemReminderStart + "\n" + text + "\n" + claudeSystemReminderEnd, true
}

func claudeSystemTextParts(content gjson.Result) []string {
	if !content.Exists() {
		return nil
	}
	if content.Type == gjson.String {
		text := content.String()
		if text == "" || util.IsClaudeCodeAttributionSystemText(text) {
			return nil
		}
		return []string{text}
	}
	if !content.IsArray() {
		return nil
	}
	parts := make([]string, 0)
	content.ForEach(func(_, item gjson.Result) bool {
		if item.Get("type").String() != "text" {
			return true
		}
		text := item.Get("text").String()
		if text == "" || util.IsClaudeCodeAttributionSystemText(text) {
			return true
		}
		parts = append(parts, text)
		return true
	})
	return parts
}
