package openai

import (
	"strings"

	"github.com/tidwall/gjson"
)

const codexLocalCompactionSummaryPrefix = "Another language model started to solve this problem and produced a summary of its thinking process. You also have access to the state of the tools that were used by that language model. Use this to build on the work that has already been done and avoid duplicating work. Here is the summary produced by the other language model, use the information in this summary to assist with your own analysis:"

func inputHasCodexLocalCompactionSummary(input gjson.Result) bool {
	if !input.IsArray() {
		return false
	}
	hasSummary := false
	for index, item := range input.Array() {
		itemType := item.Get("type").String()
		role := item.Get("role").String()
		if itemType == "additional_tools" {
			tools := item.Get("tools")
			if index != 0 || role != "developer" || !tools.IsArray() {
				return false
			}
			for _, tool := range tools.Array() {
				if !tool.IsObject() || strings.TrimSpace(tool.Get("type").String()) == "" {
					return false
				}
			}
			continue
		}
		if (itemType != "" && itemType != "message") || (role != "user" && role != "developer") {
			return false
		}
		if role == "user" && strings.HasPrefix(codexLocalCompactionMessageText(item), codexLocalCompactionSummaryPrefix+"\n") {
			hasSummary = true
		}
	}
	return hasSummary
}

func codexLocalCompactionMessageText(message gjson.Result) string {
	content := message.Get("content")
	if content.Type == gjson.String {
		return content.String()
	}
	if !content.IsArray() {
		return ""
	}
	var text strings.Builder
	for _, part := range content.Array() {
		if part.Get("type").String() == "input_text" && part.Get("text").Type == gjson.String {
			text.WriteString(part.Get("text").String())
		}
	}
	return text.String()
}
