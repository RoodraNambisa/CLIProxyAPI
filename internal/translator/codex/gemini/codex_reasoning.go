package gemini

import (
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func appendCodexGeminiReasoningParts(parts [][]byte, item gjson.Result) [][]byte {
	appendText := func(text string) {
		if text == "" {
			return
		}
		part, _ := sjson.SetBytes([]byte(`{"text":"","thought":true}`), "text", text)
		parts = append(parts, part)
	}
	for _, field := range []string{"summary", "content"} {
		value := item.Get(field)
		if value.Type == gjson.String {
			appendText(value.String())
			continue
		}
		if !value.IsArray() {
			continue
		}
		for _, part := range value.Array() {
			switch part.Get("type").String() {
			case "reasoning_text", "summary_text", "text":
				if text := part.Get("text"); text.Type == gjson.String {
					appendText(text.String())
				}
			}
		}
	}
	return parts
}
