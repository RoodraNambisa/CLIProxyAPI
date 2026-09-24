package helps

const chatGPTWebImageMention = "@Create image"

// ChatGPTWebImageToolInput serializes the same explicit tool selection as the
// website's image composer. The ASCII label keeps byte and UTF-16 offsets equal.
// User text is kept verbatim after the separate tool mention.
func ChatGPTWebImageToolInput(prompt string) (string, map[string]any) {
	return chatGPTWebImageMention + " " + prompt, map[string]any{
		"system_hints":    []string{"picture_v2"},
		"submission_mode": "manual_send",
		"serialization_metadata": map[string]any{
			"custom_symbol_offsets": []any{map[string]any{
				"id": "picture_v2", "symbol": "ecosystemMention",
				"startIndex": 0, "endIndex": len(chatGPTWebImageMention),
			}},
		},
	}
}
