package helps

import "github.com/tidwall/gjson"

// IsOpenAIImageTokenEvent recognizes image data, not progress or usage metadata.
func IsOpenAIImageTokenEvent(payload []byte) bool {
	if !gjson.ValidBytes(payload) {
		return false
	}
	root := gjson.ParseBytes(payload)
	switch root.Get("type").String() {
	case "image_generation.partial_image", "image_generation.completed", "image_edit.partial_image", "image_edit.completed":
		return responsesContentString(root.Get("b64_json")) || responsesContentString(root.Get("url"))
	case "":
		if data := root.Get("data"); data.IsArray() {
			for _, item := range data.Array() {
				if responsesContentString(item.Get("b64_json")) || responsesContentString(item.Get("url")) {
					return true
				}
			}
		}
		return false
	default:
		return IsResponsesTokenEvent(payload)
	}
}
