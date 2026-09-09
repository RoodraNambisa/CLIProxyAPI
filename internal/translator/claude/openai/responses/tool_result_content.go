package responses

import (
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func applyClaudeResponsesToolResultContent(result []byte, output gjson.Result) []byte {
	if output.IsArray() {
		var parts [][]byte
		complete := true
		output.ForEach(func(_, item gjson.Result) bool {
			part := claudeResponsesToolResultPart(item)
			if len(part) == 0 {
				complete = false
				return false
			}
			parts = append(parts, part)
			return true
		})
		// Unknown or incomplete arrays may be business JSON. Preserve their old
		// string representation instead of dropping unrecognized sibling items.
		if complete && len(parts) > 0 {
			if len(parts) == 1 && gjson.GetBytes(parts[0], "type").String() == "text" {
				result, _ = sjson.SetBytes(result, "content", gjson.GetBytes(parts[0], "text").String())
			} else {
				result, _ = sjson.SetRawBytes(result, "content", claudeResponsesRawArray(parts))
			}
			return result
		}
	}
	result, _ = sjson.SetBytes(result, "content", output.String())
	return result
}

func claudeResponsesToolResultPart(part gjson.Result) []byte {
	switch part.Get("type").String() {
	case "input_text", "output_text":
		if text := part.Get("text"); text.Type == gjson.String {
			return claudeResponsesTextPart(text.String())
		}
	case "input_image":
		url := part.Get("image_url")
		if url.Type != gjson.String || url.String() == "" {
			url = part.Get("url")
		}
		if url.Type != gjson.String || url.String() == "" {
			return nil
		}
		value := url.String()
		if !strings.HasPrefix(value, "data:") {
			image, _ := sjson.SetBytes([]byte(`{"type":"image","source":{"type":"url","url":""}}`), "source.url", value)
			return image
		}
		return claudeResponsesInlineResult("image", value)
	case "input_file":
		data := part.Get("file_data")
		if data.Type != gjson.String || data.String() == "" {
			return nil
		}
		return claudeResponsesInlineResult("document", data.String())
	}
	return nil
}

func claudeResponsesInlineResult(kind, value string) []byte {
	mediaType := "application/octet-stream"
	data := value
	if raw, isDataURL := strings.CutPrefix(value, "data:"); isDataURL {
		mime, payload, ok := strings.Cut(raw, ";base64,")
		if !ok || payload == "" {
			return nil
		}
		if mime != "" {
			mediaType = mime
		}
		data = payload
	}
	part, _ := sjson.SetBytes([]byte(`{"type":"","source":{"type":"base64","media_type":"","data":""}}`), "type", kind)
	part, _ = sjson.SetBytes(part, "source.media_type", mediaType)
	part, _ = sjson.SetBytes(part, "source.data", data)
	return part
}
