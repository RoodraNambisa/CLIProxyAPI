package chat_completions

import (
	translatorcommon "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/common"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func setCodexOpenAIToolOutput(result []byte, content gjson.Result) []byte {
	if content.Type == gjson.String {
		text := content.String()
		// Decode only complete image-content arrays, never arbitrary business JSON.
		if gjson.Valid(text) {
			structured := gjson.Parse(text)
			if structured.IsArray() {
				for _, item := range structured.Array() {
					if codexOpenAIToolImagePart(item) != nil {
						content = structured
						break
					}
				}
			}
		}
	}
	if !content.IsArray() {
		result, _ = sjson.SetBytes(result, "output", content.String())
		return result
	}
	items := content.Array()
	parts := make([][]byte, 0, len(items))
	for _, item := range items {
		parts = append(parts, codexOpenAIToolOutputPart(item))
	}
	result, _ = sjson.SetRawBytes(result, "output", translatorcommon.JoinRawArray(parts))
	return result
}

func codexOpenAIToolImagePart(item gjson.Result) []byte {
	prefix := ""
	switch item.Get("type").String() {
	case "image_url":
		prefix = "image_url."
	case "input_image":
	default:
		return nil
	}
	urlPath := "image_url"
	if prefix != "" {
		urlPath = prefix + "url"
	}
	url, fileID := item.Get(urlPath), item.Get(prefix+"file_id")
	if (url.Type != gjson.String || url.String() == "") && (fileID.Type != gjson.String || fileID.String() == "") {
		return nil
	}
	part := []byte(`{"type":"input_image"}`)
	for _, field := range []struct {
		key   string
		value gjson.Result
	}{{"image_url", url}, {"file_id", fileID}, {"detail", item.Get(prefix + "detail")}} {
		if field.value.Type == gjson.String && field.value.String() != "" {
			part, _ = sjson.SetBytes(part, field.key, field.value.String())
		}
	}
	return part
}

func codexOpenAIToolOutputPart(item gjson.Result) []byte {
	if image := codexOpenAIToolImagePart(item); image != nil {
		return image
	}
	switch item.Get("type").String() {
	case "text", "input_text", "output_text":
		part, _ := sjson.SetBytes([]byte(`{"type":"input_text"}`), "text", item.Get("text").String())
		return part
	case "file":
		part := []byte(`{"type":"input_file"}`)
		found := false
		for _, key := range []string{"file_id", "file_data", "file_url", "filename"} {
			value := item.Get("file." + key)
			if value.Type == gjson.String && value.String() != "" {
				part, _ = sjson.SetBytes(part, key, value.String())
				found = found || key != "filename"
			}
		}
		if found {
			return part
		}
	}
	// Keep unknown parts losslessly as text instead of discarding tool output.
	text := item.Raw
	if text == "" {
		text = item.String()
	}
	part, _ := sjson.SetBytes([]byte(`{"type":"input_text"}`), "text", text)
	return part
}
