package responses

import (
	"strings"

	translatorcommon "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/common"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func setResponsesChatToolOutput(message []byte, output gjson.Result) []byte {
	content := output
	if output.Type == gjson.String {
		text := strings.TrimSpace(output.String())
		if strings.HasPrefix(text, "[") && gjson.Valid(text) {
			content = gjson.Parse(text)
		}
	}
	items := content.Array()
	hasImage, valid := false, content.IsArray()
	for _, item := range items {
		switch item.Get("type").String() {
		case "text", "input_text", "output_text":
			valid = valid && item.Get("text").Type == gjson.String
		case "input_image", "image_url":
			_, _, ok := responsesChatToolImage(item)
			valid, hasImage = valid && ok, true
		}
	}
	if !valid || !hasImage {
		message, _ = sjson.SetBytes(message, "content", responsesToolOutputText(output))
		return message
	}
	parts := make([][]byte, 0, len(items))
	for _, item := range items {
		if url, detail, ok := responsesChatToolImage(item); ok {
			part, _ := sjson.SetBytes([]byte(`{"type":"image_url","image_url":{}}`), "image_url.url", url)
			if detail != "" {
				part, _ = sjson.SetBytes(part, "image_url.detail", detail)
			}
			parts = append(parts, part)
			continue
		}
		text := item.Raw
		if item.Type == gjson.String {
			text = item.String()
		} else {
			switch item.Get("type").String() {
			case "text", "input_text", "output_text":
				text = item.Get("text").String()
			}
		}
		part, _ := sjson.SetBytes([]byte(`{"type":"text"}`), "text", text)
		parts = append(parts, part)
	}
	message, _ = sjson.SetRawBytes(message, "content", translatorcommon.JoinRawArray(parts))
	return message
}

func responsesChatToolImage(item gjson.Result) (string, string, bool) {
	var url, detail gjson.Result
	switch item.Get("type").String() {
	case "input_image":
		url, detail = item.Get("image_url"), item.Get("detail")
	case "image_url":
		url, detail = item.Get("image_url.url"), item.Get("image_url.detail")
	default:
		return "", "", false
	}
	if url.Type != gjson.String || strings.TrimSpace(url.String()) == "" || detail.Exists() && detail.Type != gjson.String {
		return "", "", false
	}
	resolution := strings.ToLower(strings.TrimSpace(detail.String()))
	switch resolution {
	case "auto", "low", "high":
	case "original":
		resolution = "high"
	default:
		resolution = ""
	}
	return url.String(), resolution, true
}
