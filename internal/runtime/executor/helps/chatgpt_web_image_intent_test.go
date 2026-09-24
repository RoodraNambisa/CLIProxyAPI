package helps

import (
	"encoding/json"
	"testing"
	"unicode/utf16"

	"github.com/tidwall/gjson"
)

func TestChatGPTWebImageToolInput(t *testing.T) {
	const prompt = "Draw a cat \U0001f431 with a cake"
	text, metadata := ChatGPTWebImageToolInput(prompt)
	if text != "@Create image "+prompt {
		t.Fatalf("prompt changed: %q", text)
	}
	body, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	offset := gjson.GetBytes(body, "serialization_metadata.custom_symbol_offsets.0")
	if offset.Get("id").String() != "picture_v2" || offset.Get("symbol").String() != "ecosystemMention" || offset.Get("startIndex").Int() != 0 || offset.Get("endIndex").Int() != int64(len(utf16.Encode([]rune("@Create image")))) {
		t.Fatalf("incorrect tool mention: %s", body)
	}
	metadata["submission_mode"] = "changed"
	_, next := ChatGPTWebImageToolInput(prompt)
	if next["submission_mode"] != "manual_send" {
		t.Fatal("metadata shared between requests")
	}
}
