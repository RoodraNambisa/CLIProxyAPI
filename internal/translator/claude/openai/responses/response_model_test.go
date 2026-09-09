package responses

import (
	"bytes"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestClaudeResponsesEarlyEventsKeepOriginalModel(t *testing.T) {
	for _, tc := range []struct{ original, translated, fallback, want string }{
		{`{"model":"team/model(high)"}`, `{"model":"private"}`, "argument", "team/model(high)"},
		{`{"model":"  public  "}`, `{"model":"private"}`, "argument", "  public  "},
		{`{"model":null,"request":{"model":"wrapped"}}`, `{"model":"private"}`, "argument", "wrapped"},
		{`{}`, `{"model":"private"}`, "argument", "private"},
		{`{"model":`, `{"model":"private"}`, "argument", "private"},
		{`{"model":42}`, `{"model":false}`, "argument", "argument"},
		{`{"model":" "}`, `{}`, "", ""},
	} {
		var state any
		original := []byte(tc.original)
		out := ConvertClaudeResponseToOpenAIResponses(t.Context(), tc.fallback, original, []byte(tc.translated), []byte(`data: {"type":"message_start","message":{"id":"fixture","model":"upstream-private"}}`), &state)
		clear(original)
		if len(out) != 2 {
			t.Fatal("message start did not emit both response metadata events")
		}
		for _, chunk := range out {
			for _, line := range bytes.Split(chunk, []byte("\n")) {
				if !bytes.HasPrefix(line, []byte("data:")) {
					continue
				}
				item := gjson.ParseBytes(bytes.TrimPrefix(line, []byte("data:")))
				if !strings.HasPrefix(item.Get("type").String(), "response.") || item.Get("response.model").String() != tc.want ||
					item.Get("response.output").Raw != "[]" || (tc.want == "" && item.Get("response.model").Exists()) {
					t.Fatal("early response metadata lost the requested model or retained an input buffer")
				}
			}
		}
	}
}
