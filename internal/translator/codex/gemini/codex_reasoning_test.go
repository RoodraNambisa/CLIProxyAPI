package gemini

import (
	"fmt"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexGeminiReasoningUsesTextParts(t *testing.T) {
	for _, tc := range []struct {
		name, item string
		want       []string
	}{
		{"content", `{"type":"reasoning","content":[{"type":"reasoning_text","text":"one"},{"type":"reasoning_text","text":"two"}]}`, []string{"one", "two"}},
		{"summary", `{"type":"reasoning","summary":[{"type":"summary_text","text":"summary"}]}`, []string{"summary"}},
		{"legacy", `{"type":"reasoning","content":"legacy"}`, []string{"legacy"}},
		{"invalid shape", `{"type":"reasoning","content":{"type":"text","text":"untyped"}}`, nil},
		{"opaque", `{"type":"reasoning","encrypted_content":"opaque","content":[{"type":"encrypted_content","text":"hidden"},{"type":"reasoning_text","text":42}]}`, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := []byte(fmt.Sprintf(`{"type":"response.completed","response":{"status":"completed","output":[{"type":"function_call","name":"lookup","call_id":"pair","arguments":"{}"},%s,{"type":"message","content":[{"type":"output_text","text":"answer"}]}]}}`, tc.item))
			out := ConvertCodexResponseToGeminiNonStream(t.Context(), "model", nil, nil, raw, nil)
			parts := gjson.GetBytes(out, "candidates.0.content.parts").Array()
			if len(parts) != len(tc.want)+2 || parts[0].Get("functionCall.name").String() != "lookup" {
				t.Fatal("reasoning parts were lost or opaque fields leaked")
			}
			for i, want := range tc.want {
				if parts[i+1].Get("text").String() != want || !parts[i+1].Get("thought").Bool() {
					t.Fatal("reasoning was stringified as JSON or reordered")
				}
			}
			if parts[len(parts)-1].Get("text").String() != "answer" || parts[len(parts)-1].Get("thought").Bool() {
				t.Fatal("answer was merged into reasoning")
			}
		})
	}
	var state any
	for _, kind := range []string{"response.reasoning_summary_text.delta", "response.reasoning_text.delta"} {
		out := ConvertCodexResponseToGemini(t.Context(), "model", nil, nil, []byte(fmt.Sprintf(`data: {"type":%q,"delta":"thought"}`, kind)), &state)
		if len(out) != 1 || gjson.GetBytes(out[0], "candidates.0.content.parts.0.text").String() != "thought" || !gjson.GetBytes(out[0], "candidates.0.content.parts.0.thought").Bool() {
			t.Fatal("reasoning delta was not delivered as a thought")
		}
	}
}
