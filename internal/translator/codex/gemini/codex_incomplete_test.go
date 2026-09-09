package gemini

import (
	"fmt"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexGeminiIncompleteKeepsToolsAndSingleFinish(t *testing.T) {
	for _, tc := range []struct{ reason, finish string }{{"max_tokens", "MAX_TOKENS"}, {"max_output_tokens", "MAX_TOKENS"}, {"content_filter", "SAFETY"}, {"unknown", "OTHER"}, {"", "STOP"}} {
		t.Run(tc.finish+tc.reason, func(t *testing.T) {
			event, status := "response.incomplete", "incomplete"
			if tc.reason == "" {
				event, status = "response.completed", "completed"
			}
			items := `[{"type":"function_call","call_id":"first","name":"one","arguments":"{}"},{"type":"function_call","call_id":"second","name":"two","arguments":"{}"},{"type":"message","content":[{"type":"output_text","text":"partial answer"}]}]`
			terminal := []byte(fmt.Sprintf(`{"type":%q,"response":{"status":%q,"incomplete_details":{"reason":%q},"output":%s,"usage":{"input_tokens":2,"output_tokens":3}}}`, event, status, tc.reason, items))
			nonstream := ConvertCodexResponseToGeminiNonStream(t.Context(), "model", nil, nil, terminal, nil)
			if !gjson.ValidBytes(nonstream) || gjson.GetBytes(nonstream, "candidates.0.finishReason").String() != tc.finish || gjson.GetBytes(nonstream, "candidates.0.content.parts.#").Int() != 3 || gjson.GetBytes(nonstream, "usageMetadata.totalTokenCount").Int() != 5 {
				t.Fatal("non-stream partial result lost its content, finish reason or usage")
			}
			var state any
			var chunks [][]byte
			for _, item := range gjson.Parse(items).Array() {
				chunks = append(chunks, ConvertCodexResponseToGemini(t.Context(), "model", nil, nil, []byte(`data: {"type":"response.output_item.done","item":`+item.Raw+`}`), &state)...)
			}
			for _, chunk := range chunks {
				if gjson.GetBytes(chunk, "candidates.0.finishReason").Exists() {
					t.Fatal("a tool emitted a finish before the response terminal")
				}
			}
			chunks = append(chunks, ConvertCodexResponseToGemini(t.Context(), "model", nil, nil, append([]byte("data: "), terminal...), &state)...)
			tools, finishes := 0, 0
			for _, chunk := range chunks {
				for _, part := range gjson.GetBytes(chunk, "candidates.0.content.parts").Array() {
					if part.Get("functionCall").Exists() {
						tools++
					}
				}
				if finish := gjson.GetBytes(chunk, "candidates.0.finishReason"); finish.Exists() {
					finishes++
					if finish.String() != tc.finish {
						t.Fatal("incorrect stream finish")
					}
				}
			}
			if tools != 2 || finishes != 1 {
				t.Fatalf("tools=%d finishes=%d, want 2 and 1", tools, finishes)
			}
			if late := ConvertCodexResponseToGemini(t.Context(), "model", nil, nil, []byte(`data: {"type":"response.completed","response":{}}`), &state); len(late) != 0 {
				t.Fatal("late terminal produced another finish")
			}
		})
	}
}
