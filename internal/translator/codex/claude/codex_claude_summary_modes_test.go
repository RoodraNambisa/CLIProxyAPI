package claude

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexClaudeSummaryPartsMatchAcrossModes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		parts []string
		want  string
	}{
		{"multipart", []string{"first", "second"}, "first\n\nsecond"},
		{"empty first part", []string{"", "second"}, "\n\nsecond"},
		{"single", []string{"single"}, "single"},
		{"signature only", nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var state any
			var chunks [][]byte
			var summary []map[string]string
			for _, part := range tc.parts {
				summary = append(summary, map[string]string{"type": "summary_text", "text": part})
				chunks = append(chunks, ConvertCodexResponseToClaude(t.Context(), "", nil, nil, []byte(`data: {"type":"response.reasoning_summary_part.added"}`), &state)...)
				delta, _ := json.Marshal(map[string]string{"type": "response.reasoning_summary_text.delta", "delta": part})
				chunks = append(chunks, ConvertCodexResponseToClaude(t.Context(), "", nil, nil, append([]byte("data: "), delta...), &state)...)
			}
			item := map[string]any{"type": "reasoning", "id": "rs_test", "summary": summary, "encrypted_content": "opaque"}
			done, _ := json.Marshal(map[string]any{"type": "response.output_item.done", "item": item})
			chunks = append(chunks, ConvertCodexResponseToClaude(t.Context(), "", nil, nil, append([]byte("data: "), done...), &state)...)
			terminal, _ := json.Marshal(map[string]any{"type": "response.completed", "response": map[string]any{"output": []any{item}}})
			plain := ConvertCodexResponseToClaudeNonStream(t.Context(), "", nil, nil, terminal, nil)
			var streamed strings.Builder
			for _, chunk := range chunks {
				for _, line := range strings.Split(string(chunk), "\n") {
					if strings.HasPrefix(line, "data: ") {
						streamed.WriteString(gjson.Get(strings.TrimPrefix(line, "data: "), "delta.thinking").String())
					}
				}
			}
			if gjson.GetBytes(plain, "content.0.thinking").String() != tc.want || streamed.String() != tc.want {
				t.Fatal("streaming and non-streaming summary boundaries differ")
			}
			if gjson.GetBytes(plain, "content.0.signature").String() != "opaque" {
				t.Fatal("summary formatting changed the opaque signature")
			}
		})
	}
}
