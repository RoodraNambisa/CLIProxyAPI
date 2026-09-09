package responses

import (
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexResponsesNonStreamPreservesIncomplete(t *testing.T) {
	for _, event := range []string{"response.completed", "response.incomplete", "response.failed"} {
		response := `{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"output":[{"type":"message","id":"msg_partial","content":[{"type":"output_text","text":"partial"}]}],"usage":{"input_tokens":2,"output_tokens":3,"total_tokens":5}}`
		if event == "response.completed" {
			response = `{"status":"completed","output":[]}`
		}
		raw := []byte(`{"type":"` + event + `","response":` + response + `}`)
		got := ConvertCodexResponseToOpenAIResponsesNonStream(t.Context(), "", nil, nil, raw, nil)
		if event == "response.failed" {
			if len(got) != 0 {
				t.Fatal("failure was converted into a non-stream response")
			}
			continue
		}
		if string(got) != gjson.GetBytes(raw, "response").Raw {
			t.Fatal("terminal response content or incomplete metadata was lost")
		}
	}
}
