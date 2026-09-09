package interactions

import (
	"bytes"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexInteractionsRetainsPartialStatus(t *testing.T) {
	for _, status := range []string{"completed", "incomplete"} {
		t.Run(status, func(t *testing.T) {
			terminal := []byte(`{"type":"response.` + status + `","response":{"id":"partial","status":"` + status + `","incomplete_details":{"reason":"max_tokens"},"output":[{"type":"message","content":[{"type":"output_text","text":"available"}]}],"usage":{"input_tokens":2,"output_tokens":3,"total_tokens":5}}}`)
			out := ConvertCodexResponseToInteractionsNonStream(t.Context(), "model", nil, nil, terminal, nil)
			if gjson.GetBytes(out, "status").String() != status || !bytes.Contains(out, []byte("available")) {
				t.Fatal("non-stream partial status or content was lost")
			}
			var state any
			chunks := ConvertCodexResponseToInteractions(t.Context(), "model", nil, nil, terminal, &state)
			finishes := 0
			for _, line := range bytes.Split(bytes.Join(chunks, nil), []byte("\n")) {
				if !bytes.HasPrefix(line, []byte("data: ")) {
					continue
				}
				if root := gjson.ParseBytes(line[6:]); root.Get("event_type").String() == "interaction.completed" {
					finishes++
					if root.Get("interaction.status").String() != status {
						t.Fatal("stream partial status was lost")
					}
				}
			}
			if finishes != 1 {
				t.Fatalf("got %d terminals", finishes)
			}
			if late := ConvertCodexResponseToInteractions(t.Context(), "model", nil, nil, []byte(`{"type":"response.output_text.delta","delta":"late"}`), &state); len(late) != 0 {
				t.Fatal("late content was delivered after the terminal")
			}
		})
	}
}
