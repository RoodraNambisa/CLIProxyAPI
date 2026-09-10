package chat_completions

import (
	"bytes"
	"testing"
)

func TestOpenAIPassthroughStopsAtDonePerStream(t *testing.T) {
	for _, marker := range []string{"[DONE]", "data: [DONE]", "data: \t[DONE]\r"} {
		var state any
		data := []byte(`{"choices":[{"delta":{"content":"fixture"}}]}`)
		call := func(raw []byte, param *any) [][]byte {
			return ConvertOpenAIResponseToOpenAI(t.Context(), "fixture", nil, nil, raw, param)
		}
		if got := call(data, &state); len(got) != 1 || !bytes.Equal(got[0], data) {
			t.Fatal("ordinary payload changed")
		}
		if got := call([]byte(marker), &state); len(got) != 0 {
			t.Fatal("done marker leaked")
		}
		for _, late := range [][]byte{data, []byte(`data: {"usage":{"total_tokens":999}}`), []byte(marker)} {
			if got := call(late, &state); len(got) != 0 {
				t.Fatal("late payload escaped terminal state")
			}
		}
		var next any
		if got := call(data, &next); len(got) != 1 {
			t.Fatal("one stream terminated another")
		}
		if got := call(data, nil); len(got) != 1 {
			t.Fatal("stateless caller behavior changed")
		}
	}
}
