package responses

import (
	"bytes"
	"testing"

	"github.com/tidwall/gjson"
)

func TestClaudeResponsesPrefillUsesModelFamilyAndCompatibility(t *testing.T) {
	for _, tc := range []struct {
		model   string
		rejects bool
	}{
		{"claude-fable-5", true}, {"claude-opus-5", true}, {"claude-sonnet-4-6", true},
		{" CLAUDE-OPUS-5 ", true}, {"claude-sonnet-4-5", false}, {"claude-opus-4-6", false}, {"custom-model", false},
	} {
		for _, compat := range []bool{false, true} {
			for _, leading := range []string{"", `{"role":"user","content":"question"},`} {
				raw := []byte(`{"input":[` + leading + `{"role":"assistant","content":"prefix"}]}`)
				before := bytes.Clone(raw)
				out := convertOpenAIResponsesRequestToClaude(tc.model, raw, false, compat)
				messages := gjson.GetBytes(out, "messages").Array()
				if len(messages) == 0 {
					t.Fatal("prefill handling removed the minimal user turn")
				}
				last := messages[len(messages)-1]
				wantAssistant := !tc.rejects || compat
				if (last.Get("role").String() == "assistant") != wantAssistant ||
					(wantAssistant && last.Get("content").String() != "prefix") || !bytes.Equal(raw, before) {
					t.Fatalf("prefill policy mismatch: model=%s compat=%t", tc.model, compat)
				}
			}
		}
	}
	raw := []byte(`{"input":[{"role":"assistant","content":"historical answer"},{"role":"user","content":"next"}]}`)
	if got := ConvertOpenAIResponsesRequestToClaude("claude-sonnet-4-6", raw, false); gjson.GetBytes(got, "messages.0.role").String() != "assistant" || gjson.GetBytes(got, "messages.#").Int() != 2 {
		t.Fatal("model prefill handling removed an earlier assistant turn")
	}
}
