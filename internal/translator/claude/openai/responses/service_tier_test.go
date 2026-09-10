package responses

import (
	"testing"

	"github.com/tidwall/gjson"
)

func TestResponsesPrioritySelectsClaudeFastMode(t *testing.T) {
	for _, tier := range []string{`"priority"`, `"auto"`, `"default"`, `"flex"`, `""`, `"Priority"`, `null`, `true`, `1`} {
		for _, stream := range []bool{false, true} {
			for _, convert := range []func(string, []byte, bool) []byte{ConvertOpenAIResponsesRequestToClaude, ConvertOpenAIResponsesRequestToClaudeWithCompat} {
				body := []byte(`{"input":"fixture","service_tier":` + tier + `}`)
				out := convert("claude-sonnet-4-5", body, stream)
				want := ""
				if tier == `"priority"` {
					want = "fast"
				}
				if got := gjson.GetBytes(out, "speed").String(); got != want {
					t.Errorf("tier %s: speed = %q, want %q", tier, got, want)
				}
				if gjson.GetBytes(out, "service_tier").Exists() || gjson.GetBytes(out, "stream").Bool() != stream {
					t.Error("source tier leaked or requested stream mode changed")
				}
			}
		}
	}
	if gjson.GetBytes(ConvertOpenAIResponsesRequestToClaude("claude-sonnet-4-5", []byte(`{"input":"fixture"}`), false), "speed").Exists() {
		t.Fatal("missing service tier enabled fast mode")
	}
}
