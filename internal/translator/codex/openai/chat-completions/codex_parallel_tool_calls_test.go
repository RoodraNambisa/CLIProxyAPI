package chat_completions

import (
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexChatParallelToolCallsPreservesExplicitValue(t *testing.T) {
	for _, tc := range []struct {
		name, field, want string
	}{
		{"default", "", "true"},
		{"enabled", `,"parallel_tool_calls":true`, "true"},
		{"disabled", `,"parallel_tool_calls":false`, "false"},
		{"escaped", `,"parallel_tool_ca\u006cls":false`, "false"},
		{"null", `,"parallel_tool_calls":null`, "null"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := []byte(`{"messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object"}}}]` + tc.field + `}`)
			for _, stream := range []bool{false, true} {
				output := ConvertOpenAIRequestToCodex("gpt-6-astra", input, stream)
				if got := gjson.GetBytes(output, "parallel_tool_calls").Raw; got != tc.want {
					t.Fatalf("stream=%t parallel_tool_calls=%s, want %s", stream, got, tc.want)
				}
			}
		})
	}
}
