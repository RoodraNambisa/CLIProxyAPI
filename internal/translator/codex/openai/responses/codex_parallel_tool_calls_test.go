package responses

import (
	"bytes"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexResponsesParallelToolCallsPreservesExplicitValue(t *testing.T) {
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
			input := []byte(`{"input":[{"role":"user","content":"hello"}],"tools":[{"type":"function","name":"lookup"}]` + tc.field + `}`)
			original := bytes.Clone(input)
			fast, ok := convertOpenAIResponsesRequestToCodexFastPath(input)
			if !ok {
				t.Fatal("fixture did not exercise the fast path")
			}
			for _, output := range [][]byte{
				fast,
				convertOpenAIResponsesRequestToCodexFallback(input),
				ConvertOpenAIResponsesRequestToCodex("gpt-6-astra", input, true),
			} {
				if got := gjson.GetBytes(output, "parallel_tool_calls").Raw; got != tc.want {
					t.Fatalf("parallel_tool_calls=%s, want %s", got, tc.want)
				}
			}
			if !bytes.Equal(input, original) {
				t.Fatal("conversion mutated the caller's request")
			}
		})
	}
}
