package responses

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexResponsesPreservesInclude(t *testing.T) {
	for _, tc := range []struct {
		name, field, want string
	}{
		{"missing", "", `["reasoning.encrypted_content"]`},
		{"empty", `,"include":[]`, `["reasoning.encrypted_content"]`},
		{"replay", `,"include":["output.encrypted_content"]`, `["output.encrypted_content","reasoning.encrypted_content"]`},
		{"existing", `,"include":["output.encrypted_content","reasoning.encrypted_content"]`, `["output.encrypted_content","reasoning.encrypted_content"]`},
		{"other", `,"include":["web_search_call.action.sources","message.output_text.logprobs"]`, `["web_search_call.action.sources","message.output_text.logprobs","reasoning.encrypted_content"]`},
		{"escaped", `,"incl\u0075de":["reasoning.encrypted\u005fcontent","output.encrypted_content"]`, `["reasoning.encrypted_content","output.encrypted_content"]`},
		{"invalid-type", `,"include":"invalid"`, `"invalid"`},
		{"null", `,"include":null`, `null`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := []byte(`{"input":[{"role":"user","content":"hello"}]` + tc.field + `}`)
			original := bytes.Clone(input)
			fast, ok := convertOpenAIResponsesRequestToCodexFastPath(input)
			if !ok {
				t.Fatal("fixture did not exercise the fast path")
			}
			for name, output := range map[string][]byte{
				"fast": fast, "fallback": convertOpenAIResponsesRequestToCodexFallback(input),
				"public": ConvertOpenAIResponsesRequestToCodex("gpt-6-astra", input, true),
			} {
				var got, want any
				if err := json.Unmarshal([]byte(gjson.GetBytes(output, "include").Raw), &got); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal([]byte(tc.want), &want); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Errorf("%s include=%v, want %v", name, got, want)
				}
			}
			if !bytes.Equal(input, original) {
				t.Fatal("conversion mutated the caller's request")
			}
		})
	}
}

func TestCodexResponsesPreservesServiceTierForRuntimePolicy(t *testing.T) {
	for _, value := range []string{`"priority"`, `"flex"`, `"auto"`, `"default"`, `"fast"`, `"future-tier"`, `null`, `17`} {
		input := []byte(`{"input":[],"service_tier":` + value + `}`)
		fast, ok := convertOpenAIResponsesRequestToCodexFastPath(input)
		if !ok {
			t.Fatal("fixture did not exercise the fast path")
		}
		for _, output := range [][]byte{fast, convertOpenAIResponsesRequestToCodexFallback(input)} {
			if got := gjson.GetBytes(output, "service_tier").Raw; got != value {
				t.Errorf("service_tier=%s, want %s", got, value)
			}
		}
	}
}
