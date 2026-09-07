package responses

import (
	"bytes"
	"testing"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestInteractionsCustomDeclarationHistoryAndChoice(t *testing.T) {
	for _, input := range []string{"", "line1\nline2", `literal {"input":"nested"}`, "中文 😀"} {
		raw := []byte(`{"tools":[{"type":"function","name":"lookup","parameters":{"type":"object","properties":{"value":{"default":9007199254740993}}}}],"input":[{"type":"additional_tools","tools":[{"type":"namespace","name":"editor","children":[{"type":"custom","name":"patch","description":"Apply text"}]}]},{"type":"custom_tool_call","namespace":"editor","name":"patch","call_id":"pair","input":""},{"type":"custom_tool_call_output","call_id":"pair","output":{"accepted":true}}],"tool_choice":{"type":"custom","namespace":"editor","name":"patch"}}`)
		raw, _ = sjson.SetBytes(raw, "input.1.input", input)
		original := bytes.Clone(raw)
		got := ConvertOpenAIResponsesRequestToInteractions("gemini-2.5-flash", raw, false)
		if gjson.GetBytes(got, "tools.#").Int() != 2 || gjson.GetBytes(got, "tools.1.name").String() != "editor__patch" || gjson.GetBytes(got, "tools.1.type").String() != "function" || gjson.GetBytes(got, "tools.1.parameters.properties.input.type").String() != "string" || gjson.GetBytes(got, "tools.0.parameters").Raw != gjson.GetBytes(raw, "tools.0.parameters").Raw {
			t.Fatal("custom or ordinary declaration changed its contract")
		}
		if gjson.GetBytes(got, "input.0.type").String() != "function_call" || gjson.GetBytes(got, "input.0.name").String() != "editor__patch" || gjson.GetBytes(got, "input.0.call_id").String() != "pair" || gjson.GetBytes(got, "input.0.arguments.input").String() != input || gjson.GetBytes(got, "input.1.type").String() != "function_result" || gjson.GetBytes(got, "input.1.name").String() != "editor__patch" || gjson.GetBytes(got, "input.1.call_id").String() != "pair" || !gjson.GetBytes(got, "input.1.result.accepted").Bool() {
			t.Fatal("custom history lost input, output or pairing")
		}
		if gjson.GetBytes(got, "generation_config.tool_choice.type").String() != "function" || gjson.GetBytes(got, "generation_config.tool_choice.name").String() != "editor__patch" || gjson.GetBytes(got, "generation_config.tool_choice.namespace").Exists() || !bytes.Equal(raw, original) {
			t.Fatal("choice differs from its declaration or source was mutated")
		}
	}
}
