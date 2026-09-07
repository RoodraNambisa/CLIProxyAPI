package responses

import (
	"bytes"
	"testing"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestClaudeResponsesCustomDeclarationsHistoryAndSelection(t *testing.T) {
	for _, input := range []string{"", "line1\nline2", `literal {"input":"nested"} and \\ escapes`, "工具内容 😀"} {
		raw := []byte(`{"tools":[{"type":"function","name":"lookup","parameters":{"type":"object","properties":{"value":{"default":9007199254740993}}}}],"input":[{"type":"additional_tools","tools":[{"type":"namespace","name":"editor","tools":[{"type":"custom","name":"patch","description":"Apply text","format":{"type":"text"}}]}]},{"type":"custom_tool_call","namespace":"editor","name":"patch","call_id":"pair","input":""},{"type":"custom_tool_call_output","call_id":"pair","output":"done\nnext"}],"tool_choice":{"type":"custom","namespace":"editor","name":"patch"}}`)
		raw, _ = sjson.SetBytes(raw, "input.1.input", input)
		original := bytes.Clone(raw)
		got := ConvertOpenAIResponsesRequestToClaude("claude-sonnet-4-6", raw, false)
		if gjson.GetBytes(got, "tools.#").Int() != 2 || gjson.GetBytes(got, "tools.1.name").String() != "editor__patch" || gjson.GetBytes(got, "tools.1.description").String() != "Apply text" || gjson.GetBytes(got, "tools.1.input_schema.properties.input.type").String() != "string" || gjson.GetBytes(got, "tools.1.input_schema.required.0").String() != "input" {
			t.Fatal("custom tool did not become a named string-input tool")
		}
		if gjson.GetBytes(got, "tools.0.input_schema").Raw != gjson.GetBytes(raw, "tools.0.parameters").Raw {
			t.Fatal("ordinary function schema changed")
		}
		if gjson.GetBytes(got, "messages.0.content.0.name").String() != "editor__patch" || gjson.GetBytes(got, "messages.0.content.0.id").String() != "pair" || gjson.GetBytes(got, "messages.0.content.0.input.input").String() != input || gjson.GetBytes(got, "messages.1.content.0.tool_use_id").String() != "pair" || gjson.GetBytes(got, "messages.1.content.0.content").String() != "done\nnext" {
			t.Fatal("custom history lost pairing, input text or result ordering")
		}
		if gjson.GetBytes(got, "tool_choice.type").String() != "tool" || gjson.GetBytes(got, "tool_choice.name").String() != "editor__patch" || !bytes.Equal(raw, original) {
			t.Fatal("selection disagrees with its declaration or the source was mutated")
		}
	}
}

func TestClaudeResponsesCustomDeclarationDoesNotOverrideRootFunction(t *testing.T) {
	raw := []byte(`{"tools":[{"type":"function","name":"editor__patch","parameters":{"type":"object","properties":{"kept":{"type":"integer"}}}}],"input":[{"type":"additional_tools","tools":[{"type":"namespace","name":"editor","tools":[{"type":"custom","name":"patch"}]}]}]}`)
	got := ConvertOpenAIResponsesRequestToClaude("claude-sonnet-4-6", raw, false)
	if gjson.GetBytes(got, "tools.#").Int() != 1 || gjson.GetBytes(got, "tools.0.input_schema.properties.kept.type").String() != "integer" || gjson.GetBytes(got, "tools.0.input_schema.properties.input").Exists() {
		t.Fatal("an additional custom declaration replaced the root function contract")
	}
}
