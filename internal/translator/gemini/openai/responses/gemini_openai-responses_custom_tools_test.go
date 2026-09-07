package responses

import (
	"bytes"
	"testing"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestGeminiResponsesCustomDeclarationsAndMixedHistory(t *testing.T) {
	for _, input := range []string{"", "one\ntwo", `literal {"input":"nested"}`, "中文 😀"} {
		raw := []byte(`{"tools":[{"type":"function","name":"lookup","parameters":{"type":"object","properties":{"value":{"default":9007199254740993}}}}],"input":[{"type":"additional_tools","tools":[{"type":"namespace","name":"editor","tools":[{"type":"custom","name":"patch/file","description":"Apply text","format":{"type":"text"}}]}]},{"type":"function_call","name":"lookup","call_id":"normal","arguments":"{\"value\":1}"},{"type":"custom_tool_call","namespace":"editor","name":"patch/file","call_id":"custom","input":""},{"type":"custom_tool_call_output","call_id":"custom","output":{"accepted":true}},{"type":"function_call_output","call_id":"normal","output":"found"}],"tool_choice":{"type":"custom","namespace":"editor","name":"patch/file"}}`)
		raw, _ = sjson.SetBytes(raw, "input.2.input", input)
		original := bytes.Clone(raw)
		got := ConvertOpenAIResponsesRequestToGemini("gemini-2.5-flash", raw, false)
		tools := gjson.GetBytes(got, "tools.0.functionDeclarations")
		if len(tools.Array()) != 2 || tools.Get("1.name").String() != "editor__patch_file" || tools.Get("1.parametersJsonSchema.properties.input.type").String() != "string" || tools.Get("1.parametersJsonSchema.required.0").String() != "input" || tools.Get("0.parametersJsonSchema").Raw != gjson.GetBytes(raw, "tools.0.parameters").Raw {
			t.Fatal("custom declaration or normal schema changed")
		}
		calls := make(map[string]gjson.Result)
		results := make(map[string]gjson.Result)
		for _, content := range gjson.GetBytes(got, "contents").Array() {
			for _, part := range content.Get("parts").Array() {
				if call := part.Get("functionCall"); call.Exists() {
					calls[call.Get("id").String()] = call
				}
				if result := part.Get("functionResponse"); result.Exists() {
					results[result.Get("id").String()] = result
				}
			}
		}
		if len(calls) != 2 || len(results) != 2 || calls["custom"].Get("name").String() != "editor__patch_file" || calls["custom"].Get("args.input").String() != input || results["custom"].Get("name").String() != "editor__patch_file" || !results["custom"].Get("response.result.accepted").Bool() || results["normal"].Get("name").String() != "lookup" || results["normal"].Get("response.result").String() != "found" {
			t.Fatal("mixed custom/function history lost names, pairing, input or results")
		}
		if gjson.GetBytes(got, "toolConfig.functionCallingConfig.allowedFunctionNames.0").String() != "editor__patch_file" || !bytes.Equal(raw, original) {
			t.Fatal("selection or source ownership changed")
		}
	}
}
