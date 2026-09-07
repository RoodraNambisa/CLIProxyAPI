package helps

import (
	"encoding/json"

	translatorcommon "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/common"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// PromoteXAIAdditionalTools feeds Lite declarations through xAI's existing
// namespace flattening and tool filters without changing its short-name policy.
func PromoteXAIAdditionalTools(body []byte) []byte {
	if !gjson.GetBytes(body, `input.#(type=="additional_tools")`).Exists() {
		return body
	}
	root := gjson.ParseBytes(body)
	if !root.Get("input").IsArray() || root.Get("tools").Exists() && !root.Get("tools").IsArray() {
		return body
	}
	items := root.Get("input").Array()
	found := false
	for _, item := range items {
		if item.Get("type").String() == "additional_tools" {
			if !item.Get("tools").IsArray() {
				return body
			}
			found = true
		}
	}
	if !found || !gjson.ValidBytes(body) {
		return body
	}
	tools := make([]json.RawMessage, 0)
	for _, declaration := range translatorcommon.ResponsesToolDeclarations(root) {
		raw := []byte(declaration.Tool.Raw)
		if declaration.Namespace != "" {
			raw, _ = sjson.SetBytes(raw, "name", declaration.Name)
			group := []byte(`{"type":"namespace","name":"","tools":[]}`)
			group, _ = sjson.SetBytes(group, "name", declaration.Namespace)
			group, _ = sjson.SetRawBytes(group, "tools.-1", raw)
			raw = group
		}
		tools = append(tools, raw)
	}
	input := make([]json.RawMessage, 0, len(items))
	for _, item := range items {
		if item.Get("type").String() != "additional_tools" {
			input = append(input, json.RawMessage(item.Raw))
		}
	}
	toolsJSON, errTools := json.Marshal(tools)
	inputJSON, errInput := json.Marshal(input)
	if errTools != nil || errInput != nil {
		return body
	}
	out, errTools := sjson.SetRawBytes(body, "tools", toolsJSON)
	out, errInput = sjson.SetRawBytes(out, "input", inputJSON)
	if errTools != nil || errInput != nil {
		return body
	}
	return out
}
