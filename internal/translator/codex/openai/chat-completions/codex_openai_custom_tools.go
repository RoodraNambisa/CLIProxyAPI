package chat_completions

import "github.com/tidwall/gjson"

// codexOpenAIToolNames shares one shortening map between definitions, replay, and responses.
// A function/custom name collision keeps function-envelope replay unambiguous.
func codexOpenAIToolNames(body []byte) (map[string]string, map[string]bool) {
	var names []string
	seen, functions, custom := make(map[string]bool), make(map[string]bool), make(map[string]bool)
	for _, tool := range gjson.GetBytes(body, "tools").Array() {
		name := ""
		switch tool.Get("type").String() {
		case "function":
			name = tool.Get("function.name").String()
			functions[name] = true
		case "custom":
			name = codexOpenAICustomToolDefinition(tool).Get("name").String()
			custom[name] = true
		}
		if name != "" && !seen[name] {
			names = append(names, name)
			seen[name] = true
		}
	}
	for name := range functions {
		delete(custom, name)
	}
	return buildShortNameMap(names), custom
}

func codexOpenAIReplayToolCall(call gjson.Result, customNames map[string]bool) (name, input string, custom, valid bool) {
	switch call.Get("type").String() {
	case "custom":
		return call.Get("custom.name").String(), call.Get("custom.input").String(), true, true
	case "function":
		name = call.Get("function.name").String()
		return name, call.Get("function.arguments").String(), customNames[name], true
	default:
		return "", "", false, false
	}
}

// Chat Completions nests custom definitions; the flat form remains compatible
// with clients that already normalize their tools to the Responses shape.
func codexOpenAICustomToolDefinition(tool gjson.Result) gjson.Result {
	if nested := tool.Get("custom"); nested.IsObject() {
		return nested
	}
	return tool
}
