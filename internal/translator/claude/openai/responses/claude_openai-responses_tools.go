package responses

import (
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func qualifyClaudeResponsesToolName(namespace, name string) string {
	if namespace == "" || strings.HasPrefix(name, "mcp__") || strings.HasPrefix(name, namespace+".") {
		return name
	}
	prefix := namespace
	if !strings.HasSuffix(prefix, "__") {
		prefix += "__"
	}
	if strings.HasPrefix(name, prefix) {
		return name
	}
	return prefix + name
}

// claudeResponsesTools shares first-wins order across root and additional tools.
func claudeResponsesTools(root gjson.Result) []gjson.Result {
	var result []gjson.Result
	seen := make(map[string]bool)
	var visit func(gjson.Result, string)
	visit = func(tools gjson.Result, namespace string) {
		for _, tool := range tools.Array() {
			name := strings.TrimSpace(tool.Get("name").String())
			if tool.Get("type").String() == "namespace" {
				visit(tool.Get("tools"), qualifyClaudeResponsesToolName(namespace, name))
				continue
			}
			qualified := qualifyClaudeResponsesToolName(namespace, name)
			if qualified != "" && seen[qualified] {
				continue
			}
			if qualified != "" {
				seen[qualified] = true
			}
			if qualified != name {
				updated, err := sjson.Set(tool.Raw, "name", qualified)
				if err != nil {
					continue
				}
				tool = gjson.Parse(updated)
			}
			result = append(result, tool)
		}
	}
	visit(root.Get("tools"), "")
	for _, item := range root.Get("input").Array() {
		if item.Get("type").String() == "additional_tools" {
			visit(item.Get("tools"), "")
		}
	}
	return result
}
