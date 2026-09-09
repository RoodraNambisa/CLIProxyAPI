package common

import (
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// ResponsesToolDeclaration preserves the caller identity alongside its wire name.
type ResponsesToolDeclaration struct {
	Tool                           gjson.Result
	Name, Namespace, QualifiedName string
}

// ResponsesToolArgumentsObject validates a complete object without decoding its
// numbers or repairing a partial argument string into a usable tool call.
func ResponsesToolArgumentsObject(arguments gjson.Result) (string, bool) {
	raw := arguments.Raw
	if arguments.Type == gjson.String {
		raw = arguments.String()
	}
	if !gjson.Valid(raw) || !gjson.Parse(raw).IsObject() {
		return "", false
	}
	return raw, true
}

func QualifyResponsesToolName(namespace, name string) string {
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

// ResponsesToolDeclarations uses root-first, first-wins order for declarations.
func ResponsesToolDeclarations(root gjson.Result) []ResponsesToolDeclaration {
	var result []ResponsesToolDeclaration
	seen := make(map[string]bool)
	var visit func(gjson.Result, string)
	visit = func(tools gjson.Result, namespace string) {
		for _, tool := range tools.Array() {
			name := strings.TrimSpace(tool.Get("name").String())
			if tool.Get("type").String() == "namespace" {
				visit(tool.Get("tools"), QualifyResponsesToolName(namespace, name))
				continue
			}
			qualified := QualifyResponsesToolName(namespace, name)
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
			result = append(result, ResponsesToolDeclaration{Tool: tool, Name: name, Namespace: namespace, QualifiedName: qualified})
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
