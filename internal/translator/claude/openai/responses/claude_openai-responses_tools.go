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

type claudeResponsesTool struct {
	tool                       gjson.Result
	name, namespace, qualified string
}

// claudeResponsesTools shares first-wins order across root and additional tools.
func claudeResponsesTools(root gjson.Result) []claudeResponsesTool {
	var result []claudeResponsesTool
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
			result = append(result, claudeResponsesTool{tool: tool, name: name, namespace: namespace, qualified: qualified})
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

type claudeResponsesToolIdentity struct {
	name, namespace string
}

func claudeResponsesToolIdentities(original, translated []byte) map[string]claudeResponsesToolIdentity {
	identities := make(map[string]claudeResponsesToolIdentity)
	for _, declaration := range claudeResponsesTools(gjson.ParseBytes(pickRequestJSON(original, translated))) {
		identities[strings.Clone(declaration.qualified)] = claudeResponsesToolIdentity{
			name: strings.Clone(declaration.name), namespace: strings.Clone(declaration.namespace),
		}
	}
	return identities
}

func restoreClaudeResponsesToolIdentity(payload []byte, prefix, wireName string, identities map[string]claudeResponsesToolIdentity) []byte {
	name, namespace := wireName, ""
	if identity, exists := identities[wireName]; exists {
		name, namespace = identity.name, identity.namespace
	}
	payload, _ = sjson.SetBytes(payload, prefix+"name", name)
	if namespace != "" {
		payload, _ = sjson.SetBytes(payload, prefix+"namespace", namespace)
	}
	return payload
}
