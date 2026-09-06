package responses

import (
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type responsesToolDeclaration struct {
	tool      gjson.Result
	chatName  string
	name      string
	namespace string
	custom    bool
}

// responsesToolDeclarations uses one first-wins order for request definitions,
// history names, and response classification. Additional tools never override tools.
func responsesToolDeclarations(root gjson.Result) []responsesToolDeclaration {
	var declarations []responsesToolDeclaration
	seen := make(map[string]bool)
	emit := func(tool gjson.Result, namespace string) {
		kind := tool.Get("type").String()
		if kind != "" && kind != "function" && kind != "custom" {
			return
		}
		name := strings.TrimSpace(tool.Get("name").String())
		if name == "" {
			name = strings.TrimSpace(tool.Get("function.name").String())
		}
		if name == "" {
			return
		}
		chatName := qualifyResponsesToolName(namespace, name)
		if seen[chatName] {
			return
		}
		seen[chatName] = true
		declarations = append(declarations, responsesToolDeclaration{tool: tool, chatName: chatName, name: name, namespace: namespace, custom: kind == "custom"})
	}
	scan := func(tools gjson.Result) {
		for _, tool := range tools.Array() {
			if tool.Get("type").String() == "namespace" {
				for _, child := range tool.Get("tools").Array() {
					emit(child, strings.TrimSpace(tool.Get("name").String()))
				}
			} else {
				emit(tool, "")
			}
		}
	}
	scan(root.Get("tools"))
	for _, item := range root.Get("input").Array() {
		if item.Get("type").String() == "additional_tools" {
			scan(item.Get("tools"))
		}
	}
	return declarations
}

func qualifyResponsesToolName(namespace, name string) string {
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

func mergeResponsesRequestChatTools(root gjson.Result) [][]byte {
	var tools [][]byte
	for _, declaration := range responsesToolDeclarations(root) {
		tool := []byte(`{"type":"function","function":{"name":"","parameters":{}}}`)
		tool, _ = sjson.SetBytes(tool, "function.name", declaration.chatName)
		description := declaration.tool.Get("description").String()
		if description == "" {
			description = declaration.tool.Get("function.description").String()
		}
		if description != "" {
			tool, _ = sjson.SetBytes(tool, "function.description", description)
		}
		if declaration.custom {
			tool, _ = sjson.SetRawBytes(tool, "function.parameters", []byte(`{"type":"object","properties":{"input":{"type":"string"}},"required":["input"]}`))
		} else {
			for _, path := range []string{"parameters", "parametersJsonSchema", "input_schema", "function.parameters", "function.parametersJsonSchema"} {
				if parameters := declaration.tool.Get(path); parameters.Exists() {
					tool, _ = sjson.SetRawBytes(tool, "function.parameters", []byte(parameters.Raw))
					break
				}
			}
			strict := declaration.tool.Get("strict")
			if !strict.Exists() {
				strict = declaration.tool.Get("function.strict")
			}
			if strict.Exists() {
				tool, _ = sjson.SetRawBytes(tool, "function.strict", []byte(strict.Raw))
			}
		}
		tools = append(tools, tool)
	}
	return tools
}

type responsesToolIdentity struct {
	name, namespace string
	custom          bool
}

// The stream owns only small identity strings, not schema or original request buffers.
func responsesToolIdentities(payload []byte) map[string]responsesToolIdentity {
	identities := make(map[string]responsesToolIdentity)
	for _, declaration := range responsesToolDeclarations(gjson.ParseBytes(payload)) {
		identities[strings.Clone(declaration.chatName)] = responsesToolIdentity{name: strings.Clone(declaration.name), namespace: strings.Clone(declaration.namespace), custom: declaration.custom}
	}
	return identities
}

func responsesToolOutputText(output gjson.Result) string {
	if output.Type == gjson.String {
		return output.String()
	}
	if output.IsArray() {
		var text strings.Builder
		for _, part := range output.Array() {
			if part.Type == gjson.String {
				text.WriteString(part.String())
			} else if value := part.Get("text"); value.Exists() {
				text.WriteString(value.String())
			}
		}
		return text.String()
	}
	return output.Raw
}

func unwrapCustomToolInput(arguments string) string {
	if value := gjson.Get(arguments, "input"); value.Exists() {
		if value.Type == gjson.String {
			return value.String()
		}
		return value.Raw
	}
	return arguments
}

func responsesHistoryToolName(item gjson.Result) string {
	name := strings.TrimSpace(item.Get("name").String())
	if name == "" {
		name = strings.TrimSpace(item.Get("function.name").String())
	}
	return qualifyResponsesToolName(strings.TrimSpace(item.Get("namespace").String()), name)
}

func pickResponsesRequestJSON(original, translated []byte) []byte {
	if len(original) > 0 && gjson.ValidBytes(original) {
		return original
	}
	return translated
}

func joinResponsesRawArray(items [][]byte) []byte {
	size := 2
	for _, item := range items {
		size += len(item) + 1
	}
	out := make([]byte, 0, size)
	out = append(out, '[')
	for index, item := range items {
		if index > 0 {
			out = append(out, ',')
		}
		out = append(out, item...)
	}
	return append(out, ']')
}
