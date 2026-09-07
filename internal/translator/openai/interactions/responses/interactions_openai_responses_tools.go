package responses

import (
	"strings"

	translatorcommon "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/common"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Preserve the existing Interactions aliases while sharing declaration order.
func responsesInteractionToolDeclarations(root gjson.Result) []translatorcommon.ResponsesToolDeclaration {
	var declarations []translatorcommon.ResponsesToolDeclaration
	seen := make(map[string]bool)
	var visit func(gjson.Result, string)
	visit = func(tools gjson.Result, namespace string) {
		for _, tool := range tools.Array() {
			name := strings.TrimSpace(firstNonEmpty(tool.Get("name").String(), tool.Get("function.name").String()))
			if tool.Get("type").String() == "namespace" {
				children := firstExisting(tool.Get("children"), tool.Get("tools"))
				visit(children, translatorcommon.QualifyResponsesToolName(namespace, name))
				continue
			}
			if kind := tool.Get("type").String(); kind != "function" && kind != "custom" && kind != "" || name == "" {
				continue
			}
			qualified := translatorcommon.QualifyResponsesToolName(namespace, name)
			if seen[qualified] {
				continue
			}
			seen[qualified] = true
			wire, _ := sjson.Set(tool.Raw, "name", qualified)
			declarations = append(declarations, translatorcommon.ResponsesToolDeclaration{Tool: gjson.Parse(wire), Name: name, Namespace: namespace, QualifiedName: qualified})
		}
	}
	visit(root.Get("tools"), "")
	for _, item := range root.Get("input").Array() {
		if item.Get("type").String() == "additional_tools" {
			visit(item.Get("tools"), "")
		}
	}
	return declarations
}

type interactionsResponsesToolIdentity struct {
	name, namespace string
	custom          bool
}

func interactionsResponsesToolIdentities(original, translated []byte) map[string]interactionsResponsesToolIdentity {
	if !gjson.ValidBytes(original) {
		original = translated
	}
	identities := make(map[string]interactionsResponsesToolIdentity)
	for _, declaration := range responsesInteractionToolDeclarations(gjson.ParseBytes(original)) {
		identities[strings.Clone(declaration.QualifiedName)] = interactionsResponsesToolIdentity{
			name: strings.Clone(declaration.Name), namespace: strings.Clone(declaration.Namespace), custom: declaration.Tool.Get("type").String() == "custom",
		}
	}
	return identities
}

func restoreInteractionsResponsesToolIdentity(payload []byte, prefix, wireName string, identities map[string]interactionsResponsesToolIdentity) []byte {
	if identity, exists := identities[wireName]; exists {
		payload, _ = sjson.SetBytes(payload, prefix+"name", identity.name)
		if identity.namespace != "" {
			payload, _ = sjson.SetBytes(payload, prefix+"namespace", identity.namespace)
		}
	}
	return payload
}

func interactionsResponsesToolArguments(identities map[string]interactionsResponsesToolIdentity, name, arguments string) (string, string, string) {
	if !identities[name].custom {
		return "response.function_call_arguments", "arguments", arguments
	}
	if gjson.Valid(arguments) {
		if input := gjson.Get(arguments, "input"); input.Exists() {
			if input.Type == gjson.String {
				arguments = input.String()
			} else {
				arguments = input.Raw
			}
		}
	}
	return "response.custom_tool_call_input", "input", arguments
}

func buildInteractionsResponsesToolItem(identities map[string]interactionsResponsesToolIdentity, name, itemID, callID, arguments, status string) []byte {
	kind := "function_call"
	if identities[name].custom {
		kind, itemID = "custom_tool_call", "ctc_"+callID
	}
	_, field, value := interactionsResponsesToolArguments(identities, name, arguments)
	item := []byte(`{"type":"","call_id":"","name":""}`)
	item, _ = sjson.SetBytes(item, "type", kind)
	if itemID != "" {
		item, _ = sjson.SetBytes(item, "id", itemID)
	}
	if identities[name].custom {
		item, _ = sjson.SetBytes(item, "status", status)
	}
	item, _ = sjson.SetBytes(item, "call_id", callID)
	item, _ = sjson.SetBytes(item, "name", name)
	item, _ = sjson.SetBytes(item, field, value)
	return restoreInteractionsResponsesToolIdentity(item, "", name, identities)
}

func qualifiedResponsesInteractionName(item gjson.Result) string {
	name := strings.TrimSpace(item.Get("name").String())
	if name == "" {
		return ""
	}
	return translatorcommon.QualifyResponsesToolName(strings.TrimSpace(item.Get("namespace").String()), name)
}
