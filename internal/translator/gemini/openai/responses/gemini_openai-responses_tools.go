package responses

import (
	"strings"

	translatorcommon "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/common"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type geminiResponsesToolIdentity struct {
	name, namespace string
	ambiguous       bool
	custom          bool
}

// Keep only detached names; response streams must not retain tool schemas or prompts.
func geminiResponsesToolIdentities(original, translated []byte) map[string]geminiResponsesToolIdentity {
	identities := make(map[string]geminiResponsesToolIdentity)
	root := unwrapRequestRoot(gjson.ParseBytes(pickRequestJSON(original, translated)))
	for _, declaration := range translatorcommon.ResponsesToolDeclarations(root) {
		kind := declaration.Tool.Get("type").String()
		if (kind != "function" && kind != "custom") || declaration.Name == "" {
			continue
		}
		wireName := util.SanitizeFunctionName(declaration.QualifiedName)
		identity := geminiResponsesToolIdentity{name: strings.Clone(declaration.Name), namespace: strings.Clone(declaration.Namespace), custom: kind == "custom"}
		if previous, exists := identities[wireName]; exists && previous != identity {
			identity = geminiResponsesToolIdentity{ambiguous: true}
		}
		identities[strings.Clone(wireName)] = identity
	}
	return identities
}

func restoreGeminiResponsesToolIdentity(payload []byte, prefix, wireName string, identities map[string]geminiResponsesToolIdentity) []byte {
	name, namespace := wireName, ""
	if identity, exists := identities[wireName]; exists && !identity.ambiguous {
		name, namespace = identity.name, identity.namespace
	}
	payload, _ = sjson.SetBytes(payload, prefix+"name", name)
	if namespace != "" {
		payload, _ = sjson.SetBytes(payload, prefix+"namespace", namespace)
	}
	return payload
}

func geminiResponsesToolIsCustom(identities map[string]geminiResponsesToolIdentity, name string) bool {
	identity := identities[name]
	return identity.custom && !identity.ambiguous
}

func geminiResponsesToolItemID(identities map[string]geminiResponsesToolIdentity, name, callID string) string {
	if geminiResponsesToolIsCustom(identities, name) {
		return "ctc_" + callID
	}
	return "fc_" + callID
}

func geminiResponsesToolArguments(identities map[string]geminiResponsesToolIdentity, name, arguments string) (string, string, string) {
	if !geminiResponsesToolIsCustom(identities, name) {
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

func buildGeminiResponsesToolItem(identities map[string]geminiResponsesToolIdentity, name, callID, arguments, status string) []byte {
	kind := "function_call"
	if geminiResponsesToolIsCustom(identities, name) {
		kind = "custom_tool_call"
	}
	_, field, value := geminiResponsesToolArguments(identities, name, arguments)
	item := []byte(`{"id":"","type":"","status":"","call_id":"","name":""}`)
	item, _ = sjson.SetBytes(item, "id", geminiResponsesToolItemID(identities, name, callID))
	item, _ = sjson.SetBytes(item, "type", kind)
	item, _ = sjson.SetBytes(item, "status", status)
	item, _ = sjson.SetBytes(item, "call_id", callID)
	item, _ = sjson.SetBytes(item, field, value)
	return restoreGeminiResponsesToolIdentity(item, "", name, identities)
}
