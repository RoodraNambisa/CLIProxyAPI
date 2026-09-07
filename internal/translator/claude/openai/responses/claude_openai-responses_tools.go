package responses

import (
	"strings"

	translatorcommon "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/common"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func qualifyClaudeResponsesToolName(namespace, name string) string {
	return translatorcommon.QualifyResponsesToolName(namespace, name)
}

func claudeResponsesTools(root gjson.Result) []translatorcommon.ResponsesToolDeclaration {
	return translatorcommon.ResponsesToolDeclarations(root)
}

type claudeResponsesToolIdentity struct {
	name, namespace string
	custom          bool
}

func claudeResponsesBlockIndex(value gjson.Result) (int, bool) {
	index := value.Int()
	if value.Type != gjson.Number || index < 0 || value.Float() != float64(index) || int64(int(index)) != index {
		return 0, false
	}
	return int(index), true
}

func claudeResponsesToolIdentities(original, translated []byte) map[string]claudeResponsesToolIdentity {
	identities := make(map[string]claudeResponsesToolIdentity)
	for _, declaration := range claudeResponsesTools(gjson.ParseBytes(pickRequestJSON(original, translated))) {
		identities[strings.Clone(declaration.QualifiedName)] = claudeResponsesToolIdentity{
			name: strings.Clone(declaration.Name), namespace: strings.Clone(declaration.Namespace),
			custom: declaration.Tool.Get("type").String() == "custom",
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

func claudeResponsesToolItemID(identities map[string]claudeResponsesToolIdentity, name, callID string) string {
	if identities[name].custom {
		return "ctc_" + callID
	}
	return "fc_" + callID
}

func claudeResponsesCustomInput(arguments string) string {
	if !gjson.Valid(arguments) {
		return arguments
	}
	if input := gjson.Get(arguments, "input"); input.Exists() {
		if input.Type == gjson.String {
			return input.String()
		}
		return input.Raw
	}
	return arguments
}

func buildClaudeResponsesToolItem(identities map[string]claudeResponsesToolIdentity, name, callID, arguments, status string) []byte {
	kind, field := "function_call", "arguments"
	if identities[name].custom {
		kind, field = "custom_tool_call", "input"
		arguments = claudeResponsesCustomInput(arguments)
	} else if status == "completed" && arguments == "" {
		arguments = "{}"
	}
	item := []byte(`{"id":"","type":"","status":"","call_id":"","name":""}`)
	item, _ = sjson.SetBytes(item, "id", claudeResponsesToolItemID(identities, name, callID))
	item, _ = sjson.SetBytes(item, "type", kind)
	item, _ = sjson.SetBytes(item, "status", status)
	item, _ = sjson.SetBytes(item, "call_id", callID)
	item, _ = sjson.SetBytes(item, field, arguments)
	return restoreClaudeResponsesToolIdentity(item, "", name, identities)
}
