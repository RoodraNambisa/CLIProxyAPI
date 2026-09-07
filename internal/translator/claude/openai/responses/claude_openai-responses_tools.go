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
