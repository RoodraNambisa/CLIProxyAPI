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
}

// Keep only detached names; response streams must not retain tool schemas or prompts.
func geminiResponsesToolIdentities(original, translated []byte) map[string]geminiResponsesToolIdentity {
	identities := make(map[string]geminiResponsesToolIdentity)
	root := unwrapRequestRoot(gjson.ParseBytes(pickRequestJSON(original, translated)))
	for _, declaration := range translatorcommon.ResponsesToolDeclarations(root) {
		if declaration.Tool.Get("type").String() != "function" || declaration.Name == "" {
			continue
		}
		wireName := util.SanitizeFunctionName(declaration.QualifiedName)
		identity := geminiResponsesToolIdentity{name: strings.Clone(declaration.Name), namespace: strings.Clone(declaration.Namespace)}
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
