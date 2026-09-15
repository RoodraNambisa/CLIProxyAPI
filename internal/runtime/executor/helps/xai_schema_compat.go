package helps

import (
	"fmt"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func NormalizeXAIObjectRootUnionBranchTypes(tool []byte) ([]byte, bool, bool) {
	parameters := gjson.GetBytes(tool, "parameters")
	rootType := parameters.Get("type")
	if rootType.Type != gjson.String || rootType.String() != "object" {
		return tool, false, true
	}

	original := tool
	changed := false
	for _, unionName := range []string{"anyOf", "oneOf"} {
		union := parameters.Get(unionName)
		if !union.IsArray() {
			continue
		}
		for index, branch := range union.Array() {
			if !branch.IsObject() || branch.Get("type").Exists() || branch.Get("$ref").Exists() {
				continue
			}
			updated, errSet := sjson.SetBytes(tool, fmt.Sprintf("parameters.%s.%d.type", unionName, index), "object")
			if errSet != nil {
				return original, false, false
			}
			tool = updated
			changed = true
		}
	}
	return tool, changed, true
}

func xaiSchemaTypeIsObjectOnly(schemaType gjson.Result) bool {
	if schemaType.Type == gjson.String {
		return strings.EqualFold(strings.TrimSpace(schemaType.String()), "object")
	}
	if !schemaType.IsArray() {
		return false
	}
	types := schemaType.Array()
	if len(types) == 0 {
		return false
	}
	for _, schemaTypeItem := range types {
		if schemaTypeItem.Type != gjson.String || !strings.EqualFold(strings.TrimSpace(schemaTypeItem.String()), "object") {
			return false
		}
	}
	return true
}

func isXAICodexAppAutomationUpdate(toolName, namespaceName string) bool {
	cleanNamespace := strings.TrimPrefix(strings.TrimSpace(namespaceName), "mcp__")
	cleanTool := strings.TrimPrefix(strings.TrimSpace(toolName), "mcp__")
	if strings.EqualFold(cleanTool, "automation_update") && (strings.EqualFold(cleanNamespace, "codex_app") || strings.EqualFold(cleanNamespace, "codex_apps")) {
		return true
	}
	if strings.EqualFold(cleanTool, "codex_app"+"__"+"automation_update") || strings.EqualFold(cleanTool, "codex_apps__"+"automation_update") {
		return true
	}
	return false
}

func XAIFunctionParametersNeedSimplification(tool gjson.Result, namespaceName string) bool {
	toolType := strings.TrimSpace(tool.Get("type").String())
	isFunction := strings.EqualFold(toolType, "function")
	isNormalizedCustom := strings.EqualFold(toolType, "custom")
	if !isFunction && !isNormalizedCustom {
		return false
	}

	toolName := strings.TrimSpace(tool.Get("name").String())
	if isFunction && isXAICodexAppAutomationUpdate(toolName, namespaceName) {
		return true
	}

	parameters := tool.Get("parameters")
	for _, unionName := range []string{"anyOf", "oneOf"} {
		union := parameters.Get(unionName)
		if !union.IsArray() {
			continue
		}
		for _, branch := range union.Array() {
			if branch.Get("$ref").Exists() || !xaiSchemaTypeIsObjectOnly(branch.Get("type")) {
				return true
			}
		}
	}
	return false
}
