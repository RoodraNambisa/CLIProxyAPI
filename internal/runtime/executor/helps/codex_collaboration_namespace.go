package helps

import (
	"fmt"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// OptimizeCodexCollaborationNamespace renames declared collaboration groups
// containing spawn_agent. A reserved-name conflict leaves every group unchanged.
func OptimizeCodexCollaborationNamespace(payload []byte) ([]byte, bool) {
	scan := scanCodexCollaborationTools(payload)
	if scan.conflict || len(scan.spawnAgentPaths) == 0 {
		return payload, false
	}
	updated := payload
	optimized := false
	for _, path := range scan.spawnAgentPaths {
		separator := strings.LastIndex(path, ".tools.")
		if separator < 0 {
			continue
		}
		namespacePath := path[:separator]
		namespace := gjson.GetBytes(updated, namespacePath)
		if strings.TrimSpace(namespace.Get("type").String()) != "namespace" || strings.TrimSpace(namespace.Get("name").String()) != codexCollaborationNamespace {
			continue
		}
		var errSet error
		updated, errSet = sjson.SetBytes(updated, namespacePath+".name", codexOptimizedCollaborationNamespace)
		if errSet != nil {
			return payload, false
		}
		optimized = true
	}
	if optimized {
		var errChoice error
		updated, errChoice = rewriteCodexCollaborationChoice(updated, "tool_choice", codexCollaborationNamespace, codexOptimizedCollaborationNamespace)
		if errChoice != nil {
			return payload, false
		}
	}
	return updated, optimized
}

type codexProtocolNode struct {
	value gjson.Result
	path  string
	kind  string
}

// RestoreCodexMultiAgentV2Response restores tool identities in Responses items,
// events and echoed tool declarations. Opaque business data is never traversed.
func RestoreCodexMultiAgentV2Response(payload []byte, optimized bool) []byte {
	return RewriteCodexMultiAgentV2Response(payload, optimized, optimized)
}

// RewriteCodexMultiAgentV2Response also supports prepared plaintext tools that
// did not require a namespace rename, such as non-Codex provider responses.
func RewriteCodexMultiAgentV2Response(payload []byte, restoreNamespace, plaintextCalls bool) []byte {
	if (!restoreNamespace && !plaintextCalls) || !gjson.ValidBytes(payload) {
		return payload
	}
	updated := payload
	pending := []codexProtocolNode{{value: gjson.ParseBytes(payload), kind: "envelope"}}
	fieldPath := func(base, name string) string {
		if base == "" {
			return name
		}
		return base + "." + name
	}
	pushArray := func(array gjson.Result, path, kind string) {
		if !array.IsArray() {
			return
		}
		for index, item := range array.Array() {
			pending = append(pending, codexProtocolNode{value: item, path: fmt.Sprintf("%s.%d", path, index), kind: kind})
		}
	}
	set := func(path, value string) bool {
		var errSet error
		updated, errSet = sjson.SetBytes(updated, path, value)
		return errSet == nil
	}
	for len(pending) > 0 {
		index := len(pending) - 1
		node := pending[index]
		pending[index] = codexProtocolNode{}
		pending = pending[:index]
		if !node.value.IsObject() {
			continue
		}
		itemType := strings.TrimSpace(node.value.Get("type").String())
		switch node.kind {
		case "envelope":
			switch itemType {
			case "", "response":
				node.kind = "response"
			case "function_call", "custom_tool_call", "additional_tools", "namespace":
				node.kind = "item"
			case "response.output_item.added", "response.output_item.done":
				pending = append(pending, codexProtocolNode{value: node.value.Get("item"), path: fieldPath(node.path, "item"), kind: "item"})
			case "response.function_call_arguments.delta", "response.function_call_arguments.done",
				"response.custom_tool_call_input.delta", "response.custom_tool_call_input.done":
				node.kind = "call"
			}
			if strings.HasPrefix(itemType, "response.") {
				pending = append(pending, codexProtocolNode{value: node.value.Get("response"), path: fieldPath(node.path, "response"), kind: "response"})
			}
		}
		if node.kind == "response" {
			if restoreNamespace {
				var errChoice error
				updated, errChoice = rewriteCodexCollaborationChoice(updated, fieldPath(node.path, "tool_choice"), codexOptimizedCollaborationNamespace, codexCollaborationNamespace)
				if errChoice != nil {
					return payload
				}
			}
			pushArray(node.value.Get("output"), fieldPath(node.path, "output"), "item")
			pushArray(node.value.Get("tools"), fieldPath(node.path, "tools"), "tool")
			continue
		}
		if itemType == "additional_tools" && node.kind == "item" {
			pushArray(node.value.Get("tools"), fieldPath(node.path, "tools"), "tool")
			continue
		}
		if itemType == "namespace" && (node.kind == "tool" || node.kind == "item") {
			if restoreNamespace && node.value.Get("name").String() == codexOptimizedCollaborationNamespace &&
				!set(fieldPath(node.path, "name"), codexCollaborationNamespace) {
				return payload
			}
			pushArray(node.value.Get("tools"), fieldPath(node.path, "tools"), "tool")
			continue
		}
		isCall := node.kind == "call" || node.kind == "item" && (itemType == "function_call" || itemType == "custom_tool_call")
		if !isCall {
			continue
		}
		if restoreNamespace && node.value.Get("namespace").String() == codexOptimizedCollaborationNamespace &&
			!set(fieldPath(node.path, "namespace"), codexCollaborationNamespace) {
			return payload
		}
		name := node.value.Get("name").String()
		if restoreNamespace && strings.HasPrefix(name, codexOptimizedCollaborationNamePrefix) &&
			!set(fieldPath(node.path, "name"), codexCollaborationNamespace+"__"+strings.TrimPrefix(name, codexOptimizedCollaborationNamePrefix)) {
			return payload
		}
		if plaintextCalls && itemType == "function_call" && codexPlaintextCollaborationCall(node.value, restoreNamespace) {
			marker := node.value.Get("encrypted_function_args")
			if !marker.Exists() || marker.Type == gjson.Null {
				var errSet error
				updated, errSet = sjson.SetRawBytes(updated, fieldPath(node.path, "encrypted_function_args"), []byte("[]"))
				if errSet != nil {
					return payload
				}
			}
		}
	}
	return updated
}

func codexPlaintextCollaborationCall(item gjson.Result, restoreNamespace bool) bool {
	namespace, name := item.Get("namespace").String(), item.Get("name").String()
	if restoreNamespace {
		if namespace == codexOptimizedCollaborationNamespace {
			namespace = codexCollaborationNamespace
		}
		if strings.HasPrefix(name, codexOptimizedCollaborationNamePrefix) {
			name = codexCollaborationNamespace + "__" + strings.TrimPrefix(name, codexOptimizedCollaborationNamePrefix)
		}
	}
	if namespace != "" && namespace != codexCollaborationNamespace {
		return false
	}
	if strings.HasPrefix(name, codexCollaborationNamespace+"__") {
		name = strings.TrimPrefix(name, codexCollaborationNamespace+"__")
	} else if namespace != codexCollaborationNamespace {
		return false
	}
	switch name {
	case "spawn_agent", "send_message", "followup_task":
		return true
	default:
		return false
	}
}
