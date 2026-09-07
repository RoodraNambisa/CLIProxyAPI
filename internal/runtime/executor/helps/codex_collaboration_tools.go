package helps

import (
	"fmt"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	codexCollaborationNamespace           = "collaboration"
	codexOptimizedCollaborationNamespace  = "collaboration-optimize"
	codexOptimizedCollaborationNamePrefix = codexOptimizedCollaborationNamespace + "__"
)

type codexCollaborationToolScan struct {
	spawnAgentPaths []string
	messagePaths    []string
	conflict        bool
}

type codexToolVisit struct {
	tool      gjson.Result
	path      string
	namespace string
}

// scanCodexCollaborationTools visits protocol tool declarations only. Other
// namespaces may define same-named tools with their own schema extensions.
func scanCodexCollaborationTools(payload []byte) codexCollaborationToolScan {
	var result codexCollaborationToolScan
	if !gjson.ValidBytes(payload) {
		return result
	}
	var pending []codexToolVisit
	push := func(tools gjson.Result, path, namespace string) {
		if !tools.IsArray() {
			return
		}
		entries := tools.Array()
		for index := len(entries) - 1; index >= 0; index-- {
			pending = append(pending, codexToolVisit{tool: entries[index], path: fmt.Sprintf("%s.%d", path, index), namespace: namespace})
		}
	}
	input := gjson.GetBytes(payload, "input")
	if input.IsArray() {
		items := input.Array()
		for index := len(items) - 1; index >= 0; index-- {
			if strings.TrimSpace(items[index].Get("type").String()) == "additional_tools" {
				push(items[index].Get("tools"), fmt.Sprintf("input.%d.tools", index), "")
			}
		}
	}
	push(gjson.GetBytes(payload, "tools"), "tools", "")
	for len(pending) > 0 {
		index := len(pending) - 1
		visit := pending[index]
		pending[index] = codexToolVisit{}
		pending = pending[:index]
		kind := strings.TrimSpace(visit.tool.Get("type").String())
		name := strings.TrimSpace(visit.tool.Get("name").String())
		if name == codexOptimizedCollaborationNamespace || strings.HasPrefix(name, codexOptimizedCollaborationNamePrefix) {
			result.conflict = true
		}
		if kind == "namespace" {
			push(visit.tool.Get("tools"), visit.path+".tools", name)
			continue
		}
		if kind != "function" || (visit.namespace != "" && visit.namespace != codexCollaborationNamespace) {
			continue
		}
		switch name {
		case "spawn_agent":
			result.spawnAgentPaths = append(result.spawnAgentPaths, visit.path)
			result.messagePaths = append(result.messagePaths, visit.path)
		case "send_message", "followup_task":
			result.messagePaths = append(result.messagePaths, visit.path)
		}
	}
	return result
}

func codexSpawnAgentToolPaths(payload []byte) []string {
	return scanCodexCollaborationTools(payload).spawnAgentPaths
}

func codexCollaborationMessageToolPaths(payload []byte) []string {
	return scanCodexCollaborationTools(payload).messagePaths
}

// HasCodexMultiAgentV2NamespaceConflict detects declarations using the reserved
// namespace without scanning tool arguments, schema examples, or metadata.
func HasCodexMultiAgentV2NamespaceConflict(payload []byte) bool {
	return scanCodexCollaborationTools(payload).conflict
}

// CodexCollaborationNeedsModelList avoids building a catalog for ordinary turns.
func CodexCollaborationNeedsModelList(payload []byte) bool {
	scan := scanCodexCollaborationTools(payload)
	return len(scan.spawnAgentPaths) > 0 && !scan.conflict
}

func removeCodexCollaborationMessageEncryption(payload []byte, toolPaths []string) []byte {
	if !gjson.ValidBytes(payload) {
		return payload
	}
	updated := payload
	for _, path := range toolPaths {
		encryptedPath := path + ".parameters.properties.message.encrypted"
		if !gjson.GetBytes(updated, encryptedPath).Exists() {
			continue
		}
		var errDelete error
		updated, errDelete = sjson.DeleteBytes(updated, encryptedPath)
		if errDelete != nil {
			return payload
		}
	}
	return updated
}
