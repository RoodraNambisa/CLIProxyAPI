package openai

import (
	"encoding/json"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func isCompleteResponsesWebsocketToolCall(item gjson.Result) bool {
	if !item.IsObject() {
		return false
	}
	for _, field := range []string{"call_id", "name"} {
		value := item.Get(field)
		if value.Type != gjson.String || strings.TrimSpace(value.String()) == "" {
			return false
		}
	}
	if status := item.Get("status").String(); status != "" && status != "completed" {
		return false
	}
	switch item.Get("type").String() {
	case "function_call":
		return item.Get("arguments").Type == gjson.String
	case "custom_tool_call":
		return item.Get("input").Type == gjson.String
	default:
		return false
	}
}

func isResponsesWebsocketToolPlaceholder(item gjson.Result) bool {
	status := item.Get("status").String()
	return (status == "" || status == "in_progress" || status == "completed") &&
		isResponsesToolCallType(item.Get("type").String()) && !isCompleteResponsesWebsocketToolCall(item)
}

// Reconcile only incomplete placeholders with an unambiguous completed call.
// Complete terminal items and unrelated extension fields remain authoritative.
func reconcileResponsesWebsocketCompletionToolCalls(output gjson.Result, indexed map[int64][]byte, fallback [][]byte) ([]byte, bool) {
	if len(indexed) == 0 && len(fallback) == 0 {
		return nil, false
	}
	items := output.Array()
	counts := make(map[string]int)
	needsRepair := false
	for _, item := range items {
		if !isResponsesToolCallType(item.Get("type").String()) {
			continue
		}
		if id := item.Get("call_id"); id.Type == gjson.String && id.String() != "" {
			counts[id.String()]++
			needsRepair = needsRepair || isResponsesWebsocketToolPlaceholder(item)
		}
	}
	if !needsRepair {
		return nil, false
	}
	collected := make(map[string]gjson.Result)
	ambiguous := make(map[string]bool)
	record := func(raw []byte) {
		item := gjson.ParseBytes(raw)
		if !isCompleteResponsesWebsocketToolCall(item) {
			return
		}
		id := item.Get("call_id").String()
		if previous, ok := collected[id]; ok && previous.Raw != item.Raw {
			ambiguous[id] = true
		}
		collected[id] = item
	}
	for _, raw := range indexed {
		record(raw)
	}
	for _, raw := range fallback {
		record(raw)
	}
	result := make([]json.RawMessage, 0, len(items))
	changed := false
	for _, item := range items {
		raw := []byte(item.Raw)
		id := item.Get("call_id")
		if isResponsesWebsocketToolPlaceholder(item) && id.Type == gjson.String && counts[id.String()] == 1 && !ambiguous[id.String()] {
			if complete, ok := collected[id.String()]; ok {
				conflict := false
				for _, field := range []string{"name", "namespace"} {
					value := item.Get(field)
					if value.Type == gjson.String && strings.TrimSpace(value.String()) != "" && value.String() != complete.Get(field).String() {
						conflict = true
					}
				}
				if !conflict {
					var errSet error
					updated := raw
					for _, field := range []string{"id", "type", "call_id", "name", "namespace", "arguments", "input"} {
						value := complete.Get(field)
						if !value.Exists() {
							continue
						}
						updated, errSet = sjson.SetRawBytes(updated, field, []byte(value.Raw))
						if errSet != nil {
							break
						}
					}
					if errSet == nil {
						obsolete := "input"
						if complete.Get("type").String() == "custom_tool_call" {
							obsolete = "arguments"
						}
						updated, errSet = sjson.DeleteBytes(updated, obsolete)
					}
					if errSet == nil {
						updated, errSet = sjson.SetBytes(updated, "status", "completed")
					}
					if errSet == nil {
						raw, changed = updated, true
					}
				}
			}
		}
		result = append(result, raw)
	}
	if !changed {
		return nil, false
	}
	encoded, errMarshal := json.Marshal(result)
	return encoded, errMarshal == nil
}
