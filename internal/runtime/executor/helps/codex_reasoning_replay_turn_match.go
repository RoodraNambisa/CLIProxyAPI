package helps

import (
	"crypto/sha256"
	"encoding/json"
	"reflect"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func codexReplayTurnMatchesItem(turn codexReasoningReplayTurn, item gjson.Result) bool {
	if turn.assistant != ([sha256.Size]byte{}) {
		if fingerprint, ok := codexReplayAssistantFingerprint(item); ok && fingerprint == turn.assistant {
			return true
		}
	}
	kind := strings.TrimSpace(item.Get("type").String())
	callKind := strings.TrimSuffix(kind, "_output")
	if callKind != "function_call" && callKind != "custom_tool_call" {
		return false
	}
	for _, raw := range turn.items {
		call := gjson.ParseBytes(raw)
		if call.Get("type").String() != callKind || !codexReplayCallIDsMatch(call.Get("call_id").String(), item.Get("call_id").String()) {
			continue
		}
		if kind != callKind {
			return true
		}
		content := "arguments"
		if callKind == "custom_tool_call" {
			content = "input"
		}
		if call.Get("name").String() == item.Get("name").String() && codexReplayCallContentMatches(call.Get(content), item.Get(content), callKind == "custom_tool_call") {
			return true
		}
	}
	return false
}

func codexReplayCallContentMatches(first, second gjson.Result, custom bool) bool {
	if first.Raw == second.Raw {
		return true
	}
	if first.Type != gjson.String || second.Type != gjson.String {
		return false
	}
	a, b := first.String(), second.String()
	if a == b {
		return true
	}
	if custom || !gjson.Valid(a) || !gjson.Valid(b) {
		return false
	}
	var left, right any
	decode := func(raw string, target *any) error {
		decoder := json.NewDecoder(strings.NewReader(raw))
		decoder.UseNumber()
		return decoder.Decode(target)
	}
	return decode(a, &left) == nil && decode(b, &right) == nil && reflect.DeepEqual(left, right)
}

func codexReplayCallIDsMatch(first, second string) bool {
	for _, a := range comparableCodexCallIDs(first) {
		if a == strings.TrimSpace(second) {
			return true
		}
	}
	return false
}

// Only a unique output item immediately following the captured request prefix
// can anchor a turn. Session identity alone never selects an insertion point.
func insertCodexReasoningReplayTurns(body []byte, items []gjson.Result, prefixes map[[sha256.Size]byte]int, turns []codexReasoningReplayTurn) ([]byte, bool) {
	anchors := make(map[int]int)
	for turnIndex, turn := range turns {
		index, ok := prefixes[turn.prefix]
		if !ok || index >= len(items) || !codexReplayTurnMatchesItem(turn, items[index]) {
			continue
		}
		if _, exists := anchors[index]; exists {
			anchors[index] = -1
		} else {
			anchors[index] = turnIndex
		}
	}
	if len(anchors) == 0 {
		return body, false
	}
	existingReasoning := make(map[string]bool)
	existingCalls := make(map[string]bool)
	outputs := make(map[string]string)
	ambiguousOutputs := make(map[string]bool)
	for _, item := range items {
		kind := strings.TrimSpace(item.Get("type").String())
		if kind == "reasoning" {
			existingReasoning[item.Get("encrypted_content").String()] = true
		}
		for _, key := range codexReplayCallKeys(item) {
			existingCalls[key] = true
		}
		if kind == "function_call_output" || kind == "custom_tool_call_output" {
			id := item.Get("call_id").String()
			for _, candidate := range comparableCodexCallIDs(id) {
				key := strings.TrimSuffix(kind, "_output") + ":" + candidate
				if previous, ok := outputs[key]; ok && previous != id {
					ambiguousOutputs[key] = true
				}
				outputs[key] = id
			}
		}
	}
	var rebuilt []string
	for index, item := range items {
		if turnIndex, ok := anchors[index]; ok && turnIndex >= 0 {
			for _, raw := range turns[turnIndex].items {
				call := gjson.ParseBytes(raw)
				kind := call.Get("type").String()
				switch kind {
				case "reasoning":
					signature := call.Get("encrypted_content").String()
					if signature == "" || existingReasoning[signature] {
						continue
					}
					existingReasoning[signature] = true
				case "function_call", "custom_tool_call":
					outputID := ""
					present := false
					for _, key := range codexReplayCallKeys(call) {
						present = present || existingCalls[key]
						if outputID == "" && !ambiguousOutputs[key] && codexReplayCallIDsMatch(call.Get("call_id").String(), outputs[key]) {
							outputID = outputs[key]
						}
					}
					if present || outputID == "" {
						continue
					}
					if outputID != call.Get("call_id").String() {
						var errSet error
						raw, errSet = sjson.SetBytes(raw, "call_id", outputID)
						if errSet != nil {
							continue
						}
					}
					for _, key := range codexReplayCallKeys(gjson.ParseBytes(raw)) {
						existingCalls[key] = true
					}
				default:
					continue
				}
				if rebuilt == nil {
					rebuilt = make([]string, 0, len(items)+len(turns))
					for _, prior := range items[:index] {
						rebuilt = append(rebuilt, prior.Raw)
					}
				}
				rebuilt = append(rebuilt, string(raw))
			}
		}
		if rebuilt != nil {
			rebuilt = append(rebuilt, item.Raw)
		}
	}
	if rebuilt == nil {
		return body, false
	}
	updated, errSet := sjson.SetRawBytes(body, "input", []byte("["+strings.Join(rebuilt, ",")+"]"))
	if errSet != nil {
		return body, false
	}
	return updated, true
}
