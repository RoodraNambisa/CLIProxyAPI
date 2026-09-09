package responses

import (
	"fmt"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func responsesStreamIndex(value gjson.Result, fallback int) (int, bool) {
	if !value.Exists() {
		return fallback, true
	}
	number := value.Int()
	if value.Type != gjson.Number || number < 0 || value.Float() != float64(number) || int64(int(number)) != number {
		return 0, false
	}
	return int(number), true
}

func buildResponsesToolItem(identities map[string]responsesToolIdentity, name, callID, arguments, status string) []byte {
	identity, known := identities[name]
	kind, prefix, field := "function_call", "fc_", "arguments"
	if known && identity.custom {
		kind, prefix, field = "custom_tool_call", "ctc_", "input"
		arguments = unwrapCustomToolInput(arguments)
	}
	if status == "completed" && !identity.custom && arguments == "" {
		arguments = "{}"
	}
	item := []byte(`{"id":"","type":"","status":"","call_id":"","name":""}`)
	item, _ = sjson.SetBytes(item, "id", prefix+callID)
	item, _ = sjson.SetBytes(item, "type", kind)
	item, _ = sjson.SetBytes(item, "status", status)
	item, _ = sjson.SetBytes(item, "call_id", callID)
	item, _ = sjson.SetBytes(item, field, arguments)
	if known {
		name = identity.name
		if identity.namespace != "" {
			item, _ = sjson.SetBytes(item, "namespace", identity.namespace)
		}
	}
	item, _ = sjson.SetBytes(item, "name", name)
	return item
}

// emitToolEvents waits for stable tool identity before publishing its item. Custom
// tools buffer JSON wrapper fragments and publish decoded input at completion.
func (st *oaiToResponsesState) emitToolEvents(key string, final bool, nextSeq func() int) [][]byte {
	if st.FuncItemDone[key] {
		return nil
	}
	if final && st.FinishReasons[st.FuncChoices[key]] == "" {
		buffer := st.FuncArgsBuf[key]
		if buffer == nil || !gjson.Valid(buffer.String()) {
			return nil
		}
	}
	var out [][]byte
	if !st.FuncItemAdded[key] {
		name, callID := st.FuncNames[key], st.FuncCallIDs[key]
		if !final && (name == "" || callID == "") {
			return nil
		}
		if name == "" && len(st.ToolIdentities) == 1 {
			for candidate, identity := range st.ToolIdentities {
				if identity.custom {
					name = candidate
					st.FuncNames[key] = name
				}
			}
		}
		if name == "" {
			return nil
		}
		if callID == "" {
			callID = "call_" + st.ResponseID + "_" + strings.ReplaceAll(key, ":", "_")
			st.FuncCallIDs[key] = callID
		}
		st.FuncItemCustom[key] = st.ToolIdentities[name].custom
		item := buildResponsesToolItem(st.ToolIdentities, name, callID, "", "in_progress")
		event := []byte(`{"type":"response.output_item.added","sequence_number":0,"output_index":0}`)
		event, _ = sjson.SetBytes(event, "sequence_number", nextSeq())
		event, _ = sjson.SetBytes(event, "output_index", st.FuncOutputIx[key])
		event, _ = sjson.SetRawBytes(event, "item", item)
		out = append(out, emitRespEvent("response.output_item.added", event))
		st.FuncItemAdded[key] = true
	}
	args := ""
	if buffer := st.FuncArgsBuf[key]; buffer != nil {
		args = buffer.String()
	}
	callID := st.FuncCallIDs[key]
	if !st.FuncItemCustom[key] && len(args) > st.FuncArgsSent[key] {
		event := []byte(`{"type":"response.function_call_arguments.delta","sequence_number":0,"item_id":"","output_index":0,"delta":""}`)
		event, _ = sjson.SetBytes(event, "sequence_number", nextSeq())
		event, _ = sjson.SetBytes(event, "item_id", "fc_"+callID)
		event, _ = sjson.SetBytes(event, "output_index", st.FuncOutputIx[key])
		event, _ = sjson.SetBytes(event, "delta", args[st.FuncArgsSent[key]:])
		out = append(out, emitRespEvent("response.function_call_arguments.delta", event))
		st.FuncArgsSent[key] = len(args)
	}
	if !final {
		return out
	}
	status := responsesItemStatus(st.FinishReasons[st.FuncChoices[key]])
	if args == "" && !st.FuncItemCustom[key] && status == "completed" {
		args = "{}"
	}
	kind, field, prefix, value := "response.function_call_arguments.done", "arguments", "fc_", args
	if st.FuncItemCustom[key] {
		kind, field, prefix, value = "response.custom_tool_call_input.done", "input", "ctc_", unwrapCustomToolInput(args)
	}
	done := []byte(`{"type":"","sequence_number":0,"item_id":"","output_index":0}`)
	done, _ = sjson.SetBytes(done, "type", kind)
	done, _ = sjson.SetBytes(done, "sequence_number", nextSeq())
	done, _ = sjson.SetBytes(done, "item_id", prefix+callID)
	done, _ = sjson.SetBytes(done, "output_index", st.FuncOutputIx[key])
	done, _ = sjson.SetBytes(done, field, value)
	out = append(out, emitRespEvent(kind, done))
	itemDone := []byte(`{"type":"response.output_item.done","sequence_number":0,"output_index":0}`)
	itemDone, _ = sjson.SetBytes(itemDone, "sequence_number", nextSeq())
	itemDone, _ = sjson.SetBytes(itemDone, "output_index", st.FuncOutputIx[key])
	itemDone, _ = sjson.SetRawBytes(itemDone, "item", buildResponsesToolItem(st.ToolIdentities, st.FuncNames[key], callID, args, status))
	out = append(out, emitRespEvent("response.output_item.done", itemDone))
	st.FuncArgsDone[key], st.FuncItemDone[key] = true, true
	return out
}

func (st *oaiToResponsesState) acceptToolCallDelta(choiceIndex, toolIndex int, callID, name, arguments string) string {
	key := fmt.Sprintf("%d:%d", choiceIndex, toolIndex)
	if st.FuncItemDone[key] {
		return key
	}
	if existing := st.FuncCallIDs[key]; existing != "" && callID != "" && existing != callID {
		return ""
	}
	if callID != "" {
		for otherKey, existingID := range st.FuncCallIDs {
			if otherKey != key && existingID == callID {
				return ""
			}
		}
	}
	if st.FuncArgsBuf[key] == nil {
		st.FuncChoices[key] = choiceIndex
		st.FuncArgsBuf[key] = &strings.Builder{}
		st.FuncOutputIx[key] = st.NextOutputIx
		st.NextOutputIx++
	}
	if callID != "" && st.FuncCallIDs[key] == "" {
		st.FuncCallIDs[key] = strings.Clone(callID)
	}
	if name != "" && !st.FuncItemAdded[key] {
		st.FuncNames[key] = strings.Clone(name)
	}
	st.FuncArgsBuf[key].WriteString(arguments)
	return key
}
