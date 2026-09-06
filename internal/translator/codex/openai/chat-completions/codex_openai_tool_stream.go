package chat_completions

import (
	"strconv"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type toolCallStreamState struct {
	Index            int
	Kind             string
	CallID           string
	NativeCustom     bool
	Announced        bool
	ArgumentsEmitted bool
	ArgumentsDone    bool
	Done             bool
}

func convertCodexToolEvent(p *ConvertCliToOpenAIParams, event gjson.Result, template, original []byte) ([][]byte, bool) {
	kind := event.Get("type").String()
	item := event.Get("item")
	itemEvent := (kind == "response.output_item.added" || kind == "response.output_item.done") && isCodexToolCallType(item.Get("type").String())
	delta := kind == "response.function_call_arguments.delta" || kind == "response.custom_tool_call_input.delta"
	argumentsDone := kind == "response.function_call_arguments.done" || kind == "response.custom_tool_call_input.done"
	if !itemEvent && !delta && !argumentsDone {
		return nil, false
	}
	keys := codexToolEventKeys(event, item)
	state, ambiguous := findCodexToolCallState(p, keys, !itemEvent || kind == "response.output_item.done")
	if ambiguous {
		return nil, true
	}
	if state == nil {
		// A complete terminal item can repair a missing start event. Argument-only
		// events without an identifiable call must not attach to another tool.
		if !itemEvent || item.Get("name").String() == "" || item.Get("call_id").String() == "" {
			return nil, true
		}
		p.FunctionCallIndex++
		state = &toolCallStreamState{Index: p.FunctionCallIndex, Kind: item.Get("type").String(), CallID: item.Get("call_id").String()}
		name := item.Get("name").String()
		if restored, ok := buildReverseMapFromOriginalOpenAI(original)[name]; ok {
			name = restored
		}
		state.NativeCustom = state.Kind == "custom_tool_call" && codexOpenAIUsesNativeCustomEnvelope(original, name)
	}
	eventKind := "function_call"
	if itemEvent {
		eventKind = item.Get("type").String()
	} else if kind == "response.custom_tool_call_input.delta" || kind == "response.custom_tool_call_input.done" {
		eventKind = "custom_tool_call"
	}
	if state.Kind != eventKind {
		return nil, true
	}
	// call_id pairs execution results and cannot be reassigned by index fallback.
	for _, callID := range []string{event.Get("call_id").String(), item.Get("call_id").String()} {
		if callID != "" && callID != state.CallID {
			return nil, true
		}
	}
	if state.Done {
		return nil, true
	}
	if p.toolCallStates == nil {
		p.toolCallStates = make(map[string]*toolCallStreamState)
	}
	for _, key := range keys {
		p.toolCallStates[key] = state
	}
	var arguments string
	announce := !state.Announced && itemEvent
	switch {
	case kind == "response.output_item.added":
		if !announce {
			return nil, true
		}
	case delta:
		if state.ArgumentsDone {
			return nil, true
		}
		arguments = event.Get("delta").String()
		if arguments == "" {
			return nil, true
		}
		state.ArgumentsEmitted = true
	case argumentsDone:
		if state.ArgumentsDone {
			return nil, true
		}
		state.ArgumentsDone = true
		if state.ArgumentsEmitted {
			return nil, true
		}
		arguments = event.Get("arguments").String()
		if kind == "response.custom_tool_call_input.done" {
			arguments = event.Get("input").String()
		}
		if arguments == "" {
			return nil, true
		}
		state.ArgumentsEmitted = true
	case kind == "response.output_item.done":
		state.Done = true
		if !state.ArgumentsEmitted {
			arguments = codexToolCallArguments(item)
		}
		if !announce && arguments == "" {
			return nil, true
		}
	}
	call := []byte(`{"index":0,"function":{"arguments":""}}`)
	inputPath, namePath, callType := "function.arguments", "function.name", "function"
	if state.NativeCustom {
		call = []byte(`{"index":0,"custom":{"input":""}}`)
		inputPath, namePath, callType = "custom.input", "custom.name", "custom"
	}
	call, _ = sjson.SetBytes(call, "index", state.Index)
	call, _ = sjson.SetBytes(call, inputPath, arguments)
	if announce {
		state.Announced = true
		call, _ = sjson.SetBytes(call, "id", item.Get("call_id").String())
		call, _ = sjson.SetBytes(call, "type", callType)
		name := item.Get("name").String()
		if restored, ok := buildReverseMapFromOriginalOpenAI(original)[name]; ok {
			name = restored
		}
		call, _ = sjson.SetBytes(call, namePath, name)
		template, _ = sjson.SetBytes(template, "choices.0.delta.role", "assistant")
	}
	template, _ = sjson.SetRawBytes(template, "choices.0.delta.tool_calls", append(append([]byte{'['}, call...), ']'))
	return [][]byte{template}, true
}

func codexToolEventKeys(event, item gjson.Result) []string {
	keys := make([]string, 0, 5)
	for _, id := range []string{event.Get("item_id").String(), item.Get("id").String()} {
		if id != "" {
			keys = append(keys, "item:"+id)
		}
	}
	if index := event.Get("output_index"); index.Type == gjson.Number && index.Int() >= 0 && index.Float() == float64(index.Int()) {
		keys = append(keys, "output:"+strconv.FormatInt(index.Int(), 10))
	}
	for _, id := range []string{event.Get("call_id").String(), item.Get("call_id").String()} {
		if id != "" {
			keys = append(keys, "call:"+id)
		}
	}
	return keys
}

func findCodexToolCallState(p *ConvertCliToOpenAIParams, keys []string, fallback bool) (*toolCallStreamState, bool) {
	var found *toolCallStreamState
	for _, key := range keys {
		if state := p.toolCallStates[key]; state != nil {
			if found != nil && found != state {
				return nil, true
			}
			found = state
		}
	}
	if found != nil || !fallback || len(keys) > 0 {
		return found, false
	}
	// Only an unlabelled event with one remaining active call has an unambiguous fallback.
	for _, state := range p.toolCallStates {
		if !state.Done {
			if found != nil && found != state {
				return nil, true
			}
			found = state
		}
	}
	return found, false
}
