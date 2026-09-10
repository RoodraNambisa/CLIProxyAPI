package claude

import (
	"fmt"
	"strconv"
	"strings"

	translatorcommon "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/common"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type codexFunctionCallStream struct {
	CallID, Name                         string
	Arguments                            strings.Builder
	Emitted                              int
	Started, Done, Closed, ArgumentsDone bool
}

func codexFunctionEventKeys(root, item gjson.Result) []string {
	keys := make([]string, 0, 5)
	for _, id := range []string{root.Get("item_id").String(), item.Get("id").String()} {
		if id != "" {
			keys = append(keys, "item:"+id)
		}
	}
	if index := root.Get("output_index"); index.Type == gjson.Number && index.Int() >= 0 && index.Float() == float64(index.Int()) {
		keys = append(keys, "output:"+strconv.FormatInt(index.Int(), 10))
	}
	for _, id := range []string{root.Get("call_id").String(), item.Get("call_id").String()} {
		if id != "" {
			keys = append(keys, "call:"+id)
		}
	}
	return keys
}

// All known identities must agree before an event can add aliases or arguments.
func codexFunctionForEvent(params *ConvertCodexResponseToClaudeParams, root, item gjson.Result) *codexFunctionCallStream {
	keys := codexFunctionEventKeys(root, item)
	var call *codexFunctionCallStream
	for _, key := range keys {
		if found := params.FunctionCalls[key]; found != nil {
			if call != nil && call != found {
				return nil
			}
			call = found
		}
	}
	if call == nil && len(keys) == 0 {
		for _, pending := range params.FunctionCallQueue {
			if pending.Done || pending.Closed {
				continue
			}
			if call != nil {
				return nil
			}
			call = pending
		}
	}
	created := call == nil
	if created {
		if len(keys) == 0 {
			return nil
		}
		call = &codexFunctionCallStream{}
	}
	callID := call.CallID
	for _, id := range []string{root.Get("call_id").String(), item.Get("call_id").String()} {
		if id != "" {
			if callID != "" && callID != id {
				return nil
			}
			callID = id
		}
	}
	name := item.Get("name").String()
	if call.Name != "" && name != "" && call.Name != name {
		return nil
	}
	if call.Closed {
		return call
	}
	if params.FunctionCalls == nil {
		params.FunctionCalls = make(map[string]*codexFunctionCallStream)
	}
	if created {
		params.FunctionCallQueue = append(params.FunctionCallQueue, call)
	}
	for _, key := range keys {
		params.FunctionCalls[key] = call
	}
	call.CallID = strings.Clone(callID)
	if name != "" {
		call.Name = strings.Clone(name)
	}
	return call
}

func updateCodexFunctionArguments(call *codexFunctionCallStream, value string, delta bool) {
	if call == nil || call.Closed || call.Done {
		return
	}
	if delta {
		if !call.ArgumentsDone {
			call.Arguments.WriteString(value)
		}
	} else {
		// A terminal snapshot may extend a partial delta, but must not overwrite
		// bytes already emitted to the client with conflicting content.
		current := call.Arguments.String()
		if strings.HasPrefix(value, current) {
			call.Arguments.WriteString(value[len(current):])
		}
		call.ArgumentsDone = true
	}
}

func convertCodexClaudeFunctionEvent(params *ConvertCodexResponseToClaudeParams, root gjson.Result, original []byte) ([]byte, bool) {
	kind, item := root.Get("type").String(), root.Get("item")
	itemEvent := (kind == "response.output_item.added" || kind == "response.output_item.done") && item.Get("type").String() == "function_call"
	delta, argumentsDone := kind == "response.function_call_arguments.delta", kind == "response.function_call_arguments.done"
	if !itemEvent && !delta && !argumentsDone {
		return nil, false
	}
	call := codexFunctionForEvent(params, root, item)
	if call == nil || call.Closed || call.Done {
		return nil, true
	}
	switch {
	case delta:
		updateCodexFunctionArguments(call, root.Get("delta").String(), true)
	case argumentsDone:
		updateCodexFunctionArguments(call, root.Get("arguments").String(), false)
	case kind == "response.output_item.done":
		updateCodexFunctionArguments(call, item.Get("arguments").String(), false)
		call.Done = true
	}
	return appendCodexFunctionQueue(nil, params, original), true
}

func appendCodexFunctionQueue(output []byte, params *ConvertCodexResponseToClaudeParams, original []byte) []byte {
	for len(params.FunctionCallQueue) > 0 {
		call := params.FunctionCallQueue[0]
		if call.Name == "" || call.CallID == "" {
			return output
		}
		if !call.Started {
			output = append(output, finalizeCodexThinkingBlock(params)...)
			output = append(output, stopCodexTextBlock(params)...)
			output = appendCodexFunctionCallStart(output, original, call.CallID, call.Name, params.BlockIndex)
			call.Started = true
			params.ActiveFunctionCall = call
			params.HasToolCall = true
		}
		if call.Emitted < call.Arguments.Len() {
			output = appendCodexFunctionCallArgumentDelta(output, call.Arguments.String()[call.Emitted:], params.BlockIndex)
			call.Emitted = call.Arguments.Len()
		}
		if !call.Done {
			return output
		}
		output = appendCodexFunctionCallStop(output, params.BlockIndex)
		params.BlockIndex++
		call.Closed = true
		call.Arguments.Reset()
		params.ActiveFunctionCall = nil
		params.FunctionCallQueue[0] = nil
		params.FunctionCallQueue = params.FunctionCallQueue[1:]
	}
	return output
}

func finishCodexClaudeFunctionCalls(output []byte, params *ConvertCodexResponseToClaudeParams, original []byte, response gjson.Result) []byte {
	response.Get("output").ForEach(func(index, item gjson.Result) bool {
		if item.Get("type").String() != "function_call" {
			return true
		}
		// Array positions are only a fallback: a compact terminal list may omit
		// preceding reasoning items while preserving the real call/item IDs.
		root := gjson.Result{}
		known := false
		for _, key := range codexFunctionEventKeys(root, item) {
			if params.FunctionCalls[key] != nil {
				known = true
				break
			}
		}
		if !known {
			root = gjson.Parse(fmt.Sprintf(`{"output_index":%d}`, index.Int()))
		}
		call := codexFunctionForEvent(params, root, item)
		if call != nil && !call.Closed && !call.Done {
			updateCodexFunctionArguments(call, item.Get("arguments").String(), false)
			call.Done = true
		}
		return true
	})
	queue := params.FunctionCallQueue[:0]
	for _, call := range params.FunctionCallQueue {
		if call.Name == "" || call.CallID == "" {
			call.Closed = true
			call.Arguments.Reset()
			continue
		}
		call.Done = true
		queue = append(queue, call)
	}
	clear(params.FunctionCallQueue[len(queue):])
	params.FunctionCallQueue = queue
	output = appendCodexFunctionQueue(output, params, original)
	params.FunctionCalls = nil
	params.FunctionCallQueue = nil
	return output
}

func isCodexDeferredContentEvent(kind string, root gjson.Result) bool {
	switch kind {
	case "response.output_item.added", "response.output_item.done":
		return root.Get("item.type").String() != "function_call"
	case "response.content_part.added", "response.content_part.done", "response.output_text.delta",
		"response.reasoning_summary_part.added", "response.reasoning_summary_part.done", "response.reasoning_summary_text.delta":
		return true
	}
	return false
}

func appendDeferredCodexContent(output, original []byte, param *any) []byte {
	params := (*param).(*ConvertCodexResponseToClaudeParams)
	if params.ActiveFunctionCall != nil {
		return output
	}
	events := params.DeferredContentEvents
	params.DeferredContentEvents = nil
	for _, event := range events {
		for _, chunk := range ConvertCodexResponseToClaude(nil, "", original, nil, event, param) {
			output = append(output, chunk...)
		}
	}
	return output
}

func appendCodexFunctionCallStart(output, originalRequest []byte, callID, name string, blockIndex int) []byte {
	template := []byte(`{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"","name":"","input":{}}}`)
	template, _ = sjson.SetBytes(template, "index", blockIndex)
	template, _ = sjson.SetBytes(template, "content_block.id", util.SanitizeClaudeToolID(callID))
	if original, ok := buildReverseMapFromClaudeOriginalShortToOriginal(originalRequest)[name]; ok {
		name = original
	}
	template, _ = sjson.SetBytes(template, "content_block.name", name)
	return translatorcommon.AppendSSEEventBytes(output, "content_block_start", template, 2)
}

func appendCodexFunctionCallArgumentDelta(output []byte, arguments string, blockIndex int) []byte {
	template := []byte(`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":""}}`)
	template, _ = sjson.SetBytes(template, "index", blockIndex)
	template, _ = sjson.SetBytes(template, "delta.partial_json", arguments)
	return translatorcommon.AppendSSEEventBytes(output, "content_block_delta", template, 2)
}

func appendCodexFunctionCallStop(output []byte, blockIndex int) []byte {
	template := []byte(`{"type":"content_block_stop","index":0}`)
	template, _ = sjson.SetBytes(template, "index", blockIndex)
	return translatorcommon.AppendSSEEventBytes(output, "content_block_stop", template, 2)
}
