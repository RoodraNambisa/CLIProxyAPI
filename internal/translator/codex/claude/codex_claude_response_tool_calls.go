package claude

import (
	"fmt"

	translatorcommon "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/common"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func codexFunctionCallID(item gjson.Result) string {
	return item.Get("call_id").String()
}

func codexFunctionCallKey(root, item gjson.Result) string {
	if outputIndex := root.Get("output_index"); outputIndex.Exists() {
		return "output:" + outputIndex.Raw
	}
	if callID := codexFunctionCallID(item); callID != "" {
		return "call:" + callID
	}
	return "last"
}

func recordPendingCodexFunctionCall(params *ConvertCodexResponseToClaudeParams, root, item gjson.Result) {
	if params.PendingFunctionCalls == nil {
		params.PendingFunctionCalls = make(map[string]*pendingCodexFunctionCall)
	}
	pending := &pendingCodexFunctionCall{CallID: codexFunctionCallID(item)}
	key := codexFunctionCallKey(root, item)
	params.PendingFunctionCalls[key] = pending
	if pending.CallID != "" {
		params.PendingFunctionCalls["call:"+pending.CallID] = pending
	}
	params.LastPendingFunctionCallKey = key
}

func pendingCodexFunctionCallForArguments(params *ConvertCodexResponseToClaudeParams, root gjson.Result) *pendingCodexFunctionCall {
	if params == nil || params.PendingFunctionCalls == nil {
		return nil
	}
	if outputIndex := root.Get("output_index"); outputIndex.Exists() {
		if pending := params.PendingFunctionCalls["output:"+outputIndex.Raw]; pending != nil {
			return pending
		}
	}
	return params.PendingFunctionCalls[params.LastPendingFunctionCallKey]
}

func pendingCodexFunctionCallForEvent(params *ConvertCodexResponseToClaudeParams, root, item gjson.Result) *pendingCodexFunctionCall {
	if params == nil || params.PendingFunctionCalls == nil {
		return nil
	}
	key := codexFunctionCallKey(root, item)
	if pending := params.PendingFunctionCalls[key]; pending != nil {
		return pending
	}
	if callID := codexFunctionCallID(item); callID != "" {
		if pending := params.PendingFunctionCalls["call:"+callID]; pending != nil {
			return pending
		}
		return nil
	}
	if root.Get("output_index").Exists() {
		return nil
	}
	return params.PendingFunctionCalls[params.LastPendingFunctionCallKey]
}

func deletePendingCodexFunctionCall(params *ConvertCodexResponseToClaudeParams, root, item gjson.Result) {
	if params == nil || params.PendingFunctionCalls == nil {
		return
	}
	pending := pendingCodexFunctionCallForEvent(params, root, item)
	if pending == nil {
		return
	}
	for key, candidate := range params.PendingFunctionCalls {
		if candidate == pending {
			delete(params.PendingFunctionCalls, key)
			if params.LastPendingFunctionCallKey == key {
				params.LastPendingFunctionCallKey = ""
			}
		}
	}
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

func currentCodexFunctionCallBlockIndex(params *ConvertCodexResponseToClaudeParams) int {
	if params != nil && params.FunctionCallBlockOpen {
		return params.FunctionCallBlockIndex
	}
	if params == nil {
		return 0
	}
	return params.BlockIndex
}

func appendCodexOpenFunctionCallStop(output []byte, params *ConvertCodexResponseToClaudeParams) []byte {
	if params == nil || !params.FunctionCallBlockOpen {
		return output
	}
	blockIndex := params.FunctionCallBlockIndex
	output = appendCodexFunctionCallStop(output, blockIndex)
	if params.BlockIndex <= blockIndex {
		params.BlockIndex = blockIndex + 1
	}
	params.FunctionCallBlockOpen = false
	params.FunctionCallBlockCallID = ""
	params.FunctionCallBlockIndex = 0
	return output
}

func hydrateOpenCodexFunctionCallFromTerminal(output []byte, params *ConvertCodexResponseToClaudeParams, response gjson.Result) []byte {
	if params == nil || !params.FunctionCallBlockOpen || params.HasReceivedArgumentsDelta {
		return output
	}
	response.Get("output").ForEach(func(_, item gjson.Result) bool {
		if item.Get("type").String() != "function_call" || codexFunctionCallID(item) != params.FunctionCallBlockCallID {
			return true
		}
		if arguments := item.Get("arguments").String(); arguments != "" {
			output = appendCodexFunctionCallArgumentDelta(output, arguments, params.FunctionCallBlockIndex)
			params.HasReceivedArgumentsDelta = true
		}
		return false
	})
	return output
}

func appendPendingCodexFunctionCallsFromTerminal(output []byte, params *ConvertCodexResponseToClaudeParams, originalRequest []byte, response gjson.Result) []byte {
	if params == nil || len(params.PendingFunctionCalls) == 0 {
		return output
	}
	response.Get("output").ForEach(func(index, item gjson.Result) bool {
		if item.Get("type").String() != "function_call" {
			return true
		}
		root := gjson.Parse(fmt.Sprintf(`{"output_index":%d}`, index.Int()))
		pending := pendingCodexFunctionCallForEvent(params, root, item)
		if pending == nil {
			return true
		}
		name := item.Get("name").String()
		if name == "" {
			deletePendingCodexFunctionCall(params, root, item)
			return true
		}
		callID := pending.CallID
		if callID == "" {
			callID = codexFunctionCallID(item)
		}
		blockIndex := params.BlockIndex
		output = appendCodexFunctionCallStart(output, originalRequest, callID, name, blockIndex)
		params.HasToolCall = true
		arguments := item.Get("arguments").String()
		if arguments == "" {
			arguments = pending.Arguments
		}
		if arguments != "" {
			output = appendCodexFunctionCallArgumentDelta(output, arguments, blockIndex)
		}
		output = appendCodexFunctionCallStop(output, blockIndex)
		params.BlockIndex++
		deletePendingCodexFunctionCall(params, root, item)
		return true
	})
	params.PendingFunctionCalls = nil
	params.LastPendingFunctionCallKey = ""
	return output
}
