package claude

import (
	"context"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestConvertCodexResponseToClaude_StreamThinkingIncludesSignature(t *testing.T) {
	ctx := context.Background()
	originalRequest := []byte(`{"messages":[]}`)
	var param any

	chunks := [][]byte{
		[]byte("data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_123\",\"model\":\"gpt-5\"}}"),
		[]byte("data: {\"type\":\"response.reasoning_summary_part.added\"}"),
		[]byte("data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"Let me think\"}"),
		[]byte("data: {\"type\":\"response.reasoning_summary_part.done\"}"),
		[]byte("data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"reasoning\",\"encrypted_content\":\"enc_sig_123\"}}"),
	}

	var outputs [][]byte
	for _, chunk := range chunks {
		outputs = append(outputs, ConvertCodexResponseToClaude(ctx, "", originalRequest, nil, chunk, &param)...)
	}

	startFound := false
	signatureDeltaFound := false
	stopFound := false

	for _, out := range outputs {
		for _, line := range strings.Split(string(out), "\n") {
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			data := gjson.Parse(strings.TrimPrefix(line, "data: "))
			switch data.Get("type").String() {
			case "content_block_start":
				if data.Get("content_block.type").String() == "thinking" {
					startFound = true
					if data.Get("content_block.signature").Exists() {
						t.Fatalf("thinking start block should NOT have signature field when signature is unknown: %s", line)
					}
				}
			case "content_block_delta":
				if data.Get("delta.type").String() == "signature_delta" {
					signatureDeltaFound = true
					if got := data.Get("delta.signature").String(); got != "enc_sig_123" {
						t.Fatalf("unexpected signature delta: %q", got)
					}
				}
			case "content_block_stop":
				stopFound = true
			}
		}
	}

	if !startFound {
		t.Fatal("expected thinking content_block_start event")
	}
	if !signatureDeltaFound {
		t.Fatal("expected signature_delta event for thinking block")
	}
	if !stopFound {
		t.Fatal("expected content_block_stop event for thinking block")
	}
}

func TestConvertCodexResponseToClaude_StreamThinkingWithoutReasoningItemStillIncludesSignatureField(t *testing.T) {
	ctx := context.Background()
	originalRequest := []byte(`{"messages":[]}`)
	var param any

	chunks := [][]byte{
		[]byte("data: {\"type\":\"response.reasoning_summary_part.added\"}"),
		[]byte("data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"Let me think\"}"),
		[]byte("data: {\"type\":\"response.reasoning_summary_part.done\"}"),
		[]byte("data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}"),
	}

	var outputs [][]byte
	for _, chunk := range chunks {
		outputs = append(outputs, ConvertCodexResponseToClaude(ctx, "", originalRequest, nil, chunk, &param)...)
	}

	thinkingStartFound := false
	thinkingStopFound := false
	signatureDeltaFound := false

	for _, out := range outputs {
		for _, line := range strings.Split(string(out), "\n") {
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			data := gjson.Parse(strings.TrimPrefix(line, "data: "))
			if data.Get("type").String() == "content_block_start" && data.Get("content_block.type").String() == "thinking" {
				thinkingStartFound = true
				if data.Get("content_block.signature").Exists() {
					t.Fatalf("thinking start block should NOT have signature field without encrypted_content: %s", line)
				}
			}
			if data.Get("type").String() == "content_block_stop" && data.Get("index").Int() == 0 {
				thinkingStopFound = true
			}
			if data.Get("type").String() == "content_block_delta" && data.Get("delta.type").String() == "signature_delta" {
				signatureDeltaFound = true
			}
		}
	}

	if !thinkingStartFound {
		t.Fatal("expected thinking content_block_start event")
	}
	if !thinkingStopFound {
		t.Fatal("expected thinking content_block_stop event")
	}
	if signatureDeltaFound {
		t.Fatal("did not expect signature_delta without encrypted_content")
	}
}

func TestConvertCodexResponseToClaude_StreamThinkingFinalizesPendingBlockBeforeNextSummaryPart(t *testing.T) {
	ctx := context.Background()
	originalRequest := []byte(`{"messages":[]}`)
	var param any

	chunks := [][]byte{
		[]byte("data: {\"type\":\"response.reasoning_summary_part.added\"}"),
		[]byte("data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"First part\"}"),
		[]byte("data: {\"type\":\"response.reasoning_summary_part.done\"}"),
		[]byte("data: {\"type\":\"response.reasoning_summary_part.added\"}"),
	}

	var outputs [][]byte
	for _, chunk := range chunks {
		outputs = append(outputs, ConvertCodexResponseToClaude(ctx, "", originalRequest, nil, chunk, &param)...)
	}

	startCount := 0
	stopCount := 0
	for _, out := range outputs {
		for _, line := range strings.Split(string(out), "\n") {
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			data := gjson.Parse(strings.TrimPrefix(line, "data: "))
			if data.Get("type").String() == "content_block_start" && data.Get("content_block.type").String() == "thinking" {
				startCount++
			}
			if data.Get("type").String() == "content_block_stop" {
				stopCount++
			}
		}
	}

	if startCount != 2 {
		t.Fatalf("expected 2 thinking block starts, got %d", startCount)
	}
	if stopCount != 1 {
		t.Fatalf("expected pending thinking block to be finalized before second start, got %d stops", stopCount)
	}
}

func TestConvertCodexResponseToClaude_StreamThinkingRetainsSignatureAcrossMultipartReasoning(t *testing.T) {
	ctx := context.Background()
	originalRequest := []byte(`{"messages":[]}`)
	var param any

	chunks := [][]byte{
		[]byte("data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"reasoning\",\"encrypted_content\":\"enc_sig_multipart\"}}"),
		[]byte("data: {\"type\":\"response.reasoning_summary_part.added\"}"),
		[]byte("data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"First part\"}"),
		[]byte("data: {\"type\":\"response.reasoning_summary_part.done\"}"),
		[]byte("data: {\"type\":\"response.reasoning_summary_part.added\"}"),
		[]byte("data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"Second part\"}"),
		[]byte("data: {\"type\":\"response.reasoning_summary_part.done\"}"),
		[]byte("data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"reasoning\"}}"),
	}

	var outputs [][]byte
	for _, chunk := range chunks {
		outputs = append(outputs, ConvertCodexResponseToClaude(ctx, "", originalRequest, nil, chunk, &param)...)
	}

	signatureDeltaCount := 0
	for _, out := range outputs {
		for _, line := range strings.Split(string(out), "\n") {
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			data := gjson.Parse(strings.TrimPrefix(line, "data: "))
			if data.Get("type").String() == "content_block_delta" && data.Get("delta.type").String() == "signature_delta" {
				signatureDeltaCount++
				if got := data.Get("delta.signature").String(); got != "enc_sig_multipart" {
					t.Fatalf("unexpected signature delta: %q", got)
				}
			}
		}
	}

	if signatureDeltaCount != 2 {
		t.Fatalf("expected signature_delta for both multipart thinking blocks, got %d", signatureDeltaCount)
	}
}

func TestConvertCodexResponseToClaude_StreamThinkingUsesEarlyCapturedSignatureWhenDoneOmitsIt(t *testing.T) {
	ctx := context.Background()
	originalRequest := []byte(`{"messages":[]}`)
	var param any

	chunks := [][]byte{
		[]byte("data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"reasoning\",\"encrypted_content\":\"enc_sig_early\"}}"),
		[]byte("data: {\"type\":\"response.reasoning_summary_part.added\"}"),
		[]byte("data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"Let me think\"}"),
		[]byte("data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"reasoning\"}}"),
	}

	var outputs [][]byte
	for _, chunk := range chunks {
		outputs = append(outputs, ConvertCodexResponseToClaude(ctx, "", originalRequest, nil, chunk, &param)...)
	}

	signatureDeltaCount := 0
	for _, out := range outputs {
		for _, line := range strings.Split(string(out), "\n") {
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			data := gjson.Parse(strings.TrimPrefix(line, "data: "))
			if data.Get("type").String() == "content_block_delta" && data.Get("delta.type").String() == "signature_delta" {
				signatureDeltaCount++
				if got := data.Get("delta.signature").String(); got != "enc_sig_early" {
					t.Fatalf("unexpected signature delta: %q", got)
				}
			}
		}
	}

	if signatureDeltaCount != 1 {
		t.Fatalf("expected signature_delta from early-captured signature, got %d", signatureDeltaCount)
	}
}

func TestConvertCodexResponseToClaudeNonStream_ThinkingIncludesSignature(t *testing.T) {
	ctx := context.Background()
	originalRequest := []byte(`{"messages":[]}`)
	response := []byte(`{
		"type":"response.completed",
		"response":{
			"id":"resp_123",
			"model":"gpt-5",
			"usage":{"input_tokens":10,"output_tokens":20},
			"output":[
				{
					"type":"reasoning",
					"encrypted_content":"enc_sig_nonstream",
					"summary":[{"type":"summary_text","text":"internal reasoning"}]
				},
				{
					"type":"message",
					"content":[{"type":"output_text","text":"final answer"}]
				}
			]
		}
	}`)

	out := ConvertCodexResponseToClaudeNonStream(ctx, "", originalRequest, nil, response, nil)
	parsed := gjson.ParseBytes(out)

	thinking := parsed.Get("content.0")
	if thinking.Get("type").String() != "thinking" {
		t.Fatalf("expected first content block to be thinking, got %s", thinking.Raw)
	}
	if got := thinking.Get("signature").String(); got != "enc_sig_nonstream" {
		t.Fatalf("expected signature to be preserved, got %q", got)
	}
	if got := thinking.Get("thinking").String(); got != "internal reasoning" {
		t.Fatalf("unexpected thinking text: %q", got)
	}
}

func TestConvertCodexResponseToClaude_StreamEmptyOutputUsesOutputItemDoneMessageFallback(t *testing.T) {
	ctx := context.Background()
	originalRequest := []byte(`{"tools":[]}`)
	var param any

	chunks := [][]byte{
		[]byte("data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-5\"}}"),
		[]byte("data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"ok\"}]},\"output_index\":0}"),
		[]byte("data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}"),
	}

	var outputs [][]byte
	for _, chunk := range chunks {
		outputs = append(outputs, ConvertCodexResponseToClaude(ctx, "", originalRequest, nil, chunk, &param)...)
	}

	foundText := false
	for _, out := range outputs {
		for _, line := range strings.Split(string(out), "\n") {
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			data := gjson.Parse(strings.TrimPrefix(line, "data: "))
			if data.Get("type").String() == "content_block_delta" && data.Get("delta.type").String() == "text_delta" && data.Get("delta.text").String() == "ok" {
				foundText = true
				break
			}
		}
		if foundText {
			break
		}
	}
	if !foundText {
		t.Fatalf("expected fallback content from response.output_item.done message; outputs=%q", outputs)
	}
}

func TestConvertCodexResponseToClaude_FinalizesPendingFunctionCallFromTerminalResponse(t *testing.T) {
	ctx := context.Background()
	originalRequest := []byte(`{"tools":[{"name":"lookup"}]}`)
	var param any

	chunks := [][]byte{
		[]byte(`data: {"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","call_id":"call_1","name":""}}`),
		[]byte(`data: {"type":"response.function_call_arguments.done","output_index":0,"arguments":"{\"q\":\"go\"}"}`),
		[]byte(`data: {"type":"response.completed","response":{"output":[{"type":"function_call","call_id":"call_1","name":"lookup","arguments":"{\"q\":\"go\"}"}],"usage":{"input_tokens":1,"output_tokens":1}}}`),
	}

	var stream strings.Builder
	for _, chunk := range chunks {
		for _, output := range ConvertCodexResponseToClaude(ctx, "", originalRequest, nil, chunk, &param) {
			stream.Write(output)
		}
	}

	result := stream.String()
	if strings.Count(result, `"type":"content_block_start"`) != 1 {
		t.Fatalf("expected one tool block start, got: %s", result)
	}
	if !strings.Contains(result, `"name":"lookup"`) || !strings.Contains(result, `"partial_json":"{\"q\":\"go\"}"`) {
		t.Fatalf("pending tool call was not hydrated from terminal response: %s", result)
	}
	if !strings.Contains(result, `"stop_reason":"tool_use"`) {
		t.Fatalf("terminal response should report tool_use: %s", result)
	}
}

func TestConvertCodexResponseToClaude_KeepsMultiplePendingFunctionCallsByCallID(t *testing.T) {
	var param any
	chunks := [][]byte{
		[]byte(`data: {"type":"response.output_item.added","output_index":1,"item":{"type":"function_call","call_id":"call_first"}}`),
		[]byte(`data: {"type":"response.output_item.added","output_index":2,"item":{"type":"function_call","call_id":"call_second"}}`),
		[]byte(`data: {"type":"response.output_item.done","item":{"type":"function_call","call_id":"call_first","name":"lookup","arguments":"{\"id\":1}"}}`),
		[]byte(`data: {"type":"response.output_item.done","item":{"type":"function_call","call_id":"call_second","name":"lookup","arguments":"{\"id\":2}"}}`),
	}

	var stream strings.Builder
	for _, chunk := range chunks {
		for _, output := range ConvertCodexResponseToClaude(context.Background(), "", nil, nil, chunk, &param) {
			stream.Write(output)
		}
	}
	result := stream.String()
	if strings.Count(result, `"type":"tool_use"`) != 2 || !strings.Contains(result, `"id":"call_first"`) || !strings.Contains(result, `"id":"call_second"`) {
		t.Fatalf("pending function calls were not preserved independently: %s", result)
	}
	if !strings.Contains(result, `"partial_json":"{\"id\":1}"`) || !strings.Contains(result, `"partial_json":"{\"id\":2}"`) {
		t.Fatalf("pending arguments were not preserved: %s", result)
	}
}

func TestConvertCodexResponseToClaude_UnresolvedPendingFunctionCallEndsTurn(t *testing.T) {
	var param any
	chunks := [][]byte{
		[]byte(`data: {"type":"response.output_item.added","output_index":1,"item":{"type":"function_call","call_id":"call_hidden"}}`),
		[]byte(`data: {"type":"response.completed","response":{"stop_reason":"stop","output":[],"usage":{}}}`),
	}

	var stream strings.Builder
	for _, chunk := range chunks {
		for _, output := range ConvertCodexResponseToClaude(context.Background(), "", nil, nil, chunk, &param) {
			stream.Write(output)
		}
	}
	result := stream.String()
	if strings.Contains(result, `"type":"tool_use"`) {
		t.Fatalf("unresolved pending function call emitted tool_use: %s", result)
	}
	if !strings.Contains(result, `"stop_reason":"end_turn"`) {
		t.Fatalf("unresolved pending function call should end turn: %s", result)
	}
	params := param.(*ConvertCodexResponseToClaudeParams)
	if len(params.PendingFunctionCalls) != 0 || params.LastPendingFunctionCallKey != "" {
		t.Fatalf("pending state was not cleared: %#v", params)
	}
}

func TestConvertCodexResponseToClaude_ClosesOpenFunctionCallAtTerminalResponse(t *testing.T) {
	ctx := context.Background()
	var param any
	chunks := [][]byte{
		[]byte(`data: {"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","call_id":"call_1","name":"lookup"}}`),
		[]byte(`data: {"type":"response.completed","response":{"output":[{"type":"function_call","call_id":"call_1","name":"lookup","arguments":"{\"q\":1}"}],"usage":{}}}`),
	}

	var stream strings.Builder
	for _, chunk := range chunks {
		for _, output := range ConvertCodexResponseToClaude(ctx, "", nil, nil, chunk, &param) {
			stream.Write(output)
		}
	}
	result := stream.String()
	if !strings.Contains(result, `"partial_json":"{\"q\":1}"`) {
		t.Fatalf("terminal response should hydrate missing arguments: %s", result)
	}
	if strings.Count(result, `"type":"content_block_stop"`) != 1 {
		t.Fatalf("open tool block should be closed once: %s", result)
	}
}

func TestConvertCodexResponseToClaude_EmitsWebSearchServerToolBlocks(t *testing.T) {
	ctx := context.Background()
	var param any
	chunk := []byte(`data: {"type":"response.output_item.done","output_index":0,"item":{"type":"web_search_call","id":"ws_1","action":{"query":"golang"},"results":[{"title":"Go","url":"https://go.dev"}]}}`)

	outputs := ConvertCodexResponseToClaude(ctx, "", nil, nil, chunk, &param)
	joined := ""
	for _, output := range outputs {
		joined += string(output)
	}
	if !strings.Contains(joined, `"type":"server_tool_use"`) || !strings.Contains(joined, `"name":"web_search"`) {
		t.Fatalf("missing web search server tool block: %s", joined)
	}
	if !strings.Contains(joined, `"type":"web_search_tool_result"`) || !strings.Contains(joined, `"url":"https://go.dev"`) {
		t.Fatalf("missing web search result block: %s", joined)
	}
}

func TestConvertCodexResponseToClaudeNonStream_EmitsWebSearchServerToolBlocks(t *testing.T) {
	response := []byte(`{"type":"response.completed","response":{"output":[{"type":"web_search_call","id":"ws_1","action":{"query":"golang"},"results":[{"title":"Go","url":"https://go.dev"}]}]}}`)
	out := ConvertCodexResponseToClaudeNonStream(context.Background(), "", nil, nil, response, nil)

	if got := gjson.GetBytes(out, "content.0.type").String(); got != "server_tool_use" {
		t.Fatalf("content.0.type = %q, want server_tool_use; output=%s", got, out)
	}
	if got := gjson.GetBytes(out, "content.1.type").String(); got != "web_search_tool_result" {
		t.Fatalf("content.1.type = %q, want web_search_tool_result; output=%s", got, out)
	}
}
