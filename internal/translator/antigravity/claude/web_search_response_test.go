package claude

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func antigravityWebSearchTestRequest() ([]byte, []byte) {
	original := []byte(`{"model":"gemini-web-search-test","tools":[{"type":"web_search_20250305","name":"web_search"}]}`)
	translated := []byte(`{"model":"gemini-web-search-test","request":{"tools":[{"googleSearch":{}}]}}`)
	return original, translated
}

func antigravityWebSearchGroundingResponse(text string) []byte {
	return []byte(`{
		"response":{
			"responseId":"response-1","modelVersion":"gemini-web-search-test",
			"candidates":[{"content":{"parts":[{"text":"` + text + `"}]},"groundingMetadata":{
				"webSearchQueries":["weather"],
				"groundingChunks":[{"web":{"uri":"https://example.com/weather","title":"Weather"}}],
				"groundingSupports":[{"segment":{"startIndex":0,"endIndex":` + "31" + `,"text":"` + text + `"},"groundingChunkIndices":[0]}]
			},"finishReason":"STOP"}],
			"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":6,"totalTokenCount":16}
		}
	}`)
}

func TestConvertAntigravityResponseToClaudeNonStreamWebSearchGrounding(t *testing.T) {
	original, translated := antigravityWebSearchTestRequest()
	output := ConvertAntigravityResponseToClaudeNonStream(context.Background(), "gemini-web-search-test", original, translated, antigravityWebSearchGroundingResponse("Beijing weather is clear today."), nil)

	if got := gjson.GetBytes(output, "content.0.type").String(); got != "server_tool_use" {
		t.Fatalf("content.0.type = %q, want server_tool_use: %s", got, output)
	}
	if got := gjson.GetBytes(output, "content.1.type").String(); got != "web_search_tool_result" {
		t.Fatalf("content.1.type = %q, want web_search_tool_result: %s", got, output)
	}
	if got := gjson.GetBytes(output, "content.2.citations.0.url").String(); got != "https://example.com/weather" {
		t.Fatalf("citation URL = %q: %s", got, output)
	}
	if got := gjson.GetBytes(output, "usage.server_tool_use.web_search_requests").Int(); got != 1 {
		t.Fatalf("web_search_requests = %d, want 1: %s", got, output)
	}
}

func TestConvertAntigravityResponseToClaudeStreamBuffersUntilGrounding(t *testing.T) {
	original, translated := antigravityWebSearchTestRequest()
	first := []byte(`{"response":{"candidates":[{"content":{"parts":[{"text":"Beijing weather "}]}}],"cpaUsageMetadata":{"promptTokenCount":10,"candidatesTokenCount":2}}}`)
	final := antigravityWebSearchGroundingResponse("is clear today.")

	var param any
	output := bytes.Join(ConvertAntigravityResponseToClaude(context.Background(), "gemini-web-search-test", original, translated, first, &param), nil)
	output = append(output, bytes.Join(ConvertAntigravityResponseToClaude(context.Background(), "gemini-web-search-test", original, translated, final, &param), nil)...)
	output = append(output, bytes.Join(ConvertAntigravityResponseToClaude(context.Background(), "gemini-web-search-test", original, translated, []byte("[DONE]"), &param), nil)...)
	text := string(output)

	serverTool := strings.Index(text, `"content_block":{"type":"server_tool_use"`)
	visibleText := strings.Index(text, `"content_block":{"type":"text"`)
	if serverTool < 0 || visibleText >= 0 && visibleText < serverTool {
		t.Fatalf("web search blocks must precede visible text:\n%s", text)
	}
	for _, needle := range []string{`"type":"web_search_tool_result"`, `"web_search_requests":1`, `"type":"citations_delta"`, "event: message_stop"} {
		if !strings.Contains(text, needle) {
			t.Fatalf("stream output missing %q:\n%s", needle, text)
		}
	}
	messageStart := webSearchSSEData(t, text, "message_start")
	if got := gjson.Get(messageStart, "message.usage.output_tokens").Int(); got != 0 {
		t.Fatalf("message_start output_tokens = %d, want 0", got)
	}
}

func TestConvertAntigravityResponseToClaudeWebSearchRequiresTranslatedGoogleSearch(t *testing.T) {
	original, _ := antigravityWebSearchTestRequest()
	translated := []byte(`{"model":"gemini-web-search-test","request":{"contents":[]}}`)
	output := ConvertAntigravityResponseToClaudeNonStream(context.Background(), "gemini-web-search-test", original, translated, antigravityWebSearchGroundingResponse("Beijing weather is clear today."), nil)
	if gjson.GetBytes(output, "content.0.type").String() == "server_tool_use" {
		t.Fatalf("request without googleSearch synthesized web search blocks: %s", output)
	}
}

func TestConvertAntigravityResponseToClaudeStreamWaitsForTerminalMetadata(t *testing.T) {
	original, translated := antigravityWebSearchTestRequest()
	first := []byte(`{"response":{"candidates":[{"content":{"parts":[{"text":"answer"}]},"groundingMetadata":{"webSearchQueries":["query one"],"groundingChunks":[{"web":{"uri":"https://example.com/one","title":"One"}}]}}]}}`)
	final := []byte(`{"response":{"candidates":[{"content":{"parts":[]},"groundingMetadata":{"webSearchQueries":["query two"],"groundingChunks":[{"web":{"uri":"https://example.com/two","title":"Two"}}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}}`)

	var param any
	firstOutput := string(bytes.Join(ConvertAntigravityResponseToClaude(context.Background(), "gemini-web-search-test", original, translated, first, &param), nil))
	if strings.Contains(firstOutput, `"type":"server_tool_use"`) {
		t.Fatalf("web search emitted before terminal metadata:\n%s", firstOutput)
	}
	finalOutput := string(bytes.Join(ConvertAntigravityResponseToClaude(context.Background(), "gemini-web-search-test", original, translated, final, &param), nil))
	if strings.Contains(finalOutput, `"type":"server_tool_use"`) || strings.Contains(finalOutput, `"type":"message_delta"`) {
		t.Fatalf("web search content or terminal event flushed before DONE:\n%s", finalOutput)
	}
	doneOutput := string(bytes.Join(ConvertAntigravityResponseToClaude(context.Background(), "gemini-web-search-test", original, translated, []byte("[DONE]"), &param), nil))
	if got := strings.Count(doneOutput, `"type":"server_tool_use"`); got != 1 {
		t.Fatalf("server tool count = %d, want 1:\n%s", got, doneOutput)
	}
	if !strings.Contains(doneOutput, `"web_search_requests":1`) {
		t.Fatalf("web search usage did not count the native search request:\n%s", doneOutput)
	}
}

func TestConvertAntigravityResponseToClaudeStreamDoneFlushesBufferedText(t *testing.T) {
	original, translated := antigravityWebSearchTestRequest()
	chunk := []byte(`{"response":{"candidates":[{"content":{"parts":[{"text":"fallback answer"}]}}]}}`)

	var param any
	_ = ConvertAntigravityResponseToClaude(context.Background(), "gemini-web-search-test", original, translated, chunk, &param)
	done := string(bytes.Join(ConvertAntigravityResponseToClaude(context.Background(), "gemini-web-search-test", original, translated, []byte("[DONE]"), &param), nil))
	if !strings.Contains(done, "fallback answer") || !strings.Contains(done, `"type":"server_tool_use"`) ||
		!strings.Contains(done, `"web_search_requests":1`) || !strings.Contains(done, "event: message_stop") {
		t.Fatalf("DONE did not flush buffered text and terminal event:\n%s", done)
	}
}

func TestConvertAntigravityResponseToClaudeStreamDoneFlushesEmptySearch(t *testing.T) {
	original, translated := antigravityWebSearchTestRequest()
	finish := []byte(`{"response":{"candidates":[{"content":{"parts":[]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"totalTokenCount":1}}}`)

	var param any
	_ = ConvertAntigravityResponseToClaude(context.Background(), "gemini-web-search-test", original, translated, finish, &param)
	done := string(bytes.Join(ConvertAntigravityResponseToClaude(context.Background(), "gemini-web-search-test", original, translated, []byte("[DONE]"), &param), nil))
	for _, want := range []string{`"type":"server_tool_use"`, `"type":"web_search_tool_result"`, `"web_search_requests":1`, "event: message_stop"} {
		if !strings.Contains(done, want) {
			t.Fatalf("empty search DONE missing %q:\n%s", want, done)
		}
	}
}

func TestConvertAntigravityResponseToClaudeNonStreamWebSearchWithoutGrounding(t *testing.T) {
	original, translated := antigravityWebSearchTestRequest()
	response := []byte(`{"response":{"candidates":[{"content":{"parts":[{"text":"fallback answer"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":2,"totalTokenCount":3}}}`)

	output := ConvertAntigravityResponseToClaudeNonStream(context.Background(), "gemini-web-search-test", original, translated, response, nil)
	if got := gjson.GetBytes(output, "content.0.type").String(); got != "server_tool_use" {
		t.Fatalf("content.0.type = %q, want server_tool_use: %s", got, output)
	}
	if got := gjson.GetBytes(output, "content.1.type").String(); got != "web_search_tool_result" {
		t.Fatalf("content.1.type = %q, want web_search_tool_result: %s", got, output)
	}
	if got := gjson.GetBytes(output, "content.2.text").String(); got != "fallback answer" {
		t.Fatalf("fallback text = %q: %s", got, output)
	}
	if got := gjson.GetBytes(output, "usage.server_tool_use.web_search_requests").Int(); got != 1 {
		t.Fatalf("web_search_requests = %d, want 1: %s", got, output)
	}
}

func TestConvertAntigravityResponseToClaudeStreamCollectsGroundingAfterFinish(t *testing.T) {
	original, translated := antigravityWebSearchTestRequest()
	finish := []byte(`{"response":{"candidates":[{"content":{"parts":[{"text":"answer"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}}`)
	grounding := []byte(`{"response":{"candidates":[{"content":{"parts":[]},"groundingMetadata":{"webSearchQueries":["late query"],"groundingChunks":[{"web":{"uri":"https://example.com/late","title":"Late"}}],"groundingSupports":[{"segment":{"partIndex":0,"startIndex":0,"endIndex":6,"text":"answer"},"groundingChunkIndices":[0]}]}}]}}`)

	var param any
	finishOutput := string(bytes.Join(ConvertAntigravityResponseToClaude(context.Background(), "gemini-web-search-test", original, translated, finish, &param), nil))
	groundingOutput := string(bytes.Join(ConvertAntigravityResponseToClaude(context.Background(), "gemini-web-search-test", original, translated, grounding, &param), nil))
	if strings.Contains(finishOutput+groundingOutput, `"type":"server_tool_use"`) || strings.Contains(finishOutput+groundingOutput, `"type":"message_delta"`) {
		t.Fatalf("web search content or terminal event flushed before DONE:\n%s%s", finishOutput, groundingOutput)
	}
	doneOutput := string(bytes.Join(ConvertAntigravityResponseToClaude(context.Background(), "gemini-web-search-test", original, translated, []byte("[DONE]"), &param), nil))
	for _, want := range []string{"late query", "https://example.com/late", `"type":"citations_delta"`} {
		if !strings.Contains(doneOutput, want) {
			t.Fatalf("DONE output missing %q:\n%s", want, doneOutput)
		}
	}
}

func TestConvertAntigravityResponseToClaudeStreamRecoversSparsePartIndex(t *testing.T) {
	original, translated := antigravityWebSearchTestRequest()
	thought := []byte(`{"response":{"candidates":[{"content":{"parts":[{"text":"private thought","thought":true}]}}]}}`)
	visible := []byte(`{"response":{"candidates":[{"content":{"parts":[{"text":"visible answer"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":2,"totalTokenCount":3}}}`)
	grounding := []byte(`{"response":{"candidates":[{"content":{"parts":[]},"groundingMetadata":{"webSearchQueries":["query"],"groundingChunks":[{"web":{"uri":"https://example.com/result","title":"Result"}}],"groundingSupports":[{"segment":{"partIndex":1,"startIndex":0,"endIndex":14,"text":"visible answer"},"groundingChunkIndices":[0]}]}}]}}`)

	var param any
	_ = ConvertAntigravityResponseToClaude(context.Background(), "gemini-web-search-test", original, translated, thought, &param)
	_ = ConvertAntigravityResponseToClaude(context.Background(), "gemini-web-search-test", original, translated, visible, &param)
	_ = ConvertAntigravityResponseToClaude(context.Background(), "gemini-web-search-test", original, translated, grounding, &param)
	doneOutput := string(bytes.Join(ConvertAntigravityResponseToClaude(context.Background(), "gemini-web-search-test", original, translated, []byte("[DONE]"), &param), nil))
	if strings.Contains(doneOutput, "private thought") {
		t.Fatalf("thought text leaked into web search output:\n%s", doneOutput)
	}
	for _, want := range []string{"visible answer", "https://example.com/result", `"type":"citations_delta"`} {
		if !strings.Contains(doneOutput, want) {
			t.Fatalf("DONE output missing %q:\n%s", want, doneOutput)
		}
	}
}

func TestBuildWebSearchCitedTextBlocksKeepsTextWithoutValidSource(t *testing.T) {
	blocks := buildWebSearchCitedTextBlocks("visible answer", []webSearchGroundingSupport{{
		StartIndex: 0,
		EndIndex:   14,
		Text:       "visible answer",
	}})
	if len(blocks) != 1 || blocks[0].Text != "visible answer" {
		t.Fatalf("blocks = %#v, want uncited visible text", blocks)
	}
	if len(blocks[0].Citations) != 0 {
		t.Fatalf("unexpected citations without a valid source: %#v", blocks[0].Citations)
	}
}

func TestBuildWebSearchCitedTextBlocksMergesOverlappingSources(t *testing.T) {
	blocks := buildWebSearchCitedTextBlocks("0123456789abcde", []webSearchGroundingSupport{
		{StartIndex: 0, EndIndex: 10, Sources: []webSearchGroundingSource{{URL: "https://example.com/a", Title: "A"}}},
		{StartIndex: 5, EndIndex: 15, Sources: []webSearchGroundingSource{{URL: "https://example.com/b", Title: "B"}}},
	})
	if len(blocks) != 3 {
		t.Fatalf("blocks = %#v, want three boundary segments", blocks)
	}
	if blocks[1].Text != "56789" || len(blocks[1].Citations) != 2 {
		t.Fatalf("overlap block = %#v, want both sources", blocks[1])
	}
}

func TestParseWebSearchGroundingSupportsMergesLateSources(t *testing.T) {
	metadata := webSearchGroundingResults([]string{
		`{"groundingChunks":[{"web":{"uri":"https://example.com/a","title":"A"}}],"groundingSupports":[{"segment":{"partIndex":0,"startIndex":0,"endIndex":6,"text":"answer"},"groundingChunkIndices":[0]}]}`,
		`{"groundingChunks":[{"web":{"uri":"https://example.com/b","title":"B"}}],"groundingSupports":[{"segment":{"partIndex":0,"startIndex":0,"endIndex":6,"text":"answer"},"groundingChunkIndices":[0]}]}`,
	})
	supports := parseWebSearchGroundingSupports(metadata, []string{"answer"})
	if len(supports) != 1 || len(supports[0].Sources) != 2 {
		t.Fatalf("supports = %#v, want one segment with two sources", supports)
	}
}

func TestParseWebSearchGroundingSupportsUsesCursorForRepeatedText(t *testing.T) {
	metadata := webSearchGroundingResults([]string{
		`{"groundingChunks":[{"web":{"uri":"https://example.com/a","title":"A"}},{"web":{"uri":"https://example.com/b","title":"B"}}],"groundingSupports":[{"segment":{"partIndex":1,"startIndex":0,"endIndex":4,"text":"same"},"groundingChunkIndices":[0]},{"segment":{"partIndex":1,"startIndex":5,"endIndex":9,"text":"same"},"groundingChunkIndices":[1]}]}`,
	})
	supports := parseWebSearchGroundingSupports(metadata, []string{"same same"})
	if len(supports) != 2 {
		t.Fatalf("supports = %#v, want two segments", supports)
	}
	if supports[0].StartIndex != 0 || supports[1].StartIndex != 5 {
		t.Fatalf("support starts = %d, %d; want 0, 5", supports[0].StartIndex, supports[1].StartIndex)
	}
}

func TestConvertAntigravityResponseToClaudeNonStreamWebSearchPreservesPartAndSources(t *testing.T) {
	original, translated := antigravityWebSearchTestRequest()
	response := []byte(`{
		"response":{
			"candidates":[{
				"content":{"parts":[{"text":"private thought","thought":true},{"text":"visible answer"}]},
				"groundingMetadata":{
					"webSearchQueries":["query one","query two"],
					"groundingChunks":[
						{"web":{"uri":"https://example.com/a","title":"A"}},
						{"web":{"uri":"https://example.com/b","title":"B"}}
					],
					"groundingSupports":[{"segment":{"partIndex":1,"startIndex":0,"endIndex":14,"text":"visible answer"},"groundingChunkIndices":[0,1]}]
				},
				"finishReason":"MAX_TOKENS"
			}],
			"usageMetadata":{"promptTokenCount":2,"candidatesTokenCount":3,"totalTokenCount":5}
		}
	}`)

	output := ConvertAntigravityResponseToClaudeNonStream(context.Background(), "gemini-web-search-test", original, translated, response, nil)
	if strings.Contains(string(output), "private thought") {
		t.Fatalf("thought text leaked into visible web search response: %s", output)
	}
	if got := gjson.GetBytes(output, "stop_reason").String(); got != "max_tokens" {
		t.Fatalf("stop_reason = %q, want max_tokens: %s", got, output)
	}
	if got := gjson.GetBytes(output, "usage.server_tool_use.web_search_requests").Int(); got != 1 {
		t.Fatalf("web_search_requests = %d, want 1: %s", got, output)
	}
	if got := gjson.GetBytes(output, `content.#(type=="text").citations.#`).Int(); got != 2 {
		t.Fatalf("citation count = %d, want 2: %s", got, output)
	}
}

func webSearchSSEData(t *testing.T, output, eventName string) string {
	t.Helper()
	current := ""
	for _, line := range strings.Split(output, "\n") {
		if strings.HasPrefix(line, "event: ") {
			current = strings.TrimPrefix(line, "event: ")
			continue
		}
		if current == eventName && strings.HasPrefix(line, "data: ") {
			return strings.TrimPrefix(line, "data: ")
		}
	}
	t.Fatalf("event %q not found in:\n%s", eventName, output)
	return ""
}
