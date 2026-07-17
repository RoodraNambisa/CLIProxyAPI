package helps

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestChatGPTWebSSEDecoderHandlesChunkedMultilineFrames(t *testing.T) {
	decoder := NewChatGPTWebSSEDecoder(1024)
	var got [][]byte
	for _, chunk := range [][]byte{
		[]byte("event: message\ndata: {\"a\":"),
		[]byte("1}\ndata: tail\n\n: keepalive\n"),
		[]byte("data: [DONE]\n\n"),
	} {
		payloads, err := decoder.Feed(chunk, false)
		if err != nil {
			t.Fatalf("Feed() error = %v", err)
		}
		got = append(got, payloads...)
	}
	want := [][]byte{[]byte("{\"a\":1}\ntail"), []byte("[DONE]")}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("payloads = %q, want %q", got, want)
	}
}

func TestChatGPTWebSSEDecoderRejectsOversizedFrame(t *testing.T) {
	decoder := NewChatGPTWebSSEDecoder(8)
	if _, err := decoder.Feed([]byte("data: "+string(bytes.Repeat([]byte("x"), 9))+"\n\n"), false); err == nil {
		t.Fatal("expected oversized frame error")
	}
}

func TestChatGPTWebSSEDecoderCountsEmptyDataLineSeparators(t *testing.T) {
	decoder := NewChatGPTWebSSEDecoder(3)
	if _, err := decoder.Feed([]byte("data:\ndata:\ndata:\ndata:\ndata:\n\n"), false); err == nil {
		t.Fatal("expected empty data lines to exceed the frame limit")
	}
}

func TestChatGPTWebSSEDecoderEmitsEmptyDataFrame(t *testing.T) {
	decoder := NewChatGPTWebSSEDecoder(8)
	payloads, err := decoder.Feed([]byte("data:\n\n"), false)
	if err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
	if len(payloads) != 1 || len(payloads[0]) != 0 {
		t.Fatalf("payloads = %#v, want one empty payload", payloads)
	}
}

func TestParseChatGPTWebRequest(t *testing.T) {
	payload := []byte(`{
		"model":"gpt-5-5",
		"instructions":"be concise",
		"input":[
			{"type":"message","role":"assistant","content":[{"type":"output_text","text":"old"}]},
			{"type":"message","role":"user","content":[{"type":"input_text","text":"draw"},{"type":"input_image","image_url":"data:image/png;base64,AAAA"}]}
			],
			"tools":[{"type":"web_search_preview"},{"type":"image_generation","size":"1024x1024","quality":"high","input_image_mask":{"image_url":"data:image/png;base64,MASK"}}],
			"tool_choice":{"type":"image_generation"},
			"reasoning":{"effort":"high"}
		}`)
	request, err := ParseChatGPTWebRequest(payload)
	if err != nil {
		t.Fatalf("ParseChatGPTWebRequest() error = %v", err)
	}
	if request.Model != "gpt-5-5" || request.ReasoningEffort != "high" || request.WebSearch {
		t.Fatalf("request metadata = %#v", request)
	}
	if len(request.Messages) != 3 || request.Messages[0].Role != "developer" || request.Messages[2].Role != "user" {
		t.Fatalf("messages = %#v", request.Messages)
	}
	if request.Image == nil || request.Image.Prompt != "be concise\n\ndraw" || request.Image.MaskURL != "data:image/png;base64,MASK" || request.Image.Size != "1024x1024" || len(request.Image.Images) != 1 {
		t.Fatalf("image request = %#v", request.Image)
	}
}

func TestParseChatGPTWebRequestRejectsTrailingJSON(t *testing.T) {
	for _, payload := range []string{
		`{"model":"gpt-5","input":"hello"} trailing`,
		`{"model":"gpt-5","input":"hello"} {"model":"gpt-5","input":"again"}`,
	} {
		if _, err := ParseChatGPTWebRequest([]byte(payload)); err == nil {
			t.Fatalf("ParseChatGPTWebRequest(%q) accepted trailing data", payload)
		}
	}
	if _, err := ParseChatGPTWebRequest([]byte("{\"model\":\"gpt-5\",\"input\":\"hello\"}\n\t ")); err != nil {
		t.Fatalf("ParseChatGPTWebRequest() rejected trailing whitespace: %v", err)
	}
}

func TestParseChatGPTWebRequestRejectsStructuredTextFormat(t *testing.T) {
	_, err := ParseChatGPTWebRequest([]byte(`{
		"model":"gpt-5",
		"input":"return JSON",
		"text":{"format":{"type":"json_schema","name":"answer","schema":{"type":"object"}}}
	}`))
	var unsupported *ChatGPTWebUnsupportedRequestError
	if !errors.As(err, &unsupported) {
		t.Fatalf("ParseChatGPTWebRequest() error = %v", err)
	}
}

func TestParseChatGPTWebRequestRejectsUnsupportedTool(t *testing.T) {
	_, err := ParseChatGPTWebRequest([]byte(`{"model":"gpt-5","input":"hi","tools":[{"type":"function","name":"shell"}]}`))
	if err == nil {
		t.Fatal("expected unsupported tool error")
	}
	var unsupported *ChatGPTWebUnsupportedToolError
	if !errors.As(err, &unsupported) {
		t.Fatalf("unsupported tool error type = %T", err)
	}
}

func TestParseChatGPTWebRequestAcceptsPNGAndRejectsUnsupportedImageFormat(t *testing.T) {
	if _, err := ParseChatGPTWebRequest([]byte(`{"model":"gpt-image-2","input":"draw","tools":[{"type":"image_generation","output_format":"png"}]}`)); err != nil {
		t.Fatalf("ParseChatGPTWebRequest() PNG error = %v", err)
	}
	_, err := ParseChatGPTWebRequest([]byte(`{"model":"gpt-image-2","input":"draw","tools":[{"type":"image_generation","output_format":"webp"}]}`))
	if err == nil || !strings.Contains(err.Error(), `output_format "webp"`) {
		t.Fatalf("ParseChatGPTWebRequest() error = %v", err)
	}
	var unsupported *ChatGPTWebUnsupportedToolError
	if !errors.As(err, &unsupported) {
		t.Fatalf("unsupported image format error type = %T", err)
	}
}

func TestParseChatGPTWebRequestHonorsImageAction(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		action  string
		wantErr string
	}{
		{
			name:    "generate rejects input image",
			input:   `[{"type":"message","role":"user","content":[{"type":"input_text","text":"draw"},{"type":"input_image","image_url":"data:image/png;base64,AAAA"}]}]`,
			action:  "generate",
			wantErr: "generate",
		},
		{
			name:    "edit requires input image",
			input:   `"draw"`,
			action:  "edit",
			wantErr: "requires an input image",
		},
		{
			name:    "unknown action",
			input:   `"draw"`,
			action:  "replace",
			wantErr: "action",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload := []byte(`{"model":"gpt-image-2","input":` + test.input +
				`,"tools":[{"type":"image_generation","action":` + strconv.Quote(test.action) +
				`}],"tool_choice":{"type":"image_generation"}}`)
			_, err := ParseChatGPTWebRequest(payload)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("ParseChatGPTWebRequest() error = %v", err)
			}
			var unsupported *ChatGPTWebUnsupportedToolError
			if !errors.As(err, &unsupported) {
				t.Fatalf("unsupported image action error type = %T", err)
			}
		})
	}
}

func TestParseChatGPTWebImageRequestUsesOnlyCurrentUserTurn(t *testing.T) {
	request, err := ParseChatGPTWebRequest([]byte(`{
		"model":"gpt-image-2",
		"instructions":"follow style",
		"input":[
			{"type":"message","role":"user","content":[
				{"type":"input_text","text":"old prompt"},
				{"type":"input_image","image_url":"data:image/png;base64,AAAA"}
			]},
			{"type":"message","role":"assistant","content":[{"type":"output_text","text":"old answer"}]},
			{"type":"message","role":"user","content":[
				{"type":"input_text","text":"current prompt"},
				{"type":"input_image","image_url":"data:image/png;base64,AQID"}
			]}
		],
		"tools":[{"type":"image_generation","action":"auto"}],
		"tool_choice":{"type":"image_generation"}
	}`))
	if err != nil {
		t.Fatalf("ParseChatGPTWebRequest() error = %v", err)
	}
	if request.Image == nil {
		t.Fatal("image request is nil")
	}
	if request.Image.Prompt != "follow style\n\ncurrent prompt" {
		t.Fatalf("image prompt = %q", request.Image.Prompt)
	}
	if len(request.Image.Images) != 1 || request.Image.Images[0] != "data:image/png;base64,AQID" {
		t.Fatalf("image inputs = %#v", request.Image.Images)
	}
	if request.Image.Action != "edit" {
		t.Fatalf("image action = %q, want edit", request.Image.Action)
	}
}

func TestParseChatGPTWebRequestReportsImageCapabilityMismatch(t *testing.T) {
	for _, tool := range []string{
		`{"type":"image_generation","background":"transparent"}`,
		`{"type":"image_generation","input_fidelity":"high"}`,
		`{"type":"image_generation","partial_images":1}`,
	} {
		_, err := ParseChatGPTWebRequest([]byte(`{"model":"gpt-image-2","input":"draw","tools":[` + tool + `]}`))
		var unsupported *ChatGPTWebUnsupportedToolError
		if !errors.As(err, &unsupported) {
			t.Fatalf("tool %s error = %v", tool, err)
		}
	}
}

func TestParseChatGPTWebRequestRejectsUnsupportedInputHistory(t *testing.T) {
	_, err := ParseChatGPTWebRequest([]byte(`{
		"model":"gpt-5",
		"input":[
			{"type":"message","role":"user","content":[{"type":"input_text","text":"run it"}]},
			{"type":"function_call","call_id":"call_1","name":"lookup","arguments":"{}"}
		]
	}`))
	if err == nil || !strings.Contains(err.Error(), "function_call") {
		t.Fatalf("ParseChatGPTWebRequest() error = %v", err)
	}
	var unsupported *ChatGPTWebUnsupportedRequestError
	if !errors.As(err, &unsupported) {
		t.Fatalf("unsupported input error type = %T", err)
	}
}

func TestParseChatGPTWebRequestRejectsMixedUnsupportedContentPart(t *testing.T) {
	_, err := ParseChatGPTWebRequest([]byte(`{
		"model":"gpt-5",
		"input":[{
			"type":"message",
			"role":"user",
			"content":[
				{"type":"input_text","text":"summarize the file"},
				{"type":"input_file","file_id":"file-1"}
			]
		}]
	}`))
	if err == nil || !strings.Contains(err.Error(), "message") {
		t.Fatalf("ParseChatGPTWebRequest() error = %v", err)
	}
	var unsupported *ChatGPTWebUnsupportedRequestError
	if !errors.As(err, &unsupported) {
		t.Fatalf("unsupported content error type = %T", err)
	}
}

func TestParseChatGPTWebRequestRejectsUnsupportedImageFileReference(t *testing.T) {
	_, err := ParseChatGPTWebRequest([]byte(`{
		"model":"gpt-5",
		"input":[{
			"type":"message",
			"role":"user",
			"content":[
				{"type":"input_text","text":"describe this image"},
				{"type":"input_image","file_id":"file-1"}
			]
		}]
	}`))
	var unsupported *ChatGPTWebUnsupportedRequestError
	if !errors.As(err, &unsupported) {
		t.Fatalf("ParseChatGPTWebRequest() error = %v", err)
	}
}

func TestParseChatGPTWebRequestRejectsUnpreservedControls(t *testing.T) {
	tests := []string{
		`"previous_response_id":"resp-1"`,
		`"conversation":"conv-1"`,
		`"max_output_tokens":128`,
		`"max_tool_calls":2`,
		`"temperature":0.2`,
		`"top_p":0.8`,
		`"truncation":"disabled"`,
		`"background":true`,
		`"service_tier":"priority"`,
		`"reasoning":{"summary":"detailed"}`,
		`"text":{"verbosity":"high"}`,
	}
	for _, control := range tests {
		payload := []byte(`{"model":"gpt-5","input":"hello",` + control + `}`)
		_, err := ParseChatGPTWebRequest(payload)
		if err == nil || !IsChatGPTWebProviderUnsupported(err) {
			t.Fatalf("ParseChatGPTWebRequest(%s) error = %v", control, err)
		}
	}

	for _, payload := range []string{
		`{"model":"gpt-5","input":"hello","background":false}`,
		`{"model":"gpt-5","input":"hello","service_tier":"auto"}`,
		`{"model":"gpt-5","input":"hello","service_tier":"default"}`,
		`{"model":"gpt-5","input":"hello","reasoning":{"effort":"high","summary":"auto"}}`,
	} {
		if _, err := ParseChatGPTWebRequest([]byte(payload)); err != nil {
			t.Fatalf("ParseChatGPTWebRequest(%s) error = %v", payload, err)
		}
	}
}

func TestParseChatGPTWebRequestRejectsSearchReasoningControls(t *testing.T) {
	for _, reasoning := range []string{
		`{"effort":"high"}`,
		`{"summary":"auto"}`,
	} {
		_, err := ParseChatGPTWebRequest([]byte(`{
			"model":"gpt-5",
			"input":"search",
			"reasoning":` + reasoning + `,
			"tools":[{"type":"web_search_preview"}],
			"tool_choice":{"type":"web_search_preview"}
		}`))
		var unsupported *ChatGPTWebUnsupportedRequestError
		if !errors.As(err, &unsupported) || !strings.Contains(err.Error(), "search") {
			t.Fatalf("reasoning %s error = %v", reasoning, err)
		}
	}
}

func TestParseChatGPTWebRequestRecognizesImageToolChoice(t *testing.T) {
	request, err := ParseChatGPTWebRequest([]byte(`{
		"model":"gpt-image-2",
		"input":"draw",
		"tool_choice":{"type":"function","name":"image_gen.imagegen"}
	}`))
	if err != nil {
		t.Fatalf("ParseChatGPTWebRequest() error = %v", err)
	}
	if request.Image == nil || request.Image.Prompt != "draw" {
		t.Fatalf("image request = %#v", request.Image)
	}
}

func TestParseChatGPTWebRequestRequiresExactImageFunctionName(t *testing.T) {
	for _, payload := range []string{
		`{"model":"gpt-5","input":"draw","tools":[{"type":"function","name":"imagegen"}],"tool_choice":{"type":"function","name":"imagegen"}}`,
		`{"model":"gpt-5","input":"draw","tools":[{"type":"namespace","name":"image_gen","tools":[{"type":"function","name":"other"}]}],"tool_choice":{"type":"namespace","name":"image_gen"}}`,
	} {
		_, err := ParseChatGPTWebRequest([]byte(payload))
		var unsupported *ChatGPTWebUnsupportedToolError
		if !errors.As(err, &unsupported) {
			t.Fatalf("ParseChatGPTWebRequest(%s) error = %v", payload, err)
		}
	}
}

func TestParseChatGPTWebRequestRecognizesImageNamespaceMember(t *testing.T) {
	request, err := ParseChatGPTWebRequest([]byte(`{
		"model":"gpt-5",
		"input":"draw",
		"tools":[{"type":"namespace","name":"image_gen","tools":[{"type":"function","name":"imagegen"}]}],
		"tool_choice":{"type":"namespace","name":"image_gen"}
	}`))
	if err != nil {
		t.Fatalf("ParseChatGPTWebRequest() error = %v", err)
	}
	if request.Image == nil || request.Image.Prompt != "draw" {
		t.Fatalf("image request = %#v", request.Image)
	}
}

func TestParseChatGPTWebRequestHonorsToolChoiceNone(t *testing.T) {
	request, err := ParseChatGPTWebRequest([]byte(`{
		"model":"gpt-5",
		"input":"answer without a tool",
			"tools":[{"type":"web_search_preview"},{"type":"image_generation"},{"type":"function","name":"lookup"}],
		"tool_choice":"none"
	}`))
	if err != nil {
		t.Fatalf("ParseChatGPTWebRequest() error = %v", err)
	}
	if request.WebSearch || request.Image != nil {
		t.Fatalf("tool_choice none selected a special tool: %#v", request)
	}
}

func TestParseChatGPTWebRequestHonorsToolChoiceAuto(t *testing.T) {
	_, err := ParseChatGPTWebRequest([]byte(`{
				"model":"gpt-5",
				"input":"search",
				"tools":[{"type":"web_search_preview"}],
				"tool_choice":"auto"
			}`))
	var unsupported *ChatGPTWebUnsupportedToolError
	if !errors.As(err, &unsupported) {
		t.Fatalf("explicit auto search error = %v", err)
	}

	_, err = ParseChatGPTWebRequest([]byte(`{
					"model":"gpt-5",
					"input":"search",
					"tools":[{"type":"web_search"}]
				}`))
	if !errors.As(err, &unsupported) {
		t.Fatalf("implicit auto search error = %v", err)
	}

	search, err := ParseChatGPTWebRequest([]byte(`{
					"model":"gpt-5-search-api",
					"input":"search",
				"tools":[{"type":"web_search_preview"}],
			"tool_choice":"auto"
		}`))
	if err != nil || !search.WebSearch || search.Image != nil {
		t.Fatalf("search model request = %#v, error = %v", search, err)
	}

	image, err := ParseChatGPTWebRequest([]byte(`{
		"model":"gpt-image-2",
		"input":"draw",
		"tools":[{"type":"image_generation"}],
		"tool_choice":"auto"
	}`))
	if err != nil {
		t.Fatalf("ParseChatGPTWebRequest(image) error = %v", err)
	}
	if image.WebSearch || image.Image == nil {
		t.Fatalf("auto image request = %#v", image)
	}
}

func TestParseChatGPTWebRequestHonorsExplicitToolChoice(t *testing.T) {
	request, err := ParseChatGPTWebRequest([]byte(`{
		"model":"gpt-5",
		"input":"search",
			"tools":[{"type":"web_search_preview"},{"type":"image_generation"},{"type":"function","name":"lookup"}],
		"tool_choice":{"type":"web_search_preview"}
	}`))
	if err != nil {
		t.Fatalf("ParseChatGPTWebRequest() error = %v", err)
	}
	if !request.WebSearch || request.Image != nil {
		t.Fatalf("explicit search choice = %#v", request)
	}
}

func TestParseChatGPTWebRequestFallsBackForDynamicRequiredChoice(t *testing.T) {
	_, err := ParseChatGPTWebRequest([]byte(`{
		"model":"gpt-5",
		"input":"use a tool",
		"tools":[{"type":"web_search_preview"},{"type":"image_generation"}],
		"tool_choice":"required"
	}`))
	var unsupported *ChatGPTWebUnsupportedToolError
	if !errors.As(err, &unsupported) {
		t.Fatalf("ParseChatGPTWebRequest() error = %v", err)
	}
}

func TestChatGPTWebConversationAccumulatorFullAndPatchEvents(t *testing.T) {
	accumulator := NewChatGPTWebConversationAccumulator([]ChatGPTWebMessage{{
		ID:    "history-id",
		Role:  "assistant",
		Parts: []ChatGPTWebContentPart{{Text: "history"}},
	}})
	if delta, done, err := accumulator.Apply([]byte(`{"message":{"id":"history-id","author":{"role":"assistant"},"content":{"content_type":"text","parts":["history"]}}}`)); err != nil || done || delta != "" {
		t.Fatalf("history Apply() = (%q, %t, %v)", delta, done, err)
	}
	delta, done, err := accumulator.Apply([]byte(`{"message":{"id":"answer-id","author":{"role":"assistant"},"content":{"content_type":"text","parts":["Hello"]}}}`))
	if err != nil || done || delta != "Hello" {
		t.Fatalf("first Apply() = (%q, %t, %v)", delta, done, err)
	}
	delta, done, err = accumulator.Apply([]byte(`{"o":"patch","v":[{"p":"/message/content/parts/0","o":"append","v":" world"}]}`))
	if err != nil || done || delta != " world" || accumulator.Text() != "Hello world" {
		t.Fatalf("patch Apply() = (%q, %t, %v), text=%q", delta, done, err, accumulator.Text())
	}
	_, done, err = accumulator.Apply([]byte(`[DONE]`))
	if err != nil || !done {
		t.Fatalf("done Apply() = (%t, %v)", done, err)
	}
}

func TestChatGPTWebConversationAccumulatorIgnoresHistoricalTerminalFailure(t *testing.T) {
	accumulator := NewChatGPTWebConversationAccumulator([]ChatGPTWebMessage{{
		ID:    "history-id",
		Role:  "assistant",
		Parts: []ChatGPTWebContentPart{{Text: "history"}},
	}})
	history := []byte(`{
		"message":{
			"id":"history-id",
			"author":{"role":"assistant"},
			"metadata":{"finish_details":{"type":"content_filter"}},
			"content":{"parts":["history"]}
		}
	}`)
	if delta, done, err := accumulator.Apply(history); err != nil || done || delta != "" {
		t.Fatalf("history Apply() = (%q, %t, %v)", delta, done, err)
	}
	if delta, done, err := accumulator.Apply([]byte(`{
		"message":{
			"id":"answer-id",
			"author":{"role":"assistant"},
			"metadata":{"finish_details":{"type":"finished_successfully"}},
			"content":{"parts":["current answer"]}
		}
	}`)); err != nil || done || delta != "current answer" {
		t.Fatalf("answer Apply() = (%q, %t, %v)", delta, done, err)
	}
}

func TestChatGPTWebConversationAccumulatorRejectsFailedAndEmptyTerminal(t *testing.T) {
	accumulator := NewChatGPTWebConversationAccumulator(nil)
	if _, _, err := accumulator.Apply([]byte(`{"message":{"author":{"role":"assistant"},"metadata":{"finish_details":{"type":"finished_partial_completion"}},"content":{"parts":["partial"]}}}`)); err == nil {
		t.Fatal("expected partial completion error")
	}
	for _, status := range []string{"max_tokens", "max_output_tokens", "content_filter"} {
		incomplete := NewChatGPTWebConversationAccumulator(nil)
		payload := []byte(`{"message":{"author":{"role":"assistant"},"metadata":{"finish_details":{"type":"` + status + `"}},"content":{"parts":["partial"]}}}`)
		if _, _, err := incomplete.Apply(payload); err == nil {
			t.Fatalf("expected %s error", status)
		}
	}

	empty := NewChatGPTWebConversationAccumulator(nil)
	if _, _, err := empty.Apply([]byte(`[DONE]`)); err == nil || !strings.Contains(err.Error(), "without assistant text") {
		t.Fatalf("empty terminal error = %v", err)
	}
}

func TestChatGPTWebConversationAccumulatorRejectsOversizedText(t *testing.T) {
	payload, err := json.Marshal(map[string]any{
		"message": map[string]any{
			"id":     "oversized-answer",
			"author": map[string]any{"role": "assistant"},
			"content": map[string]any{
				"parts": []string{strings.Repeat("x", chatGPTWebMaxConversationTextBytes+1)},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal oversized payload: %v", err)
	}
	accumulator := NewChatGPTWebConversationAccumulator(nil)
	_, _, err = accumulator.Apply(payload)
	var limitErr *ChatGPTWebResponseLimitError
	if !errors.As(err, &limitErr) {
		t.Fatalf("Apply() error = %v", err)
	}
	if limitErr.RetryOtherAuth() {
		t.Fatal("oversized conversation response requested another auth")
	}
}

func TestChatGPTWebConversationAccumulatorPreservesLegitimateHistoryPrefix(t *testing.T) {
	accumulator := NewChatGPTWebConversationAccumulator([]ChatGPTWebMessage{{
		ID:    "history-id",
		Role:  "assistant",
		Parts: []ChatGPTWebContentPart{{Text: "Yes."}},
	}})
	delta, done, err := accumulator.Apply([]byte(`{"message":{"id":"new-answer","author":{"role":"assistant"},"content":{"parts":["Yes. More details..."]}}}`))
	if err != nil || done || delta != "Yes. More details..." {
		t.Fatalf("Apply() = (%q, %t, %v)", delta, done, err)
	}
}

func TestChatGPTWebConversationAccumulatorPreservesRepeatedAnswerWithNewID(t *testing.T) {
	accumulator := NewChatGPTWebConversationAccumulator([]ChatGPTWebMessage{{
		ID:    "history-id",
		Role:  "assistant",
		Parts: []ChatGPTWebContentPart{{Text: "Same answer."}},
	}})
	delta, done, err := accumulator.Apply([]byte(`{"message":{"id":"new-answer","author":{"role":"assistant"},"content":{"parts":["Same answer."]}}}`))
	if err != nil || done || delta != "Same answer." {
		t.Fatalf("Apply() = (%q, %t, %v)", delta, done, err)
	}
}

func TestChatGPTWebConversationAccumulatorSkipsSeparateHistoryMessages(t *testing.T) {
	accumulator := NewChatGPTWebConversationAccumulator([]ChatGPTWebMessage{
		{Role: "assistant", Parts: []ChatGPTWebContentPart{{Text: "first history"}}},
		{Role: "user", Parts: []ChatGPTWebContentPart{{Text: "continue"}}},
		{Role: "assistant", Parts: []ChatGPTWebContentPart{{Text: "second history"}}},
		{Role: "user", Parts: []ChatGPTWebContentPart{{Text: "answer now"}}},
	})
	for _, history := range []string{"first history", "second history"} {
		encodedHistory, errMarshal := json.Marshal(history)
		if errMarshal != nil {
			t.Fatalf("marshal history: %v", errMarshal)
		}
		delta, done, err := accumulator.Apply([]byte(`{"message":{"author":{"role":"assistant"},"content":{"parts":[` + string(encodedHistory) + `]}}}`))
		if err != nil || done || delta != "" {
			t.Fatalf("history Apply(%q) = (%q, %t, %v)", history, delta, done, err)
		}
	}
	delta, done, err := accumulator.Apply([]byte(`{"message":{"author":{"role":"assistant"},"content":{"parts":["new answer"]}}}`))
	if err != nil || done || delta != "new answer" || accumulator.Text() != "new answer" {
		t.Fatalf("answer Apply() = (%q, %t, %v), text=%q", delta, done, err, accumulator.Text())
	}
}

func TestChatGPTWebConversationAccumulatorRejectsRewriteAfterEmission(t *testing.T) {
	accumulator := NewChatGPTWebConversationAccumulator(nil)
	if _, _, err := accumulator.Apply([]byte(`{"message":{"author":{"role":"assistant"},"content":{"parts":["hello"]}}}`)); err != nil {
		t.Fatalf("first Apply() error = %v", err)
	}
	if _, _, err := accumulator.Apply([]byte(`{"p":"/message/content/parts/0","o":"replace","v":"goodbye"}`)); err == nil {
		t.Fatal("expected rewrite error")
	}
}

func TestChatGPTWebConversationAccumulatorWithholdsSplitAnnotation(t *testing.T) {
	accumulator := NewChatGPTWebConversationAccumulator(nil)
	delta, done, err := accumulator.Apply([]byte(`{"message":{"author":{"role":"assistant"},"content":{"parts":["before\uE200cite"]}}}`))
	if err != nil || done || delta != "before" {
		t.Fatalf("first Apply() = (%q, %t, %v)", delta, done, err)
	}
	delta, done, err = accumulator.Apply([]byte(`{"p":"/message/content/parts/0","o":"append","v":" secret\uE201 after"}`))
	if err != nil || done || delta != " after" || accumulator.Text() != "before after" {
		t.Fatalf("second Apply() = (%q, %t, %v), text=%q", delta, done, err, accumulator.Text())
	}
}

func TestCleanChatGPTWebTextRemovesAnnotationPayload(t *testing.T) {
	if got := CleanChatGPTWebText("answer \ue200cite\u00b7turn0search0\ue201 done"); got != "answer  done" {
		t.Fatalf("CleanChatGPTWebText() = %q", got)
	}
}

func TestValidateChatGPTWebImageReferences(t *testing.T) {
	if err := ValidateChatGPTWebImageReferences([]string{"data:image/png;base64,QUJDRA=="}, 4, 4); err != nil {
		t.Fatalf("valid image references error = %v", err)
	}
	if err := ValidateChatGPTWebImageReferences([]string{"data:image/png;base64,QUJDRA=="}, 3, 8); err == nil {
		t.Fatal("expected per-image limit error")
	}
	if err := ValidateChatGPTWebImageReferences([]string{
		"data:image/png;base64,QUJDRA==",
		"data:image/png;base64,QUJDRA==",
	}, 4, 7); err == nil {
		t.Fatal("expected total image limit error")
	}
	references := make([]string, ChatGPTWebMaxImageInputs+1)
	for index := range references {
		references[index] = "data:image/png;base64,QQ=="
	}
	if err := ValidateChatGPTWebImageReferences(references, 4, len(references)); err == nil ||
		!strings.Contains(err.Error(), "items") {
		t.Fatalf("image count limit error = %v", err)
	}
	for _, reference := range []string{
		"data:text/plain;base64,QQ==",
		"data:image/png,QQ==",
		"data:image/png;base64,@@@@",
		"QQ=",
	} {
		if _, err := ChatGPTWebEncodedImageSize(reference, 4); err == nil {
			t.Fatalf("ChatGPTWebEncodedImageSize(%q) accepted invalid image data", reference)
		}
	}
}

func TestChatGPTWebImageAccumulatorIgnoresUserInputIDs(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{}
	user := []byte(`{"conversation_id":"conv_1","message":{"author":{"role":"user"},"content":{"content_type":"multimodal_text","parts":[{"content_type":"image_asset_pointer","asset_pointer":"file-service://input_file"}]}}}`)
	if _, err := accumulator.Apply(user); err != nil {
		t.Fatalf("Apply(user) error = %v", err)
	}
	if len(accumulator.FileIDs) != 0 {
		t.Fatalf("input file IDs captured: %v", accumulator.FileIDs)
	}
	tool := []byte(`{"message":{"author":{"role":"tool"},"metadata":{"async_task_type":"image_gen"},"content":{"content_type":"multimodal_text","parts":[{"content_type":"image_asset_pointer","asset_pointer":"file-service://output_file"},{"asset_pointer":"sediment://asset_2"}]}}}`)
	if _, err := accumulator.Apply(tool); err != nil {
		t.Fatalf("Apply(tool) error = %v", err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"output_file"}) || !reflect.DeepEqual(accumulator.SedimentIDs, []string{"asset_2"}) {
		t.Fatalf("captured IDs = files %v sediment %v", accumulator.FileIDs, accumulator.SedimentIDs)
	}
}

func TestChatGPTWebImageAccumulatorCapturesExplicitAssistantImagePointers(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{}
	payload := []byte(`{"message":{"author":{"role":"assistant"},"content":{"parts":[{"asset_pointer":"file-service://not_output"}]}}}`)
	if _, err := accumulator.Apply(payload); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"not_output"}) {
		t.Fatalf("assistant file IDs = %v", accumulator.FileIDs)
	}
}

func TestChatGPTWebImageAccumulatorCapturesToolPatch(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{}
	payload := []byte(`{"o":"patch","v":[{"p":"/message/author/role","o":"replace","v":"tool"},{"p":"/message/metadata/async_task_type","o":"replace","v":"image_gen"},{"p":"/message/content/parts/0","o":"replace","v":{"content_type":"image_asset_pointer","asset_pointer":"file-service://file_00000000abcdefabcdefabcdef"}}]}`)
	if _, err := accumulator.Apply(payload); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"file_00000000abcdefabcdefabcdef"}) {
		t.Fatalf("file IDs = %v", accumulator.FileIDs)
	}
}

func TestChatGPTWebImageAccumulatorCapturesPatchRegardlessOfOperationOrder(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{}
	payload := []byte(`{"o":"patch","v":[
		{"p":"/message/content/parts/0","o":"replace","v":{"asset_pointer":"sediment://first"}},
		{"p":"/message/content/parts/1","o":"replace","v":{"asset_pointer":"file-service://second"}},
		{"p":"/message/metadata/async_task_type","o":"replace","v":"image_gen"},
		{"p":"/message/author/role","o":"replace","v":"tool"}
	]}`)
	if _, err := accumulator.Apply(payload); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	want := []ChatGPTWebImageReference{{Kind: "sediment", ID: "first"}, {Kind: "file", ID: "second"}}
	if !reflect.DeepEqual(accumulator.References, want) {
		t.Fatalf("references = %#v, want %#v", accumulator.References, want)
	}
}

func TestChatGPTWebImageAccumulatorCapturesExplicitStreamTerminalStates(t *testing.T) {
	tests := []struct {
		name          string
		payload       string
		wantFailure   string
		wantReference string
	}{
		{
			name: "success",
			payload: `{"message":{
				"author":{"role":"tool"},
				"metadata":{
					"async_task_type":"image_gen",
					"finish_details":{"type":"finished_successfully"},
					"is_complete":true
				},
				"content":{"parts":[{"asset_pointer":"file-service://generated"}]}
			}}`,
			wantReference: "generated",
		},
		{
			name: "failure",
			payload: `{"message":{
				"author":{"role":"tool"},
				"metadata":{
					"async_task_type":"image_gen",
					"finish_details":{"type":"finished_with_error"}
				},
				"content":{"parts":[]}
			}}`,
			wantFailure: "finished_with_error",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			accumulator := &ChatGPTWebImageAccumulator{}
			done, err := accumulator.Apply([]byte(test.payload))
			if err != nil {
				t.Fatalf("Apply() error = %v", err)
			}
			if done {
				t.Fatal("explicit terminal event was treated as the SSE [DONE] marker")
			}
			if !accumulator.Terminal || accumulator.FailureStatus != test.wantFailure {
				t.Fatalf("terminal state = (%t, %q)", accumulator.Terminal, accumulator.FailureStatus)
			}
			if test.wantReference != "" && !reflect.DeepEqual(accumulator.FileIDs, []string{test.wantReference}) {
				t.Fatalf("file IDs = %v", accumulator.FileIDs)
			}
		})
	}
}

func TestChatGPTWebImageAccumulatorIgnoresReferenceText(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{}
	payload := []byte(`{"message":{
		"author":{"role":"tool"},
		"metadata":{"async_task_type":"image_gen"},
		"content":{"parts":[
			"diagnostic file-service://not-an-output sediment://also-not-output",
			{"asset_pointer":"file-service://real-output"}
		]}
	}}`)
	if _, err := accumulator.Apply(payload); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"real-output"}) || len(accumulator.SedimentIDs) != 0 {
		t.Fatalf("captured references = files %v sediment %v", accumulator.FileIDs, accumulator.SedimentIDs)
	}
}

func TestChatGPTWebImageAccumulatorLimitsOutputReferences(t *testing.T) {
	parts := make([]any, 0, chatGPTWebMaxImageOutputReferences+1)
	for index := 0; index <= chatGPTWebMaxImageOutputReferences; index++ {
		parts = append(parts, map[string]any{
			"asset_pointer": fmt.Sprintf("file-service://output-%d", index),
		})
	}
	payload, err := json.Marshal(map[string]any{
		"message": map[string]any{
			"author":   map[string]any{"role": "tool"},
			"metadata": map[string]any{"async_task_type": "image_gen"},
			"content":  map[string]any{"parts": parts},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	accumulator := &ChatGPTWebImageAccumulator{}
	_, err = accumulator.Apply(payload)
	var limitErr *ChatGPTWebResponseLimitError
	if !errors.As(err, &limitErr) {
		t.Fatalf("Apply() error = %v", err)
	}
	if limitErr.RetryOtherAuth() {
		t.Fatal("oversized image output requested another auth")
	}
}

func TestCaptureChatGPTWebImageConversationUsesExplicitOutputs(t *testing.T) {
	payload := []byte(`{"mapping":{
		"user":{"message":{"author":{"role":"user"},"content":{"content_type":"multimodal_text","parts":[{"asset_pointer":"file-service://input"}]}}},
		"later":{"message":{"author":{"role":"assistant"},"create_time":2,"content":{"content_type":"multimodal_text","parts":[{"asset_pointer":"file-service://second"}]}}},
		"earlier":{"message":{"author":{"role":"tool"},"create_time":1,"metadata":{"async_task_type":"image_gen"},"content":{"content_type":"multimodal_text","parts":[{"asset_pointer":"sediment://result"}]}}}
	}}`)
	accumulator := &ChatGPTWebImageAccumulator{}
	if err := CaptureChatGPTWebImageConversation(payload, accumulator); err != nil {
		t.Fatalf("CaptureChatGPTWebImageConversation() error = %v", err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"second"}) || !reflect.DeepEqual(accumulator.SedimentIDs, []string{"result"}) {
		t.Fatalf("captured IDs = files %v sediment %v", accumulator.FileIDs, accumulator.SedimentIDs)
	}
	if want := []ChatGPTWebImageReference{{Kind: "sediment", ID: "result"}, {Kind: "file", ID: "second"}}; !reflect.DeepEqual(accumulator.References, want) {
		t.Fatalf("references = %#v, want %#v", accumulator.References, want)
	}
}

func TestCaptureChatGPTWebImageConversationDetectsStructuredTerminalState(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{}
	err := CaptureChatGPTWebImageConversation([]byte(`{
		"mapping":{
			"image":{"message":{
				"author":{"role":"tool"},
				"metadata":{
					"async_task_type":"image_gen",
					"finish_details":{"type":"finished_successfully"},
					"is_complete":true
				},
				"content":{"parts":[{"asset_pointer":"file-service://image-one"}]}
			}}
		}
	}`), accumulator)
	if err != nil {
		t.Fatalf("CaptureChatGPTWebImageConversation() error = %v", err)
	}
	if !accumulator.Terminal {
		t.Fatal("structured terminal state was not detected")
	}
}

func TestCaptureChatGPTWebImageConversationDoesNotTreatStatusAloneAsTerminal(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{}
	err := CaptureChatGPTWebImageConversation([]byte(`{
		"mapping":{
			"image":{"message":{
				"author":{"role":"tool"},
				"metadata":{"async_task_type":"image_gen","status":"finished_successfully"},
				"content":{"parts":[{"asset_pointer":"file-service://image-one"}]}
			}}
		}
	}`), accumulator)
	if err != nil {
		t.Fatalf("CaptureChatGPTWebImageConversation() error = %v", err)
	}
	if accumulator.Terminal {
		t.Fatal("status without a completion marker was treated as terminal")
	}
}

func TestCaptureChatGPTWebImageConversationIgnoresUnrelatedTerminalMessage(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{}
	err := CaptureChatGPTWebImageConversation([]byte(`{
		"mapping":{
			"completed":{"message":{
				"author":{"role":"assistant"},
				"create_time":1,
				"metadata":{"finish_details":{"type":"finished_successfully"},"is_complete":true},
				"content":{"parts":["unrelated answer"]}
			}},
			"image":{"message":{
				"author":{"role":"tool"},
				"create_time":2,
				"metadata":{"async_task_type":"image_gen","status":"running"},
				"content":{"parts":[{"asset_pointer":"file-service://image-one"}]}
			}}
		}
	}`), accumulator)
	if err != nil {
		t.Fatalf("CaptureChatGPTWebImageConversation() error = %v", err)
	}
	if accumulator.Terminal {
		t.Fatal("unrelated terminal message completed the running image task")
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"image-one"}) {
		t.Fatalf("file IDs = %v", accumulator.FileIDs)
	}
}
