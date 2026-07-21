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
	if request.Image == nil || request.Image.Prompt != "Instructions:\nbe concise\n\nTranscript:\nAssistant: old\nUser: draw" || request.Image.MaskURL != "data:image/png;base64,MASK" || request.Image.Size != "1024x1024" || len(request.Image.Images) != 1 {
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

func TestParseChatGPTWebImageRequestPreservesTextAndImageHistory(t *testing.T) {
	request, err := ParseChatGPTWebRequest([]byte(`{
		"model":"gpt-image-2",
		"instructions":"follow style",
		"input":[
			{"type":"message","role":"system","content":[{"type":"input_text","text":"keep the logo"}]},
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
	if request.Image.Prompt != "Instructions:\nfollow style\n\nkeep the logo\n\nTranscript:\nUser: old prompt\nAssistant: old answer\nUser: current prompt" {
		t.Fatalf("image prompt = %q", request.Image.Prompt)
	}
	if len(request.Image.Images) != 2 ||
		request.Image.Images[0] != "data:image/png;base64,AAAA" ||
		request.Image.Images[1] != "data:image/png;base64,AQID" {
		t.Fatalf("image inputs = %#v", request.Image.Images)
	}
	if request.Image.MaskImageIndex != 1 {
		t.Fatalf("mask image index = %d, want current image index 1", request.Image.MaskImageIndex)
	}
	if request.Image.Action != "edit" {
		t.Fatalf("image action = %q, want edit", request.Image.Action)
	}
}

func TestParseChatGPTWebImageRequestUsesHistoricalImageForFollowUpEdit(t *testing.T) {
	request, err := ParseChatGPTWebRequest([]byte(`{
		"model":"gpt-image-2",
		"input":[
			{"type":"message","role":"user","content":[
				{"type":"input_text","text":"make a logo"},
				{"type":"input_image","image_url":"data:image/png;base64,AAAA"}
			]},
			{"type":"message","role":"assistant","content":[{"type":"output_text","text":"logo ready"}]},
			{"type":"message","role":"user","content":[{"type":"input_text","text":"make it blue"}]}
		],
		"tools":[{"type":"image_generation","action":"auto"}],
		"tool_choice":{"type":"image_generation"}
	}`))
	if err != nil {
		t.Fatalf("ParseChatGPTWebRequest() error = %v", err)
	}
	if request.Image == nil || request.Image.Action != "edit" ||
		len(request.Image.Images) != 1 || request.Image.Images[0] != "data:image/png;base64,AAAA" {
		t.Fatalf("follow-up image request = %#v", request.Image)
	}
	if !strings.Contains(request.Image.Prompt, "User: make a logo") ||
		!strings.Contains(request.Image.Prompt, "User: make it blue") {
		t.Fatalf("follow-up image prompt = %q", request.Image.Prompt)
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

func TestChatGPTWebImageAccumulatorCapturesExplicitMessageTerminalStates(t *testing.T) {
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
				t.Fatal("single image message ended the complete image SSE phase")
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

func TestChatGPTWebImageAccumulatorWaitsForStreamTerminalAcrossImageMessages(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{}
	for _, payload := range []string{
		`{"message":{"author":{"role":"tool"},"metadata":{"async_task_type":"image_gen","finish_details":{"type":"finished_successfully"},"is_complete":true},"content":{"parts":["file-service://first"]}}}`,
		`{"message":{"author":{"role":"tool"},"metadata":{"async_task_type":"image_gen","finish_details":{"type":"finished_successfully"},"is_complete":true},"content":{"parts":["file-service://second"]}}}`,
	} {
		done, err := accumulator.Apply([]byte(payload))
		if err != nil || done {
			t.Fatalf("Apply() = done %t, error %v", done, err)
		}
	}
	done, err := accumulator.Apply([]byte(`[DONE]`))
	if err != nil || !done {
		t.Fatalf("terminal Apply() = done %t, error %v", done, err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"first", "second"}) {
		t.Fatalf("file IDs = %v", accumulator.FileIDs)
	}
}

func TestChatGPTWebImageAccumulatorCapturesUnmarkedFailuresOnly(t *testing.T) {
	tests := []struct {
		name        string
		payload     string
		wantFailure string
	}{
		{
			name:        "explicit error",
			payload:     `{"message":{"author":{"role":"assistant"},"is_error":true,"content":{"content_type":"text","parts":["image request failed"]}}}`,
			wantFailure: "image request failed",
		},
		{
			name:        "terminal rejection",
			payload:     `{"message":{"author":{"role":"assistant"},"end_turn":true,"content":{"content_type":"text","parts":["This image request was blocked"]}}}`,
			wantFailure: "This image request was blocked",
		},
		{
			name:        "terminal failure status",
			payload:     `{"message":{"author":{"role":"assistant"},"metadata":{"status":"finished_with_error"},"content":{"content_type":"text","parts":[]}}}`,
			wantFailure: "finished_with_error",
		},
		{
			name:    "ordinary terminal text",
			payload: `{"message":{"author":{"role":"assistant"},"end_turn":true,"content":{"content_type":"text","parts":["The request completed normally."]}}}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			accumulator := &ChatGPTWebImageAccumulator{}
			done, err := accumulator.Apply([]byte(test.payload))
			if err != nil || done {
				t.Fatalf("Apply() = done %t, error %v", done, err)
			}
			if !accumulator.Terminal || accumulator.FailureStatus != test.wantFailure {
				t.Fatalf("terminal state = (%t, %q), want failure %q", accumulator.Terminal, accumulator.FailureStatus, test.wantFailure)
			}
		})
	}
}

func TestChatGPTWebImageAccumulatorCapturesEmptyAssistantTerminal(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{}
	for _, payload := range []string{
		`{"message":{"author":{"role":"tool"},"metadata":{"async_task_type":"image_gen"},"content":{"parts":["file-service://generated"]}}}`,
		`{"message":{"author":{"role":"assistant"},"end_turn":true,"content":{"content_type":"text","parts":[]}}}`,
	} {
		if done, err := accumulator.Apply([]byte(payload)); err != nil || done {
			t.Fatalf("Apply() = done %t, error %v", done, err)
		}
	}
	if !accumulator.Terminal || accumulator.FailureStatus != "" {
		t.Fatalf("terminal state = (%t, %q)", accumulator.Terminal, accumulator.FailureStatus)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) {
		t.Fatalf("file IDs = %v", accumulator.FileIDs)
	}
}

func TestChatGPTWebImageAccumulatorStopsAtStreamComplete(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{ConversationID: "conversation"}
	done, err := accumulator.Apply([]byte(`{"type":"message_stream_complete"}`))
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if !done {
		t.Fatal("message_stream_complete did not end the image SSE phase")
	}
	if accumulator.Terminal {
		t.Fatal("stream completion was treated as image task completion")
	}
}

func TestChatGPTWebImageAccumulatorClassifiesOuterFailureEvents(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    string
	}{
		{
			name:    "response failed",
			payload: `{"type":"response.failed","response":{"error":{"message":"image policy rejected"}}}`,
			want:    "image policy rejected",
		},
		{
			name:    "response incomplete",
			payload: `{"type":"response.incomplete","response":{"incomplete_details":{"reason":"max_output_tokens"}}}`,
			want:    "max_output_tokens",
		},
		{
			name:    "moderation blocked",
			payload: `{"type":"moderation","moderation_response":{"blocked":true,"reason":"safety policy"}}`,
			want:    "safety policy",
		},
		{
			name:    "stream completion with nested response error",
			payload: `{"type":"message_stream_complete","response":{"error":{"message":"image generation failed"}}}`,
			want:    "image generation failed",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := (&ChatGPTWebImageAccumulator{}).Apply([]byte(test.payload))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Apply() error = %v, want detail %q", err, test.want)
			}
		})
	}
}

func TestChatGPTWebImageAccumulatorIgnoresFalseOuterFailure(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{}
	for _, payload := range []string{
		`{"error":false}`,
		`{"error":""}`,
		`{"error":{}}`,
		`{"error":[]}`,
		`{"type":"moderation","moderation_response":{"blocked":false}}`,
	} {
		if _, err := accumulator.Apply([]byte(payload)); err != nil {
			t.Fatalf("Apply(%s) error = %v", payload, err)
		}
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

func TestChatGPTWebImageAccumulatorCapturesExactStringReferences(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{}
	payload := []byte(`{"message":{
		"author":{"role":"tool"},
		"metadata":{"async_task_type":"image_gen"},
		"content":{"parts":["file-service://file-output","sediment://sediment-output"]}
	}}`)
	if _, err := accumulator.Apply(payload); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	want := []ChatGPTWebImageReference{{Kind: "file", ID: "file-output"}, {Kind: "sediment", ID: "sediment-output"}}
	if !reflect.DeepEqual(accumulator.References, want) {
		t.Fatalf("references = %#v, want %#v", accumulator.References, want)
	}
}

func TestChatGPTWebImageAccumulatorIgnoresUnstructuredAssistantReference(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{}
	payload := []byte(`{"message":{
		"author":{"role":"assistant"},
		"content":{"content_type":"text","parts":["file-service://not-an-image-output"]}
	}}`)
	if _, err := accumulator.Apply(payload); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if len(accumulator.References) != 0 {
		t.Fatalf("assistant text references = %#v", accumulator.References)
	}
}

func TestChatGPTWebImageAccumulatorIgnoresMetadataReferenceText(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{}
	payload := []byte(`{"message":{
		"author":{"role":"tool"},
		"metadata":{"async_task_type":"image_gen","diagnostic":"file-service://not-an-image-output"},
		"content":{"parts":[]}
	}}`)
	if _, err := accumulator.Apply(payload); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if len(accumulator.References) != 0 {
		t.Fatalf("metadata references = %#v", accumulator.References)
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

func TestCaptureChatGPTWebImageConversationRecognizesStringPointers(t *testing.T) {
	payload := []byte(`{"mapping":{
		"image":{"message":{"author":{"role":"tool"},"metadata":{"is_complete":true,"finish_details":{"type":"finished_successfully"}},"content":{"content_type":"multimodal_text","parts":["file-service://first","sediment://second"]}}}
	}}`)
	accumulator := &ChatGPTWebImageAccumulator{}
	if err := CaptureChatGPTWebImageConversation(payload, accumulator); err != nil {
		t.Fatalf("CaptureChatGPTWebImageConversation() error = %v", err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"first"}) || !reflect.DeepEqual(accumulator.SedimentIDs, []string{"second"}) {
		t.Fatalf("captured IDs = files %v sediment %v", accumulator.FileIDs, accumulator.SedimentIDs)
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

func TestCaptureChatGPTWebImageTasksFiltersConversationAndCapturesCompletedOutput(t *testing.T) {
	payload := []byte(`{"tasks":[
		{"conversation_id":"other","status":"completed","image_gen_message":{"author":{"role":"tool"},"metadata":{"async_task_type":"image_gen"},"content":{"parts":[{"asset_pointer":"file-service://wrong"}]}}},
		{"original_conversation_id":"target","status":"completed","image_gen_message":{"author":{"role":"tool"},"status":"finished_successfully","metadata":{"async_task_type":"image_gen"},"content":{"parts":[{"asset_pointer":"file-service://generated"},{"asset_pointer":"sediment://asset"}]}}}
	]}`)
	accumulator := &ChatGPTWebImageAccumulator{}
	state, err := CaptureChatGPTWebImageTasks(payload, "target", accumulator)
	if err != nil {
		t.Fatalf("CaptureChatGPTWebImageTasks() error = %v", err)
	}
	if !state.AllTerminal() || !accumulator.Terminal {
		t.Fatalf("task state = %+v, terminal %t", state, accumulator.Terminal)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) || !reflect.DeepEqual(accumulator.SedimentIDs, []string{"asset"}) {
		t.Fatalf("captured task IDs = files %v sediment %v", accumulator.FileIDs, accumulator.SedimentIDs)
	}
}

func TestCaptureChatGPTWebImageTasksCapturesFailure(t *testing.T) {
	payload := []byte(`{"tasks":[{"conversation_id":"target","status":"failed","image_gen_message":{"author":{"role":"tool"},"metadata":{"async_task_type":"image_gen"},"content":{"parts":[]}}}]}`)
	accumulator := &ChatGPTWebImageAccumulator{}
	state, err := CaptureChatGPTWebImageTasks(payload, "target", accumulator)
	if err != nil {
		t.Fatalf("CaptureChatGPTWebImageTasks() error = %v", err)
	}
	if !state.AllTerminal() || !accumulator.Terminal || accumulator.FailureStatus != "failed" {
		t.Fatalf("task state = %+v, terminal %t, failure %q", state, accumulator.Terminal, accumulator.FailureStatus)
	}
}

func TestCaptureChatGPTWebImageTasksReplacesPreviousSnapshot(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{}
	state, err := CaptureChatGPTWebImageTasks([]byte(`{"tasks":[{
		"conversation_id":"target","status":"failed",
		"image_gen_message":{"author":{"role":"tool"},"metadata":{"async_task_type":"image_gen"},"content":{"parts":[{"asset_pointer":"file-service://old-output"}]}}
	}]}`), "target", accumulator)
	if err != nil || !state.AllTerminal() || !accumulator.Terminal || accumulator.FailureStatus == "" {
		t.Fatalf("initial snapshot = state %+v, accumulator %+v, error %v", state, accumulator, err)
	}
	state, err = CaptureChatGPTWebImageTasks([]byte(`{"tasks":[{
		"conversation_id":"target","status":"running",
		"image_gen_message":{"author":{"role":"tool"},"metadata":{"async_task_type":"image_gen"},"content":{"parts":[]}}
	}]}`), "target", accumulator)
	if err != nil {
		t.Fatal(err)
	}
	if !state.HasPending() || accumulator.Terminal || accumulator.FailureStatus != "" || len(accumulator.References) != 0 {
		t.Fatalf("replacement snapshot = state %+v, accumulator %+v", state, accumulator)
	}
}

func TestCaptureChatGPTWebImageTasksRecognizesConversationFailureStatuses(t *testing.T) {
	for _, status := range []string{"content_filter", "max_tokens", "max_output_tokens", "expired", "interrupted", "incomplete"} {
		t.Run(status, func(t *testing.T) {
			payload := []byte(fmt.Sprintf(`{"tasks":[{"conversation_id":"target","type":"image_gen","status":%q}]}`, status))
			accumulator := &ChatGPTWebImageAccumulator{}
			state, err := CaptureChatGPTWebImageTasks(payload, "target", accumulator)
			if err != nil {
				t.Fatal(err)
			}
			if !state.AllTerminal() || !accumulator.Terminal || accumulator.FailureStatus != status {
				t.Fatalf("task state = %+v, terminal %t, failure %q", state, accumulator.Terminal, accumulator.FailureStatus)
			}
		})
	}
}

func TestCaptureChatGPTWebImageTasksIgnoresNonImageTasks(t *testing.T) {
	payload := []byte(`{"tasks":[
		{"conversation_id":"target","type":"file_upload","status":"running"},
		{"conversation_id":"target","type":"other","status":"failed"},
		{"conversation_id":"target","status":"completed","image_gen_message":{
			"author":{"role":"tool"},
			"metadata":{"async_task_type":"image_gen"},
			"content":{"parts":[{"asset_pointer":"file-service://generated"}]}
		}}
	]}`)
	accumulator := &ChatGPTWebImageAccumulator{}
	state, err := CaptureChatGPTWebImageTasks(payload, "target", accumulator)
	if err != nil {
		t.Fatal(err)
	}
	if state.Matched != 1 || state.Terminal != 1 || !accumulator.Terminal {
		t.Fatalf("task state = %+v, terminal %t", state, accumulator.Terminal)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) || accumulator.FailureStatus != "" {
		t.Fatalf("image snapshot = files %v, failure %q", accumulator.FileIDs, accumulator.FailureStatus)
	}
}

func TestCaptureChatGPTWebImageTasksCapturesBareReferenceIDs(t *testing.T) {
	payload := []byte(`{"tasks":[{"conversation_id":"target","status":"completed","image_gen_message":{
		"author":{"role":"tool"},
		"metadata":{"async_task_type":"image_gen"},
		"content":{"parts":[{"file_id":"raw-file","image_id":"raw-image","sediment_id":"raw-sediment"}]}
	}}]}`)
	accumulator := &ChatGPTWebImageAccumulator{}
	state, err := CaptureChatGPTWebImageTasks(payload, "target", accumulator)
	if err != nil {
		t.Fatal(err)
	}
	if !state.AllTerminal() || !reflect.DeepEqual(accumulator.FileIDs, []string{"raw-file", "raw-image"}) ||
		!reflect.DeepEqual(accumulator.SedimentIDs, []string{"raw-sediment"}) {
		t.Fatalf("task state = %+v, files %v, sediments %v", state, accumulator.FileIDs, accumulator.SedimentIDs)
	}
}

func TestCaptureChatGPTWebImageTasksCapturesStructuredMessageFailure(t *testing.T) {
	payload := []byte(`{"tasks":[{"conversation_id":"target","status":"completed","image_gen_message":{
		"author":{"role":"assistant"},
		"metadata":{"async_task_type":"image_gen","is_error":true},
		"content":{"content_type":"text","parts":["Your request was blocked"]}
	}}]}`)
	accumulator := &ChatGPTWebImageAccumulator{}
	state, err := CaptureChatGPTWebImageTasks(payload, "target", accumulator)
	if err != nil {
		t.Fatalf("CaptureChatGPTWebImageTasks() error = %v", err)
	}
	if !state.AllTerminal() || !accumulator.Terminal || accumulator.FailureStatus != "Your request was blocked" {
		t.Fatalf("task state = %+v, terminal %t, failure %q", state, accumulator.Terminal, accumulator.FailureStatus)
	}
}

func TestCaptureChatGPTWebImageTasksTreatsTerminalAssistantTextAsSuccess(t *testing.T) {
	payload := []byte(`{"tasks":[{"conversation_id":"target","status":"completed","image_gen_message":{
		"author":{"role":"assistant"},
		"metadata":{"async_task_type":"image_gen"},
		"end_turn":true,
		"content":{"content_type":"text","parts":["The image has been generated."]}
	}}]}`)
	accumulator := &ChatGPTWebImageAccumulator{}
	state, err := CaptureChatGPTWebImageTasks(payload, "target", accumulator)
	if err != nil {
		t.Fatalf("CaptureChatGPTWebImageTasks() error = %v", err)
	}
	if !state.AllTerminal() || !accumulator.Terminal || accumulator.FailureStatus != "" {
		t.Fatalf("task state = %+v, terminal %t, failure %q", state, accumulator.Terminal, accumulator.FailureStatus)
	}
}

func TestCaptureChatGPTWebImageTasksPrefersSpecificMessageFailure(t *testing.T) {
	payload := []byte(`{"tasks":[{"conversation_id":"target","status":"failed","image_gen_message":{
		"author":{"role":"assistant"},
		"metadata":{"async_task_type":"image_gen","is_error":true},
		"content":{"content_type":"text","parts":["image request rejected by policy"]}
	}}]}`)
	accumulator := &ChatGPTWebImageAccumulator{}
	state, err := CaptureChatGPTWebImageTasks(payload, "target", accumulator)
	if err != nil {
		t.Fatalf("CaptureChatGPTWebImageTasks() error = %v", err)
	}
	if !state.AllTerminal() || accumulator.FailureStatus != "image request rejected by policy" {
		t.Fatalf("task state = %+v, failure %q", state, accumulator.FailureStatus)
	}
}

func TestCaptureChatGPTWebImageTasksPrefersSpecificFailureRegardlessOfTaskOrder(t *testing.T) {
	generic := `{"conversation_id":"target","status":"completed","image_gen_message":{"author":{"role":"tool"},"metadata":{"async_task_type":"image_gen","is_error":true},"content":{"parts":[]}}}`
	specific := `{"conversation_id":"target","status":"failed","image_gen_message":{"author":{"role":"assistant"},"metadata":{"async_task_type":"image_gen","is_error":true},"content":{"content_type":"text","parts":["image request rejected by policy"]}}}`
	for _, tasks := range []string{generic + "," + specific, specific + "," + generic} {
		accumulator := &ChatGPTWebImageAccumulator{}
		state, err := CaptureChatGPTWebImageTasks([]byte(`{"tasks":[`+tasks+`]}`), "target", accumulator)
		if err != nil {
			t.Fatal(err)
		}
		if !state.AllTerminal() || accumulator.FailureStatus != "image request rejected by policy" {
			t.Fatalf("task state = %+v, failure %q", state, accumulator.FailureStatus)
		}
	}
}

func TestCaptureChatGPTWebImageTasksChoosesStableSpecificFailure(t *testing.T) {
	alpha := `{"conversation_id":"target","status":"failed","image_gen_message":{"author":{"role":"assistant"},"metadata":{"async_task_type":"image_gen","is_error":true},"content":{"content_type":"text","parts":["alpha policy failure"]}}}`
	zeta := `{"conversation_id":"target","status":"failed","image_gen_message":{"author":{"role":"assistant"},"metadata":{"async_task_type":"image_gen","is_error":true},"content":{"content_type":"text","parts":["zeta service failure"]}}}`
	for _, tasks := range []string{alpha + "," + zeta, zeta + "," + alpha} {
		accumulator := &ChatGPTWebImageAccumulator{}
		state, err := CaptureChatGPTWebImageTasks([]byte(`{"tasks":[`+tasks+`]}`), "target", accumulator)
		if err != nil {
			t.Fatal(err)
		}
		if !state.AllTerminal() || accumulator.FailureStatus != "alpha policy failure" {
			t.Fatalf("task state = %+v, failure %q", state, accumulator.FailureStatus)
		}
	}
}

func TestCaptureChatGPTWebImageTasksWaitsForAllMatchingTasks(t *testing.T) {
	payload := []byte(`{"tasks":[
		{"conversation_id":"target","status":"completed","image_gen_message":{"author":{"role":"tool"},"metadata":{"async_task_type":"image_gen"},"content":{"parts":[{"asset_pointer":"file-service://first"}]}}},
		{"conversation_id":"target","type":"image_gen","status":"running"}
	]}`)
	accumulator := &ChatGPTWebImageAccumulator{}
	state, err := CaptureChatGPTWebImageTasks(payload, "target", accumulator)
	if err != nil {
		t.Fatalf("CaptureChatGPTWebImageTasks() error = %v", err)
	}
	if state.Matched != 2 || state.Terminal != 1 || !state.HasPending() || accumulator.Terminal {
		t.Fatalf("task state = %+v, accumulator terminal = %t", state, accumulator.Terminal)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"first"}) {
		t.Fatalf("task outputs = %v", accumulator.FileIDs)
	}
}

func TestCaptureChatGPTWebImageTasksKeepsRunningOuterTaskPending(t *testing.T) {
	payload := []byte(`{"tasks":[{"conversation_id":"target","status":"running","image_gen_message":{
		"author":{"role":"tool"},
		"status":"finished_successfully",
		"metadata":{"async_task_type":"image_gen","is_complete":true,"finish_details":{"type":"finished_successfully"}},
		"content":{"parts":[{"asset_pointer":"file-service://early-output"}]}
	}}]}`)
	accumulator := &ChatGPTWebImageAccumulator{}
	state, err := CaptureChatGPTWebImageTasks(payload, "target", accumulator)
	if err != nil {
		t.Fatalf("CaptureChatGPTWebImageTasks() error = %v", err)
	}
	if state.Matched != 1 || state.Terminal != 0 || !state.HasPending() || accumulator.Terminal {
		t.Fatalf("task state = %+v, accumulator terminal = %t", state, accumulator.Terminal)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"early-output"}) {
		t.Fatalf("file IDs = %v", accumulator.FileIDs)
	}
}

func TestCaptureChatGPTWebImageTasksFiltersHistoricalConversationTasks(t *testing.T) {
	payload := []byte(`{"tasks":[
		{"conversation_id":"target","status":"completed","create_time":99,"image_gen_message":{"author":{"role":"tool"},"metadata":{"async_task_type":"image_gen"},"content":{"parts":[{"asset_pointer":"file-service://historical"}]}}},
		{"conversation_id":"target","status":"completed","parent_message_id":"current-user","create_time":101,"image_gen_message":{"author":{"role":"tool"},"metadata":{"async_task_type":"image_gen"},"content":{"parts":[{"asset_pointer":"file-service://current"}]}}},
		{"conversation_id":"target","status":"completed","parent_message_id":"other-user","create_time":102,"image_gen_message":{"author":{"role":"tool"},"metadata":{"async_task_type":"image_gen"},"content":{"parts":[{"asset_pointer":"file-service://unrelated"}]}}}
	]}`)
	accumulator := &ChatGPTWebImageAccumulator{Turn: ChatGPTWebImageTurn{MessageID: "current-user", CreatedAt: 100}}
	state, err := CaptureChatGPTWebImageTasks(payload, "target", accumulator)
	if err != nil {
		t.Fatal(err)
	}
	if state.Matched != 1 || !state.AllTerminal() {
		t.Fatalf("task state = %+v", state)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"current"}) {
		t.Fatalf("file IDs = %v", accumulator.FileIDs)
	}
}

func TestCaptureChatGPTWebImageTasksUsesCurrentMessageRelationWithoutTimestamp(t *testing.T) {
	payload := []byte(`{"tasks":[
		{"conversation_id":"target","status":"completed","parent_message_id":"old-user","image_gen_message":{"author":{"role":"tool"},"metadata":{"async_task_type":"image_gen"},"content":{"parts":[{"asset_pointer":"file-service://historical"}]}}},
		{"conversation_id":"target","status":"completed","parent_message_id":"current-user","image_gen_message":{"author":{"role":"tool"},"metadata":{"async_task_type":"image_gen"},"content":{"parts":[{"asset_pointer":"file-service://current-one"},{"asset_pointer":"file-service://current-two"}]}}}
	]}`)
	accumulator := &ChatGPTWebImageAccumulator{Turn: ChatGPTWebImageTurn{MessageID: "current-user", CreatedAt: 100}}
	state, err := CaptureChatGPTWebImageTasks(payload, "target", accumulator)
	if err != nil {
		t.Fatal(err)
	}
	if state.Matched != 1 || !reflect.DeepEqual(accumulator.FileIDs, []string{"current-one", "current-two"}) {
		t.Fatalf("task state = %+v, file IDs = %v", state, accumulator.FileIDs)
	}
}

func TestCaptureChatGPTWebImageTasksUsesCreatedAtWithoutMessageID(t *testing.T) {
	payload := []byte(`{"tasks":[
		{"conversation_id":"target","status":"completed","parent_message_id":"old-user","create_time":99,"image_gen_message":{"author":{"role":"tool"},"metadata":{"async_task_type":"image_gen"},"content":{"parts":[{"asset_pointer":"file-service://historical"}]}}},
		{"conversation_id":"target","status":"completed","parent_message_id":"other-user","create_time":101,"image_gen_message":{"author":{"role":"tool"},"metadata":{"async_task_type":"image_gen"},"content":{"parts":[{"asset_pointer":"file-service://current"}]}}}
	]}`)
	accumulator := &ChatGPTWebImageAccumulator{Turn: ChatGPTWebImageTurn{CreatedAt: 100}}
	state, err := CaptureChatGPTWebImageTasks(payload, "target", accumulator)
	if err != nil {
		t.Fatal(err)
	}
	if state.Matched != 1 || !state.AllTerminal() || !reflect.DeepEqual(accumulator.FileIDs, []string{"current"}) {
		t.Fatalf("task state = %+v, file IDs = %v", state, accumulator.FileIDs)
	}
}

func TestCaptureChatGPTWebImageTasksUsesEarliestTimeFallbackWithoutRelation(t *testing.T) {
	payload := []byte(`{"tasks":[
		{"conversation_id":"target","status":"completed","create_time":101,"image_gen_message":{"author":{"role":"tool"},"metadata":{"async_task_type":"image_gen"},"content":{"parts":[{"asset_pointer":"file-service://current-one"},{"asset_pointer":"file-service://current-two"}]}}},
		{"conversation_id":"target","status":"completed","create_time":102,"image_gen_message":{"author":{"role":"tool"},"metadata":{"async_task_type":"image_gen"},"content":{"parts":[{"asset_pointer":"file-service://future"}]}}}
	]}`)
	accumulator := &ChatGPTWebImageAccumulator{Turn: ChatGPTWebImageTurn{MessageID: "current-user", CreatedAt: 100}}
	state, err := CaptureChatGPTWebImageTasks(payload, "target", accumulator)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"current-one", "current-two"}
	if state.Matched != 1 || !state.AllTerminal() || !reflect.DeepEqual(accumulator.FileIDs, want) {
		t.Fatalf("task state = %+v, file IDs = %v, want %v", state, accumulator.FileIDs, want)
	}
}

func TestCaptureChatGPTWebImageTasksUsesOnlyEarliestTimeFallbackBatchWithoutMessageID(t *testing.T) {
	payload := []byte(`{"tasks":[
		{"conversation_id":"target","status":"completed","create_time":99,"image_gen_message":{"author":{"role":"tool"},"metadata":{"async_task_type":"image_gen"},"content":{"parts":[{"asset_pointer":"file-service://historical"}]}}},
		{"conversation_id":"target","status":"completed","create_time":101,"image_gen_message":{"author":{"role":"tool"},"metadata":{"async_task_type":"image_gen"},"content":{"parts":[{"asset_pointer":"file-service://current-one"},{"asset_pointer":"file-service://current-two"}]}}},
		{"conversation_id":"target","status":"completed","create_time":101,"image_gen_message":{"author":{"role":"tool"},"metadata":{"async_task_type":"image_gen"},"content":{"parts":[{"asset_pointer":"file-service://current-three"}]}}},
		{"conversation_id":"target","status":"completed","create_time":102,"image_gen_message":{"author":{"role":"tool"},"metadata":{"async_task_type":"image_gen"},"content":{"parts":[{"asset_pointer":"file-service://future"}]}}}
	]}`)
	accumulator := &ChatGPTWebImageAccumulator{Turn: ChatGPTWebImageTurn{CreatedAt: 100}}
	state, err := CaptureChatGPTWebImageTasks(payload, "target", accumulator)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"current-one", "current-two", "current-three"}
	if state.Matched != 2 || !state.AllTerminal() || !reflect.DeepEqual(accumulator.FileIDs, want) {
		t.Fatalf("task state = %+v, file IDs = %v, want %v", state, accumulator.FileIDs, want)
	}
}

func TestMergeChatGPTWebImageAccumulatorsEnforcesReferenceLimit(t *testing.T) {
	primary := &ChatGPTWebImageAccumulator{}
	secondary := &ChatGPTWebImageAccumulator{}
	for index := 0; index < chatGPTWebMaxImageOutputReferences; index++ {
		target := primary
		if index >= chatGPTWebMaxImageOutputReferences/2 {
			target = secondary
		}
		if err := target.appendReference("file", fmt.Sprintf("image-%d", index)); err != nil {
			t.Fatalf("append reference %d: %v", index, err)
		}
	}
	merged, err := MergeChatGPTWebImageAccumulators(primary, secondary)
	if err != nil || len(merged.References) != chatGPTWebMaxImageOutputReferences {
		t.Fatalf("bounded merge = refs %d, err %v", len(merged.References), err)
	}
	if err = secondary.appendReference("file", "overflow"); err != nil {
		t.Fatalf("append overflow source: %v", err)
	}
	if _, err = MergeChatGPTWebImageAccumulators(primary, secondary); err == nil {
		t.Fatal("MergeChatGPTWebImageAccumulators() accepted too many references")
	}
}

func TestMergeChatGPTWebImageAccumulatorsPrefersSpecificFailure(t *testing.T) {
	for _, test := range []struct {
		name      string
		primary   string
		secondary string
	}{
		{name: "specific arrives second", primary: "failed", secondary: "image request rejected by policy"},
		{name: "generic arrives second", primary: "image request rejected by policy", secondary: "failed"},
		{name: "content filter arrives second", primary: "image request rejected by policy", secondary: "content_filter"},
		{name: "max tokens arrives first", primary: "max_tokens", secondary: "image request rejected by policy"},
		{name: "incomplete arrives second", primary: "image request rejected by policy", secondary: "incomplete"},
	} {
		t.Run(test.name, func(t *testing.T) {
			merged, err := MergeChatGPTWebImageAccumulators(
				&ChatGPTWebImageAccumulator{Terminal: true, FailureStatus: test.primary},
				&ChatGPTWebImageAccumulator{Terminal: true, FailureStatus: test.secondary},
			)
			if err != nil {
				t.Fatalf("MergeChatGPTWebImageAccumulators() error = %v", err)
			}
			if merged.FailureStatus != "image request rejected by policy" {
				t.Fatalf("failure = %q", merged.FailureStatus)
			}
		})
	}
}

func TestChatGPTWebImageRejectionTextHonorsWordBoundariesAndNegation(t *testing.T) {
	for _, test := range []struct {
		text string
		want bool
	}{
		{text: "The image request was blocked by policy.", want: true},
		{text: "The image request was denied.", want: true},
		{text: "The image request is unblocked.", want: false},
		{text: "The image request was not blocked.", want: false},
		{text: "The image request wasn't rejected.", want: false},
	} {
		if got := chatGPTWebImageRejectionText(test.text); got != test.want {
			t.Fatalf("chatGPTWebImageRejectionText(%q) = %t, want %t", test.text, got, test.want)
		}
	}
}

func TestChatGPTWebImageRejectionTextScansRepeatedNegationsLinearly(t *testing.T) {
	detail := strings.Repeat("not blocked ", 2_000) + "blocked"
	if !chatGPTWebImageRejectionText(detail) {
		t.Fatal("final unnegated rejection was not detected")
	}
}

func TestChatGPTWebImageConversationStateSelectsDeterministicFailure(t *testing.T) {
	terminal, failure := chatGPTWebImageConversationState(map[string]any{
		"author":   map[string]any{"role": "assistant"},
		"end_turn": true,
		"metadata": map[string]any{
			"finish_details": map[string]any{"type": "finished_with_error"},
		},
		"content": map[string]any{"content_type": "text", "parts": []any{"Something went wrong."}},
	})
	if !terminal || failure != "finished_with_error" {
		t.Fatalf("terminal text with structured failure = (%t, %q)", terminal, failure)
	}

	message := map[string]any{
		"status": "failed",
		"metadata": map[string]any{
			"finish_details": map[string]any{"type": "content_filter"},
		},
	}
	for iteration := 0; iteration < 100; iteration++ {
		terminal, failure = chatGPTWebImageConversationState(message)
		if !terminal || failure != "content_filter" {
			t.Fatalf("iteration %d: state = (%t, %q)", iteration, terminal, failure)
		}
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

func TestCaptureChatGPTWebImageConversationCapturesCurrentTerminalTextReply(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{}
	err := CaptureChatGPTWebImageConversation([]byte(`{
		"current_node":"rejected",
		"mapping":{
			"history":{"message":{
				"author":{"role":"assistant"},"create_time":1,"end_turn":true,
				"content":{"content_type":"text","parts":["old answer"]}
			}},
			"rejected":{"message":{
				"author":{"role":"assistant"},"create_time":2,"end_turn":true,
				"content":{"content_type":"text","parts":["This image request was blocked by our safety system."]}
			}}
		}
	}`), accumulator)
	if err != nil {
		t.Fatalf("CaptureChatGPTWebImageConversation() error = %v", err)
	}
	if !accumulator.Terminal || accumulator.FailureStatus != "This image request was blocked by our safety system." {
		t.Fatalf("terminal = %t, failure = %q", accumulator.Terminal, accumulator.FailureStatus)
	}
}

func TestCaptureChatGPTWebImageConversationUsesCurrentNodeAncestry(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{}
	err := CaptureChatGPTWebImageConversation([]byte(`{
		"current_node":"rejected",
		"mapping":{
			"old-user":{"message":{"author":{"role":"user"},"create_time":1}},
			"old-image":{"parent":"old-user","message":{
				"author":{"role":"tool"},"create_time":2,
				"metadata":{"async_task_type":"image_gen"},
				"content":{"parts":[{"asset_pointer":"file-service://historical"}]}
			}},
			"newer-branch-image":{"parent":"old-image","message":{
				"author":{"role":"tool"},"create_time":5,
				"metadata":{"async_task_type":"image_gen"},
				"content":{"parts":[{"asset_pointer":"file-service://off-branch"}]}
			}},
			"current-user":{"parent":"old-image","message":{"author":{"role":"user"},"create_time":3}},
			"rejected":{"parent":"current-user","message":{
				"author":{"role":"assistant"},"create_time":4,"end_turn":true,
				"content":{"content_type":"text","parts":["Current request was blocked"]}
			}}
		}
	}`), accumulator)
	if err != nil {
		t.Fatalf("CaptureChatGPTWebImageConversation() error = %v", err)
	}
	if len(accumulator.References) != 0 {
		t.Fatalf("historical references leaked into current turn: %#v", accumulator.References)
	}
	if !accumulator.Terminal || accumulator.FailureStatus != "Current request was blocked" {
		t.Fatalf("terminal = %t, failure = %q", accumulator.Terminal, accumulator.FailureStatus)
	}
}

func TestCaptureChatGPTWebImageConversationReplacesPreviousSnapshot(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{}
	for _, snapshot := range []string{
		`{"current_node":"old","mapping":{"old":{"message":{"author":{"role":"tool"},"metadata":{"async_task_type":"image_gen"},"content":{"parts":[{"asset_pointer":"file-service://old-branch"}]}}}}}`,
		`{"current_node":"new","mapping":{"new":{"message":{"author":{"role":"tool"},"metadata":{"async_task_type":"image_gen"},"content":{"parts":[{"asset_pointer":"file-service://new-branch"}]}}}}}`,
	} {
		if err := CaptureChatGPTWebImageConversation([]byte(snapshot), accumulator); err != nil {
			t.Fatalf("CaptureChatGPTWebImageConversation() error = %v", err)
		}
	}
	want := []ChatGPTWebImageReference{{Kind: "file", ID: "new-branch"}}
	if !reflect.DeepEqual(accumulator.References, want) || !reflect.DeepEqual(accumulator.FileIDs, []string{"new-branch"}) {
		t.Fatalf("snapshot references = %#v, file IDs = %v", accumulator.References, accumulator.FileIDs)
	}
}

func TestCaptureChatGPTWebImageConversationFallsBackWithoutGraphMetadata(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{}
	err := CaptureChatGPTWebImageConversation([]byte(`{"mapping":{
		"old-image":{"message":{
			"author":{"role":"tool"},"create_time":1,
			"metadata":{"async_task_type":"image_gen"},
			"content":{"parts":[{"asset_pointer":"file-service://historical"}]}
		}},
		"current-user":{"message":{"author":{"role":"user"},"create_time":2}},
		"current-image":{"message":{
			"author":{"role":"tool"},"create_time":3,
			"metadata":{"async_task_type":"image_gen","is_complete":true,"finish_details":{"type":"finished_successfully"}},
			"content":{"parts":[{"asset_pointer":"file-service://current"}]}
		}}
	}}`), accumulator)
	if err != nil {
		t.Fatalf("CaptureChatGPTWebImageConversation() error = %v", err)
	}
	want := []ChatGPTWebImageReference{{Kind: "file", ID: "current"}}
	if !reflect.DeepEqual(accumulator.References, want) {
		t.Fatalf("references = %#v, want %#v", accumulator.References, want)
	}
}

func TestCaptureChatGPTWebImageConversationIgnoresAmbiguousUntimestampedImageWithoutGraphMetadata(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{}
	err := CaptureChatGPTWebImageConversation([]byte(`{"mapping":{
		"historical-image":{"message":{
			"author":{"role":"tool"},"create_time":1,
			"metadata":{"async_task_type":"image_gen"},
			"content":{"parts":[{"asset_pointer":"file-service://historical"}]}
		}},
		"current-user":{"message":{"author":{"role":"user"},"create_time":"2"}},
		"current-image":{"message":{
			"author":{"role":"tool"},
			"metadata":{"async_task_type":"image_gen","is_complete":true,"finish_details":{"type":"finished_successfully"}},
			"content":{"parts":[{"asset_pointer":"file-service://current"}]}
		}}
	}}`), accumulator)
	if err != nil {
		t.Fatalf("CaptureChatGPTWebImageConversation() error = %v", err)
	}
	if len(accumulator.References) != 0 {
		t.Fatalf("ambiguous references leaked into current turn: %#v", accumulator.References)
	}
}

func TestCaptureChatGPTWebImageConversationKeepsUntimestampedCurrentDescendant(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{Turn: ChatGPTWebImageTurn{MessageID: "current-user", CreatedAt: 2}}
	err := CaptureChatGPTWebImageConversation([]byte(`{"mapping":{
		"historical-image":{"message":{
			"author":{"role":"tool"},
			"metadata":{"async_task_type":"image_gen"},
			"content":{"parts":[{"asset_pointer":"file-service://historical"}]}
		}},
		"current-user":{"message":{"id":"current-user","author":{"role":"user"},"create_time":2}},
		"current-image":{"parent":"current-user","message":{
			"author":{"role":"tool"},
			"metadata":{"async_task_type":"image_gen","is_complete":true,"finish_details":{"type":"finished_successfully"}},
			"content":{"parts":[{"asset_pointer":"file-service://current"}]}
		}}
	}}`), accumulator)
	if err != nil {
		t.Fatal(err)
	}
	want := []ChatGPTWebImageReference{{Kind: "file", ID: "current"}}
	if !reflect.DeepEqual(accumulator.References, want) {
		t.Fatalf("references = %#v, want %#v", accumulator.References, want)
	}
}

func TestCaptureChatGPTWebImageConversationUsesPerCandidateParentFallback(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{Turn: ChatGPTWebImageTurn{MessageID: "current-user", CreatedAt: 2}}
	err := CaptureChatGPTWebImageConversation([]byte(`{"mapping":{
		"historical-user":{"message":{"id":"historical-user","author":{"role":"user"},"create_time":1}},
		"historical-image":{"parent":"historical-user","message":{"author":{"role":"tool"},"create_time":1.5,"metadata":{"async_task_type":"image_gen"},"content":{"parts":[{"asset_pointer":"file-service://historical"}]}}},
		"current-user":{"message":{"id":"current-user","author":{"role":"user"},"create_time":2}},
		"current-image":{"message":{"author":{"role":"tool"},"create_time":3,"metadata":{"async_task_type":"image_gen","is_complete":true,"status":"finished_successfully"},"content":{"parts":[{"asset_pointer":"file-service://current"}]}}}
	}}`), accumulator)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"current"}) || !accumulator.Terminal {
		t.Fatalf("current fallback result = terminal %t, files %v", accumulator.Terminal, accumulator.FileIDs)
	}
}

func TestCaptureChatGPTWebImageConversationKeepsSameTimestampOutputAfterCurrentUser(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{Turn: ChatGPTWebImageTurn{MessageID: "z-user", CreatedAt: 2}}
	err := CaptureChatGPTWebImageConversation([]byte(`{"mapping":{
		"z-user":{"message":{"id":"z-user","author":{"role":"user"},"create_time":2}},
		"a-image":{"message":{
			"author":{"role":"tool"},"create_time":2,
			"metadata":{"async_task_type":"image_gen","is_complete":true,"finish_details":{"type":"finished_successfully"}},
			"content":{"parts":[{"asset_pointer":"file-service://current"}]}
		}}
	}}`), accumulator)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"current"}) {
		t.Fatalf("file IDs = %v", accumulator.FileIDs)
	}
}

func TestCaptureChatGPTWebImageConversationWaitsForMissingTargetMessage(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{Turn: ChatGPTWebImageTurn{MessageID: "pending-user", CreatedAt: 10}}
	err := CaptureChatGPTWebImageConversation([]byte(`{"current_node":"historical-image","mapping":{
		"historical-user":{"message":{"author":{"role":"user"},"create_time":1}},
		"historical-image":{"parent":"historical-user","message":{
			"author":{"role":"tool"},"create_time":2,
			"metadata":{"async_task_type":"image_gen","is_complete":true},
			"content":{"parts":[{"asset_pointer":"file-service://historical"}]}
		}}
	}}`), accumulator)
	if err != nil {
		t.Fatal(err)
	}
	if accumulator.Terminal || len(accumulator.References) != 0 {
		t.Fatalf("missing target captured terminal=%t references=%#v", accumulator.Terminal, accumulator.References)
	}
}

func TestCaptureChatGPTWebImageConversationStopsAtNextUser(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{Turn: ChatGPTWebImageTurn{MessageID: "current-user", CreatedAt: 1}}
	err := CaptureChatGPTWebImageConversation([]byte(`{"mapping":{
		"current-user":{"message":{"id":"current-user","author":{"role":"user"},"create_time":1}},
		"current-image":{"message":{"author":{"role":"tool"},"create_time":2,"metadata":{"async_task_type":"image_gen","is_complete":true},"content":{"parts":[{"asset_pointer":"file-service://current"}]}}},
		"next-user":{"message":{"id":"next-user","author":{"role":"user"},"create_time":3}},
		"next-image":{"message":{"author":{"role":"tool"},"create_time":4,"metadata":{"async_task_type":"image_gen","is_complete":true},"content":{"parts":[{"asset_pointer":"file-service://next"}]}}}
	}}`), accumulator)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"current"}) {
		t.Fatalf("file IDs = %v", accumulator.FileIDs)
	}
}

func TestCaptureChatGPTWebImageConversationExcludesOtherParentBranch(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{Turn: ChatGPTWebImageTurn{MessageID: "current-user", CreatedAt: 2}}
	err := CaptureChatGPTWebImageConversation([]byte(`{"mapping":{
		"old-user":{"message":{"id":"old-user","author":{"role":"user"},"create_time":1}},
		"current-user":{"parent":"old-user","message":{"id":"current-user","author":{"role":"user"},"create_time":2}},
		"current-image":{"parent":"current-user","message":{"author":{"role":"tool"},"create_time":3,"metadata":{"async_task_type":"image_gen","is_complete":true},"content":{"parts":[{"asset_pointer":"file-service://current"}]}}},
		"off-branch":{"parent":"old-user","message":{"author":{"role":"tool"},"create_time":4,"metadata":{"async_task_type":"image_gen","is_complete":true},"content":{"parts":[{"asset_pointer":"file-service://off-branch"}]}}}
	}}`), accumulator)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"current"}) {
		t.Fatalf("file IDs = %v", accumulator.FileIDs)
	}
}

func TestCaptureChatGPTWebImageConversationExcludesLaterUntimestampedTurn(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{Turn: ChatGPTWebImageTurn{MessageID: "current-user", CreatedAt: 2}}
	err := CaptureChatGPTWebImageConversation([]byte(`{"mapping":{
		"old-user":{"message":{"id":"old-user","author":{"role":"user"},"create_time":1}},
		"current-user":{"parent":"old-user","message":{"id":"current-user","author":{"role":"user"},"create_time":2}},
		"current-image":{"parent":"current-user","message":{"author":{"role":"tool"},"metadata":{"async_task_type":"image_gen","is_complete":true},"content":{"parts":[{"asset_pointer":"file-service://current"}]}}},
		"next-user":{"parent":"current-image","message":{"id":"next-user","author":{"role":"user"},"create_time":3}},
		"a-next-image":{"parent":"next-user","message":{"author":{"role":"tool"},"metadata":{"async_task_type":"image_gen","is_complete":true},"content":{"parts":[{"asset_pointer":"file-service://next"}]}}}
	}}`), accumulator)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"current"}) {
		t.Fatalf("file IDs = %v", accumulator.FileIDs)
	}
}

func TestCaptureChatGPTWebImageConversationAcceptsTerminalCodeReply(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{}
	err := CaptureChatGPTWebImageConversation([]byte(`{
		"current_node":"rejected",
		"mapping":{"rejected":{"message":{
			"author":{"role":"assistant"},"end_turn":true,
			"content":{"content_type":"code","text":"image_generation_denied"}
		}}}
	}`), accumulator)
	if err != nil {
		t.Fatalf("CaptureChatGPTWebImageConversation() error = %v", err)
	}
	if !accumulator.Terminal || accumulator.FailureStatus != "image_generation_denied" {
		t.Fatalf("terminal = %t, failure = %q", accumulator.Terminal, accumulator.FailureStatus)
	}
}

func TestCaptureChatGPTWebImageConversationCapturesCurrentEmptyStructuredFailure(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{}
	err := CaptureChatGPTWebImageConversation([]byte(`{
		"current_node":"current-failure",
		"mapping":{
			"historical-failure":{"message":{
				"author":{"role":"assistant"},"create_time":1,"is_error":true,
				"error":{"message":"historical failure"},
				"content":{"content_type":"text","parts":[]}
			}},
			"current-user":{"message":{"author":{"role":"user"},"create_time":2}},
			"current-failure":{"parent":"current-user","message":{
				"author":{"role":"assistant"},"create_time":3,"is_error":true,
				"error":{"message":"current image request failed"},
				"content":{"content_type":"text","parts":[]}
			}}
		}
	}`), accumulator)
	if err != nil {
		t.Fatalf("CaptureChatGPTWebImageConversation() error = %v", err)
	}
	if !accumulator.Terminal || accumulator.FailureStatus != "current image request failed" {
		t.Fatalf("terminal = %t, failure = %q", accumulator.Terminal, accumulator.FailureStatus)
	}
}

func TestCaptureChatGPTWebImageConversationCapturesStatusOnlyFailure(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{Turn: ChatGPTWebImageTurn{MessageID: "current-user"}}
	err := CaptureChatGPTWebImageConversation([]byte(`{
		"mapping":{
			"current-user":{"message":{"id":"current-user","author":{"role":"user"}}},
			"current-failure":{"parent":"current-user","message":{
				"author":{"role":"assistant"},
				"metadata":{"finish_details":{"type":"content_filter"}},
				"content":{"content_type":"text","parts":[]}
			}}
		}
	}`), accumulator)
	if err != nil {
		t.Fatalf("CaptureChatGPTWebImageConversation() error = %v", err)
	}
	if !accumulator.Terminal || accumulator.FailureStatus != "content_filter" {
		t.Fatalf("terminal = %t, failure = %q", accumulator.Terminal, accumulator.FailureStatus)
	}
}

func TestCaptureChatGPTWebImageConversationCapturesNestedStringReference(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{Turn: ChatGPTWebImageTurn{MessageID: "current-user"}}
	err := CaptureChatGPTWebImageConversation([]byte(`{
		"mapping":{
			"current-user":{"message":{"id":"current-user","author":{"role":"user"}}},
			"current-image":{"parent":"current-user","message":{
				"author":{"role":"tool"},
				"metadata":{
					"async_task_type":"image_gen",
					"is_complete":true,
					"status":"finished_successfully",
					"nested":{"asset":"file-service://nested-image"}
				},
				"content":{"parts":[]}
			}}
		}
	}`), accumulator)
	if err != nil {
		t.Fatalf("CaptureChatGPTWebImageConversation() error = %v", err)
	}
	if !accumulator.Terminal || !reflect.DeepEqual(accumulator.FileIDs, []string{"nested-image"}) {
		t.Fatalf("terminal = %t, file IDs = %v", accumulator.Terminal, accumulator.FileIDs)
	}
}

func TestCaptureChatGPTWebImageConversationCapturesCurrentRootFailure(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{Turn: ChatGPTWebImageTurn{MessageID: "current-user"}}
	err := CaptureChatGPTWebImageConversation([]byte(`{
		"error":{"message":"current root failure"},
		"mapping":{"current-user":{"message":{"id":"current-user","author":{"role":"user"}}}}
	}`), accumulator)
	if err != nil {
		t.Fatal(err)
	}
	if !accumulator.Terminal || accumulator.FailureStatus != "current root failure" {
		t.Fatalf("root failure = terminal %t, failure %q", accumulator.Terminal, accumulator.FailureStatus)
	}
}

func TestCaptureChatGPTWebImageConversationRootFailureOverridesPendingImage(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{Turn: ChatGPTWebImageTurn{MessageID: "current-user"}}
	err := CaptureChatGPTWebImageConversation([]byte(`{
		"error":{"message":"current root failure"},
		"mapping":{
			"current-user":{"message":{"id":"current-user","author":{"role":"user"},"create_time":1}},
			"current-image":{"parent":"current-user","message":{
				"author":{"role":"tool"},"create_time":2,
				"metadata":{"async_task_type":"image_gen","status":"running"},
				"content":{"parts":[{"asset_pointer":"file-service://preview"}]}
			}}
		}
	}`), accumulator)
	if err != nil {
		t.Fatal(err)
	}
	if !accumulator.Terminal || accumulator.FailureStatus != "current root failure" {
		t.Fatalf("root failure = terminal %t, failure %q", accumulator.Terminal, accumulator.FailureStatus)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"preview"}) {
		t.Fatalf("file IDs = %v", accumulator.FileIDs)
	}
}

func TestCaptureChatGPTWebImageConversationWaitsForAllCurrentImages(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{Turn: ChatGPTWebImageTurn{MessageID: "current-user"}}
	err := CaptureChatGPTWebImageConversation([]byte(`{"mapping":{
		"current-user":{"message":{"id":"current-user","author":{"role":"user"},"create_time":1}},
		"completed":{"parent":"current-user","message":{"author":{"role":"tool"},"create_time":2,"metadata":{"async_task_type":"image_gen","is_complete":true,"status":"finished_successfully"},"content":{"parts":[{"asset_pointer":"file-service://completed"}]}}},
		"running":{"parent":"current-user","message":{"author":{"role":"tool"},"create_time":3,"metadata":{"async_task_type":"image_gen","status":"running"},"content":{"parts":[{"asset_pointer":"file-service://preview"}]}}}
	}}`), accumulator)
	if err != nil {
		t.Fatal(err)
	}
	if accumulator.Terminal {
		t.Fatal("one completed image marked a multi-image turn terminal")
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"completed", "preview"}) {
		t.Fatalf("file IDs = %v", accumulator.FileIDs)
	}
}

func TestCaptureChatGPTWebImageConversationDoesNotLetTerminalTextOverridePendingImage(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{Turn: ChatGPTWebImageTurn{MessageID: "current-user"}}
	err := CaptureChatGPTWebImageConversation([]byte(`{"mapping":{
		"current-user":{"message":{"id":"current-user","author":{"role":"user"},"create_time":1}},
		"running":{"parent":"current-user","message":{"author":{"role":"tool"},"create_time":2,"metadata":{"async_task_type":"image_gen","status":"running"},"content":{"parts":[{"asset_pointer":"file-service://preview"}]}}},
		"assistant":{"parent":"running","message":{"author":{"role":"assistant"},"create_time":3,"end_turn":true,"content":{"content_type":"text","parts":["Still generating"]}}}
	}}`), accumulator)
	if err != nil {
		t.Fatal(err)
	}
	if accumulator.Terminal {
		t.Fatal("terminal assistant text overrode a pending image task")
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"preview"}) {
		t.Fatalf("file IDs = %v", accumulator.FileIDs)
	}
}

func TestCaptureChatGPTWebImageConversationWaitsForPendingImageAfterFailure(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{Turn: ChatGPTWebImageTurn{MessageID: "current-user"}}
	err := CaptureChatGPTWebImageConversation([]byte(`{"mapping":{
		"current-user":{"message":{"id":"current-user","author":{"role":"user"},"create_time":1}},
		"failed":{"parent":"current-user","message":{
			"author":{"role":"tool"},"create_time":2,
			"metadata":{"async_task_type":"image_gen","status":"finished_with_error","is_complete":true},
			"content":{"parts":[]}
		}},
		"running":{"parent":"current-user","message":{
			"author":{"role":"tool"},"create_time":3,
			"metadata":{"async_task_type":"image_gen","status":"running"},
			"content":{"parts":[]}
		}}
	}}`), accumulator)
	if err != nil {
		t.Fatal(err)
	}
	if accumulator.Terminal || accumulator.FailureStatus != "finished_with_error" {
		t.Fatalf("terminal = %t, failure = %q", accumulator.Terminal, accumulator.FailureStatus)
	}
}

func TestCaptureChatGPTWebImageConversationDoesNotClearEarlierFailure(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{}
	err := CaptureChatGPTWebImageConversation([]byte(`{
		"current_node":"current-empty",
		"mapping":{
			"current-user":{"message":{"author":{"role":"user"}}},
			"current-failure":{"parent":"current-user","message":{
				"author":{"role":"assistant"},"is_error":true,
				"error":{"message":"current image request failed"},
				"content":{"content_type":"text","parts":[]}
			}},
			"current-empty":{"parent":"current-failure","message":{
				"author":{"role":"assistant"},"end_turn":true,
				"content":{"content_type":"text","parts":[]}
			}}
		}
	}`), accumulator)
	if err != nil {
		t.Fatal(err)
	}
	if !accumulator.Terminal || accumulator.FailureStatus != "current image request failed" {
		t.Fatalf("terminal = %t, failure = %q", accumulator.Terminal, accumulator.FailureStatus)
	}
}

func TestCaptureChatGPTWebImageConversationCapturesEmptyAssistantTerminal(t *testing.T) {
	for _, terminalFields := range []string{`"end_turn":true`, `"status":"completed"`} {
		accumulator := &ChatGPTWebImageAccumulator{}
		payload := []byte(`{"current_node":"done","mapping":{"done":{"message":{"author":{"role":"assistant"},` + terminalFields + `,"content":{"content_type":"text","parts":[]}}}}}`)
		if err := CaptureChatGPTWebImageConversation(payload, accumulator); err != nil {
			t.Fatalf("CaptureChatGPTWebImageConversation() error = %v", err)
		}
		if !accumulator.Terminal || accumulator.FailureStatus != "" || len(accumulator.References) != 0 {
			t.Fatalf("terminal state = (%t, %q), references = %#v", accumulator.Terminal, accumulator.FailureStatus, accumulator.References)
		}
	}
}

func TestChatGPTWebImageMessageFailureUsesTruthyErrorValues(t *testing.T) {
	for _, field := range []string{"message", "metadata"} {
		for _, value := range []any{false, "", map[string]any{}, []any{}} {
			message := map[string]any{}
			if field == "metadata" {
				message["metadata"] = map[string]any{"error": value, "is_error": false, "blocked": false}
			} else {
				message["error"] = value
				message["is_error"] = false
				message["blocked"] = false
			}
			failed, detail := chatGPTWebImageMessageFailure(message)
			if failed || detail != "" {
				t.Fatalf("%s error value %#v classified as failure %q", field, value, detail)
			}
		}
	}
	failed, detail := chatGPTWebImageMessageFailure(map[string]any{"error": map[string]any{
		"message": "image generation blocked",
		"detail":  "fallback detail",
		"reason":  "policy",
		"code":    "image_blocked",
	}})
	if !failed || detail != "image generation blocked" {
		t.Fatalf("non-empty error classified as (%t, %q)", failed, detail)
	}
	failed, detail = chatGPTWebImageMessageFailure(map[string]any{"metadata": map[string]any{
		"error": []any{map[string]any{"detail": "structured detail", "code": "image_failed"}},
	}})
	if !failed || detail != "structured detail" {
		t.Fatalf("array error classified as (%t, %q)", failed, detail)
	}
}

func TestChatGPTWebImageFailurePrefersStructuredDetailOverGenericContent(t *testing.T) {
	message := map[string]any{
		"author":   map[string]any{"role": "assistant"},
		"is_error": true,
		"error":    map[string]any{"message": "image request rejected by policy"},
		"content":  map[string]any{"content_type": "text", "parts": []any{"failed"}},
	}
	failed, detail := chatGPTWebImageMessageFailure(message)
	if !failed || detail != "image request rejected by policy" {
		t.Fatalf("message failure = (%t, %q)", failed, detail)
	}

	statusMessage := map[string]any{
		"author":   map[string]any{"role": "assistant"},
		"metadata": map[string]any{"status": "content_filter"},
		"content":  map[string]any{"content_type": "text", "parts": []any{"failed"}},
	}
	failed, detail = chatGPTWebImageStreamMessageFailure(statusMessage)
	if !failed || detail != "content_filter" {
		t.Fatalf("stream failure = (%t, %q)", failed, detail)
	}
}

func TestChatGPTWebConversationTerminalErrorIsDeterministic(t *testing.T) {
	event := map[string]any{
		"first":  map[string]any{"status": "failed"},
		"second": map[string]any{"finish_details": map[string]any{"type": "content_filter"}},
	}
	for index := 0; index < 100; index++ {
		if status := chatGPTWebConversationTerminalError(event); status != "content_filter" {
			t.Fatalf("terminal status = %q", status)
		}
	}
}

func TestCaptureChatGPTWebImageConversationKeepsImagesBeforeTerminalAssistantText(t *testing.T) {
	accumulator := &ChatGPTWebImageAccumulator{}
	err := CaptureChatGPTWebImageConversation([]byte(`{
		"current_node":"complete",
		"mapping":{
			"image":{"message":{
				"author":{"role":"tool"},"create_time":1,
				"metadata":{"async_task_type":"image_gen"},
				"content":{"parts":[{"asset_pointer":"file-service://generated"}]}
			}},
			"complete":{"parent":"image","message":{
				"author":{"role":"assistant"},"create_time":2,"end_turn":true,
				"content":{"content_type":"text","parts":["The image has been generated."]}
			}}
		}
	}`), accumulator)
	if err != nil {
		t.Fatalf("CaptureChatGPTWebImageConversation() error = %v", err)
	}
	if !accumulator.Terminal || accumulator.FailureStatus != "" {
		t.Fatalf("terminal = %t, failure = %q", accumulator.Terminal, accumulator.FailureStatus)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) {
		t.Fatalf("file IDs = %v", accumulator.FileIDs)
	}
}
