package responses

import (
	"fmt"
	"testing"

	"github.com/tidwall/gjson"
)

func TestResponsesReasoningFallbackAndEmptyToolArrays(t *testing.T) {
	for _, field := range []string{"reasoning_content", "reasoning"} {
		var state any
		var result gjson.Result
		added, done := 0, 0
		for _, delta := range []string{
			`{"reasoning_content":"first","tool_calls":[]}`,
			fmt.Sprintf(`{%q:" second","tool_calls":[]}`, field),
			`{"content":"hello","tool_calls":[]}`,
			`{"content":" world","tool_calls":[]}`,
			`{}`,
		} {
			finish := "null"
			if delta == `{}` {
				finish = `"stop"`
			}
			source := []byte(`data: {"id":"fallback","object":"chat.completion.chunk","choices":[{"index":0,"delta":` + delta + `,"finish_reason":` + finish + `}]}`)
			for _, chunk := range ConvertOpenAIChatCompletionsResponseToOpenAIResponses(t.Context(), "model", nil, nil, source, &state) {
				kind, _ := parseOpenAIResponsesSSEEvent(t, chunk)
				if kind == "response.output_item.added" {
					added++
				}
				if kind == "response.output_item.done" {
					done++
					if delta != `{}` && delta != `{"content":"hello","tool_calls":[]}` {
						t.Fatal("empty tool array closed a content item")
					}
				}
			}
		}
		for _, chunk := range ConvertOpenAIChatCompletionsResponseToOpenAIResponses(t.Context(), "model", nil, nil, []byte("data: [DONE]"), &state) {
			kind, event := parseOpenAIResponsesSSEEvent(t, chunk)
			if kind == "response.completed" {
				result = event.Get("response")
			}
		}
		if added != 2 || done != 2 || result.Get("output.#").Int() != 2 || result.Get("output.0.summary.0.text").String() != "first second" || result.Get("output.1.content.0.text").String() != "hello world" {
			t.Fatal("reasoning/text was split, lost or completed before real tool use")
		}
	}
}

func TestResponsesNonStreamReasoningFallbackPrecedence(t *testing.T) {
	for _, tc := range []struct{ fields, want string }{
		{`"reasoning_content":"primary","reasoning":"fallback"`, "primary"},
		{`"reasoning_content":"","reasoning":"fallback"`, "fallback"},
		{`"reasoning_content":null,"reasoning":"fallback"`, "fallback"},
		{`"reasoning":"fallback"`, "fallback"},
		{`"reasoning_content":" ","reasoning":"fallback"`, " "},
		{`"reasoning_content":{},"reasoning":"fallback"`, "fallback"},
		{`"reasoning_content":3,"reasoning":true`, ""},
		{`"reasoning":[]`, ""},
	} {
		raw := []byte(`{"id":"fallback","choices":[{"message":{` + tc.fields + `,"content":"answer","tool_calls":[]}}]}`)
		result := gjson.ParseBytes(ConvertOpenAIChatCompletionsResponseToOpenAIResponsesNonStream(t.Context(), "", nil, nil, raw, nil))
		textIndex := 1
		if tc.want == "" {
			textIndex = 0
		} else if result.Get("output.0.summary.0.text").String() != tc.want {
			t.Fatal("reasoning fallback precedence changed")
		}
		if result.Get(fmt.Sprintf("output.%d.content.0.text", textIndex)).String() != "answer" || result.Get("output.#").Int() != int64(textIndex+1) {
			t.Fatal("fallback changed ordinary output")
		}
	}
}
