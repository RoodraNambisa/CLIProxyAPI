package responses

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestResponsesChoicesFinishIndependently(t *testing.T) {
	events := []string{
		`{"index":0,"delta":{"reasoning_content":"first"}}`,
		`{"index":1,"delta":{"reasoning_content":"second"}}`,
		`{"index":0,"delta":{"content":"answer"}}`,
		`{"index":1,"delta":{"tool_calls":[{"index":0,"id":"b","function":{"name":"second","arguments":"{\"b\":"}}]}}`,
		`{"index":0,"delta":{"tool_calls":[{"index":0,"id":"a","function":{"name":"first","arguments":""}}]},"finish_reason":"length"}`,
		`{"index":1,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"2}"}}]},"finish_reason":"stop"}`,
		`{"index":0,"delta":{"content":"late"},"finish_reason":"stop"}`,
	}
	var state any
	done := make(map[string]gjson.Result)
	var result gjson.Result
	for i, choice := range events {
		raw := []byte(`data: {"id":"choices","object":"chat.completion.chunk","choices":[` + choice + `]}`)
		for _, chunk := range ConvertOpenAIChatCompletionsResponseToOpenAIResponses(t.Context(), "", nil, nil, raw, &state) {
			kind, event := parseOpenAIResponsesSSEEvent(t, chunk)
			if kind == "response.output_item.done" {
				id := event.Get("item.id").String()
				if i == 4 && id == "fc_b" {
					t.Fatal("one choice completed another choice's unfinished tool")
				}
				if _, duplicate := done[id]; duplicate {
					t.Fatal("output item completed twice")
				}
				done[id] = event.Get("item")
			}
		}
	}
	ConvertOpenAIChatCompletionsResponseToOpenAIResponses(t.Context(), "", nil, nil, []byte(`data: {"choices":[],"usage":{"prompt_tokens":2,"completion_tokens":3,"total_tokens":5}}`), &state)
	for _, chunk := range ConvertOpenAIChatCompletionsResponseToOpenAIResponses(t.Context(), "", nil, nil, []byte("data: [DONE]"), &state) {
		kind, event := parseOpenAIResponsesSSEEvent(t, chunk)
		if kind == "response.incomplete" {
			result = event.Get("response")
		}
	}
	if result.Get("status").String() != "incomplete" || result.Get("incomplete_details.reason").String() != "max_output_tokens" || result.Get("usage.total_tokens").Int() != 5 {
		t.Fatal("later choice or usage overwrote the incomplete reason")
	}
	if done["fc_a"].Get("status").String() != "incomplete" || done["fc_a"].Get("arguments").String() != "" || done["fc_b"].Get("status").String() != "completed" || done["fc_b"].Get("arguments").String() != `{"b":2}` {
		t.Fatal("per-choice tool status or arguments were lost")
	}
	if done["rs_choices_0"].Get("summary.0.text").String() != "first" || done["rs_choices_1"].Get("summary.0.text").String() != "second" {
		t.Fatal("reasoning content crossed choice boundaries or was lost at completion")
	}
	if strings.Contains(result.Raw, "late") {
		t.Fatal("finished choice accepted late content")
	}
	for _, item := range result.Get("output").Array() {
		if !reflect.DeepEqual(item.Value(), done[item.Get("id").String()].Value()) {
			t.Fatal("final output differs from its completed item event")
		}
	}
	nonstream := gjson.ParseBytes(ConvertOpenAIChatCompletionsResponseToOpenAIResponsesNonStream(t.Context(), "", nil, nil, []byte(`{"choices":[{"message":{"reasoning":"first"},"finish_reason":"stop"},{"message":{"reasoning":"second"},"finish_reason":"length"}]}`), nil))
	if nonstream.Get("output.#").Int() != 2 || nonstream.Get("output.0.summary.0.text").String() != "first" || nonstream.Get("output.1.summary.0.text").String() != "second" || nonstream.Get("output.1.status").String() != "incomplete" {
		t.Fatal("non-stream translation lost a secondary choice's reasoning")
	}
}

func TestResponsesIncompleteReasonUsesChoiceIndexAcrossTransports(t *testing.T) {
	for _, choices := range []string{
		`[{"index":2,"message":{},"delta":{},"finish_reason":"length"},{"index":0,"message":{},"delta":{},"finish_reason":"content_filter"}]`,
		`[{"index":0,"message":{},"delta":{},"finish_reason":"content_filter"},{"index":2,"message":{},"delta":{},"finish_reason":"length"}]`,
	} {
		raw := []byte(`{"id":"multiple","choices":` + choices + `}`)
		nonstream := gjson.ParseBytes(ConvertOpenAIChatCompletionsResponseToOpenAIResponsesNonStream(t.Context(), "", nil, nil, raw, nil))
		var state any
		ConvertOpenAIChatCompletionsResponseToOpenAIResponses(t.Context(), "", nil, nil, raw, &state)
		var stream gjson.Result
		for _, chunk := range ConvertOpenAIChatCompletionsResponseToOpenAIResponses(t.Context(), "", nil, nil, []byte("[DONE]"), &state) {
			_, event := parseOpenAIResponsesSSEEvent(t, chunk)
			stream = event.Get("response")
		}
		if nonstream.Get("incomplete_details.reason").String() != "content_filter" || stream.Get("incomplete_details.reason").String() != "content_filter" {
			t.Fatal("transport or arrival order changed the selected incomplete reason")
		}
	}
}

func TestResponsesChoiceTerminalEventsMatchItems(t *testing.T) {
	for _, reason := range []string{"stop", "length", "content_filter"} {
		for _, kind := range []string{"text", "reasoning", "function", "custom"} {
			t.Run(reason+"/"+kind, func(t *testing.T) {
				delta := `{"content":"partial"}`
				if kind == "reasoning" {
					delta = `{"reasoning":"partial"}`
				}
				if kind == "function" || kind == "custom" {
					delta = `{"tool_calls":[{"index":0,"id":"paired","function":{"name":"tool","arguments":""}}]}`
				}
				request := []byte(`{"tools":[{"type":"` + kind + `","name":"tool"}]}`)
				var state any
				var item, result gjson.Result
				for _, source := range []string{fmt.Sprintf(`data: {"id":"terminal","choices":[{"index":0,"delta":%s,"finish_reason":%q}]}`, delta, reason), "data: [DONE]", "data: [DONE]"} {
					for _, chunk := range ConvertOpenAIChatCompletionsResponseToOpenAIResponses(t.Context(), "", request, nil, []byte(source), &state) {
						eventType, event := parseOpenAIResponsesSSEEvent(t, chunk)
						if eventType == "response.output_item.done" {
							if item.Exists() {
								t.Fatal("duplicate completed item")
							}
							item = event.Get("item")
						}
						if eventType == "response.completed" || eventType == "response.incomplete" {
							if result.Exists() {
								t.Fatal("duplicate response terminal")
							}
							result = event.Get("response")
							if eventType != "response."+result.Get("status").String() {
								t.Fatal("terminal event name disagrees with response")
							}
						}
					}
				}
				want := "incomplete"
				if reason == "stop" {
					want = "completed"
				}
				if item.Get("status").String() != want || result.Get("status").String() != want || result.Get("output.0.status").String() != want {
					t.Fatal("item completion and response terminal disagree")
				}
				if kind == "reasoning" && item.Get("summary.0.text").String() != "partial" {
					t.Fatal("reasoning done event lost its summary")
				}
			})
		}
	}
}
