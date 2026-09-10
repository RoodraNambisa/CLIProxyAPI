package responses

import "testing"

func TestGeminiResponsesFinishesDoneExactlyOnce(t *testing.T) {
	for _, stopFirst := range []bool{false, true} {
		var state any
		request := []byte(`{"tools":[{"type":"custom","name":"exec"}]}`)
		if chunks := ConvertGeminiResponseToOpenAIResponses(t.Context(), "model", request, nil, []byte("data: [DONE]"), &state); len(chunks) != 0 {
			t.Fatal("empty stream invented a response")
		}
		first := []byte(`{"responseId":"terminal","candidates":[{"content":{"parts":[{"text":"thought","thought":true},{"text":"answer"},{"functionCall":{"name":"exec","args":{"input":"pwd"}}}]}}]}`)
		events := ConvertGeminiResponseToOpenAIResponses(t.Context(), "model", request, nil, first, &state)
		if stopFirst {
			events = append(events, ConvertGeminiResponseToOpenAIResponses(t.Context(), "model", request, nil, []byte(`{"candidates":[{"finishReason":"STOP"}]}`), &state)...)
		}
		events = append(events, ConvertGeminiResponseToOpenAIResponses(t.Context(), "model", request, nil, []byte("data: [DONE]"), &state)...)
		completed := 0
		for _, chunk := range events {
			kind, event := parseSSEEvent(t, chunk)
			if kind == "response.completed" {
				completed++
				if event.Get("response.output.#").Int() != 3 || event.Get("response.output.2.type").String() != "custom_tool_call" || event.Get("response.output.2.input").String() != "pwd" {
					t.Fatal("terminal output lost accumulated text, reasoning, or custom tool")
				}
			}
		}
		if completed != 1 {
			t.Fatalf("stopFirst=%v: completions=%d", stopFirst, completed)
		}
		for _, tail := range []string{"[DONE]", `{"candidates":[{"finishReason":"STOP"}]}`, string(first)} {
			if chunks := ConvertGeminiResponseToOpenAIResponses(t.Context(), "model", request, nil, []byte(tail), &state); len(chunks) != 0 {
				t.Fatal("late frame appended output after terminal")
			}
		}
	}
}
