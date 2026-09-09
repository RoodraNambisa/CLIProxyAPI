package responses

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/tidwall/gjson"
)

func TestResponsesTextAfterToolUsesNewMessageItem(t *testing.T) {
	for _, custom := range []bool{false, true} {
		for _, finish := range []string{"stop", "length"} {
			t.Run(fmt.Sprintf("custom=%t/%s", custom, finish), func(t *testing.T) {
				request := []byte(`{"tools":[{"type":"function","name":"run"}]}`)
				if custom {
					request = []byte(`{"tools":[{"type":"custom","name":"run"}]}`)
				}
				choices := []string{
					`{"index":0,"delta":{"content":"before"}}`,
					`{"index":0,"delta":{"tool_calls":[{"index":0,"id":"a","function":{"name":"run","arguments":"{}"}}]}}`,
					`{"index":0,"delta":{"content":"middle"}}`,
					`{"index":1,"delta":{"content":"other"},"finish_reason":"stop"}`,
					`{"index":0,"delta":{"tool_calls":[{"index":1,"id":"b","function":{"name":"run","arguments":"{}"}}]}}`,
					`{"index":0,"delta":{"content":"after"},"finish_reason":"` + finish + `"}`,
				}
				var state any
				added, done := map[string]bool{}, map[string]gjson.Result{}
				var final gjson.Result
				for i := 0; i <= len(choices); i++ {
					source := "data: [DONE]"
					if i < len(choices) {
						source = `data: {"id":"segments","choices":[` + choices[i] + `]}`
					}
					for _, chunk := range ConvertOpenAIChatCompletionsResponseToOpenAIResponses(t.Context(), "", request, nil, []byte(source), &state) {
						kind, event := parseOpenAIResponsesSSEEvent(t, chunk)
						id := event.Get("item.id").String()
						switch kind {
						case "response.output_item.added":
							if added[id] {
								t.Fatal("message segment reused an announced ID")
							}
							added[id] = true
						case "response.output_text.delta":
							id = event.Get("item_id").String()
							if !added[id] || done[id].Exists() {
								t.Fatal("text delta targeted an unannounced or completed message")
							}
						case "response.output_item.done":
							if done[id].Exists() {
								t.Fatal("message segment completed twice")
							}
							done[id] = event.Get("item")
						case "response.completed", "response.incomplete":
							final = event.Get("response")
						}
					}
				}
				items := final.Get("output").Array()
				if len(items) != 6 || items[0].Get("id").String() != "msg_segments_0" || items[0].Get("content.0.text").String() != "before" || items[2].Get("content.0.text").String() != "middle" || items[3].Get("content.0.text").String() != "other" || items[5].Get("content.0.text").String() != "after" {
					t.Fatal("message segment order or independent content was lost")
				}
				for _, item := range items {
					if !reflect.DeepEqual(item.Value(), done[item.Get("id").String()].Value()) {
						t.Fatal("final output changed an already completed item")
					}
				}
				if items[0].Get("status").String() != "completed" || items[2].Get("status").String() != "completed" || items[5].Get("status").String() != responsesItemStatus(finish) {
					t.Fatal("final stop reason changed earlier message segments")
				}
			})
		}
	}
}
