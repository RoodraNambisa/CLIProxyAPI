package responses

import (
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestResponsesUnindexedToolDeltasRequireUniqueIdentity(t *testing.T) {
	tests := []struct {
		name   string
		frames []string
		want   map[string]string
	}{
		{"single sparse index", []string{
			`[{"index":7,"id":"a","function":{"name":"run","arguments":"{\"v\":"}}]`,
			`[{"function":{"arguments":"1}"}}]`,
		}, map[string]string{"a": `{"v":1}`}},
		{"ID overrides array position", []string{
			`[{"index":7,"id":"a","function":{"name":"run","arguments":"{\"v\":"}},{"index":3,"id":"b","function":{"name":"run","arguments":"{\"v\":"}}]`,
			`[{"id":"b","function":{"arguments":"2}"}},{"id":"a","function":{"arguments":"1}"}}]`,
		}, map[string]string{"a": `{"v":1}`, "b": `{"v":2}`}},
		{"ambiguous fragment ignored", []string{
			`[{"index":0,"id":"a","function":{"name":"run","arguments":"{\"v\":"}},{"index":1,"id":"b","function":{"name":"run","arguments":"{\"v\":"}}]`,
			`[{"function":{"arguments":"BAD"}}]`,
			`[{"index":0,"function":{"arguments":"1}"}},{"index":1,"function":{"arguments":"2}"}}]`,
		}, map[string]string{"a": `{"v":1}`, "b": `{"v":2}`}},
		{"ambiguous first frame ignored", []string{
			`[{"function":{"name":"run","arguments":"BAD"}},{"function":{"name":"run","arguments":"BAD"}}]`,
			`[{"index":0,"id":"a","function":{"name":"run","arguments":"{\"v\":1}"}}]`,
		}, map[string]string{"a": `{"v":1}`}},
		{"duplicate ID with conflicting index ignored", []string{
			`[{"index":0,"id":"a","function":{"name":"run","arguments":"{\"v\":1}"}}]`,
			`[{"index":1,"id":"a","function":{"name":"run","arguments":"BAD"}}]`,
		}, map[string]string{"a": `{"v":1}`}},
		{"new ID after identified call", []string{
			`[{"id":"a","function":{"name":"run","arguments":"{\"v\":1}"}}]`,
			`[{"id":"b","function":{"name":"run","arguments":"{\"v\":2}"}}]`,
		}, map[string]string{"a": `{"v":1}`, "b": `{"v":2}`}},
		{"late identity for single call", []string{
			`[{"index":7,"function":{"name":"run","arguments":"{\"v\":"}}]`,
			`[{"id":"a","function":{"arguments":"1}"}}]`,
		}, map[string]string{"a": `{"v":1}`}},
		{"ambiguous late identity ignored", []string{
			`[{"index":4,"function":{"name":"run","arguments":"{\"v\":"}},{"index":8,"function":{"name":"run","arguments":"{\"v\":"}}]`,
			`[{"id":"unknown","function":{"arguments":"BAD"}}]`,
			`[{"index":4,"id":"a","function":{"arguments":"1}"}},{"index":8,"id":"b","function":{"arguments":"2}"}}]`,
		}, map[string]string{"a": `{"v":1}`, "b": `{"v":2}`}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var state any
			var completed gjson.Result
			for i := 0; i <= len(tt.frames); i++ {
				source := "data: [DONE]"
				if i < len(tt.frames) {
					finish := "null"
					if i == len(tt.frames)-1 {
						finish = `"tool_calls"`
					}
					source = `data: {"id":"identity","choices":[{"index":0,"delta":{"tool_calls":` + tt.frames[i] + `},"finish_reason":` + finish + `}]}`
				}
				for _, chunk := range ConvertOpenAIChatCompletionsResponseToOpenAIResponses(t.Context(), "", nil, nil, []byte(source), &state) {
					if strings.Contains(string(chunk), "BAD") {
						t.Fatal("ambiguous arguments reached a tool event")
					}
					kind, event := parseOpenAIResponsesSSEEvent(t, chunk)
					if kind == "response.completed" {
						completed = event.Get("response")
					}
				}
			}
			items := completed.Get("output").Array()
			if len(items) != len(tt.want) {
				t.Fatal("unindexed tool identity lost or invented an output item")
			}
			for _, item := range items {
				if item.Get("arguments").String() != tt.want[item.Get("call_id").String()] {
					t.Fatal("arguments were associated with the wrong call")
				}
			}
		})
	}
}
