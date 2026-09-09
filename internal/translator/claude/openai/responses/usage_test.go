package responses

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestClaudeResponsesUsageKeepsPartialCacheAndReasoning(t *testing.T) {
	for _, limit := range []bool{false, true} {
		for _, correction := range []bool{false, true} {
			for _, reported := range []bool{false, true} {
				t.Run(fmt.Sprintf("limit=%t/corrected=%t/reported=%t", limit, correction, reported), func(t *testing.T) {
					events := []string{`{"type":"message_start","message":{"id":"usage","usage":{"input_tokens":2,"cache_read_input_tokens":4,"cache_creation_input_tokens":5,"output_tokens":0}}}`,
						`{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"eightchr"}}`,
						`{"type":"message_delta","usage":{"output_tokens":3}}`}
					if correction {
						events = append(events, `{"type":"message_delta","usage":{"input_tokens":1,"cache_read_input_tokens":0}}`)
					}
					if reported {
						events = append(events, `{"type":"message_delta","usage":{"output_tokens_details":{"thinking_tokens":0}}}`)
					}
					reason := "end_turn"
					if limit {
						reason = "max_tokens"
					}
					events = append(events, `{"type":"message_delta","delta":{"stop_reason":"`+reason+`"}}`, `{"type":"message_stop"}`)
					var state any
					var stream gjson.Result
					for _, source := range events {
						for _, event := range claudeResponsesTerminalEvents(t, &state, nil, source) {
							if event.Get("type").String() == "response.completed" || event.Get("type").String() == "response.incomplete" {
								stream = event.Get("response.usage")
							}
						}
					}
					nonstream := gjson.ParseBytes(ConvertClaudeResponseToOpenAIResponsesNonStream(t.Context(), "", nil, nil, []byte("data: "+strings.Join(events, "\ndata: ")+"\n"), nil)).Get("usage")
					input, cached, reasoning := int64(11), int64(4), int64(2)
					if correction {
						input, cached = 6, 0
					}
					if reported {
						reasoning = 0
					}
					if !reflect.DeepEqual(stream.Value(), nonstream.Value()) || stream.Get("input_tokens").Int() != input || stream.Get("output_tokens").Int() != 3 || stream.Get("total_tokens").Int() != input+3 ||
						stream.Get("input_tokens_details.cached_tokens").Int() != cached || stream.Get("input_tokens_details.cache_write_tokens").Int() != 5 ||
						!stream.Get("output_tokens_details.reasoning_tokens").Exists() || stream.Get("output_tokens_details.reasoning_tokens").Int() != reasoning {
						t.Fatal("public usage lost partial counts, reset explicit zero, or diverged by transport")
					}
					claudeResponsesTerminalEvents(t, &state, nil, `{"type":"message_start","message":{"id":"new","usage":{"input_tokens":0,"output_tokens":0}}}`)
					for _, event := range claudeResponsesTerminalEvents(t, &state, nil, `{"type":"message_stop"}`) {
						if event.Get("response.usage.total_tokens").Int() != 0 {
							t.Fatal("new message retained old usage")
						}
					}
				})
			}
		}
	}
}
