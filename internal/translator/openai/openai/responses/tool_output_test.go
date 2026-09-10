package responses

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	"github.com/tidwall/gjson"
)

func TestResponsesToolOutputsRetainMultimodalContent(t *testing.T) {
	const output = `[{"type":"input_text","text":"before"},{"type":"input_image","image_url":"https://fixture.invalid/image.png","detail":"original"},{"type":"output_text","text":"after"},{"unknown":{"n":9007199254740993}},"tail"]`
	for _, kind := range []string{"function_call", "custom_tool_call"} {
		for _, encoded := range []bool{false, true} {
			for _, stream := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/encoded=%t/stream=%t", kind, encoded, stream), func(t *testing.T) {
					value := output
					if encoded {
						value = strconv.Quote(value)
					}
					request := []byte(fmt.Sprintf(`{"input":[{"type":%q,"name":"tool","call_id":"call_fixture","arguments":"{}","input":"fixture"},{"type":%q,"call_id":"call_fixture","output":%s}]}`, kind, kind+"_output", value))
					before := bytes.Clone(request)
					got := ConvertOpenAIResponsesRequestToOpenAIChatCompletions("model", request, stream)
					message := gjson.GetBytes(got, "messages.1")
					parts := message.Get("content").Array()
					if message.Get("role").String() != "tool" || message.Get("tool_call_id").String() != "call_fixture" || len(parts) != 5 {
						t.Fatal("tool result lost its multimodal parts or pairing")
					}
					if parts[0].Get("text").String() != "before" || parts[1].Get("type").String() != "image_url" || parts[1].Get("image_url.url").String() != "https://fixture.invalid/image.png" || parts[1].Get("image_url.detail").String() != "high" || parts[2].Get("text").String() != "after" || parts[3].Get("text").String() != `{"unknown":{"n":9007199254740993}}` || parts[4].Get("text").String() != "tail" {
						t.Fatal("tool result changed part order or dropped image/unknown fields")
					}
					if !bytes.Equal(request, before) {
						t.Fatal("conversion mutated source request")
					}
				})
			}
		}
	}
}

func TestResponsesToolOutputKeepsLegacyTextAndBusinessJSON(t *testing.T) {
	for _, fixture := range []struct{ raw, want string }{
		{`[{"type":"input_text","text":"a"},{"type":"output_text","text":"b"}]`, "ab"},
		{`"plain"`, "plain"},
		{strconv.Quote(`{"business":{"type":"input_image","image_url":"https://fixture.invalid/image"}}`), `{"business":{"type":"input_image","image_url":"https://fixture.invalid/image"}}`},
		{strconv.Quote(`[{"type":"input_image","image_url":123}]`), `[{"type":"input_image","image_url":123}]`},
	} {
		request := []byte(`{"input":[{"type":"custom_tool_call_output","call_id":"fixture","output":` + fixture.raw + `}]}`)
		got := ConvertOpenAIResponsesRequestToOpenAIChatCompletions("model", request, false)
		if content := gjson.GetBytes(got, "messages.0.content"); content.Type != gjson.String || content.String() != fixture.want {
			t.Fatal("non-multimodal output changed from existing text behavior")
		}
	}
}
