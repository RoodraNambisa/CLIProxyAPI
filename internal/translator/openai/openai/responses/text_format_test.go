package responses

import (
	"bytes"
	"testing"

	"github.com/tidwall/gjson"
)

func TestResponsesTextFormatSurvivesChatRequestConversion(t *testing.T) {
	for _, format := range []string{`{"type":"text"}`, `{"type":"json_object"}`, `{"type":"json_schema","name":"answer","description":"test schema","strict":false,"schema":{"type":"object","properties":{"n":{"const":9007199254740993}},"additionalProperties":false}}`, `{"type":"unknown"}`, `null`} {
		for _, stream := range []bool{false, true} {
			request := []byte(`{"input":[{"role":"user","content":"test"}],"text":{"format":` + format + `}}`)
			before := bytes.Clone(request)
			out := ConvertOpenAIResponsesRequestToOpenAIChatCompletions("model", request, stream)
			source := gjson.Parse(format)
			kind := source.Get("type").String()
			if kind == "unknown" || kind == "" {
				if gjson.GetBytes(out, "response_format").Exists() {
					t.Fatal("unknown or null format gained invented support")
				}
			} else if gjson.GetBytes(out, "response_format.type").String() != kind {
				t.Fatalf("format missing: %s", out)
			}
			if kind == "json_schema" {
				if gjson.GetBytes(out, "response_format.json_schema.strict").Raw != "false" || gjson.GetBytes(out, "response_format.json_schema.schema").Raw != source.Get("schema").Raw || gjson.GetBytes(out, "response_format.json_schema.name").String() != "answer" || gjson.GetBytes(out, "response_format.json_schema.description").String() != "test schema" {
					t.Fatal("schema fields or numeric precision changed")
				}
			}
			if !bytes.Equal(request, before) || gjson.GetBytes(out, "stream").Bool() != stream || gjson.GetBytes(out, "messages.0.content").String() != "test" {
				t.Fatal("format conversion changed unrelated request data")
			}
		}
	}
	if out := ConvertOpenAIResponsesRequestToOpenAIChatCompletions("model", []byte(`{"input":[]}`), false); gjson.GetBytes(out, "response_format").Exists() {
		t.Fatal("missing format changed default request")
	}
}
