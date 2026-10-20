package chat_completions

import (
	"bytes"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexChatVerbosity(t *testing.T) {
	for _, tc := range []struct{ fields, want string }{
		{``, ""},
		{`,"verbosity":"low"`, `"low"`},
		{`,"verbosity":"medium"`, `"medium"`},
		{`,"verbosity":"high","response_format":{"type":"text"}`, `"high"`},
		{`,"text":{"verbosity":"low"}`, `"low"`},
		{`,"verbosity":"low","text":{"verbosity":"high"}`, `"low"`},
		{`,"verbosity":null,"text":{"verbosity":"high"}`, `null`},
		{`,"verbosity":"low","response_format":{"type":"json_schema","json_schema":{"name":"answer","strict":true,"schema":{"type":"object","properties":{"answer":{"type":"string"}}}}}`, `"low"`},
	} {
		input := []byte(`{"messages":[{"role":"user","content":"hello"}]` + tc.fields + `}`)
		original := bytes.Clone(input)
		output := ConvertOpenAIRequestToCodex("gpt-6-astra", input, true)
		if got := gjson.GetBytes(output, "text.verbosity").Raw; got != tc.want {
			t.Errorf("fields %s: verbosity=%s, want %s", tc.fields, got, tc.want)
		}
		if gjson.GetBytes(input, "response_format.type").String() == "json_schema" &&
			gjson.GetBytes(output, "text.format.schema.properties.answer.type").String() != "string" {
			t.Fatal("verbosity mapping lost the output schema")
		}
		if !bytes.Equal(input, original) {
			t.Fatal("conversion mutated the caller's request")
		}
	}
}

func TestCodexChatImageDetail(t *testing.T) {
	for _, detail := range []string{"", "auto", "low", "high", "original"} {
		field := ""
		if detail != "" {
			field = `,"detail":"` + detail + `"`
		}
		input := []byte(`{"messages":[{"role":"user","content":[{"type":"text","text":"describe"},{"type":"image_url","image_url":{"url":"https://example.invalid/image.png"` + field + `}}]}]}`)
		output := ConvertOpenAIRequestToCodex("gpt-6-astra", input, true)
		part := gjson.GetBytes(output, "input.0.content.1")
		if part.Get("detail").String() != detail || part.Get("detail").Exists() != (detail != "") {
			t.Errorf("detail=%s, want %q", part.Get("detail").Raw, detail)
		}
		if part.Get("type").String() != "input_image" || part.Get("image_url").String() != "https://example.invalid/image.png" {
			t.Fatal("image content was lost")
		}
	}
}

func TestCodexChatServiceTier(t *testing.T) {
	for _, tier := range []string{`"priority"`, `"flex"`, `"auto"`, `"default"`, `null`} {
		input := []byte(`{"messages":[],"service_tier":` + tier + `}`)
		output := ConvertOpenAIRequestToCodex("gpt-6-astra", input, false)
		if got := gjson.GetBytes(output, "service_tier").Raw; got != tier {
			t.Errorf("service_tier=%s, want %s", got, tier)
		}
	}
}
