package helps

import (
	"bytes"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/tidwall/gjson"
)

func TestOpenAIToolResultsTextOnlyPreservesRolesPairingAndBusinessJSON(t *testing.T) {
	body := []byte(`{"custom":9007199254740993,"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,AA=="}}]},{"role":"tool","tool_call_id":"call-a","content":[{"type":"text","text":"before"},{"type":"image_url","image_url":{"url":"data:image/png;base64,AA=="}},{"type":"input_image","image_url":"fixture"},{"type":"image","source":{"data":"fixture"}},{"image_url":"business","text":"business text","integer":9007199254740993},{"type":"text","text":"after"}]},{"role":"tool","tool_call_id":"call-b","content":"opaque [image_url]"},{"role":"tool","content":{"text":"business","integer":9007199254740993}},{"role":"tool","content":null}]}`)
	original := bytes.Clone(body)
	out := NormalizeOpenAIToolResultsTextOnly(body)
	want := "before\n\n" + openAIToolResultImageOmittedText + "\n\n" + openAIToolResultImageOmittedText + "\n\n" + openAIToolResultImageOmittedText + "\n\n" + `{"image_url":"business","text":"business text","integer":9007199254740993}` + "\n\nafter"
	if !bytes.Equal(body, original) || gjson.GetBytes(out, "messages.1.content").Str != want || gjson.GetBytes(out, "messages.1.tool_call_id").Str != "call-a" || gjson.GetBytes(out, "messages.2.content").Str != "opaque [image_url]" {
		t.Fatal("tool normalization changed the source, pairing or ordered content")
	}
	if gjson.GetBytes(out, "messages.0").Raw != gjson.GetBytes(body, "messages.0").Raw || gjson.GetBytes(out, "custom").Raw != "9007199254740993" || gjson.GetBytes(out, "messages.3.content").Str != `{"text":"business","integer":9007199254740993}` || gjson.GetBytes(out, "messages.4.content").Str != "null" {
		t.Fatal("normalization rewrote unrelated fields or interpreted business JSON as protocol content")
	}
	for _, input := range []string{`{"messages":[]}`, `{"messages":[{"role":"tool","content":"text"}]}`, `{"messages":[{"role":"tool","content":[]}] garbage`} {
		if got := NormalizeOpenAIToolResultsTextOnly([]byte(input)); string(got) != input {
			t.Fatal("normalization changed an unaffected or malformed request")
		}
	}
}

func TestOpenAIToolResultsTextOnlyRequiresExplicitSelectedModel(t *testing.T) {
	compat := &config.OpenAICompatibility{Models: []config.OpenAICompatibilityModel{
		{Name: "text-model", Alias: "pool", InputModalities: []string{" TEXT "}},
		{Name: "vision-model", Alias: "pool", InputModalities: []string{"text", "image"}},
		{Name: "text-model(high)", InputModalities: []string{"text", "audio"}},
		{Name: "kimi-k2"},
	}}
	for _, tc := range []struct {
		upstream, requested string
		want                bool
	}{
		{"text-model", "pool", true}, {"text-model(low)", "pool", true}, {"text-model(high)", "pool", false},
		{"vision-model", "pool", false}, {"unknown", "pool", false}, {"kimi-k2", "kimi-k2", false}, {"", "", false},
	} {
		if got := ShouldNormalizeOpenAIToolResultsForModel(compat, tc.upstream, tc.requested); got != tc.want {
			t.Fatalf("wrong text-only policy for %s/%s", tc.upstream, tc.requested)
		}
	}
	compat.Models[1].InputModalities = []string{"text"}
	if !ShouldNormalizeOpenAIToolResultsForModel(compat, "unknown", "pool") {
		t.Fatal("unambiguous text-only alias pool was not recognized")
	}
	compat.Disabled = true
	if ShouldNormalizeOpenAIToolResultsForModel(compat, "text-model", "pool") {
		t.Fatal("disabled compatibility configuration enabled content rewriting")
	}
}

func TestResponsesToolResultsTextOnlyPreservesCustomPairAndNonToolInput(t *testing.T) {
	body := []byte(`{"input":[{"type":"message","role":"user","content":[{"type":"input_image","image_url":"fixture"}]},{"type":"custom_tool_call_output","call_id":"custom-call","output":[{"type":"output_text","text":"before"},{"type":"input_image","image_url":"fixture"}]}]}`)
	out := NormalizeResponsesToolResultsTextOnly(body)
	if gjson.GetBytes(out, "input.0").Raw != gjson.GetBytes(body, "input.0").Raw || gjson.GetBytes(out, "input.1.call_id").Str != "custom-call" || gjson.GetBytes(out, "input.1.output").Str != "before\n\n"+openAIToolResultImageOmittedText || !gjson.GetBytes(body, "input.1.output").IsArray() {
		t.Fatal("native Responses tool normalization lost identity, content ordering or original input")
	}
}
