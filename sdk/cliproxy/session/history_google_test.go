package session

import (
	"reflect"
	"strings"
	"testing"

	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestGoogleHistoryUsesTheSameTextPrefix(t *testing.T) {
	expected := FingerprintHistory(sdktranslator.FormatCodex, []byte(`{"instructions":"system","input":[{"role":"user","content":"question"},{"role":"assistant","content":"answer"}]}`))
	gemini := `{"systemInstruction":{"parts":[{"text":"system"}]},"contents":[{"role":"user","parts":[{"text":"question"}]},{"role":"model","parts":[{"thought":true,"text":"private"},{"text":"answer"}]}]}`
	interactions := `{"system_instruction":"system","input":[{"type":"user_input","content":[{"type":"text","text":"question"}]},{"type":"model_output","content":[{"type":"text","text":"answer"}]}]}`
	for _, input := range []struct {
		format sdktranslator.Format
		body   string
	}{
		{sdktranslator.FormatGemini, gemini}, {sdktranslator.FormatAntigravity, `{"request":` + gemini + `}`},
		{sdktranslator.FormatInteractions, interactions}, {sdktranslator.FormatInteractions, `{"system_instruction":"system","input":[{"steps":[{"type":"user_input","content":[{"type":"text","text":"question"}]},{"type":"model_output","content":[{"type":"text","text":"answer"}]}]}]}`},
	} {
		got := FingerprintHistory(input.format, []byte(input.body))
		if !reflect.DeepEqual(expected, got) {
			t.Fatalf("Google history diverged for %s", input.format)
		}
	}
	if !reflect.DeepEqual(expected, FingerprintHistory("", []byte(gemini))) || !reflect.DeepEqual(expected, FingerprintHistory("", []byte(interactions))) {
		t.Fatal("format inference lost Google history")
	}
}

func TestGoogleHistoryDoesNotInferFromSharedResourcesOrTools(t *testing.T) {
	for _, input := range []struct {
		format sdktranslator.Format
		body   string
	}{
		{sdktranslator.FormatGemini, `{"cachedContent":"cached/shared","contents":[]}`},
		{sdktranslator.FormatGemini, `{"contents":[{"role":"user","parts":[{"functionResponse":{"name":"run","response":{"text":"shared"}}}]}]}`},
		{sdktranslator.FormatInteractions, `{"input":[{"type":"function_result","content":[{"type":"text","text":"shared"}]}]}`},
		{sdktranslator.FormatInteractions, `{"previous_interaction_id":"opaque","input":"question"}`},
		{sdktranslator.FormatGeminiCLI, `{"contents":[{"role":"user","parts":[{"text":"question"}]}]}`},
	} {
		if FingerprintHistory(input.format, []byte(input.body)).Usable() {
			t.Fatal("shared resource, tool output or incremental state established a user anchor")
		}
	}
	one := `{"cachedContent":"cached/one","contents":[{"role":"user","parts":[{"text":"question"}]}]}`
	two := strings.ReplaceAll(one, "cached/one", "cached/two")
	if reflect.DeepEqual(FingerprintHistory(sdktranslator.FormatGemini, []byte(one)), FingerprintHistory(sdktranslator.FormatGemini, []byte(two))) {
		t.Fatal("cached resource identity was erased")
	}
	media := FingerprintHistory(sdktranslator.FormatInteractions, []byte(`{"input":[{"type":"image","mime_type":"image/png","data":"fixture"}]}`))
	if !media.Usable() {
		t.Fatal("Interactions user media lost its anchor")
	}
}

func TestInteractionsHistoryKeepsToolBusinessStepsAndBoundsNesting(t *testing.T) {
	first := `{"input":[{"type":"user_input","content":"question"},{"type":"function_call","name":"run","steps":[{"type":"text","text":"business-one"}]}]}`
	second := strings.ReplaceAll(first, "business-one", "business-two")
	if reflect.DeepEqual(FingerprintHistory(sdktranslator.FormatInteractions, []byte(first)), FingerprintHistory(sdktranslator.FormatInteractions, []byte(second))) {
		t.Fatal("business steps replaced the complete tool object")
	}
	deep := `{"input":` + strings.Repeat(`{"steps":[`, historyMaxDepth) + `{"type":"text","text":"question"}` + strings.Repeat(`]}`, historyMaxDepth) + `}`
	if FingerprintHistory(sdktranslator.FormatInteractions, []byte(deep)).Usable() {
		t.Fatal("nested Interactions envelopes bypassed the budget")
	}
}

func TestGeminiSingleTurnAcceptsAnOmittedRole(t *testing.T) {
	expected := FingerprintHistory(sdktranslator.FormatGemini, []byte(`{"contents":[{"role":"user","parts":[{"text":"question"}]}]}`))
	for _, role := range []string{"", `"role":"",`} {
		got := FingerprintHistory(sdktranslator.FormatGemini, []byte(`{"contents":[{`+role+`"parts":[{"text":"question"}]}]}`))
		if !reflect.DeepEqual(expected, got) {
			t.Fatal("valid single-turn omitted role lost its user anchor")
		}
	}
	if FingerprintHistory(sdktranslator.FormatGemini, []byte(`{"contents":[{"parts":[{"text":"unknown producer"}]},{"parts":[{"text":"unknown producer"}]}]}`)).Usable() {
		t.Fatal("ambiguous multi-turn producers were guessed")
	}
}
