package session

import (
	"reflect"
	"strings"
	"testing"

	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestHistorySnapshotsAcrossOpenAIClaudeAndResponses(t *testing.T) {
	inputs := []struct {
		format sdktranslator.Format
		body   string
	}{
		{sdktranslator.FormatOpenAI, `{"messages":[{"role":"system","content":"instructions"},{"role":"user","content":"hello"},{"role":"assistant","content":"answer"}]}`},
		{sdktranslator.FormatClaude, `{"system":[{"type":"text","text":"instructions"}],"messages":[{"role":"user","content":[{"type":"text","text":"hello"}]},{"role":"assistant","content":[{"type":"thinking","thinking":"private"},{"type":"text","text":"answer"}]}]}`},
		{sdktranslator.FormatOpenAIResponse, `{"instructions":"instructions","input":[{"role":"user","content":[{"type":"input_text","text":"hello"}]},{"type":"reasoning","encrypted_content":"opaque"},{"role":"assistant","content":[{"type":"output_text","text":"answer"}]}]}`},
	}
	var expected History
	for _, input := range inputs {
		payload := []byte(input.body)
		got := FingerprintHistory(input.format, payload)
		inferred := FingerprintHistory("", payload)
		clear(payload)
		if !got.Usable() || got.minimum != 2 || len(got.prefixes) != 3 || !reflect.DeepEqual(got, inferred) {
			t.Fatalf("invalid bounded history for %s", input.format)
		}
		if expected.Usable() && !reflect.DeepEqual(expected, got) {
			t.Fatal("equivalent protocol conversations diverged")
		}
		expected = got
	}
}

func TestHistoryPrefixIncludesUserAndTemplateContent(t *testing.T) {
	first := FingerprintHistory(sdktranslator.FormatCodex, []byte(`{"instructions":"system","input":"question"}`))
	next := FingerprintHistory(sdktranslator.FormatCodex, []byte(`{"instructions":"system","input":[{"role":"user","content":"question"},{"role":"assistant","content":"answer"},{"role":"user","content":"next"}]}`))
	if !first.Usable() || !next.Usable() || first.prefixes[1] != next.prefixes[1] {
		t.Fatal("continued history lost its common prefix")
	}
	for _, body := range []string{
		`{"instructions":"different","input":"question"}`,
		`{"instructions":"system","input":"different"}`,
		`{"tools":[{"type":"function","name":"tool"}],"instructions":"system","input":"question"}`,
	} {
		got := FingerprintHistory(sdktranslator.FormatCodex, []byte(body))
		if !got.Usable() || got.prefixes[got.minimum-1] == first.prefixes[first.minimum-1] {
			t.Fatal("different instructions, tools or user content shared a root")
		}
	}
	for _, body := range []string{
		`{"instructions":"shared system","tools":[{"type":"function","name":"shared"}],"input":[]}`,
		`{"input":[{"type":"function_call_output","call_id":"one","output":"shared output"}]}`,
		`{"input":[{"role":"assistant","content":"shared answer"}]}`,
		`{"input":"   "}`,
		`{"previous_response_id":"opaque","input":"continuation"}`,
		`{"input":[{"role":"user","content":"question"},{"type":"item_reference","id":"opaque"}]}`,
		`{"input":[{"role":"user","content":"question"},null]}`,
		`{"input":"unterminated"`,
	} {
		if FingerprintHistory(sdktranslator.FormatCodex, []byte(body)).Usable() {
			t.Fatal("unreliable prefix became eligible for inference")
		}
	}
	if FingerprintHistory(sdktranslator.FormatClaude, []byte(`{"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"one","content":"shared"}]}]}`)).Usable() {
		t.Fatal("Claude tool results established a user anchor")
	}
}

func TestHistoryLimitsAndToolPairing(t *testing.T) {
	turn := `{"role":"user","content":"hello"}`
	for _, count := range []int{historyMaxTurns, historyMaxTurns + 1} {
		body := `{"input":[` + strings.TrimSuffix(strings.Repeat(turn+",", count), ",") + `]}`
		if got := FingerprintHistory(sdktranslator.FormatCodex, []byte(body)); got.Usable() != (count == historyMaxTurns) {
			t.Fatal("turn budget returned a truncated matching history")
		}
	}
	base := `{"input":[{"role":"user","content":"question"},{"type":"custom_tool_call","call_id":"one","name":"run","input":"hello"},{"type":"custom_tool_call_output","call_id":"one","output":"world"}]}`
	first := FingerprintHistory(sdktranslator.FormatCodex, []byte(base))
	second := FingerprintHistory(sdktranslator.FormatCodex, []byte(strings.ReplaceAll(base, `"call_id":"one"`, `"call_id":"two"`)))
	if !first.Usable() || first.prefixes[2] == second.prefixes[2] {
		t.Fatal("custom tool pairing disappeared from the history")
	}
	first = FingerprintHistory(sdktranslator.FormatCodex, []byte(`{"input":[{"role":"user","content":"question"},{"type":"future_call","name":"one"}]}`))
	second = FingerprintHistory(sdktranslator.FormatCodex, []byte(`{"input":[{"role":"user","content":"question"},{"type":"future_call","name":"two"}]}`))
	if reflect.DeepEqual(first, second) {
		t.Fatal("unrecognized history items were silently discarded")
	}
}
