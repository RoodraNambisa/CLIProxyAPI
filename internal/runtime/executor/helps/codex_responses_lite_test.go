package helps

import (
	"net/http"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexResponsesLiteExplicitSignalsAndPerFrameReset(t *testing.T) {
	for _, tc := range []struct {
		name, body, header string
		websocket, want    bool
	}{
		{name: "ordinary modern model", body: `{"model":"gpt-6-astra"}`},
		{name: "HTTP header", body: `{}`, header: "true", want: true},
		{name: "frame string", body: `{"client_metadata":{"ws_request_header_x_openai_internal_codex_responses_lite":"true"}}`, websocket: true, want: true},
		{name: "frame boolean", body: `{"client_metadata":{"ws_request_header_x_openai_internal_codex_responses_lite":true}}`, websocket: true, want: true},
		{name: "frame false overrides handshake", body: `{"client_metadata":{"ws_request_header_x_openai_internal_codex_responses_lite":false}}`, header: "true", websocket: true},
		{name: "missing frame resets", body: `{}`, header: "true", websocket: true},
		{name: "invalid type", body: `{"client_metadata":{"ws_request_header_x_openai_internal_codex_responses_lite":1}}`, websocket: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			headers := http.Header{}
			if tc.header != "" {
				headers.Set(CodexResponsesLiteHeader, tc.header)
			}
			snapshot := SnapshotCodexResponsesLite([]byte(tc.body), headers, tc.websocket)
			if snapshot.Enabled != tc.want {
				t.Fatal("unexpected Lite mode")
			}
			snapshot.ApplyHeaders(headers)
			if tc.websocket && !tc.want && headers.Get(CodexResponsesLiteHeader) != "" {
				t.Fatal("stale handshake flag leaked into HTTP fallback")
			}
			body := snapshot.ApplyBody([]byte(`{"parallel_tool_calls":true}`), tc.websocket)
			if gjson.GetBytes(body, "parallel_tool_calls").Bool() == tc.want {
				t.Fatal("wrong parallel tool mode")
			}
		})
	}
}

func TestCodexResponsesLiteRetainsAdditionalToolChoice(t *testing.T) {
	body := []byte(`{"input":[{"type":"additional_tools","tools":[{"type":"custom","name":"exec"}]}],"tool_choice":"required","parallel_tool_calls":true}`)
	normalized := NormalizeCodexToolSelection(body)
	if gjson.GetBytes(normalized, "tool_choice").String() != "required" {
		t.Fatal("additional tools were treated as missing")
	}
	lite := (CodexResponsesLiteSnapshot{Enabled: true, Specified: true}).ApplyBody(normalized, true)
	if gjson.GetBytes(lite, "parallel_tool_calls").Bool() || gjson.GetBytes(lite, CodexResponsesLiteMetadata).String() != "true" {
		t.Fatal("Lite controls were not emitted")
	}
	if gjson.GetBytes(lite, "input").Raw != gjson.GetBytes(body, "input").Raw {
		t.Fatal("tool declarations changed")
	}
}
