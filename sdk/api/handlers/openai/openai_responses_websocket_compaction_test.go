package openai

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/tidwall/gjson"
)

func TestResponsesWebsocketExplicitCompactionItemsReplaceTranscript(t *testing.T) {
	for _, typ := range []string{"compaction", "compaction_summary"} {
		for _, requestType := range []string{"response.create", "response.append"} {
			for _, previousID := range []string{"", `,"previous_response_id":"prior-response"`} {
				input := fmt.Sprintf(`[{"type":%q,"encrypted_content":"opaque"},{"role":"user","content":"continue"}]`, typ)
				raw := []byte(fmt.Sprintf(`{"type":%q,"input":%s%s}`, requestType, input, previousID))
				replace := previousID == ""
				if shouldReplaceWebsocketTranscript(raw, gjson.Parse(input)) != replace {
					t.Fatal("explicit compaction did not respect a previous response reference")
				}
				old := []byte(`{"model":"gpt-5.4","instructions":"keep","input":[{"role":"user","content":"old history"}]}`)
				got, _, errMsg := normalizeResponsesWebsocketRequestWithMode(raw, old, []byte("[]"), false)
				if errMsg != nil || bytes.Contains(got, []byte("old history")) == replace {
					t.Fatal("compaction replacement changed history selection")
				}
				if replace && (gjson.GetBytes(got, "input").Raw != input || gjson.GetBytes(got, "instructions").String() != "keep") {
					t.Fatal("compaction replacement changed explicit input or inherited instructions")
				}
			}
		}
	}
}

func TestResponsesWebsocketLocalCompactionReplacesOnlyRecognizedTranscript(t *testing.T) {
	summary := fmt.Sprintf(`{"role":"user","content":%q}`, codexLocalCompactionSummaryPrefix+"\nSummary body")
	input := `[{"type":"additional_tools","role":"developer","tools":[]},{"role":"developer","content":"workspace"},` + summary + `]`
	for _, incremental := range []bool{false, true} {
		old := []byte(`{"model":"gpt-5.6-sol","instructions":"keep","input":[{"role":"user","content":"old prompt"}]}`)
		raw := []byte(`{"type":"response.create","input":` + input + `,"client_metadata":{"ws_request_header_x_openai_internal_codex_responses_lite":"true"}}`)
		normalized, snapshot, errMsg := normalizeResponsesWebsocketRequestWithMode(raw, old, []byte(`[{"type":"message","role":"assistant","content":"old answer"}]`), incremental)
		if errMsg != nil || gjson.GetBytes(normalized, "input").Raw != input || bytes.Contains(normalized, []byte("old prompt")) || !bytes.Equal(normalized, snapshot) {
			t.Fatal("recognized compacted transcript was appended to stale history")
		}
		if gjson.GetBytes(normalized, "instructions").String() != "keep" || gjson.GetBytes(normalized, "client_metadata.ws_request_header_x_openai_internal_codex_responses_lite").String() != "true" {
			t.Fatal("replacement discarded current metadata or inherited instructions")
		}
	}
	for _, request := range []string{`{"type":"response.append"}`, `{"type":"response.create","previous_response_id":""}`, `{"type":"response.create","previous_response_id":null}`, `{"type":"response.create","previous_response_id":"resp-old"}`} {
		if shouldReplaceWebsocketTranscript([]byte(request), gjson.Parse(input)) {
			t.Fatal("explicit continuation was treated as a local compaction reset")
		}
	}
	for _, input := range []string{
		`[{"role":"user","content":"Please summarize this"}]`,
		`[` + summary + `,{"type":"additional_tools","role":"developer","tools":[]}]`,
		`[{"type":"additional_tools","role":"user","tools":[]},` + summary + `]`,
		`[{"type":"additional_tools","role":"developer","tools":[null]},` + summary + `]`,
		`[{"type":"reasoning"},` + summary + `]`,
		fmt.Sprintf(`[{"role":"user","content":%q}]`, codexLocalCompactionSummaryPrefix),
	} {
		if inputHasCodexLocalCompactionSummary(gjson.Parse(input)) {
			t.Fatal("ordinary or malformed input matched the local compaction shape")
		}
	}
	parts := fmt.Sprintf(`[{"role":"user","content":[{"type":"input_text","text":%q},{"type":"input_text","text":"\nSummary"}]}]`, codexLocalCompactionSummaryPrefix)
	if !inputHasCodexLocalCompactionSummary(gjson.Parse(parts)) {
		t.Fatal("summary split across text parts was missed")
	}
}
