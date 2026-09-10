package responses

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/tidwall/gjson"
)

func testResponsesReasoningSignature() string {
	payload := make([]byte, 73)
	payload[0], payload[8] = 0x80, 1
	return base64.URLEncoding.EncodeToString(payload)
}

func TestInteractionsResponsesReasoningSignatureTypes(t *testing.T) {
	valid := testResponsesReasoningSignature()
	for _, candidate := range []string{valid, " " + valid + " ", "EtoRforeignGeminiSignature", "gAAAAinvalid", "", "not-a-signature"} {
		t.Run(candidate, func(t *testing.T) {
			want := ""
			if candidate == valid || candidate == " "+valid+" " {
				want = valid
			}
			for _, path := range []string{"encrypted_content", "signature", "thought_signature", "thoughtSignature"} {
				raw, err := json.Marshal(map[string]any{"id": "fixture", "steps": []any{map[string]any{"type": "thought", path: candidate, "content": []any{map[string]any{"type": "text", "text": "summary"}}}}})
				if err != nil {
					t.Fatal(err)
				}
				out := ConvertInteractionsResponseToOpenAIResponsesNonStream(t.Context(), "fixture", nil, nil, raw, nil)
				if got := gjson.GetBytes(out, "output.0.encrypted_content").String(); got != want {
					t.Errorf("%s encrypted content was not filtered or preserved correctly", path)
				}
				if gjson.GetBytes(out, "output.0.summary.0.text").String() != "summary" {
					t.Error("reasoning summary was lost")
				}
			}
			var state any
			for _, event := range []string{
				`{"event_type":"step.start","index":0,"step":{"type":"thought"}}`,
				`{"event_type":"step.delta","index":0,"delta":{"type":"thought_summary","text":"summary"}}`,
				`{"event_type":"step.delta","index":0,"delta":{"type":"thought_signature","signature":` + string(mustSignatureJSON(t, candidate)) + `}}`,
			} {
				ConvertInteractionsResponseToOpenAIResponses(t.Context(), "fixture", nil, nil, []byte(event), &state)
			}
			events := ConvertInteractionsResponseToOpenAIResponses(t.Context(), "fixture", nil, nil, []byte(`{"event_type":"step.stop","index":0}`), &state)
			done := findResponsesEventPayload(events, "response.output_item.done")
			if got := gjson.GetBytes(done, "item.encrypted_content").String(); got != want {
				t.Error("streamed encrypted content differs from the nonstream contract")
			}
		})
	}
}

func mustSignatureJSON(t *testing.T, value string) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestInteractionsReasoningSignatureSkipsInvalidAliases(t *testing.T) {
	valid := testResponsesReasoningSignature()
	for _, body := range []string{
		`{"signature":"foreign","thought_signature":"` + valid + `"}`,
		`{"content":[{"signature":"foreign"},{"thought_signature":"` + valid + `"}]}`,
		`{"content":[{"signature":"foreign","thought_signature":"` + valid + `"}]}`,
	} {
		if got := interactionsThoughtSignature(gjson.Parse(body)); got != valid {
			t.Error("a foreign signature hid a valid Codex signature")
		}
	}
}
