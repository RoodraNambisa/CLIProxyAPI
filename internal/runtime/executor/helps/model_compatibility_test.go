package helps

import (
	"bytes"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestModelCompatibilityCodexSummaryAndUnsignedThinking(t *testing.T) {
	raw := []byte(`{"thinking":{"type":"enabled","display":"omitted","budget_tokens":1024},"messages":[{"role":"assistant","content":[{"type":"thinking","thinking":"","signature":""},{"type":"text","text":"answer"}]}]}`)
	for _, compat := range []bool{false, true} {
		body := TranslateRequestWithAPIKeyModelCompatibility(translator.FormatClaude, translator.FormatCodex, "gpt-5.4", raw, false, compat)
		body = SanitizeCodexReasoningEncryptedContent(t.Context(), "fixture", body, compat)
		items := gjson.GetBytes(body, "input").Array()
		want := 1
		if compat {
			want = 2
		}
		if len(items) != want || gjson.GetBytes(body, "reasoning.summary").Exists() {
			t.Fatal("compatibility lost summary visibility or unsigned reasoning")
		}
		if compat && (items[0].Get("type").String() != "reasoning" || items[0].Get("encrypted_content").Raw != `""`) {
			t.Fatal("compatibility cleanup removed the empty signature field")
		}
	}
	for _, raw := range []string{`""`, `"  "`, `"invalid"`, "1", "null", "{}"} {
		payload := []byte(`{"input":[{"type":"reasoning","encrypted_content":` + raw + `},{"type":"agent_message","content":[{"type":"encrypted_content","encrypted_content":"opaque"}]}]}`)
		for _, compat := range []bool{false, true} {
			before := bytes.Clone(payload)
			got := SanitizeCodexReasoningEncryptedContent(t.Context(), "fixture", payload, compat)
			kept := gjson.GetBytes(got, "input.0.encrypted_content")
			want := compat && (raw == `""` || raw == `"  "`)
			if kept.Exists() != want || (want && kept.Raw != raw) || !bytes.Equal(payload, before) ||
				gjson.GetBytes(got, "input.1").Raw != gjson.GetBytes(payload, "input.1").Raw {
				t.Fatal("empty-signature policy changed another type or opaque agent history")
			}
		}
	}
}
