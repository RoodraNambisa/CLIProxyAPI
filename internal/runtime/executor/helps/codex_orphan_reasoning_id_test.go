package helps

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexOrphanReasoningIDsRespectStorageAndCompatibility(t *testing.T) {
	for _, store := range []string{"", `,"store":false`, `,"store":true`} {
		for _, compat := range []bool{false, true} {
			for _, encrypted := range []string{"", `,"encrypted_content":null`, `,"encrypted_content":1`, `,"encrypted_content":"bad"`, `,"encrypted_content":""`, `,"encrypted_content":"  "`, `,"encrypted_content":"` + testCodexReasoningSignature() + `"`} {
				t.Run(fmt.Sprintf("store=%s/compat=%t/encrypted=%d", store, compat, len(encrypted)), func(t *testing.T) {
					body := []byte(`{"input":[{"type":"message","id":"msg_1","content":"before"},{"type":"reasoning","id":"rs_orphan","summary":[{"type":"summary_text","text":"visible"}]` + encrypted + `},{"type":"function_call_output","call_id":"rs_orphan","output":{"type":"reasoning","id":"rs_orphan","encrypted_content":"business"}}]` + store + `}`)
					before := bytes.Clone(body)
					got := SanitizeCodexReasoningEncryptedContent(t.Context(), "fixture", body, compat)
					kept := encrypted == `,"encrypted_content":"`+testCodexReasoningSignature()+`"` || compat && (encrypted == `,"encrypted_content":""` || encrypted == `,"encrypted_content":"  "`)
					wantID := store == `,"store":true` || kept
					if gjson.GetBytes(got, "input.1.id").Exists() != wantID || gjson.GetBytes(got, "input.1.encrypted_content").Exists() != kept || gjson.GetBytes(got, "input.1.summary.0.text").String() != "visible" || gjson.GetBytes(got, "input.2").Raw != gjson.GetBytes(body, "input.2").Raw || !bytes.Equal(body, before) {
						t.Fatal("orphan lookup, compatibility placeholder, summary or business identity changed incorrectly")
					}
					if repeated := SanitizeCodexReasoningEncryptedContent(t.Context(), "fixture", got, compat); !bytes.Equal(repeated, got) {
						t.Fatal("reasoning cleanup is not idempotent")
					}
				})
			}
		}
	}
}
