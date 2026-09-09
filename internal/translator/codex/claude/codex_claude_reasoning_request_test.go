package claude

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestClaudeCodexReasoningHistoryPreservesCompatibleSignatures(t *testing.T) {
	envelope := make([]byte, 73)
	envelope[0] = 0x80
	valid := base64.RawURLEncoding.EncodeToString(envelope)
	for _, compat := range []bool{false, true} {
		for _, signature := range []string{valid, "gpt#" + valid, "", "  ", "opaque-foreign"} {
			t.Run(fmt.Sprintf("compat=%t/signature=%d", compat, len(signature)), func(t *testing.T) {
				raw := []byte(fmt.Sprintf(`{"messages":[{"role":"user","content":[{"type":"thinking","thinking":"user text","signature":%q},{"type":"text","text":"question"}]},{"role":"assistant","content":[{"type":"text","text":"before"},{"type":"thinking","thinking":"not a model input","signature":%q},{"type":"tool_use","id":"call_1","name":"lookup","input":{}},{"type":"text","text":"after"}]}]}`, signature, signature))
				before := bytes.Clone(raw)
				fn := ConvertClaudeRequestToCodex
				if compat {
					fn = ConvertClaudeRequestToCodexWithCompat
				}
				got := fn("gpt-5.4", raw, false)
				items := gjson.GetBytes(got, "input").Array()
				wantReasoning := signature == valid || signature == "gpt#"+valid || (compat && strings.TrimSpace(signature) == "")
				wantCount := 4
				if wantReasoning {
					wantCount++
				}
				if len(items) != wantCount || !bytes.Equal(raw, before) || bytes.Contains(got, []byte("user text")) {
					t.Fatal("reasoning replay changed input ordering, role or source")
				}
				toolIndex := 2
				if wantReasoning {
					wantSignature := signature
					if signature == "gpt#"+valid {
						wantSignature = valid
					}
					if items[2].Get("type").String() != "reasoning" || items[2].Get("encrypted_content").String() != wantSignature ||
						items[2].Get("summary").Raw != "[]" || items[2].Get("content").Raw != "null" {
						t.Fatal("reasoning signature was dropped, reinterpreted or reassigned")
					}
					toolIndex++
				}
				if items[toolIndex].Get("call_id").String() != "call_1" || items[toolIndex+1].Get("content.0.text").String() != "after" {
					t.Fatal("thinking interrupted tool pairing or following text")
				}
			})
		}
	}
}
