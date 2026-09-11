package helps

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexInputIdentityChecksAvoidPlainTextAllocations(t *testing.T) {
	body := []byte(`{"input":[{"type":"message","content":"` + strings.Repeat(`text \u4f60\u597d `, 1<<16) + `"}],"include":["reasoning.encrypted_content"]}`)
	for name, sanitize := range map[string]func([]byte) []byte{
		"IDs": SanitizeCodexInputItemIDs,
		"reasoning": func(body []byte) []byte {
			return SanitizeCodexReasoningEncryptedContent(context.Background(), "fixture", body)
		},
	} {
		t.Run(name, func(t *testing.T) {
			var got []byte
			allocations := testing.AllocsPerRun(5, func() { got = sanitize(body) })
			if !bytes.Equal(got, body) || &got[0] != &body[0] {
				t.Fatal("plain text checks replaced the unchanged request")
			}
			if allocations != 0 {
				t.Fatalf("plain text checks allocated without an identity field: %.0f", allocations)
			}
		})
	}
}

func TestCodexInputIdentityChecksKeepEscapedFieldsAndOwnedRewrites(t *testing.T) {
	for _, escaped := range []bool{false, true} {
		t.Run(fmt.Sprintf("escaped=%t", escaped), func(t *testing.T) {
			idKey, encryptedKey := `"id"`, `"encrypted_content"`
			if escaped {
				idKey, encryptedKey = `"\u0069d"`, `"encrypted_\u0063ontent"`
			}
			body := []byte(`{"in\u0070ut":[{"type":"message",` + idKey + `:"raw","content":"keep","call_id":"paired"},{"type":"reasoning",` + idKey + `:"rs_orphan",` + encryptedKey + `:null}],"store":false}`)
			original := bytes.Clone(body)
			ids := SanitizeCodexInputItemIDs(body)
			if gjson.GetBytes(ids, "input.0.id").String() != "msg_raw" || gjson.GetBytes(ids, "input.0.call_id").String() != "paired" {
				t.Fatal("escaped input or ID bypassed repair or changed call pairing")
			}
			cleaned := SanitizeCodexReasoningEncryptedContent(context.Background(), "fixture", ids)
			if gjson.GetBytes(cleaned, "input.1.id").Exists() || gjson.GetBytes(cleaned, "input.1.encrypted_content").Exists() {
				t.Fatal("escaped reasoning fields bypassed cleanup")
			}
			if !bytes.Equal(body, original) || gjson.GetBytes(cleaned, "input.0.content").String() != "keep" {
				t.Fatal("identity checks changed the source or ordinary content")
			}
			wantIDs, wantCleaned := bytes.Clone(ids), bytes.Clone(cleaned)
			for i := range body {
				body[i] = 'x'
			}
			if !bytes.Equal(ids, wantIDs) || !bytes.Equal(cleaned, wantCleaned) {
				t.Fatal("rewritten requests retained the original mutable body")
			}
			for i := range ids {
				ids[i] = 'x'
			}
			if !bytes.Equal(cleaned, wantCleaned) {
				t.Fatal("reasoning cleanup retained its input buffer")
			}
		})
	}
}

func BenchmarkCodexPlainTextReasoningCheck(b *testing.B) {
	for _, size := range []int{128, 1 << 20, 10 << 20} {
		b.Run(fmt.Sprintf("bytes=%d", size), func(b *testing.B) {
			body := codexIDBenchmarkPayload(size, false)
			b.SetBytes(int64(len(body)))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				SanitizeCodexReasoningEncryptedContent(context.Background(), "fixture", body)
			}
		})
	}
}

func TestCodexMessageIDsDoNotTriggerReasoningAllocation(t *testing.T) {
	body := codexIDBenchmarkPayload(1<<20, true)
	body = append(body[:len(body)-1], `,"reasoning":{"summary":"auto"},"include":["reasoning.encrypted_content"]}`...)
	if allocations := testing.AllocsPerRun(5, func() {
		out := SanitizeCodexReasoningEncryptedContent(context.Background(), "fixture", body)
		if len(out) != len(body) || &out[0] != &body[0] {
			t.Fatal("ordinary message ID changed during reasoning cleanup")
		}
	}); allocations != 0 {
		t.Fatalf("ordinary message ID allocated %g times during reasoning cleanup", allocations)
	}
}

func TestCodexReasoningTypeHintKeepsEscapedAndPaddedItems(t *testing.T) {
	for _, kind := range []string{`"reasoning"`, `"re\u0061soning"`, `" reasoning "`, `"\treasoning\n"`, "\"reasoning\u00a0\""} {
		body := []byte(`{"input":[{"type":"message","id":"msg_keep","content":"reasoning.encrypted_content"},{"type":` + kind + `,"id":"rs_drop","encrypted_content":null}],"include":["reasoning.encrypted_content"]}`)
		out := SanitizeCodexReasoningEncryptedContent(context.Background(), "fixture", body)
		if gjson.GetBytes(out, "input.0.id").String() != "msg_keep" || gjson.GetBytes(out, "input.1.id").Exists() || gjson.GetBytes(out, "input.1.encrypted_content").Exists() {
			t.Fatalf("type %s bypassed reasoning cleanup or changed the message", kind)
		}
	}
}

func BenchmarkCodexMessageIDReasoningCheck(b *testing.B) {
	for _, size := range []int{128, 1 << 20, 10 << 20} {
		b.Run(fmt.Sprintf("bytes=%d", size), func(b *testing.B) {
			body := codexIDBenchmarkPayload(size, true)
			body = append(body[:len(body)-1], `,"include":["reasoning.encrypted_content"]}`...)
			b.SetBytes(int64(len(body)))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				SanitizeCodexReasoningEncryptedContent(context.Background(), "fixture", body)
			}
		})
	}
}
