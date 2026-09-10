package claude

import (
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexClaudeKeepsSignatureOnlyReasoning(t *testing.T) {
	for _, tc := range []struct {
		name   string
		events []string
		want   string
		count  int
	}{
		{"final signature", []string{`{"type":"response.output_item.added","item":{"type":"reasoning","id":"rs_a","encrypted_content":"early"}}`, `{"type":"response.output_item.done","item":{"type":"reasoning","id":"rs_a","encrypted_content":"final"}}`}, "final", 1},
		{"early fallback", []string{`{"type":"response.output_item.added","item":{"type":"reasoning","encrypted_content":"early"}}`, `{"type":"response.output_item.done","item":{"type":"reasoning"}}`}, "early", 1},
		{"missing start", []string{`{"type":"response.output_item.done","item":{"type":"reasoning","id":"rs_a","encrypted_content":"final"}}`}, "final", 1},
		{"duplicate terminal item", []string{`{"type":"response.output_item.done","item":{"type":"reasoning","id":"rs_a","encrypted_content":"final"}}`, `{"type":"response.output_item.done","item":{"type":"reasoning","id":"rs_a","encrypted_content":"late"}}`}, "final", 1},
		{"unlabelled duplicate", []string{`{"type":"response.output_item.done","item":{"type":"reasoning","encrypted_content":"final"}}`, `{"type":"response.output_item.done","item":{"type":"reasoning","encrypted_content":"final"}}`}, "final", 1},
		{"index duplicate", []string{`{"type":"response.output_item.done","output_index":0,"item":{"type":"reasoning","id":"rs_a","encrypted_content":"final"}}`, `{"type":"response.output_item.done","output_index":0,"item":{"type":"reasoning","encrypted_content":"late"}}`}, "final", 1},
		{"event ID duplicate", []string{`{"type":"response.output_item.done","item":{"type":"reasoning","id":"rs_a","encrypted_content":"final"}}`, `{"type":"response.output_item.done","item_id":"rs_a","item":{"type":"reasoning","encrypted_content":"late"}}`}, "final", 1},
		{"conflicting ID", []string{`{"type":"response.output_item.done","item_id":"rs_b","item":{"type":"reasoning","id":"rs_a","encrypted_content":"ambiguous"}}`, `{"type":"response.output_item.done","item_id":"rs_b","item":{"type":"reasoning","encrypted_content":"final"}}`}, "final", 1},
		{"different items", []string{`{"type":"response.output_item.done","item":{"type":"reasoning","id":"rs_a","encrypted_content":"first"}}`, `{"type":"response.output_item.done","item":{"type":"reasoning","id":"rs_b","encrypted_content":"second"}}`}, "first,second", 2},
		{"no signature", []string{`{"type":"response.output_item.done","item":{"type":"reasoning"}}`}, "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var state any
			var chunks [][]byte
			for _, event := range tc.events {
				chunks = append(chunks, ConvertCodexResponseToClaude(t.Context(), "", nil, nil, []byte("data: "+event), &state)...)
			}
			blocks := assertCodexClaudeBlocks(t, chunks)
			var signatures []string
			for _, chunk := range chunks {
				for _, line := range strings.Split(string(chunk), "\n") {
					if !strings.HasPrefix(line, "data: ") {
						continue
					}
					event := gjson.Parse(strings.TrimPrefix(line, "data: "))
					if event.Get("delta.type").String() == "signature_delta" {
						signatures = append(signatures, event.Get("delta.signature").String())
					}
				}
			}
			if len(blocks) != tc.count || strings.Join(signatures, ",") != tc.want {
				t.Fatalf("lost or duplicated signature-only item: blocks=%d signatures=%v", len(blocks), signatures)
			}
		})
	}
}
