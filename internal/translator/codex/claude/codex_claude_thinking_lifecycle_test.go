package claude

import (
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexThinkingItemLifecycle(t *testing.T) {
	const added = `{"type":"response.output_item.added","item":{"type":"reasoning","encrypted_content":"early"}}`
	const delta = `{"type":"response.reasoning_summary_text.delta","delta":"thought"}`
	for _, tc := range []struct {
		name       string
		events     []string
		signatures string
		blocks     int
	}{
		{"final snapshot", []string{added, delta, `{"type":"response.reasoning_summary_part.done"}`, `{"type":"response.output_item.done","item":{"type":"reasoning","encrypted_content":"final"}}`}, "final", 1},
		{"missing start and final signature", []string{added, delta, `{"type":"response.output_item.done","item":{"type":"reasoning"}}`}, "early", 1},
		{"missing item and signature", []string{delta, `{"type":"response.completed","response":{}}`}, "", 1},
		{"partial without part done", []string{added, delta, `{"type":"response.incomplete","response":{"incomplete_details":{"reason":"max_output_tokens"}}}`}, "early", 1},
		{"new item without preceding done", []string{added, delta, `{"type":"response.output_item.added","item":{"type":"reasoning","encrypted_content":"second"}}`, delta, `{"type":"response.output_item.done","item":{"type":"reasoning","encrypted_content":"second-final"}}`}, "early,second-final", 2},
		{"duplicate terminal", []string{added, delta, `{"type":"response.completed","response":{}}`, `{"type":"response.completed","response":{}}`, delta}, "early", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var state any
			starts, stops := 0, 0
			var signatures []string
			for _, event := range tc.events {
				for _, out := range ConvertCodexResponseToClaude(t.Context(), "", nil, nil, []byte("data: "+event), &state) {
					for _, line := range strings.Split(string(out), "\n") {
						if !strings.HasPrefix(line, "data: ") {
							continue
						}
						item := gjson.Parse(strings.TrimPrefix(line, "data: "))
						switch item.Get("type").String() {
						case "content_block_start":
							if item.Get("index").Int() != int64(starts) {
								t.Fatal("thinking block index reused")
							}
							starts++
						case "content_block_delta":
							if starts != stops+1 {
								t.Fatal("delta outside an open thinking block")
							}
							if item.Get("delta.type").String() == "signature_delta" {
								signatures = append(signatures, item.Get("delta.signature").String())
							}
						case "content_block_stop":
							stops++
						}
					}
				}
			}
			if starts != tc.blocks || stops != tc.blocks || strings.Join(signatures, ",") != tc.signatures {
				t.Fatalf("starts=%d stops=%d signatures=%v", starts, stops, signatures)
			}
		})
	}
}
