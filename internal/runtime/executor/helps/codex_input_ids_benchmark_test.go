package helps

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/tidwall/gjson"
)

func codexIDBenchmarkPayload(size int, withID bool) []byte {
	id := ""
	if withID {
		id = `,"id":"msg_existing"`
	}
	return []byte(`{"input":[{"content":"` + strings.Repeat("x", size) + `","type":"message"` + id + `}],"prompt_cache_key":"unchanged"}`)
}

func TestCodexInputIDScanStillRepairsLateItems(t *testing.T) {
	for _, last := range []string{
		`{"type":"message","id":"raw"}`,
		fmt.Sprintf(`{"type":"message","id":%q}`, strings.Repeat("界", 65)),
		fmt.Sprintf(`{"type":"reasoning","id":%q,"encrypted_content":"opaque"}`, strings.Repeat("x", 65)),
	} {
		body := []byte(`{"input":[{"type":"message","content":"unchanged"},{"type":"message","id":"msg_existing"},` + last + `]}`)
		got := SanitizeCodexInputItemIDs(body)
		if gjson.GetBytes(got, "input.0.content").String() != "unchanged" || gjson.GetBytes(got, "input.1.id").String() != "msg_existing" {
			t.Fatal("late ID repair changed earlier items")
		}
		if gjson.Get(last, "type").String() == "reasoning" {
			if gjson.GetBytes(got, "input.#").Int() != 2 {
				t.Fatal("late oversized encrypted item was not dropped")
			}
		} else if id := gjson.GetBytes(got, "input.2.id").String(); !strings.HasPrefix(id, "msg_") || utf8.RuneCountInString(id) > 64 {
			t.Fatal("late ID bypassed prefix or length repair")
		}
	}
}

func TestCodexInputIDScanKeepsLargeUnchangedItems(t *testing.T) {
	for _, withID := range []bool{false, true} {
		body := codexIDBenchmarkPayload(1<<20, withID)
		original := bytes.Clone(body)
		if got := SanitizeCodexInputItemIDs(body); !bytes.Equal(got, original) || !bytes.Equal(body, original) {
			t.Fatal("large text or an already valid ID changed during normalization")
		}
	}
}

func BenchmarkCodexInputIDScan(b *testing.B) {
	for _, size := range []int{128, 1 << 20, 10 << 20} {
		for _, withID := range []bool{false, true} {
			b.Run(fmt.Sprintf("bytes=%d/id=%t", size, withID), func(b *testing.B) {
				body := codexIDBenchmarkPayload(size, withID)
				b.SetBytes(int64(len(body)))
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					SanitizeCodexInputItemIDs(body)
				}
			})
		}
	}
}
