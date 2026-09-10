package helps

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func codexIDBenchmarkPayload(size int, withID bool) []byte {
	id := ""
	if withID {
		id = `,"id":"msg_existing"`
	}
	return []byte(`{"input":[{"content":"` + strings.Repeat("x", size) + `","type":"message"` + id + `}],"prompt_cache_key":"unchanged"}`)
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
