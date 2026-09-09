package common

import (
	"fmt"
	"strings"
	"testing"
)

func BenchmarkResponsesUsageLargeOutput(b *testing.B) {
	for _, size := range []int{1024, 1 << 20, 10 << 20} {
		b.Run(fmt.Sprintf("bytes=%d", size), func(b *testing.B) {
			payload := []byte(`{"output":[{"type":"image_generation_call","result":"` + strings.Repeat("x", size) + `"}],"usage":{"input_tokens":2,"output_tokens":1,"total_tokens":3}}`)
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if result := EnsureResponsesUsageDetails(payload); len(result) <= len(payload) {
					b.Fatal("usage details were not added")
				}
			}
		})
	}
}
