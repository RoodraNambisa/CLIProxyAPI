package thinking

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestResponsesSummaryExtractionBoundaries(t *testing.T) {
	for _, fixture := range []struct {
		body string
		want SummaryConfig
	}{
		{`{"reasoning":{"summary":"auto"},"reasoning":{"summary":"none"}}`, SummaryConfig{Mode: SummaryEnabled, Detail: "auto"}},
		{`{"reasoning":null,"reasoning":{"summary":"auto"}}`, SummaryConfig{Mode: SummaryEnabled, Detail: "auto"}},
		{`{"reasoning":{"generate_summary":"concise"},"reasoning":{"summary":"auto"}}`, SummaryConfig{Mode: SummaryEnabled, Detail: "auto"}},
		{`{"reasoning":{"summary":true,"generate_summary":"concise"}}`, SummaryConfig{Mode: SummaryEnabled, Detail: "concise"}},
		{`{"rea\u0073oning":{"sum\u006dary":"auto"}}`, SummaryConfig{Mode: SummaryEnabled, Detail: "auto"}},
		{`{"reasoning":{"generate_\u0073ummary":"none"}}`, SummaryConfig{Mode: SummaryDisabled}},
		{`{"input":[{"type":"message","content":{"summary":"auto"}}]}`, SummaryConfig{}},
		{`{"reasoning":{"summary":"auto"},"input":invalid}`, SummaryConfig{}},
		{`{"reasoning":{"summary":"auto"}}junk`, SummaryConfig{}},
		{`{"reasoning":{"summary":null},"input":invalid}`, SummaryConfig{}},
		{`{"input":"` + strings.Repeat("x", 1<<20) + `","reasoning":{"summary":"detailed"}}`, SummaryConfig{Mode: SummaryEnabled, Detail: "detailed"}},
	} {
		for _, format := range []string{"codex", "openai-response"} {
			body := []byte(fixture.body)
			got := ExtractSummaryConfig(body, format)
			if got != fixture.want {
				t.Fatalf("format=%s summary=%+v want=%+v", format, got, fixture.want)
			}
			for i := range body {
				body[i] = 'x'
			}
			if got != fixture.want {
				t.Fatal("returned summary retains caller-owned mutable bytes")
			}
		}
	}
}

func BenchmarkResponsesSummaryExtraction(b *testing.B) {
	for _, size := range []int{128, 1 << 20, 10 << 20} {
		for _, present := range []bool{false, true} {
			b.Run(fmt.Sprintf("bytes=%d/present=%t", size, present), func(b *testing.B) {
				body := append([]byte(`{"input":"`), bytes.Repeat([]byte("x"), size)...)
				body = append(body, '"')
				if present {
					body = append(body, `,"reasoning":{"summary":"auto"}`...)
				}
				body = append(body, '}')
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					ExtractSummaryConfig(body, "codex")
				}
			})
		}
	}
}
