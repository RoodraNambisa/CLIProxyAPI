package thinking

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// referenceResponsesSummaryApply retains the original mutation sequence to
// check duplicate-key, alias removal and canonical JSON encoding behavior.
func referenceResponsesSummaryApply(body []byte, config SummaryConfig) []byte {
	if config.Mode == SummaryUnspecified || !gjson.ValidBytes(body) {
		return body
	}
	if config.Mode == SummaryEnabled {
		body, _ = sjson.SetBytes(body, "reasoning.summary", normalizedSummaryDetail(config.Detail))
		body, _ = sjson.DeleteBytes(body, "reasoning.generate_summary")
		return body
	}
	body, _ = sjson.DeleteBytes(body, "reasoning.summary")
	body, _ = sjson.DeleteBytes(body, "reasoning.generate_summary")
	if reasoning := gjson.GetBytes(body, "reasoning"); reasoning.IsObject() && len(reasoning.Map()) == 0 {
		body, _ = sjson.DeleteBytes(body, "reasoning")
	}
	return body
}

func TestResponsesSummaryApplyKeepsMutationSemantics(t *testing.T) {
	for _, fixture := range []string{
		`{}`, `{"reasoning":null}`, `{"reasoning":[]}`, `{"reasoning":true}`,
		`{"reasoning":{"summary":"auto","effort":"high"}}`,
		`{"reasoning":{"summary":"none","generate_summary":null}}`,
		`{"reasoning":{"summary":false,"generate_summary":"concise"}}`,
		`{"reasoning":{"summary":null,"summary":"auto"}}`,
		`{"reasoning":null,"reasoning":{"summary":"auto"}}`,
		`{"reasoning":{},"reasoning":{"generate_summary":"detailed"}}`,
		`{"reasoning":{"summary":"auto","generate_summary":null,"generate_summary":"auto"}}`,
		`{"rea\u0073oning":{"sum\u006dary":"\u0061uto","generate_\u0073ummary":null}}`,
		`{"reasoning":{"summary":" CONCISE "},"metadata":{"reasoning":{"summary":"auto"}}}`,
		`{"reasoning":{"effort":"high"}}`, `{"reasoning": { } }`, `[]`, `null`,
		`{"reasoning":{"summary":"auto"},"input":invalid}`,
	} {
		for _, config := range []SummaryConfig{{}, {Mode: SummaryDisabled}, {Mode: SummaryEnabled}, {Mode: SummaryEnabled, Detail: "detailed"}} {
			body := []byte(fixture)
			want := referenceResponsesSummaryApply(body, config)
			for _, format := range []string{"codex", "openai-response"} {
				got := ApplySummaryConfig(body, format, config)
				if !bytes.Equal(got, want) || string(body) != fixture {
					t.Fatalf("%s config=%+v input=%s: got %s, want %s; source=%s", format, config, fixture, got, want, body)
				}
			}
		}
	}
}

func TestResponsesSummaryApplyDoesNotCopyUnchangedLargeBody(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		body := []byte(`{"input":"` + strings.Repeat("x", 1<<20) + `","reasoning":{"effort":"high"`)
		config := SummaryConfig{Mode: SummaryDisabled}
		if enabled {
			body = append(body, `,"summary":"auto"`...)
			config.Mode = SummaryEnabled
		}
		body = append(body, `}}`...)
		if allocs := testing.AllocsPerRun(5, func() {
			got := ApplySummaryConfig(body, "codex", config)
			if len(got) != len(body) || &got[0] != &body[0] {
				t.Fatal("unchanged summary rewrote the body")
			}
		}); allocs != 0 {
			t.Fatalf("enabled=%t: unchanged summary allocated %g times", enabled, allocs)
		}
	}
}

func TestResponsesSummaryApplyChangedOutputOwnsBody(t *testing.T) {
	body := []byte(`{"input":"` + strings.Repeat("x", 1<<20) + `","reasoning":{"summary":"auto","generate_summary":"auto"}}`)
	out := ApplySummaryConfig(body, "codex", SummaryConfig{Mode: SummaryEnabled, Detail: "detailed"})
	want := bytes.Clone(out)
	for i := range body {
		body[i] = 'x'
	}
	if !bytes.Equal(out, want) || gjson.GetBytes(out, "reasoning.summary").String() != "detailed" || gjson.GetBytes(out, "reasoning.generate_summary").Exists() {
		t.Fatal("changed summary output retained mutable source data or lost the update")
	}
}

func BenchmarkResponsesSummaryApplyLargeBody(b *testing.B) {
	for _, size := range []int{128, 1 << 20, 10 << 20} {
		for _, enabled := range []bool{false, true} {
			b.Run(fmt.Sprintf("bytes=%d/enabled=%t", size, enabled), func(b *testing.B) {
				body := []byte(`{"input":"` + strings.Repeat("x", size) + `","reasoning":{"effort":"high"`)
				config := SummaryConfig{Mode: SummaryDisabled}
				if enabled {
					body = append(body, `,"summary":"auto"`...)
					config.Mode = SummaryEnabled
				}
				body = append(body, `}}`...)
				b.SetBytes(int64(len(body)))
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					ApplySummaryConfig(body, "codex", config)
				}
			})
		}
	}
}

func FuzzResponsesSummaryApplyKeepsMutationSemantics(f *testing.F) {
	for _, body := range []string{`{}`, `{"reasoning":null,"reasoning":{"summary":"auto"}}`, `{"reasoning":{"summary":null,"summary":"auto","generate_summary":"auto"}}`, `{"rea\u0073oning":{"summary":"\u0061uto"}}`} {
		f.Add([]byte(body))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		if len(body) > 1<<20 || !gjson.ValidBytes(body) {
			return
		}
		for _, config := range []SummaryConfig{{Mode: SummaryEnabled}, {Mode: SummaryDisabled}} {
			if got, want := ApplySummaryConfig(body, "codex", config), referenceResponsesSummaryApply(body, config); !bytes.Equal(got, want) {
				t.Fatalf("config=%+v input=%s: got %s, want %s", config, body, got, want)
			}
		}
	})
}
