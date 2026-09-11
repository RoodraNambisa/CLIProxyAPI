package thinking

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestChatSummaryLargeRequestBoundaries(t *testing.T) {
	content, err := json.Marshal(strings.Repeat("text \"reasoning\": null\n \\u0061 ", 8192))
	if err != nil {
		t.Fatal(err)
	}
	largeMessage := `"messages":[{"role":"user","content":` + string(content) + `}]`
	for _, fixture := range []struct {
		name     string
		fields   string
		want     SummaryConfig
		explicit SummaryConfig
	}{
		{name: "absent"},
		{name: "effort", fields: `"reasoning_effort":"high"`, want: SummaryConfig{Mode: SummaryEnabled, Detail: "auto"}},
		{name: "escaped effort", fields: `"reasoning_\u0065ffort":"high"`, want: SummaryConfig{Mode: SummaryEnabled, Detail: "auto"}},
		{name: "case sensitive", fields: `"Reasoning_effort":"high"`},
		{name: "explicit hide", fields: `"reasoning":{"summary":"none"},"reasoning_effort":"high"`, want: SummaryConfig{Mode: SummaryDisabled}, explicit: SummaryConfig{Mode: SummaryDisabled}},
		{name: "google precedence", fields: `"extra_body":{"google":{"thinking_config":{"include_thoughts":false}}},"reasoning":{"summary":"auto"}`, want: SummaryConfig{Mode: SummaryDisabled}, explicit: SummaryConfig{Mode: SummaryDisabled}},
		{name: "escaped nested keys", fields: `"extra_\u0062ody":{"google":{"thinking_config":{"include_\u0074houghts":true}}}`, want: SummaryConfig{Mode: SummaryEnabled, Detail: "auto"}, explicit: SummaryConfig{Mode: SummaryEnabled, Detail: "auto"}},
		{name: "nested duplicate", fields: `"reasoning":null,"reasoning":{"summary":"detailed"}`, want: SummaryConfig{Mode: SummaryEnabled, Detail: "detailed"}, explicit: SummaryConfig{Mode: SummaryEnabled, Detail: "detailed"}},
		{name: "scalar duplicate", fields: `"include_reasoning":null,"include_reasoning":true`},
		{name: "first valid value", fields: `"reasoning":{"summary":"concise"},"reasoning":{"summary":"auto"}`, want: SummaryConfig{Mode: SummaryEnabled, Detail: "concise"}, explicit: SummaryConfig{Mode: SummaryEnabled, Detail: "concise"}},
		{name: "invalid alias falls through", fields: `"thinking":{"includeThoughts":"false"},"include_reasoning":true`, want: SummaryConfig{Mode: SummaryEnabled, Detail: "auto"}, explicit: SummaryConfig{Mode: SummaryEnabled, Detail: "auto"}},
		{name: "business metadata", fields: `"metadata":{"reasoning":{"summary":"auto"},"include_reasoning":true}`},
	} {
		for _, fieldsFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/fields-first=%t", fixture.name, fieldsFirst), func(t *testing.T) {
				parts := []string{largeMessage}
				if fixture.fields != "" {
					if fieldsFirst {
						parts = append([]string{fixture.fields}, parts...)
					} else {
						parts = append(parts, fixture.fields)
					}
				}
				body := []byte("{" + strings.Join(parts, ",") + "}")
				original := bytes.Clone(body)
				got := ExtractSummaryConfig(body, "openai")
				explicit := ExtractExplicitSummaryConfig(body, "openai")
				if got != fixture.want || explicit != fixture.explicit {
					t.Fatalf("summary=%+v explicit=%+v, want %+v and %+v", got, explicit, fixture.want, fixture.explicit)
				}
				if !bytes.Equal(body, original) {
					t.Fatal("summary extraction modified the caller's body")
				}
				for i := range body {
					body[i] = 'x'
				}
				if got != fixture.want || explicit != fixture.explicit {
					t.Fatal("summary retained the caller's mutable body")
				}
				invalid := append(original[:len(original)-1], `,"unrelated":invalid}`...)
				if got := ExtractSummaryConfig(invalid, "openai"); got != (SummaryConfig{}) {
					t.Fatalf("accepted summary from invalid JSON: %+v", got)
				}
				if got := ExtractExplicitSummaryConfig(invalid, "openai"); got != (SummaryConfig{}) {
					t.Fatalf("accepted explicit summary from invalid JSON: %+v", got)
				}
			})
		}
	}
}

func BenchmarkChatSummaryLargeRequest(b *testing.B) {
	for _, size := range []int{128, 1 << 20, 10 << 20} {
		for _, effort := range []bool{false, true} {
			b.Run(fmt.Sprintf("bytes=%d/effort=%t", size, effort), func(b *testing.B) {
				body := append([]byte(`{"messages":[{"role":"user","content":"`), bytes.Repeat([]byte("x"), size)...)
				body = append(body, `"}]`...)
				if effort {
					body = append(body, `,"reasoning_effort":"high"`...)
				}
				body = append(body, '}')
				b.SetBytes(int64(len(body)))
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					ExtractSummaryConfig(body, "openai")
				}
			})
		}
	}
}
