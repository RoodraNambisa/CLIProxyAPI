package thinking

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
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

func TestChatSummaryProjectionExcludesUnrelatedNestedPayloads(t *testing.T) {
	large, err := json.Marshal(strings.Repeat("unrelated", 1<<17))
	if err != nil {
		t.Fatal(err)
	}
	for _, fixture := range []struct {
		name, fields string
		want         SummaryConfig
	}{
		{"extra body", `"extra_body":{"unrelated_blob":` + string(large) + `}`, SummaryConfig{}},
		{"nested extra body", `"extra_body":{"extra_body":{"unrelated_blob":` + string(large) + `,"google":{"thinking_config":{"include_thoughts":true}}}}`, SummaryConfig{Mode: SummaryEnabled, Detail: "auto"}},
		{"google unrelated", `"google":{"unrelated_blob":` + string(large) + `,"thinking_config":{"includeThoughts":false}}`, SummaryConfig{Mode: SummaryDisabled}},
		{"reasoning unrelated", `"reasoning":{"unrelated_blob":` + string(large) + `,"summary":"concise"}`, SummaryConfig{Mode: SummaryEnabled, Detail: "concise"}},
		{"thinking unrelated", `"thinking":{"unrelated_blob":` + string(large) + `,"includeThoughts":true}`, SummaryConfig{Mode: SummaryEnabled, Detail: "auto"}},
		{"generation unrelated", `"generationConfig":{"thinkingConfig":{"unrelated_blob":` + string(large) + `,"includeThoughts":false}}`, SummaryConfig{Mode: SummaryDisabled}},
		{"nonobject duplicate", `"extra_body":` + string(large) + `,"extra_body":{"google":{"thinking_config":{"include_thoughts":false}}}`, SummaryConfig{Mode: SummaryDisabled}},
		{"duplicate leaf", `"reasoning":{"unrelated_blob":` + string(large) + `,"summary":null,"summary":"auto"}`, SummaryConfig{Mode: SummaryDisabled}},
		{"escaped container", `"extra_\u0062ody":{"unrelated_blob":` + string(large) + `,"google":{"thinking_config":{"include_thoughts":true}}}`, SummaryConfig{Mode: SummaryEnabled, Detail: "auto"}},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			body := []byte(`{"messages":[],` + fixture.fields + `}`)
			original := bytes.Clone(body)
			projected := openAISummaryFields(body)
			if len(projected) > 1024 {
				t.Fatalf("summary metadata copied unrelated payload: %d bytes", len(projected))
			}
			if got := ExtractSummaryConfig(body, "openai"); got != fixture.want {
				t.Fatalf("summary=%+v, want %+v", got, fixture.want)
			}
			if got := ExtractExplicitSummaryConfig(body, "openai"); got != fixture.want {
				t.Fatalf("explicit summary=%+v, want %+v", got, fixture.want)
			}
			if !bytes.Equal(body, original) {
				t.Fatal("projection changed the source request")
			}
		})
	}
}

func FuzzChatSummaryProjectionKeepsExplicitIntent(f *testing.F) {
	for _, body := range []string{
		`{"extra_body":{"google":null,"google":{"thinking_config":{"include_thoughts":true}}}}`,
		`{"reasoning":null,"reasoning":{"summary":"auto"}}`,
		`{"reasoning":{"summary":null,"summary":"none","enabled":true}}`,
		`{"extra_\u0062ody":{"extra_body":{"google":{"thinking_config":{"includeThoughts":false}}}}}`,
		`{"thinking":{"includeThoughts":false},"include_reasoning":true}`,
	} {
		f.Add([]byte(body))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		if len(body) > 1<<20 || !gjson.ValidBytes(body) {
			return
		}
		want, _ := extractOpenAIExplicitSummaryConfig(body)
		if got := ExtractExplicitSummaryConfig(body, "openai"); got != want {
			t.Fatalf("projected intent=%+v, original intent=%+v for %s", got, want, body)
		}
	})
}
