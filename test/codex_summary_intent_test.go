package test

import (
	"bytes"
	"fmt"
	"testing"

	_ "github.com/router-for-me/CLIProxyAPI/v6/internal/translator"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexSummaryConversionDoesNotInferOptIn(t *testing.T) {
	for _, tc := range []struct{ name, format, source, effort, summary string }{
		{"chat defaults", "openai", `{"messages":[{"role":"user","content":"fixture"}]}`, "medium", ""},
		{"chat legacy effort", "openai", `{"messages":[],"reasoning_effort":"high"}`, "high", "auto"},
		{"chat none", "openai", `{"messages":[],"reasoning_effort":"none"}`, "none", ""},
		{"chat explicit hidden", "openai", `{"messages":[],"reasoning_effort":"high","reasoning":{"exclude":true}}`, "high", ""},
		{"chat google hide wins", "openai", `{"messages":[],"reasoning_effort":"high","extra_body":{"google":{"thinking_config":{"include_thoughts":false}}}}`, "high", ""},
		{"claude defaults", "claude", `{"messages":[{"role":"user","content":"fixture"}]}`, "medium", ""},
		{"claude effort only", "claude", `{"messages":[],"thinking":{"type":"adaptive"},"output_config":{"effort":"high"}}`, "high", ""},
		{"claude show", "claude", `{"messages":[],"thinking":{"type":"adaptive","display":"summarized"},"output_config":{"effort":"high"}}`, "high", "auto"},
		{"claude hide", "claude", `{"messages":[],"thinking":{"type":"adaptive","display":"omitted"},"output_config":{"effort":"high"}}`, "high", ""},
		{"gemini defaults", "gemini", `{"contents":[{"role":"user","parts":[{"text":"fixture"}]}]}`, "medium", ""},
		{"gemini budget only", "gemini", `{"contents":[],"generationConfig":{"thinkingConfig":{"thinkingBudget":8192}}}`, "medium", ""},
		{"gemini show", "gemini", `{"contents":[],"generationConfig":{"thinkingConfig":{"thinkingBudget":8192,"includeThoughts":true}}}`, "medium", "auto"},
		{"gemini hide", "gemini", `{"contents":[],"generationConfig":{"thinkingConfig":{"thinkingBudget":8192,"includeThoughts":false}}}`, "medium", ""},
	} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%t", tc.name, stream), func(t *testing.T) {
				source := []byte(tc.source)
				before := bytes.Clone(source)
				out, err := translator.TranslateRequestChecked(translator.FromString(tc.format), translator.FormatCodex, "summary-fixture", source, stream)
				if err != nil {
					t.Fatal(err)
				}
				if got := gjson.GetBytes(out, "reasoning.effort").String(); got != tc.effort {
					t.Fatalf("effort=%q want=%q", got, tc.effort)
				}
				got := gjson.GetBytes(out, "reasoning.summary")
				if got.String() != tc.summary || (tc.summary == "" && got.Exists()) {
					t.Fatalf("summary=%s want=%q", got.Raw, tc.summary)
				}
				if gjson.GetBytes(out, "include.0").String() != "reasoning.encrypted_content" {
					t.Fatal("changed encrypted reasoning replay request")
				}
				if !bytes.Equal(source, before) {
					t.Fatal("mutated source input")
				}
			})
		}
	}
}
