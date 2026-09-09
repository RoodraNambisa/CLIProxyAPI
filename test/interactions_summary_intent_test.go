package test

import (
	"fmt"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestInteractionsSummaryConversionAcceptsOnlySupportedSelectors(t *testing.T) {
	for _, target := range []string{"codex", "gemini", "antigravity"} {
		for _, key := range []string{"generation_config.thinking_summaries", "generation_config.thinkingSummaries", "generationConfig.thinkingSummaries"} {
			for _, value := range []any{"auto", "none", "detailed", "concise", "off", "false", true, false, nil, 1} {
				for _, stream := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/%v/%t", target, key, value, stream), func(t *testing.T) {
						body, err := sjson.SetBytes([]byte(`{"input":"fixture"}`), key, value)
						if err != nil {
							t.Fatal(err)
						}
						out, err := translator.TranslateRequestChecked(translator.FormatInteractions, translator.FromString(target), "summary-fixture", body, stream)
						if err != nil {
							t.Fatal(err)
						}
						path := "generationConfig.thinkingConfig.includeThoughts"
						want := ""
						if value == "auto" {
							want = "true"
						} else if value == "none" {
							want = "false"
						}
						if target == "codex" {
							path = "reasoning.summary"
							want = ""
							if value == "auto" {
								want = `"auto"`
							}
						} else if target == "antigravity" {
							path = "request." + path
						}
						if got := gjson.GetBytes(out, path); got.Raw != want {
							t.Fatalf("visibility=%s want=%q", got.Raw, want)
						}
					})
				}
			}
		}
	}
}

func TestInteractionsReasoningAmountDoesNotEnableAntigravitySummary(t *testing.T) {
	for _, effort := range []string{"auto", "high", "none"} {
		body, err := sjson.SetBytes([]byte(`{"input":"fixture"}`), "reasoning.effort", effort)
		if err != nil {
			t.Fatal(err)
		}
		out, err := translator.TranslateRequestChecked(translator.FormatInteractions, translator.FromString("antigravity"), "summary-fixture", body, false)
		if err != nil || gjson.GetBytes(out, "request.generationConfig.thinkingConfig.includeThoughts").Exists() {
			t.Fatalf("amount enabled summary: %s; %v", out, err)
		}
	}
}
