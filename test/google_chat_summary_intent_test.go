package test

import (
	"fmt"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestGoogleChatSummaryUsesExplicitControlsAndLegacyEffort(t *testing.T) {
	for _, target := range []string{"gemini", "antigravity"} {
		for _, tc := range []struct{ name, fields, want string }{
			{"model name no opt in", ``, ""},
			{"effort", `,"reasoning_effort":"high"`, "true"},
			{"auto effort", `,"reasoning_effort":"auto"`, "true"},
			{"none effort", `,"reasoning_effort":"none"`, "false"},
			{"explicit hide", `,"reasoning_effort":"high","reasoning":{"exclude":true}`, "false"},
			{"legacy hide", `,"reasoning_effort":"high","include_reasoning":false`, "false"},
			{"Google extension hide", `,"reasoning_effort":"high","extra_body":{"google":{"thinking_config":{"include_thoughts":false}}}`, "false"},
			{"Google extension priority", `,"reasoning":{"exclude":true},"extra_body":{"google":{"thinking_config":{"include_thoughts":true}}}`, "true"},
			{"invalid exclude", `,"reasoning":{"exclude":"false"}`, ""},
			{"invalid summary bool", `,"reasoning":{"summary":true}`, ""},
			{"invalid bool does not hide effort", `,"reasoning_effort":"high","reasoning":{"exclude":"true"}`, "true"},
		} {
			for _, stream := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/stream=%t", target, tc.name, stream), func(t *testing.T) {
					body := []byte(`{"messages":[{"role":"user","content":"fixture"}]` + tc.fields + `}`)
					out, err := translator.TranslateRequestChecked(translator.FormatOpenAI, translator.FromString(target), "gemini-3.1-pro-low", body, stream)
					if err != nil {
						t.Fatal(err)
					}
					path := "generationConfig.thinkingConfig.includeThoughts"
					if target == "antigravity" {
						path = "request." + path
					}
					got := gjson.GetBytes(out, path)
					if got.Raw != tc.want {
						t.Fatalf("summary visibility=%s want=%q", got.Raw, tc.want)
					}
				})
			}
		}
	}
}
