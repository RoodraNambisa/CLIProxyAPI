package gemini

import (
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestGeminiCodexDropsHiddenThoughtParts(t *testing.T) {
	for _, systemKey := range []string{"systemInstruction", "system_instruction"} {
		t.Run(systemKey, func(t *testing.T) {
			payload := []byte(`{"` + systemKey + `":{"parts":[{"thought":true,"text":"hidden system"},{"text":"public system"}]},"contents":[{"role":"model","parts":[{"thought":true,"text":"hidden reasoning"},{"thought":true,"functionCall":{"id":"hidden_id","name":"hidden_tool"}},{"thought":false,"text":"visible answer"}]},{"role":"user","parts":[{"text":"ask"}]}]}`)
			for _, stream := range []bool{false, true} {
				out := ConvertGeminiRequestToCodex("gpt-5.4", payload, stream)
				if strings.Contains(string(out), "hidden") || gjson.GetBytes(out, "input.#").Int() != 3 {
					t.Fatalf("hidden parts reached Codex: %s", out)
				}
				for i, want := range []string{"public system", "visible answer", "ask"} {
					if got := gjson.GetBytes(out, "input").Array()[i].Get("content.0.text").String(); got != want {
						t.Fatal("visible content order changed")
					}
				}
			}
		})
	}
	for _, payload := range []string{
		`{"contents":[{"role":"model","parts":[{"thought":true,"text":"hidden"}]}]}`,
		`{"systemInstruction":{"parts":[{"thought":true,"text":"hidden"}]}}`,
	} {
		if out := ConvertGeminiRequestToCodex("gpt-5.4", []byte(payload), false); gjson.GetBytes(out, "input.#").Int() != 0 {
			t.Fatal("hidden-only content created an empty message")
		}
	}
}
