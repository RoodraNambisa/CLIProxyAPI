package helps

import (
	"bytes"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestClaudeToolResultDedupKeepsEmptyFinalValuesAndRequestIsolation(t *testing.T) {
	for _, source := range []translator.Format{translator.FormatOpenAIResponse, translator.FormatOpenAI} {
		for _, compat := range []bool{false, true} {
			for _, final := range []string{`""`, `null`, `{"number":9007199254740993}`} {
				raw := []byte(`{"input":[{"type":"function_call_output","call_id":"paired","output":"old","cache_control":{"type":"ephemeral"}},{"type":"function_call_output","call_id":"paired","output":` + final + `}]}`)
				if source == translator.FormatOpenAI {
					raw = []byte(`{"messages":[{"role":"tool","tool_call_id":"paired","content":"old","cache_control":{"type":"ephemeral"}},{"role":"tool","tool_call_id":"paired","content":` + final + `}]}`)
				}
				before := bytes.Clone(raw)
				out := TranslateRequestWithAPIKeyModelCompatibility(source, translator.FormatClaude, "claude-fixture", raw, false, compat)
				if !bytes.Equal(before, raw) {
					t.Fatal("deduplication changed the original request")
				}
				clear(raw)
				part := gjson.GetBytes(out, "messages.0.content.0")
				want := ""
				if final[0] == '{' {
					want = final
				} else if source == translator.FormatOpenAI && final == "null" {
					want = "null"
				}
				if gjson.GetBytes(out, "messages.#").Int() != 1 || gjson.GetBytes(out, "messages.0.content.#").Int() != 1 ||
					part.Get("content").String() != want || part.Get("cache_control").Exists() || part.Get("tool_use_id").String() != "paired" {
					t.Fatalf("final result mismatch: source=%s compat=%t value=%s messages=%d parts=%d content=%q cache=%t", source, compat, final,
						gjson.GetBytes(out, "messages.#").Int(), gjson.GetBytes(out, "messages.0.content.#").Int(), part.Get("content").String(), part.Get("cache_control").Exists())
				}
			}
		}
	}
}
