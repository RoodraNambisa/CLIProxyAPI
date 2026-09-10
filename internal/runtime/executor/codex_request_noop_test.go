package executor

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"

	codexclaude "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/codex/claude"
	codexgemini "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/codex/gemini"
	codexinteractions "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/codex/interactions"
	"github.com/tidwall/gjson"
)

type codexRequestNoopFixture struct {
	name      string
	translate func(string, []byte, bool) []byte
	input     func(string, string) []byte
}

func codexRequestNoopFixtures() []codexRequestNoopFixture {
	return []codexRequestNoopFixture{
		{"claude", codexclaude.ConvertClaudeRequestToCodex, func(text, schema string) []byte {
			return []byte(`{"messages":[{"role":"user","content":` + text + `}],"tools":[{"type":"function","name":123,"strict":false,"input_schema":` + schema + `,"cache_control":{"type":"ephemeral"},"defer_loading":false}]}`)
		}},
		{"gemini", codexgemini.ConvertGeminiRequestToCodex, func(text, schema string) []byte {
			return []byte(`{"contents":[{"role":"user","parts":[{"text":` + text + `}]}],"toolConfig":{"functionCallingConfig":{"mode":"AUTO"}},"tools":[{"functionDeclarations":[{"name":123,"parameters":` + schema + `}]}]}`)
		}},
		{"interactions", codexinteractions.ConvertInteractionsRequestToCodex, func(text, schema string) []byte {
			return []byte(`{"input":` + text + `,"stream":true,"store":false,"tool_choice":"auto","metadata":{"kept":false},"tools":[{"name":123,"parameters":` + schema + `}]}`)
		}},
	}
}

func TestCodexRequestCanonicalFieldsKeepNormalization(t *testing.T) {
	for _, fixture := range codexRequestNoopFixtures() {
		for _, changed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/changed%t", fixture.name, changed), func(t *testing.T) {
				schema := `{"type":"object","properties":{"value":{"type":"string"}},"additionalProperties":false}`
				if changed {
					schema = `{"$schema":"draft","type":"object","properties":{"value":{"type":"string"}},"additionalProperties":true}`
				}
				raw := fixture.input(`"message"`, schema)
				before := bytes.Clone(raw)
				out := fixture.translate("fixture", raw, true)
				tool := gjson.GetBytes(out, "tools.0")
				propertiesType := gjson.False
				if fixture.name == "claude" && changed {
					propertiesType = gjson.True
				}
				if tool.Get("name").Type != gjson.String || tool.Get("name").String() != "123" || tool.Get("type").String() != "function" || tool.Get("strict").Type != gjson.False || tool.Get("parameters.additionalProperties").Type != propertiesType {
					t.Fatalf("tool normalization changed: %s", tool.Raw)
				}
				for _, path := range []string{"parameters.$schema", "cache_control", "defer_loading", "input_schema"} {
					if tool.Get(path).Exists() {
						t.Fatalf("unexpected retained field: %s", path)
					}
				}
				if !bytes.Equal(raw, before) {
					t.Fatal("source request was modified")
				}
				if fixture.name == "interactions" && (gjson.GetBytes(out, "store").Type != gjson.False || gjson.GetBytes(out, "metadata.kept").Type != gjson.False) {
					t.Fatal("explicit false fields lost")
				}
			})
		}
	}
}

func TestCodexGeminiCanonicalFieldsStillNormalizeChangedSchema(t *testing.T) {
	for _, field := range []string{"parameters", "parametersJsonSchema"} {
		raw := []byte(`{"contents":[],"tools":[{"functionDeclarations":[{"name":"fixture","` + field + `":{"type":"OBJECT","$schema":null,"additionalProperties":null,"properties":{"value":{"type":"STRING"},"union":{"type":["string","null"]},"untyped":{"type":42}}}}]}],"toolConfig":{"functionCallingConfig":{"mode":"AUTO"}}}`)
		out := codexgemini.ConvertGeminiRequestToCodex("fixture", raw, true)
		params := gjson.GetBytes(out, "tools.0.parameters")
		if params.Get("type").String() != "object" || params.Get("properties.value.type").String() != "string" || params.Get("$schema").Exists() || params.Get("additionalProperties").Type != gjson.False {
			t.Fatalf("schema normalization failed: %s", params.Raw)
		}
		if !params.Get("properties.union.type").IsArray() || params.Get("properties.untyped.type").Raw != "42" || gjson.GetBytes(out, "tool_choice").String() != "auto" {
			t.Fatal("non-string schema types or auto choice changed")
		}
	}
}

func BenchmarkCodexRequestCanonicalFields(b *testing.B) {
	for _, fixture := range codexRequestNoopFixtures() {
		for _, size := range []int{128, 1 << 20} {
			b.Run(fmt.Sprintf("%s/bytes%d", fixture.name, size), func(b *testing.B) {
				var schema strings.Builder
				schema.WriteString(`{"type":"object","additionalProperties":false,"properties":{`)
				for i := 0; i < 20; i++ {
					if i > 0 {
						schema.WriteByte(',')
					}
					fmt.Fprintf(&schema, `"field%d":{"type":"string"}`, i)
				}
				schema.WriteString(`}}`)
				raw := fixture.input(strconv.Quote(strings.Repeat("x", size)), schema.String())
				b.ReportAllocs()
				b.SetBytes(int64(len(raw)))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if len(fixture.translate("fixture", raw, true)) == 0 {
						b.Fatal("missing request")
					}
				}
			})
		}
	}
}
