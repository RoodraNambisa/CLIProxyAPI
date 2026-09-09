package claude

import (
	"fmt"
	"testing"

	"github.com/tidwall/gjson"
)

func TestClaudeStructuredOutputInfersStrictWithoutOverridingCaller(t *testing.T) {
	for _, tt := range []struct {
		name, schema string
		optional     bool
	}{
		{"empty", `{}`, false},
		{"required", `{"properties":{"value":{"type":"string"}},"required":["value"]}`, false},
		{"optional", `{"properties":{"value":{"type":"string"}}}`, true},
		{"mixed", `{"properties":{"one":{},"two":{}},"required":["one"]}`, true},
		{"nested", `{"properties":{"value":{"properties":{"nested":{}}}},"required":["value"]}`, true},
		{"array", `{"items":{"properties":{"value":{}}}}`, true},
		{"definition", `{"$defs":{"nested":{"properties":{"value":{}}}}}`, true},
		{"union", `{"anyOf":[{"type":"null"},{"properties":{"value":{}}}]}`, true},
		{"examples", `{"default":{"properties":{"value":{}}},"examples":[{"properties":{"value":{}}}]}`, false},
	} {
		for _, strict := range []string{"", `,"strict":null`, `,"strict":true`, `,"strict":false`} {
			for _, stream := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/stream=%t", tt.name, strict, stream), func(t *testing.T) {
					body := []byte(`{"messages":[],"output_config":{"format":{"type":"json_schema","schema":` + tt.schema + strict + `}}}`)
					got := ConvertClaudeRequestToCodex("gpt-5.4-mini", body, stream)
					wantStrict := !tt.optional
					if strict == `,"strict":true` {
						wantStrict = true
					} else if strict == `,"strict":false` {
						wantStrict = false
					}
					if gjson.GetBytes(got, "text.format.strict").Bool() != wantStrict || gjson.GetBytes(got, "text.format.schema").Raw != tt.schema {
						t.Fatal("implicit strict mode rejected optional fields, or an explicit choice/schema was changed")
					}
				})
			}
		}
	}
}
