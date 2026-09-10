package claude

import (
	"testing"

	"github.com/tidwall/gjson"
)

func TestClaudeCodexServiceTierPreservesExplicitFastIntent(t *testing.T) {
	for _, tc := range []struct {
		name   string
		fields string
		want   string
	}{
		{"missing", ``, ""},
		{"speed fast", `,"speed":"fast"`, "priority"},
		{"fast overrides auto", `,"speed":"fast","service_tier":"auto"`, "priority"},
		{"service priority", `,"service_tier":"priority"`, "priority"},
		{"service fast", `,"service_tier":"fast"`, "priority"},
		{"service normalized", `,"service_tier":" PRIORITY "`, "priority"},
		{"ordinary speed", `,"speed":"standard"`, ""},
		{"speed case is exact", `,"speed":"Fast"`, ""},
		{"speed does not erase tier", `,"speed":"standard","service_tier":"priority"`, "priority"},
		{"auto", `,"service_tier":"auto"`, ""},
		{"unknown", `,"service_tier":"flex"`, ""},
		{"non strings", `,"speed":true,"service_tier":5`, ""},
		{"null", `,"speed":null,"service_tier":null`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, stream := range []bool{false, true} {
				input := []byte(`{"messages":[{"role":"user","content":"fixture"}]` + tc.fields + `}`)
				out := ConvertClaudeRequestToCodex("gpt-5.4-mini", input, stream)
				got := gjson.GetBytes(out, "service_tier")
				if got.String() != tc.want || got.Exists() != (tc.want != "") || gjson.GetBytes(out, "speed").Exists() {
					t.Fatalf("stream=%t service tier = %q", stream, got.String())
				}
			}
		})
	}
}
