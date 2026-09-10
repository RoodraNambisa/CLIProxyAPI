package gemini

import (
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestGeminiCodexStableToolIDsPreservePairing(t *testing.T) {
	for _, tc := range []struct{ name, input, want string }{
		{"implicit pairs", `{"contents":[{"role":"model","parts":[{"functionCall":{"name":"a","args":{}}},{"functionCall":{"name":"b","args":{}}}]},{"role":"user","parts":[{"functionResponse":{"name":"a","response":{"result":"a"}}},{"functionResponse":{"name":"b","response":{"result":"b"}}}]}]}`, "call_gemini_0000000000000001,call_gemini_0000000000000002,call_gemini_0000000000000001,call_gemini_0000000000000002"},
		{"reserved future ID", `{"contents":[{"role":"model","parts":[{"functionCall":{"name":"a"}},{"functionCall":{"name":"b","id":"call_gemini_0000000000000001"}}]},{"role":"user","parts":[{"functionResponse":{"name":"b","id":"call_gemini_0000000000000001","response":{}}},{"functionResponse":{"name":"a","response":{}}}]}]}`, "call_gemini_0000000000000002,call_gemini_0000000000000001,call_gemini_0000000000000001,call_gemini_0000000000000002"},
		{"explicit call_id", `{"contents":[{"role":"model","parts":[{"functionCall":{"name":"a","call_id":"unchanged"}}]},{"role":"user","parts":[{"functionResponse":{"name":"a","call_id":"unchanged","response":{}}}]}]}`, "unchanged,unchanged"},
		{"hidden ID excluded", `{"contents":[{"role":"model","parts":[{"thought":true,"functionCall":{"name":"hidden","id":"call_gemini_0000000000000001"}},{"functionCall":{"name":"visible"}}]},{"role":"user","parts":[{"functionResponse":{"name":"visible","response":{}}}]}]}`, "call_gemini_0000000000000001,call_gemini_0000000000000001"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var baseline string
			for i := 0; i < 10; i++ {
				out := ConvertGeminiRequestToCodex("gpt-5.4", []byte(tc.input), i%2 == 0)
				var ids []string
				for _, item := range gjson.GetBytes(out, "input").Array() {
					if id := item.Get("call_id"); id.Exists() {
						ids = append(ids, id.String())
					}
				}
				if strings.Join(ids, ",") != tc.want {
					t.Fatalf("unexpected pairing: %v", ids)
				}
				input := gjson.GetBytes(out, "input").Raw
				if i > 0 && input != baseline {
					t.Fatal("same history changed between conversions")
				}
				baseline = input
			}
		})
	}
}
