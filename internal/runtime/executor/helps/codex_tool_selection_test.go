package helps

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexToolSelectionKeepsDeclarationsAndRequestOwnership(t *testing.T) {
	for _, tc := range []struct {
		body  string
		tools bool
	}{
		{`{}`, false},
		{`{"tools":[]}`, false},
		{`{"tools":null,"input":{}}`, false},
		{`{"tools":[{"type":"function","name":"fixture"}]}`, true},
		{`{"input":[{"type":"additional_tools","tools":[{"type":"custom","name":"fixture"}]}]}`, true},
		{`{"input":[{"type":"additional_tools","tools":[]}]}`, false},
		{`{"input":[{"type":"additio\u006eal_tools","tools":[{}]}]}`, true},
		{`{"input":[{"type":"message","content":"additional_tools"}]}`, false},
		{`{"input":[{"type":"message","tools":[{}]}]}`, false},
	} {
		for _, controls := range []string{"", `,"tool_choice":null,"parallel_tool_calls":false`, `,"tool_choice":"required","parallel_tool_calls":true`} {
			body := []byte(strings.TrimSuffix(tc.body, "}") + controls + "}")
			original := bytes.Clone(body)
			if HasCodexToolDeclarations(body) != tc.tools {
				t.Fatalf("declaration classification changed: %s", tc.body)
			}
			out := NormalizeCodexToolSelection(body)
			if !bytes.Equal(body, original) || gjson.GetBytes(out, "input").Raw != gjson.GetBytes(original, "input").Raw {
				t.Fatal("tool normalization changed its input buffer or history")
			}
			if tc.tools {
				if !bytes.Equal(out, original) {
					t.Fatal("declared tools lost their controls")
				}
			} else if gjson.GetBytes(out, "tool_choice").Exists() || gjson.GetBytes(out, "parallel_tool_calls").Exists() {
				t.Fatal("undeclared tools retained tool-only controls")
			}
		}
	}
	if HasCodexToolDeclarations(nil) || NormalizeCodexToolSelection(nil) != nil {
		t.Fatal("nil body handling changed")
	}
}

func BenchmarkCodexEmptyToolSelection(b *testing.B) {
	body := []byte(fmt.Sprintf(`{"input":[{"role":"user","content":%q}]}`, strings.Repeat("x", 1<<20)))
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = NormalizeCodexToolSelection(body)
	}
}
