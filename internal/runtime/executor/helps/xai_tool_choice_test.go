package helps

import (
	"testing"

	"github.com/tidwall/gjson"
)

func TestXAIAllowedToolRestrictions(t *testing.T) {
	for _, tc := range []struct {
		name, body, before, kind, tool string
		count                          int
		fail                           bool
	}{
		{"partial", `{"tools":[{"type":"function","name":"a"},{"type":"function","name":"b"}],"tool_choice":{"type":"allowed_tools","mode":"auto","tools":[{"type":"tool_search"},{"type":"function","name":"a"}]}}`, "", "allowed_tools", "a", 1, false},
		{"custom", `{"tools":[{"type":"function","name":"a"}],"tool_choice":{"type":"allowed_tools","mode":"required","tools":[{"type":"custom","name":"a"}]}}`, "", "allowed_tools", "a", 1, false},
		{"hosted", `{"tools":[{"type":"web_search"},{"type":"function","name":"a"}],"tool_choice":{"type":"web_search"}}`, "", "allowed_tools", "", 1, false},
		{"namespace", `{"tools":[{"type":"function","name":"a"}],"tool_choice":{"type":"allowed_tools","mode":"required","tools":[{"type":"function","name":"a","namespace":"app"}]}}`, `{"tools":[{"type":"namespace","name":"app","tools":[{"type":"function","name":"a"}]}]}`, "allowed_tools", "a", 1, false},
		{"missing-namespace", `{"tools":[{"type":"function","name":"a"}],"tool_choice":{"type":"allowed_tools","mode":"required","tools":[{"type":"function","name":"a","namespace":"missing"}]}}`, `{"tools":[{"type":"namespace","name":"app","tools":[{"type":"function","name":"a"}]}]}`, "", "", 0, true},
		{"ambiguous", `{"tools":[{"type":"function","name":"a"},{"type":"function","name":"a"}],"tool_choice":{"type":"allowed_tools","mode":"required","tools":[{"type":"function","name":"a"}]}}`, "", "", "", 0, true},
		{"all-removed", `{"tools":[{"type":"function","name":"b"}],"tool_choice":{"type":"allowed_tools","mode":"auto","tools":[{"type":"tool_search"}]}}`, "", "", "", 0, true},
		{"empty-tools", `{"tools":[],"tool_choice":{"type":"allowed_tools","mode":"required","tools":[{"type":"function","name":"a"}]}}`, "", "", "", 0, true},
		{"invalid-mode", `{"tools":[{"type":"function","name":"a"}],"tool_choice":{"type":"allowed_tools","mode":"none","tools":[{"type":"function","name":"a"}]}}`, "", "", "", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, err := NormalizeXAIAllowedTools([]byte(tc.body), []byte(tc.before))
			if (err != nil) != tc.fail {
				t.Fatalf("err=%v body=%s", err, body)
			}
			if tc.fail {
				return
			}
			if gjson.GetBytes(body, "tool_choice.type").String() != tc.kind || gjson.GetBytes(body, "tool_choice.tools.#").Int() != int64(tc.count) || gjson.GetBytes(body, "tool_choice.tools.0.name").String() != tc.tool {
				t.Fatalf("wrong normalized choice: %s", body)
			}
			if gjson.GetBytes(body, "tool_choice.tools.0.namespace").Exists() || gjson.GetBytes(body, "tool_choice.tools.0.function").Exists() {
				t.Fatal("untranslated reference")
			}
		})
	}
}

func TestXAIForcedNamespaceAndMCPChoicesKeepTheirScope(t *testing.T) {
	body := []byte(`{"tools":[{"type":"function","name":"a"}],"tool_choice":{"type":"function","name":"a","namespace":"app"}}`)
	before := []byte(`{"tools":[{"type":"namespace","name":"app","tools":[{"type":"function","name":"a"}]}]}`)
	out, err := NormalizeXAIAllowedTools(body, before)
	if err != nil {
		t.Fatal(err)
	}
	if gjson.GetBytes(out, "tool_choice.type").String() != "function" || gjson.GetBytes(out, "tool_choice.name").String() != "a" || gjson.GetBytes(out, "tool_choice.namespace").Exists() {
		t.Fatalf("namespace did not follow flattened tool: %s", out)
	}
	body = []byte(`{"tools":[{"type":"mcp","server_label":"one"},{"type":"mcp","server_label":"two"}],"tool_choice":{"type":"allowed_tools","mode":"auto","tools":[{"type":"mcp","server_label":"one","name":"lookup"}]}}`)
	out, err = NormalizeXAIAllowedTools(body, body)
	if err != nil {
		t.Fatal(err)
	}
	if gjson.GetBytes(out, "tool_choice.tools.#").Int() != 1 || gjson.GetBytes(out, "tool_choice.tools.0.server_label").String() != "one" || gjson.GetBytes(out, "tool_choice.tools.0.name").String() != "lookup" {
		t.Fatal("MCP restriction broadened")
	}
}
