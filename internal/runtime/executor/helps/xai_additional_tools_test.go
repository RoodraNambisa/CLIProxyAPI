package helps

import (
	"bytes"
	"testing"

	"github.com/tidwall/gjson"
)

func TestPromoteXAIAdditionalToolsPreservesPrecedenceAndHistory(t *testing.T) {
	raw := []byte(`{"tools":[{"type":"namespace","name":"editor","tools":[{"type":"function","name":"patch","parameters":{"default":9007199254740993}}]}],"input":[{"role":"user","content":"before"},{"type":"additional_tools","tools":[{"type":"namespace","name":"editor","tools":[{"type":"function","name":"patch","parameters":{"wrong":true}},{"type":"function","name":"lookup"}]}]},{"type":"function_call","call_id":"pair","name":"business","arguments":"{\"type\":\"additional_tools\",\"tools\":[]}"},{"role":"user","content":"after"}]}`)
	original := bytes.Clone(raw)
	out := PromoteXAIAdditionalTools(raw)
	if gjson.GetBytes(out, "tools.#").Int() != 2 || gjson.GetBytes(out, "tools.0.tools.0.parameters.default").Raw != "9007199254740993" || gjson.GetBytes(out, "tools.1.name").String() != "editor" || gjson.GetBytes(out, "tools.1.tools.0.name").String() != "lookup" {
		t.Fatal("root precedence, namespace or schema precision changed")
	}
	if gjson.GetBytes(out, "input.#").Int() != 3 || gjson.GetBytes(out, "input.0.content").String() != "before" || gjson.GetBytes(out, "input.1").Raw != gjson.GetBytes(raw, "input.2").Raw || gjson.GetBytes(out, "input.2.content").String() != "after" || !bytes.Equal(raw, original) {
		t.Fatal("history ordering or business arguments changed")
	}
	if !bytes.Equal(PromoteXAIAdditionalTools(out), out) {
		t.Fatal("promotion is not idempotent")
	}
}

func TestPromoteXAIAdditionalToolsPreservesNonLiteAndMalformedInput(t *testing.T) {
	for _, raw := range []string{`{"tools":[],"input":[]}`, `{"input":"literal"}`, `{"tools":{},"input":[{"type":"additional_tools","tools":[]}]}`, `{"input":[{"type":"additional_tools","tools":{}}]}`, `{"input":[{"type":"additional_tools","tools":[]}]`} {
		if got := PromoteXAIAdditionalTools([]byte(raw)); string(got) != raw {
			t.Fatal("ordinary or malformed request was rewritten")
		}
	}
	out := PromoteXAIAdditionalTools([]byte(`{"input":[{"type":"additional_tools","tools":[]}]}`))
	if gjson.GetBytes(out, "input").Raw != "[]" || gjson.GetBytes(out, "tools").Raw != "[]" {
		t.Fatal("empty declaration arrays did not remain arrays")
	}
}
