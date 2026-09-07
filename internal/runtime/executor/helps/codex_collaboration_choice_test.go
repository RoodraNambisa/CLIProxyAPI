package helps

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexCollaborationNamespaceKeepsExplicitToolChoiceAligned(t *testing.T) {
	for _, tc := range []struct{ choice, path, want string }{
		{`{"type":"namespace","name":"collaboration"}`, "name", "collaboration-optimize"},
		{`{"type":"function","namespace":"collaboration","name":"spawn_agent"}`, "namespace", "collaboration-optimize"},
		{`{"type":"custom","namespace":"collaboration","name":"custom"}`, "namespace", "collaboration-optimize"},
		{`{"type":"function","name":"collaboration__spawn_agent"}`, "name", "collaboration-optimize__spawn_agent"},
		{`{"type":"allowed_tools","mode":"required","tools":[{"type":"function","namespace":"collaboration","name":"spawn_agent"}]}`, "tools.0.namespace", "collaboration-optimize"},
	} {
		raw := []byte(fmt.Sprintf(`{"tools":[{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"spawn_agent"}]}],"tool_choice":%s,"metadata":{"tool_choice":{"type":"namespace","name":"collaboration"}},"input":[{"type":"function_call_output","output":{"tool_choice":{"type":"namespace","name":"collaboration"}}}]}`, tc.choice))
		original := bytes.Clone(raw)
		got, active := OptimizeCodexCollaborationNamespace(raw)
		if !active || gjson.GetBytes(got, "tool_choice."+tc.path).String() != tc.want {
			t.Fatal("explicit tool selection no longer addresses the renamed declaration")
		}
		for _, path := range []string{"metadata", "input", "tools.0.tools"} {
			if gjson.GetBytes(got, path).Raw != gjson.GetBytes(raw, path).Raw {
				t.Fatalf("unrelated field changed: %s", path)
			}
		}
		response := []byte(fmt.Sprintf(`{"type":"response.completed","response":{"tools":%s,"tool_choice":%s,"metadata":{"tool_choice":{"type":"namespace","name":"collaboration-optimize"}}}}`, gjson.GetBytes(got, "tools").Raw, gjson.GetBytes(got, "tool_choice").Raw))
		restored := RewriteCodexMultiAgentV2Response(response, true, false)
		if gjson.GetBytes(restored, "response.tool_choice").Raw != gjson.GetBytes(raw, "tool_choice").Raw ||
			gjson.GetBytes(restored, "response.metadata").Raw != gjson.GetBytes(response, "response.metadata").Raw {
			t.Fatal("response tool selection was not restored exactly within its scope")
		}
		if !bytes.Equal(original, raw) {
			t.Fatal("rewrite mutated original request")
		}
	}
}

func TestCodexCollaborationChoiceLeavesOtherScopesAndScalarsUnchanged(t *testing.T) {
	for _, choice := range []string{`"auto"`, `"none"`, `{"type":"function","name":"spawn_agent"}`,
		`{"type":"function","namespace":"external","name":"collaboration__spawn_agent"}`,
		`{"type":"unknown","namespace":"collaboration","name":"collaboration"}`,
		`{"type":"allowed_tools","tools":[{"type":"function","namespace":"external","name":"spawn_agent"}]}`} {
		raw := []byte(fmt.Sprintf(`{"tools":[{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"spawn_agent"}]}],"tool_choice":%s}`, choice))
		got, active := OptimizeCodexCollaborationNamespace(raw)
		if !active || gjson.GetBytes(got, "tool_choice").Raw != choice {
			t.Fatal("unrelated selection was rewritten")
		}
	}
}
