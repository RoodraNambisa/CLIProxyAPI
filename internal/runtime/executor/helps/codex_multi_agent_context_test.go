package helps

import "testing"

func TestCodexMultiAgentDeclarationsReplaceOnlyRelatedContext(t *testing.T) {
	for _, tc := range []struct {
		payload string
		replace bool
	}{
		{`{"tools":[]}`, true},
		{`{"tools":null}`, true},
		{`{"input":[{"type":"additional_tools","tools":[{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"send_message"}]}]}]}`, true},
		{`{"input":[{"type":"additional_tools","tools":[{"type":"namespace","name":"collaboration-optimize"}]}]}`, true},
		{`{"input":[{"type":"additional_tools","tools":[{"type":"function","name":"lookup"}]}]}`, false},
		{`{"input":[{"type":"function_call_output","output":{"tools":[{"type":"function","name":"spawn_agent"}]}}]}`, false},
		{`{"metadata":{"tools":[]}}`, false},
		{"{}", false}, {"", false},
	} {
		if CodexMultiAgentDeclaresTools([]byte(tc.payload)) != tc.replace {
			t.Fatal("unrelated history or metadata replaced collaboration context")
		}
	}
}
