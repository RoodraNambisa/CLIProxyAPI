package executor

import "testing"

func TestResponsesCompactionBoundaries(t *testing.T) {
	for _, tc := range []struct {
		payload, alt string
		want         bool
	}{
		{`{"input":[]}`, "responses/compact", true},
		{`{"input":[{"type":"compaction_trigger"}]}`, "", true},
		{`{"input":[{"type":"compaction_trigger"},{"role":"user","content":"draw"}]}`, "", false},
		{`{"input":[{"type":"function_call_output","output":{"type":"compaction_trigger"}}]}`, "", false},
		{`{"input":[{"role":"user","content":"compaction_trigger"}]}`, "", false},
		{`{"input":[{"type":"compaction_trigger"}]`, "", false},
		{`{"in\u0070ut":[{"ty\u0070e":"compaction_trigger"}]}`, "", true},
	} {
		if got := IsResponsesCompaction([]byte(tc.payload), tc.alt); got != tc.want {
			t.Errorf("compaction=%t, want %t for %s/%s", got, tc.want, tc.alt, tc.payload)
		}
	}
}
