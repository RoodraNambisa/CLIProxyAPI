package helps

import (
	"bytes"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexMultiAgentResponsePolicyRequiresPreparedDeclarations(t *testing.T) {
	for _, tc := range []struct {
		name, payload string
		enabled       bool
		want          CodexMultiAgentResponsePolicy
	}{
		{"disabled", `{"tools":[{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"spawn_agent"}]}]}`, false, CodexMultiAgentResponsePolicy{}},
		{"no-tools", `{"input":[]}`, true, CodexMultiAgentResponsePolicy{}},
		{"renamed", `{"tools":[{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"spawn_agent"}]}]}`, true, CodexMultiAgentResponsePolicy{NamespaceOptimized: true, PlaintextCalls: true}},
		{"message-only", `{"tools":[{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"send_message"}]}]}`, true, CodexMultiAgentResponsePolicy{PlaintextCalls: true}},
		{"reserved-conflict", `{"tools":[{"type":"namespace","name":"collaboration-optimize","tools":[{"type":"function","name":"spawn_agent"}]}]}`, true, CodexMultiAgentResponsePolicy{NamespaceConflict: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := []byte(tc.payload)
			before := bytes.Clone(raw)
			_, policy := OptimizeCodexMultiAgentV2Request(raw, CodexMultiAgentPolicy{Enabled: tc.enabled})
			if policy != tc.want || !bytes.Equal(raw, before) {
				t.Fatal("wrong per-attempt declaration state or mutated input")
			}
			event := []byte(`{"type":"function_call","namespace":"collaboration","name":"send_message","arguments":"exact"}`)
			got := policy.Rewrite(event)
			if gjson.GetBytes(got, "encrypted_function_args").Exists() != tc.want.PlaintextCalls {
				t.Fatal("unprepared calls acquired plaintext metadata")
			}
		})
	}
}
