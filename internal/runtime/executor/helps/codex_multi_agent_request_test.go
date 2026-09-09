package helps

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexModelCompatibilityPreservesOpaqueAgentHistory(t *testing.T) {
	raw := []byte(`{"prompt_cache_key":"fixture","input":[{"type":"agent_message","author":"parent","content":[{"type":"input_text","text":"plaintext"}]},{"type":"agent_message","content":[{"type":"input_text","text":"before"},{"type":"encrypted_content","encrypted_content":"opaque"}]},{"type":"function_call_output","call_id":"paired","output":{"type":"agent_message","content":"business"}}]}`)
	for _, enabled := range []bool{false, true} {
		for _, compat := range []bool{false, true} {
			t.Run(fmt.Sprintf("enabled=%t/compat=%t", enabled, compat), func(t *testing.T) {
				original := bytes.Clone(raw)
				got, policy := OptimizeCodexMultiAgentV2Request(raw, CodexMultiAgentPolicy{Enabled: enabled}, compat)
				wantType := "agent_message"
				if enabled && compat {
					wantType = "message"
				}
				if gjson.GetBytes(got, "input.0.type").String() != wantType ||
					gjson.GetBytes(got, "input.0.role").Exists() != (enabled && compat) {
					t.Fatal("portable agent input ignored one of its required policies")
				}
				for _, path := range []string{"prompt_cache_key", "input.0.author", "input.0.content", "input.1", "input.2"} {
					if gjson.GetBytes(got, path).Raw != gjson.GetBytes(raw, path).Raw {
						t.Fatalf("compatibility changed unrelated or opaque field %s", path)
					}
				}
				if !bytes.Equal(raw, original) || policy != (CodexMultiAgentResponsePolicy{}) {
					t.Fatal("compatibility mutated input or invented tool response state")
				}
			})
		}
	}
	for _, raw := range []string{"", "not-json", `{"input":"text"}`, `{"input":[]}`} {
		got := rewriteCodexCompatibilityAgentMessages([]byte(raw))
		if string(got) != raw {
			t.Fatal("compatibility changed malformed or absent history")
		}
	}
}

func TestCodexCompatibilityAgentIDsUseFinalItemTypes(t *testing.T) {
	raw := []byte(`{"input":[{"type":"agent_message","id":"plain","content":[]},{"type":"message","id":"msg_plain","role":"user","content":[]}]}`)
	got, _ := OptimizeCodexMultiAgentV2Request(raw, CodexMultiAgentPolicy{Enabled: true}, true)
	first, second := gjson.GetBytes(got, "input.0.id").String(), gjson.GetBytes(got, "input.1.id").String()
	if len(first) < 4 || first[:4] != "msg_" || first == second || second != "msg_plain" {
		t.Fatal("compatibility conversion produced invalid or colliding message IDs")
	}
	again, _ := OptimizeCodexMultiAgentV2Request(got, CodexMultiAgentPolicy{Enabled: true}, true)
	if !bytes.Equal(got, again) {
		t.Fatal("compatibility changed a prepared item's identity twice")
	}
}

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
