package helps

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexCollaborationNamespaceOnlyRenamesDeclaredGroups(t *testing.T) {
	payload := []byte(`{"tools":[{"type":"function","name":"spawn_agent"},{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"spawn_agent"},{"type":"function","name":"send_message"}]},{"type":"namespace","name":"external","tools":[{"type":"function","name":"spawn_agent"}]}],"input":[{"type":"additional_tools","tools":[{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"spawn_agent"}]}]},{"type":"function_call","namespace":"collaboration","name":"spawn_agent","call_id":"pair","arguments":"{}"}],"prompt_cache_key":"collaboration"}`)
	original := bytes.Clone(payload)
	got, optimized := OptimizeCodexCollaborationNamespace(payload)
	if !optimized || gjson.GetBytes(got, "tools.1.name").String() != codexOptimizedCollaborationNamespace ||
		gjson.GetBytes(got, "input.0.tools.0.name").String() != codexOptimizedCollaborationNamespace {
		t.Fatal("eligible declaration groups were not renamed")
	}
	for _, path := range []string{"tools.0", "tools.1.tools", "tools.2", "input.1", "prompt_cache_key"} {
		if gjson.GetBytes(got, path).Raw != gjson.GetBytes(original, path).Raw {
			t.Fatalf("unrelated field changed: %s", path)
		}
	}
	if !bytes.Equal(original, payload) {
		t.Fatal("rename mutated the request buffer")
	}
	if again, active := OptimizeCodexCollaborationNamespace(got); active || !bytes.Equal(again, got) {
		t.Fatal("reserved namespace conflict did not leave the payload unchanged")
	}
	for _, raw := range []string{"", "{", "null", `{"tools":[{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"send_message"}]}]}`,
		`{"tools":[{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"spawn_agent"}]},{"type":"function","name":"collaboration-optimize__custom"}]}`} {
		got, active := OptimizeCodexCollaborationNamespace([]byte(raw))
		if active || string(got) != raw {
			t.Fatal("invalid, unrelated or conflicting declarations were renamed")
		}
	}
}

func TestCodexCollaborationRestoreProtocolContainers(t *testing.T) {
	call := `{"type":"function_call","id":"fc_item","call_id":"pair","namespace":"collaboration-optimize","name":"collaboration-optimize__spawn_agent","encrypted_function_args":[],"arguments":{"type":"function_call","namespace":"collaboration-optimize","name":"collaboration-optimize__business"}}`
	custom := `{"type":"custom_tool_call","name":"collaboration-optimize__custom","input":{"type":"namespace","name":"collaboration-optimize"}}`
	tools := `[{"type":"namespace","name":"collaboration-optimize","tools":[{"type":"function","name":"spawn_agent","parameters":{"example":{"type":"namespace","name":"collaboration-optimize"}}}]}]`
	response := fmt.Sprintf(`{"object":"response","output":[%s,%s,{"type":"additional_tools","tools":%s}],"tools":%s,"metadata":{"type":"namespace","name":"collaboration-optimize","counter":9007199254740993},"prompt_cache_key":"collaboration-optimize"}`, call, custom, tools, tools)
	for _, tc := range []struct{ name, payload, path string }{
		{"non-stream", response, ""},
		{"completed", fmt.Sprintf(`{"type":"response.completed","response":%s}`, response), "response."},
		{"failed-with-partial-output", fmt.Sprintf(`{"type":"response.failed","response":%s}`, response), "response."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := []byte(tc.payload)
			got := RestoreCodexMultiAgentV2Response(raw, true)
			for _, path := range []string{"output.0.namespace", "tools.0.name", "output.2.tools.0.name"} {
				if gjson.GetBytes(got, tc.path+path).String() != "collaboration" {
					t.Fatalf("protocol identity not restored: %s", path)
				}
			}
			if gjson.GetBytes(got, tc.path+"output.0.name").String() != "collaboration__spawn_agent" ||
				gjson.GetBytes(got, tc.path+"output.1.name").String() != "collaboration__custom" {
				t.Fatal("flattened function or custom identity not restored")
			}
			for _, path := range []string{"output.0.id", "output.0.call_id", "output.0.arguments", "output.0.encrypted_function_args", "output.1.input", "tools.0.tools", "metadata", "prompt_cache_key"} {
				if gjson.GetBytes(got, tc.path+path).Raw != gjson.GetBytes(raw, tc.path+path).Raw {
					t.Fatalf("opaque data changed: %s", path)
				}
			}
			if string(raw) != tc.payload || !bytes.Equal(RestoreCodexMultiAgentV2Response(got, true), got) {
				t.Fatal("restore mutated input or was not idempotent")
			}
		})
	}
	for _, event := range []string{"response.output_item.added", "response.output_item.done"} {
		got := RestoreCodexMultiAgentV2Response([]byte(fmt.Sprintf(`{"type":%q,"item":%s}`, event, call)), true)
		if gjson.GetBytes(got, "item.namespace").String() != "collaboration" {
			t.Fatalf("item event was not restored: %s", event)
		}
	}
	for _, event := range []string{"response.function_call_arguments.delta", "response.function_call_arguments.done", "response.custom_tool_call_input.delta", "response.custom_tool_call_input.done"} {
		got := RestoreCodexMultiAgentV2Response([]byte(fmt.Sprintf(`{"type":%q,"namespace":"collaboration-optimize","name":"collaboration-optimize__send_message","delta":"collaboration-optimize"}`, event)), true)
		if gjson.GetBytes(got, "namespace").String() != "collaboration" ||
			gjson.GetBytes(got, "delta").String() != "collaboration-optimize" {
			t.Fatalf("argument event identity or content changed incorrectly: %s", event)
		}
	}
}

func TestCodexCollaborationRestoreLeavesOpaqueOrInactivePayloads(t *testing.T) {
	for _, value := range []string{"", "{", "null", "[1]", `{"type":"message","name":"collaboration-optimize__plain","content":[{"type":"function_call","namespace":"collaboration-optimize"}]}`,
		`{"type":"response.completed","response":{"output":[{"type":"function_call_output","output":{"type":"namespace","name":"collaboration-optimize"}},{"type":"custom_tool_call_output","output":[{"type":"function_call","name":"collaboration-optimize__business"}]}],"input":[{"type":"function_call","namespace":"collaboration-optimize"}],"metadata":{"type":"function_call","name":"collaboration-optimize__private"}}}`,
		`{"type":"unknown","item":{"type":"function_call","namespace":"collaboration-optimize"}}`} {
		raw := []byte(value)
		if !bytes.Equal(RestoreCodexMultiAgentV2Response(raw, true), raw) {
			t.Fatal("opaque or unsupported data was rewritten")
		}
	}
	raw := []byte(`{"type":"function_call","namespace":"collaboration-optimize","name":"collaboration-optimize__spawn_agent"}`)
	if !bytes.Equal(RestoreCodexMultiAgentV2Response(raw, false), raw) {
		t.Fatal("inactive restore changed a response")
	}
}
