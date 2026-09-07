package helps

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexCollaborationToolScanUsesDeclaredScopes(t *testing.T) {
	payload := []byte("{\"tools\":[{\"type\":\"function\",\"name\":\"spawn_agent\"},{\"type\":\"namespace\",\"name\":\"collaboration\",\"tools\":[{\"type\":\"function\",\"name\":\"send_message\"}]},{\"type\":\"namespace\",\"name\":\"external\",\"tools\":[{\"type\":\"function\",\"name\":\"spawn_agent\"},{\"type\":\"namespace\",\"name\":\"collaboration\",\"tools\":[{\"type\":\"function\",\"name\":\"followup_task\"}]}]},{\"type\":\"custom\",\"name\":\"spawn_agent\"}],\"input\":[{\"type\":\"additional_tools\",\"tools\":[{\"type\":\"namespace\",\"name\":\"collaboration\",\"tools\":[{\"type\":\"function\",\"name\":\"spawn_agent\"}]}]},{\"type\":\"function_call_output\",\"output\":{\"tools\":[{\"type\":\"function\",\"name\":\"spawn_agent\"}]}}],\"metadata\":{\"tools\":[{\"type\":\"function\",\"name\":\"spawn_agent\"}]}}")
	got := scanCodexCollaborationTools(payload)
	wantMessages := []string{"tools.0", "tools.1.tools.0", "tools.2.tools.1.tools.0", "input.0.tools.0.tools.0"}
	wantSpawn := []string{"tools.0", "input.0.tools.0.tools.0"}
	if !reflect.DeepEqual(got.messagePaths, wantMessages) || !reflect.DeepEqual(got.spawnAgentPaths, wantSpawn) || got.conflict {
		t.Fatal("tool scope or declaration order changed")
	}
	if !reflect.DeepEqual(codexSpawnAgentToolPaths(payload), wantSpawn) || !reflect.DeepEqual(codexCollaborationMessageToolPaths(payload), wantMessages) {
		t.Fatal("tool path projections disagree")
	}
}

func TestCodexCollaborationSchemaOnlyRemovesMessageEncryptionMarker(t *testing.T) {
	payload := []byte("{\"prompt_cache_key\":\"cache\",\"tools\":[{\"type\":\"namespace\",\"name\":\"collaboration\",\"tools\":[{\"type\":\"function\",\"name\":\"spawn_agent\",\"strict\":true,\"parameters\":{\"type\":\"object\",\"properties\":{\"message\":{\"type\":\"string\",\"encrypted\":true,\"description\":\"keep\"},\"other\":{\"encrypted\":true}},\"required\":[\"message\"],\"additionalProperties\":false}}]},{\"type\":\"namespace\",\"name\":\"external\",\"tools\":[{\"type\":\"function\",\"name\":\"send_message\",\"parameters\":{\"properties\":{\"message\":{\"encrypted\":true}}}}]}],\"metadata\":{\"counter\":9007199254740993,\"encrypted\":true}}")
	original := append([]byte(nil), payload...)
	got := removeCodexCollaborationMessageEncryption(payload, codexCollaborationMessageToolPaths(payload))
	if !bytes.Equal(payload, original) {
		t.Fatal("schema rewrite mutated caller buffer")
	}
	if gjson.GetBytes(got, "tools.0.tools.0.parameters.properties.message.encrypted").Exists() {
		t.Fatal("collaboration message marker was retained")
	}
	for _, path := range []string{"prompt_cache_key", "tools.0.tools.0.strict", "tools.0.tools.0.parameters.type", "tools.0.tools.0.parameters.properties.message.description", "tools.0.tools.0.parameters.properties.other", "tools.0.tools.0.parameters.required", "tools.0.tools.0.parameters.additionalProperties", "tools.1", "metadata"} {
		if gjson.GetBytes(got, path).Raw != gjson.GetBytes(original, path).Raw {
			t.Fatalf("unrelated schema or data changed: %s", path)
		}
	}
	if again := removeCodexCollaborationMessageEncryption(got, codexCollaborationMessageToolPaths(got)); !bytes.Equal(again, got) {
		t.Fatal("schema rewrite is not idempotent")
	}
}

func TestCodexCollaborationReservedNamespaceConflicts(t *testing.T) {
	for _, payload := range []string{
		"{\"tools\":[{\"type\":\"namespace\",\"name\":\"collaboration-optimize\",\"tools\":[]}]}",
		"{\"tools\":[{\"type\":\"function\",\"name\":\"collaboration-optimize__spawn_agent\"}]}",
		"{\"input\":[{\"type\":\"additional_tools\",\"tools\":[{\"type\":\"namespace\",\"name\":\"outer\",\"tools\":[{\"type\":\"namespace\",\"name\":\"collaboration-optimize\"}]}]}]}",
	} {
		if !HasCodexMultiAgentV2NamespaceConflict([]byte(payload)) {
			t.Fatal("reserved declaration conflict was missed")
		}
	}
	for _, payload := range []string{
		"", "not JSON", "null", "{}",
		"{\"tools\":[{\"type\":\"function\",\"name\":\"custom\",\"parameters\":{\"example\":{\"type\":\"namespace\",\"name\":\"collaboration-optimize\"}}}],\"metadata\":{\"name\":\"collaboration-optimize\"}}",
		"{\"input\":[{\"type\":\"function_call_output\",\"output\":{\"type\":\"namespace\",\"name\":\"collaboration-optimize\"}}]}",
	} {
		raw := []byte(payload)
		if HasCodexMultiAgentV2NamespaceConflict(raw) {
			t.Fatal("business JSON or invalid input became a namespace conflict")
		}
		if got := removeCodexCollaborationMessageEncryption(raw, nil); !bytes.Equal(got, raw) {
			t.Fatal("no-op schema rewrite changed payload")
		}
	}
}
func TestCodexCollaborationModelListDemand(t *testing.T) {
	for _, tc := range []struct {
		payload string
		want    bool
	}{
		{`{"tools":[{"type":"function","name":"spawn_agent"}]}`, true},
		{`{"input":[{"type":"additional_tools","tools":[{"type":"function","name":"spawn_agent"}]}]}`, true},
		{`{"tools":[{"type":"function","name":"send_message"}]}`, false},
		{`{"tools":[{"type":"namespace","name":"other","tools":[{"type":"function","name":"spawn_agent"}]}]}`, false},
		{`{"tools":[{"type":"function","name":"spawn_agent"},{"type":"namespace","name":"collaboration-optimize"}]}`, false},
		{`{"input":"spawn_agent","metadata":{"name":"spawn_agent"}}`, false},
		{"invalid", false},
	} {
		if got := CodexCollaborationNeedsModelList([]byte(tc.payload)); got != tc.want {
			t.Fatalf("model list demand = %t, want %t", got, tc.want)
		}
	}
}
