package helps

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexAgentMessagesNormalizeOnlyDeclaredEnvelope(t *testing.T) {
	payload := []byte("{\"prompt_cache_key\":\"cache-stays\",\"metadata\":{\"counter\":9007199254740993},\"input\":[{\"type\":\"agent_message\",\"id\":\"amsg_1\",\"author\":\"/root\",\"recipient\":\"/root/worker\",\"content\":[{\"type\":\"input_text\",\"text\":\"prefix\"},{\"type\":\"encrypted_content\",\"encrypted_content\":\"task\\n\\\"quoted\\\" 中文\",\"extension\":{\"encrypted_content\":\"keep\"}}],\"internal_chat_message_metadata_passthrough\":{\"turn_id\":\"turn-original\"}},{\"type\":\"reasoning\",\"encrypted_content\":\"opaque-reasoning\"},{\"type\":\"function_call_output\",\"call_id\":\"call-original\",\"output\":{\"type\":\"agent_message\",\"content\":[{\"type\":\"encrypted_content\",\"encrypted_content\":\"business\"}]}}]}")
	for _, nonCodex := range []bool{false, true} {
		rewrite := rewriteCodexAgentMessageContent
		if nonCodex {
			rewrite = rewriteCodexAgentMessageInput
		}
		original := append([]byte(nil), payload...)
		got := rewrite(payload)
		if !bytes.Equal(payload, original) {
			t.Fatal("normalization mutated the caller buffer")
		}
		item := gjson.GetBytes(got, "input.0")
		wantType := "agent_message"
		if nonCodex {
			wantType = "message"
		}
		if item.Get("type").String() != wantType || (nonCodex && item.Get("role").String() != "user") || (!nonCodex && item.Get("role").Exists()) {
			t.Fatal("wrong target envelope")
		}
		if item.Get("content.1.type").String() != "input_text" || item.Get("content.1.text").String() != "task\n\"quoted\" 中文" || item.Get("content.1.encrypted_content").Exists() {
			t.Fatal("collaboration content was not normalized")
		}
		for _, path := range []string{"prompt_cache_key", "metadata.counter", "input.0.id", "input.0.author", "input.0.recipient", "input.0.content.0", "input.0.content.1.extension", "input.0.internal_chat_message_metadata_passthrough", "input.1", "input.2"} {
			if gjson.GetBytes(got, path).Raw != gjson.GetBytes(original, path).Raw {
				t.Fatalf("unrelated field changed: %s", path)
			}
		}
		if !bytes.Equal(rewrite(got), got) {
			t.Fatal("normalization is not idempotent")
		}
	}
}

func TestCodexAgentMessageNormalizationNoopAndMalformedInputs(t *testing.T) {
	for _, raw := range []string{"", "not JSON", "{\"input\":[{\"type\":\"agent_message\",\"content\":[]}", "null", "{}", "{\"input\":\"text\"}", "{\"input\":[]}", "{\"input\":[{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"encrypted_content\",\"encrypted_content\":\"leave\"}]}]}"} {
		original := []byte(raw)
		for _, rewrite := range []func([]byte) []byte{rewriteCodexAgentMessageInput, rewriteCodexAgentMessageContent} {
			got := rewrite(original)
			if !bytes.Equal(got, original) {
				t.Fatal("unsupported envelope was modified")
			}
			if len(original) > 0 && &got[0] != &original[0] {
				t.Fatal("no-op normalization copied the body")
			}
		}
	}
}

func TestCodexAgentMessageNormalizationPreservesUnknownContent(t *testing.T) {
	payload := []byte("{\"input\":[{\"type\":\"agent_message\",\"content\":[{\"type\":\"encrypted_content\",\"encrypted_content\":42},{\"type\":\"encrypted_content\"},{\"type\":\"unknown\",\"text\":\"keep\"}]},{\"type\":\"agent_message\",\"content\":\"legacy text\"}]}")
	if got := rewriteCodexAgentMessageContent(payload); !bytes.Equal(got, payload) {
		t.Fatal("unknown content was interpreted as text")
	}
	var before, after map[string]any
	if err := json.Unmarshal(payload, &before); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(rewriteCodexAgentMessageInput(payload), &after); err != nil {
		t.Fatal(err)
	}
	for i, raw := range before["input"].([]any) {
		expected := raw.(map[string]any)
		expected["type"], expected["role"] = "message", "user"
		if !reflect.DeepEqual(after["input"].([]any)[i], expected) {
			t.Fatal("standard envelope conversion changed unknown fields")
		}
	}
}
