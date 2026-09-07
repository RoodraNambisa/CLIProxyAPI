package responses

import (
	"bytes"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestClaudeResponsesNamespacedToolsKeepSchemaHistoryAndChoice(t *testing.T) {
	raw := []byte(`{"tools":[{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"spawn_agent","description":"Keep collaboration wording","parameters":{"type":"object","properties":{"count":{"type":"integer","default":9007199254740993},"message":{"type":"string","encrypted":true}},"required":["message"],"additionalProperties":false}}]}],"input":[{"type":"function_call","namespace":"collaboration","name":"spawn_agent","call_id":"original-pair","arguments":"{\"message\":\"work\"}"},{"type":"function_call_output","call_id":"original-pair","output":"done"}],"tool_choice":{"type":"function","namespace":"collaboration","name":"spawn_agent"}}`)
	before := bytes.Clone(raw)
	got := ConvertOpenAIResponsesRequestToClaude("claude-sonnet-4-6", raw, false)
	if gjson.GetBytes(got, "tools.#").Int() != 1 || gjson.GetBytes(got, "tools.0.name").String() != "collaboration__spawn_agent" ||
		gjson.GetBytes(got, "tools.0.description").String() != "Keep collaboration wording" ||
		gjson.GetBytes(got, "tools.0.input_schema").Raw != gjson.GetBytes(raw, "tools.0.tools.0.parameters").Raw {
		t.Fatal("namespace wrapper replaced the real tool or changed its schema")
	}
	if gjson.GetBytes(got, "messages.0.content.0.name").String() != "collaboration__spawn_agent" ||
		gjson.GetBytes(got, "messages.0.content.0.id").String() != "original-pair" ||
		gjson.GetBytes(got, "messages.1.content.0.tool_use_id").String() != "original-pair" ||
		gjson.GetBytes(got, "tool_choice.name").String() != "collaboration__spawn_agent" {
		t.Fatal("history, pairing and explicit selection disagree with the declaration")
	}
	if !bytes.Equal(raw, before) {
		t.Fatal("conversion mutated the source")
	}
}

func TestClaudeResponsesAdditionalToolsRespectFirstWinningName(t *testing.T) {
	raw := []byte(`{"tools":[{"type":"function","name":"collaboration__spawn_agent","description":"first","parameters":{"type":"object"}}],"input":[{"type":"additional_tools","tools":[{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"spawn_agent","description":"duplicate"}]},{"type":"namespace","name":"editor__","tools":[{"type":"function","name":"editor__patch","parameters":{"type":"object","properties":{"value":{"type":"string"}}}}]}]},{"type":"message","role":"user","content":"hello"}]}`)
	got := ConvertOpenAIResponsesRequestToClaude("claude-sonnet-4-6", raw, false)
	if gjson.GetBytes(got, "tools.#").Int() != 2 || gjson.GetBytes(got, "tools.0.description").String() != "first" ||
		gjson.GetBytes(got, "tools.1.name").String() != "editor__patch" || gjson.GetBytes(got, "messages.#").Int() != 1 {
		t.Fatal("additional tools shadowed root tools, gained duplicate prefixes or became messages")
	}
	for _, tc := range []struct{ namespace, name, want string }{
		{"", "plain", "plain"}, {"editor", "mcp__server__tool", "mcp__server__tool"}, {"editor", "editor.patch", "editor.patch"},
		{"editor", "editor__patch", "editor__patch"}, {"editor__", "patch", "editor__patch"},
	} {
		if got := qualifyClaudeResponsesToolName(tc.namespace, tc.name); got != tc.want {
			t.Fatal("qualified tool name changed")
		}
	}
}

func TestClaudeResponsesToolIdentityUsesFirstDeclaration(t *testing.T) {
	for _, rootFirst := range []bool{false, true} {
		prefix := ""
		if rootFirst {
			prefix = `"tools":[{"type":"function","name":"editor__patch"}],`
		}
		raw := []byte("{" + prefix + `"input":[{"type":"additional_tools","tools":[{"type":"namespace","name":"editor","tools":[{"type":"function","name":"patch"}]}]}]}`)
		identities := claudeResponsesToolIdentities(raw, nil)
		item := restoreClaudeResponsesToolIdentity([]byte(`{"type":"function_call","call_id":"pair","arguments":"opaque"}`), "", "editor__patch", identities)
		if rootFirst {
			if gjson.GetBytes(item, "name").String() != "editor__patch" || gjson.GetBytes(item, "namespace").Exists() {
				t.Fatal("literal root tool was reinterpreted as a namespace")
			}
		} else if gjson.GetBytes(item, "name").String() != "patch" || gjson.GetBytes(item, "namespace").String() != "editor" {
			t.Fatal("winning namespace was not restored")
		}
		unknown := restoreClaudeResponsesToolIdentity([]byte("{}"), "", "unknown__patch", identities)
		if gjson.GetBytes(unknown, "name").String() != "unknown__patch" || gjson.GetBytes(unknown, "namespace").Exists() {
			t.Fatal("unknown output name was guessed")
		}
	}
}

func TestClaudeResponsesStreamToolIdentitySurvivesSourceRelease(t *testing.T) {
	original := []byte(`{"input":[{"type":"additional_tools","tools":[{"type":"namespace","name":"editor","tools":[{"type":"function","name":"patch"}]}]}]}`)
	var state any
	ConvertClaudeResponseToOpenAIResponses(t.Context(), "", original, nil, []byte(`data: {"type":"message_start","message":{"id":"msg","role":"assistant","content":[]}}`), &state)
	for index := range original {
		original[index] = ' '
	}
	checked := 0
	for _, event := range []string{
		`{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"pair","name":"editor__patch","input":{}}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"value\":\"opaque\"}"}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"message_stop"}`,
	} {
		for _, chunk := range ConvertClaudeResponseToOpenAIResponses(t.Context(), "", nil, nil, []byte("data: "+event), &state) {
			for _, line := range strings.Split(string(chunk), "\n") {
				if !strings.HasPrefix(line, "data:") {
					continue
				}
				payload := gjson.Parse(strings.TrimPrefix(line, "data:"))
				item := payload.Get("item")
				if payload.Get("type").String() == "response.completed" {
					item = payload.Get("response.output.0")
				}
				if item.Get("type").String() != "function_call" {
					continue
				}
				checked++
				if item.Get("name").String() != "patch" || item.Get("namespace").String() != "editor" || item.Get("call_id").String() != "pair" {
					t.Fatal("stream event lost its original tool identity")
				}
			}
		}
	}
	if checked != 3 {
		t.Fatalf("checked %d tool events, want added, done and completed", checked)
	}
}
