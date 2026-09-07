package helps

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

func TestCodexOrphanDelegationRepairsOnlyUnpairedKnownResults(t *testing.T) {
	const target = `{"type":"function_call_output","namespace":"codex_app","name":"create_thread","call_id":"pair","output":" exact\ntext "}`
	const paired = `{"type":"function_call","name":"create_thread","call_id":"pair","arguments":"{}"}`
	const custom = `{"type":"custom_tool_call_output","namespace":"codex_app","name":"create_thread","call_id":"custom","output":"business"}`
	const other = `{"type":"function_call_output","namespace":"my_app","name":"create_thread","output":"business"}`
	for _, tc := range []struct {
		name, input string
		enabled     bool
		messages    int
	}{
		{"off", target, false, 0},
		{"orphan", target, true, 1},
		{"call before", paired + "," + target, true, 0},
		{"call later", target + "," + paired, true, 0},
		{"duplicate output", paired + "," + target + "," + target, true, 1},
		{"two pairs", paired + "," + paired + "," + target + "," + target, true, 0},
		{"unrelated", custom + "," + other, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := []byte(`{"input":[` + tc.input + `],"prompt_cache_key":"keep","unknown":42}`)
			got := RewriteCodexOrphanDelegationInput(payload, tc.enabled)
			messages := 0
			for _, item := range gjson.GetBytes(got, "input").Array() {
				if item.Get("type").String() == "message" {
					messages++
					if item.Get("role").String() != "user" || item.Get("content.0.text").String() != "Tool output from codex_app__create_thread:\n exact\ntext " {
						t.Fatal("orphan content or role changed")
					}
				}
			}
			if messages != tc.messages {
				t.Fatalf("repaired %d outputs, want %d", messages, tc.messages)
			}
			if gjson.GetBytes(got, "prompt_cache_key").String() != "keep" || gjson.GetBytes(got, "unknown").Int() != 42 {
				t.Fatal("unrelated fields changed")
			}
			if tc.messages == 0 && string(got) != string(payload) {
				t.Fatal("no-op rewrote original bytes")
			}
			if again := RewriteCodexOrphanDelegationInput(got, tc.enabled); string(again) != string(got) {
				t.Fatal("normalization is not idempotent")
			}
		})
	}
}

func TestCodexOrphanDelegationPreservesNonStringOutputAndSequence(t *testing.T) {
	for _, value := range []string{`{"type":"function_call","n":9007199254740993}`, `["a", "b"]`, `null`, `42`, `false`} {
		payload := []byte(fmt.Sprintf(`{"input":[{"type":"message","role":"user","content":"before"},{"type":"function_call_output","namespace":"codex_app","name":"send_message_to_thread","output":%s},{"type":"message","role":"user","content":"after"}]}`, value))
		got := RewriteCodexOrphanDelegationInput(payload, true)
		if gjson.GetBytes(got, "input.1.content.0.text").String() != "Tool output from codex_app__send_message_to_thread:\n"+value || gjson.GetBytes(got, "input.0.content").String() != "before" || gjson.GetBytes(got, "input.2.content").String() != "after" {
			t.Fatal("structured output or message order changed")
		}
	}
}

func TestCodexOrphanDelegationRequiresUnambiguousIncomingSubagentHeader(t *testing.T) {
	for _, header := range []http.Header{nil, {"X-Openai-Subagent": {"other"}}, {"X-Openai-Subagent": {"collab_spawn", "other"}}, {"x-openai-subagent": {"collab_spawn"}, "X-Openai-Subagent": {"other"}}} {
		if CodexOrphanDelegationEnabled(nil, header, true) {
			t.Fatal("unexpected subagent identity enabled compatibility")
		}
	}
	good := http.Header{"x-openai-subagent": {" COLLAB_SPAWN "}}
	if !CodexOrphanDelegationEnabled(nil, good, true) || CodexOrphanDelegationEnabled(nil, good, false) {
		t.Fatal("header or disabled policy handling changed")
	}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	ctx := context.WithValue(t.Context(), "gin", c)
	if CodexOrphanDelegationEnabled(ctx, good, true) {
		t.Fatal("outbound option headers replaced incoming caller identity")
	}
}

func TestCodexOrphanDelegationConsumesCachedPairOnlyOnce(t *testing.T) {
	const output = `{"type":"function_call_output","namespace":"codex_app","name":"create_thread","call_id":"cached","output":"exact result"}`
	calls := 0
	got := RewriteCodexOrphanDelegationInput([]byte(`{"input":[`+output+`,`+output+`]}`), true, func(id string) bool {
		calls++
		return id == "cached"
	})
	if calls != 1 || gjson.GetBytes(got, "input.0.type").String() != "function_call_output" || gjson.GetBytes(got, "input.1.type").String() != "message" {
		t.Fatal("cached pair was reused for multiple outputs or lost")
	}
}

func TestCodexOrphanDelegationLeavesUnsupportedShapesUnchanged(t *testing.T) {
	for _, input := range []string{"", "{", "null", `{"input":{}}`, `{"input":[{"type":"message","content":{"namespace":"codex_app","name":"create_thread","output":"business"}}]}`, `{"input":[{"type":"function_call_output","namespace":"codex_app","name":"unknown","output":"business"}]}`} {
		if got := RewriteCodexOrphanDelegationInput([]byte(input), true); string(got) != input {
			t.Fatal("unsupported input or nested business object was rewritten")
		}
	}
}
