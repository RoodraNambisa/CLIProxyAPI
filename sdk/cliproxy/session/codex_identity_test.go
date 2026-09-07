package session

import (
	"net/http"
	"strings"
	"testing"
)

func TestExtractCodexIdentity(t *testing.T) {
	for _, test := range []struct {
		name, body string
		headers    http.Header
		want       Identity
	}{
		{"normal", `{}`, http.Header{"session_id": {" root "}}, Identity{SessionID: "codex:root"}},
		{"native outranks generic", `{}`, http.Header{"Session-Id": {"root"}, "X-Session-Id": {"other"}}, Identity{SessionID: "codex:root"}},
		{"thread child", `{}`, http.Header{"Session-Id": {"root"}, "Thread-Id": {"child"}}, Identity{"codex:child", "codex:root", false, true}},
		{"explicit parent", `{}`, http.Header{"Thread-Id": {"child"}, "X-Codex-Parent-Thread-Id": {"parent"}}, Identity{"codex:child", "codex:parent", false, true}},
		{"body child", `{"request":{"thread_id":"child","parent_thread_id":"parent"}}`, http.Header{"Session-Id": {"root"}}, Identity{"codex:child", "codex:parent", false, true}},
		{"fork precedence", `{"forked_from_id":"parent"}`, http.Header{"Session-Id": {"child"}, "X-Openai-Subagent": {"true"}}, Identity{"codex:child", "codex:parent", true, false}},
		{"fork reused thread", `{"forked_from_id":"parent"}`, http.Header{"Session-Id": {"child"}, "Thread-Id": {"parent"}}, Identity{"codex:child", "codex:parent", true, false}},
		{"self parent", `{}`, http.Header{"Session-Id": {"root"}, "X-Codex-Parent-Thread-Id": {"root"}}, Identity{SessionID: "codex:root"}},
		{"turn metadata", `{}`, http.Header{"x-codex-turn-metadata": {`{"session_id":"root","subagent_kind":"thread_spawn","agent_name":"/root/reviewer"}`}}, Identity{agentIdentity("root", "reviewer"), "codex:root", false, true}},
		{"false signal", `{}`, http.Header{"Session-Id": {"root"}, "X-Openai-Subagent": {"false"}}, Identity{SessionID: "codex:root"}},
		{"invalid turn", `{}`, http.Header{"Session-Id": {"root"}, "X-Codex-Turn-Metadata": {`{"parent_thread_id":"other"`}}, Identity{SessionID: "codex:root"}},
		{"business json ignored", `{"input":[{"arguments":{"thread_id":"child","forked_from_id":"parent"}}]}`, http.Header{"Session-Id": {"root"}}, Identity{SessionID: "codex:root"}},
		{"nested business request ignored", `{"contents":[],"request":{"thread_id":"child","forked_from_id":"parent"}}`, http.Header{"Session-Id": {"root"}}, Identity{SessionID: "codex:root"}},
		{"invalid type ignored", `{"thread_id":42,"parent_thread_id":true}`, http.Header{"Session-Id": {"root"}}, Identity{SessionID: "codex:root"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, ok := ExtractCodexIdentity(test.headers, []byte(test.body))
			if !ok || got != test.want {
				t.Fatalf("identity = %+v, %v; want %+v", got, ok, test.want)
			}
		})
	}
}

func TestCodexIdentityRejectsUnreliableInputs(t *testing.T) {
	for _, headers := range []http.Header{
		nil, {"Session-Id": {strings.Repeat("a", 257)}}, {"Session-Id": {"bad\x00id"}}, {"Session-Id": {"\nroot"}},
		{"Session-Id": {"first", "second"}}, {"Session-Id": {"first"}, "session-id": {"second"}},
		{"X-Codex-Turn-Metadata": {`{"session_id":42}`}},
	} {
		if _, ok := ExtractCodexIdentity(headers, []byte(`{"prompt_cache_key":"shared","input":"hello"}`)); ok {
			t.Fatal("unreliable or absent identity was accepted")
		}
	}
	if agentIdentity("root", "a:agent:b") == agentIdentity("root:agent:a", "b") {
		t.Fatal("component delimiters caused an identity collision")
	}
}

func TestCodexIdentityDetachesPayload(t *testing.T) {
	payload := []byte(`{"thread_id":"child","parent_thread_id":"parent"}`)
	got, ok := ExtractCodexIdentity(http.Header{"Session-Id": {"root"}}, payload)
	clear(payload)
	if !ok || got != (Identity{"codex:child", "codex:parent", false, true}) {
		t.Fatal("identity retained a released body")
	}
}
