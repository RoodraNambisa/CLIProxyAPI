package session

import (
	"net/http"
	"testing"
)

func TestExplicitIdentityNativeAndGenericSources(t *testing.T) {
	for _, test := range []struct {
		name, body, execution string
		headers               http.Header
		want                  Identity
	}{
		{"claude precedence", `{}`, "", http.Header{"X-Claude-Code-Session-Id": {"root"}, "Session-Id": {"other"}}, Identity{SessionID: "claude:root"}},
		{"claude agent", `{}`, "", http.Header{"X-Claude-Code-Session-Id": {"root"}, "X-Claude-Code-Agent-Id": {"child"}, "X-Claude-Code-Parent-Agent-Id": {"parent"}}, Identity{agentIdentityForClient("claude", "root", "child"), agentIdentityForClient("claude", "root", "parent"), false, true}},
		{"claude metadata", `{"metadata":{"user_id":"{\"session_id\":\"root\",\"agent_id\":\"child\"}"}}`, "", http.Header{"X-Session-Id": {"other"}}, Identity{agentIdentityForClient("claude", "root", "child"), "claude:root", false, true}},
		{"claude legacy", `{"metadata":{"user_id":"user_fixture_session_abcd-1234"}}`, "", nil, Identity{SessionID: "claude:abcd-1234"}},
		{"generic header", `{}`, "", http.Header{"X-Session-Id": {"child"}, "X-Parent-Session-Id": {"parent"}}, Identity{"header:child", "header:parent", false, true}},
		{"agy nested", `{"request":{"parent_session_id":"parent"}}`, "", http.Header{"X-Http-Session-Id": {"child"}}, Identity{"agy:child", "agy:parent", false, true}},
		{"thread fork", `{"thread_id":"child","parent_thread_id":"other","forked_from_id":"parent"}`, "", nil, Identity{"thread:child", "thread:parent", true, false}},
		{"conversation", `{"conversation":{"id":"root"}}`, "", nil, Identity{SessionID: "conv:root"}},
		{"user", `{"metadata":{"user_id":"fixture-user"}}`, "", nil, Identity{SessionID: "user:fixture-user"}},
		{"execution", `{}`, "fixture-connection", nil, Identity{SessionID: "execution:fixture-connection"}},
		{"self reference", `{"session_id":"root","parent_session_id":"root"}`, "", nil, Identity{SessionID: "session:root"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, ok := ExtractExplicitIdentity(test.headers, []byte(test.body), test.execution)
			if !ok || got != test.want {
				t.Fatalf("identity = %+v, %v; want %+v", got, ok, test.want)
			}
		})
	}
	for _, body := range []string{
		`{"prompt_cache_key":"shared-prefix"}`, `{"session_id":42}`, `{"metadata":{"user_id":{"session_id":"nested-business"}}}`,
		`{"input":[{"arguments":{"session_id":"business","parent_session_id":"business-parent"}}]}`,
	} {
		if got, ok := ExtractExplicitIdentity(nil, []byte(body), ""); ok || got != (Identity{}) {
			t.Fatal("cache key, invalid type or arbitrary business JSON established an identity")
		}
	}
}
