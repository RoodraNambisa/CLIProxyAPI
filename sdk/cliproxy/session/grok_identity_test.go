package session

import (
	"net/http"
	"testing"
)

func TestGrokSessionPrecedenceAndExcludedTransientIDs(t *testing.T) {
	for _, test := range []struct {
		headers http.Header
		body    string
		want    string
	}{
		{http.Header{"X-Grok-Session-Id": {"main"}, "X-Grok-Conv-Id": {"side"}}, `{"prompt_cache_key":"cache"}`, "grok:main"},
		{http.Header{"X-Grok-Conv-Id": {"side"}}, `{}`, "grok:side"},
		{http.Header{"X-Grok-Req-Id": {"request"}, "X-Grok-Agent-Id": {"agent"}, "X-Grok-User-Id": {"user"}, "X-Client-Request-Id": {"request"}}, `{"metadata":{"user_id":"account-only"}}`, ""},
		{http.Header{}, `{"metadata":{"user_id":"{\"session_id\":\"claude-main\"}"}}`, "claude:claude-main"},
	} {
		got, ok := ExtractGrokExplicitIdentity(test.headers, []byte(test.body), "")
		if got.SessionID != test.want || ok != (test.want != "") {
			t.Fatalf("got %q, want %q", got.SessionID, test.want)
		}
	}
	identity, _ := ExtractGrokExplicitIdentity(http.Header{"X-Grok-Conv-Id": {"side"}}, []byte(`{"prompt_cache_key":"independent-cache"}`), "")
	if identity.SessionID == "grok:side" || identity.SessionID == "" {
		t.Fatal("conversation masked explicit cache key")
	}
}
