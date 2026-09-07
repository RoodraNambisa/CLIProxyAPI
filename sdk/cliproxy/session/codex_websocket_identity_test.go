package session

import (
	"net/http"
	"reflect"
	"testing"
)

func TestCodexFrameIdentityDoesNotReuseHandshakeLineage(t *testing.T) {
	headers := http.Header{
		"Session-Id": {"old-child"}, "X-Codex-Parent-Thread-Id": {"old-parent"},
		"X-Codex-Turn-Metadata": {`{"session_id":"old-child","forked_from_thread_id":"old-parent"}`},
	}
	original := headers.Clone()
	for _, test := range []struct {
		body string
		want Identity
		ok   bool
	}{
		{`{"client_metadata":{"session_id":"child","thread_id":"child","x-codex-parent-thread-id":"parent","x-openai-subagent":"collab_spawn","x-codex-turn-metadata":"{\"session_id\":\"child\",\"parent_thread_id\":\"parent\"}"}}`, Identity{"codex:child", "codex:parent", false, true}, true},
		{`{"client_metadata":{"session_id":"next","thread_id":"next"}}`, Identity{SessionID: "codex:next"}, true},
		{`{"client_metadata":{"session_id":null}}`, Identity{}, false},
		{`{"client_metadata":{"session_id":42}}`, Identity{}, false},
		{`{"client_metadata":{"x-codex-turn-metadata":"{\"session_id\":\"fork\",\"forked_from_thread_id\":\"root\"}"}}`, Identity{"codex:fork", "codex:root", true, false}, true},
		{`{"client_metadata":{"ws_request_header_x_openai_internal_codex_responses_lite":"true"}}`, Identity{"codex:old-child", "codex:old-parent", true, false}, true},
		{`{}`, Identity{"codex:old-child", "codex:old-parent", true, false}, true},
	} {
		got, ok := ExtractCodexIdentity(headers, []byte(test.body))
		if got != test.want || ok != test.ok {
			t.Errorf("frame identity = %+v, %v; want %+v, %v", got, ok, test.want, test.ok)
		}
	}
	if !reflect.DeepEqual(headers, original) {
		t.Fatal("frame projection modified handshake headers")
	}
}
