package helps

import (
	"net/http"
	"testing"
)

func TestCodexRequestScopeUsesOriginalThreadAndRequestKind(t *testing.T) {
	headers := http.Header{"Session-Id": {"header-session"}, "Thread-Id": {"header-thread"}, "X-Codex-Turn-Metadata": {`{"request_kind":"memory"}`}}
	body := []byte(`{"prompt_cache_key":"shared-cache","client_metadata":{"thread_id":"body-thread","x-codex-turn-metadata":"{\"session_id\":\"body-session\",\"request_kind\":\"turn\"}"},"input":{"thread_id":"business-thread"}}`)
	snapshot := SnapshotCodexRequestScope(body, headers)
	clear(body)
	if snapshot.ThreadID != "body-thread" || snapshot.SessionID != "body-session" || snapshot.Kind != "turn" {
		t.Fatalf("invalid or non-owned scope: %+v", snapshot)
	}
	if got := SnapshotCodexRequestScope([]byte(`{}`), headers); got.Kind != "memory" || got.ThreadID != "header-thread" {
		t.Fatalf("missing header-only identity: %+v", got)
	}
	if got := SnapshotCodexRequestScope([]byte(`{"generate":false}`), headers); got.Kind != "prewarm" {
		t.Fatal("prewarm misclassified")
	}
	first := CodexRequestScopeDigest(WithCodexRequestScope(t.Context(), snapshot, "model-a"))
	for _, scope := range []CodexRequestScope{{ThreadID: "other-thread", SessionID: snapshot.SessionID, Kind: snapshot.Kind}, {ThreadID: snapshot.ThreadID, SessionID: snapshot.SessionID, Kind: "memory"}} {
		if CodexRequestScopeDigest(WithCodexRequestScope(t.Context(), scope, "model-a")) == first {
			t.Fatal("distinct request contexts share upstream state")
		}
	}
	if CodexRequestScopeDigest(WithCodexRequestScope(t.Context(), snapshot, "model-b")) == first {
		t.Fatal("different models share upstream context")
	}
}

func TestCodexReplayScopeSeparatesThreadAndMemoryWithSharedCacheKey(t *testing.T) {
	key := func(payload string) string {
		return codexReasoningReplayScopeFromRequest(t.Context(), "claude", "same-account-and-caller", "model", []byte(payload), []byte(`{"prompt_cache_key":"same-cache"}`), nil, nil, nil).sessionKey
	}
	main := key(`{"client_metadata":{"thread_id":"main"}}`)
	other := key(`{"client_metadata":{"thread_id":"other"}}`)
	memory := key(`{"client_metadata":{"thread_id":"main","request_kind":"memory"}}`)
	if main == "" || main == other || main == memory {
		t.Fatal("replay shares a cache across original thread or kind")
	}
}

func TestCodexParentHeaderRecoveryDoesNotInventOrOverrideReferences(t *testing.T) {
	headers := make(http.Header)
	RestoreCodexParentThreadHeader(headers, []byte(`{"client_metadata":{"x-codex-turn-metadata":"{\"parent_thread_id\":\"parent\"}"}}`))
	if headers.Get("X-Codex-Parent-Thread-Id") != "parent" {
		t.Fatal("existing parent metadata was not recovered")
	}
	RestoreCodexParentThreadHeader(headers, []byte(`{"client_metadata":{"parent_thread_id":"other"}}`))
	if headers.Get("X-Codex-Parent-Thread-Id") != "parent" {
		t.Fatal("explicit/projected header was overwritten")
	}
	for _, payload := range []string{`{}`, `{"input":{"parent_thread_id":"business"}}`, `{"client_metadata":{"parent_thread_id":"bad\u0000"}}`} {
		headers = make(http.Header)
		RestoreCodexParentThreadHeader(headers, []byte(payload))
		if len(headers) != 0 {
			t.Fatal("invented a parent reference")
		}
	}
}
