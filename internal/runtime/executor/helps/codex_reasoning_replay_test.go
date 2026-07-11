package helps

import (
	"context"
	"encoding/base64"
	"net/http"
	"testing"
	"time"

	"github.com/tidwall/gjson"
)

func TestSanitizeCodexReasoningEncryptedContent(t *testing.T) {
	valid := testCodexReasoningSignature()
	body := []byte(`{"input":[{"type":"reasoning","encrypted_content":"` + valid + `"},{"type":"reasoning","encrypted_content":"bad"},{"type":"message","content":"ok"}]}`)

	got := SanitizeCodexReasoningEncryptedContent(context.Background(), "test", body)
	if value := gjson.GetBytes(got, "input.0.encrypted_content").String(); value != valid {
		t.Fatalf("valid encrypted_content = %q, want preserved", value)
	}
	if gjson.GetBytes(got, "input.1.encrypted_content").Exists() {
		t.Fatalf("invalid encrypted_content should be removed: %s", got)
	}
	if value := gjson.GetBytes(got, "input.2.content").String(); value != "ok" {
		t.Fatalf("non-reasoning input changed: %s", got)
	}
}

func TestApplyCodexReasoningReplayInjectsReasoningAndMatchedToolCall(t *testing.T) {
	resetCodexReasoningReplayStoreForTest()
	signature := testCodexReasoningSignature()
	scope := CodexReasoningReplayScope{namespace: "tenant-a", modelName: "gpt-5", sessionKey: "execution:session-1"}
	completed := []byte(`{"response":{"output":[{"type":"reasoning","encrypted_content":"` + signature + `"},{"type":"function_call","call_id":"call_1","name":"lookup","arguments":"{\"q\":1}"}]}}`)
	if !CacheCodexReasoningReplayFromCompleted(scope, completed) {
		t.Fatal("expected completed response to be cached")
	}

	body := []byte(`{"input":[{"type":"function_call_output","call_id":"call_1","output":"ok"}]}`)
	got, gotScope := ApplyCodexReasoningReplay(context.Background(), "claude", "tenant-a", "gpt-5", nil, body, nil, map[string]any{"execution_session_id": "session-1"}, nil)
	if !gotScope.valid() {
		t.Fatal("expected replay scope")
	}
	if gotType := gjson.GetBytes(got, "input.0.type").String(); gotType != "reasoning" {
		t.Fatalf("input.0.type = %q, want reasoning; body=%s", gotType, got)
	}
	if gotType := gjson.GetBytes(got, "input.1.type").String(); gotType != "function_call" {
		t.Fatalf("input.1.type = %q, want function_call; body=%s", gotType, got)
	}
	if gotType := gjson.GetBytes(got, "input.2.type").String(); gotType != "function_call_output" {
		t.Fatalf("input.2.type = %q, want function_call_output; body=%s", gotType, got)
	}
}

func TestApplyCodexReasoningReplaySkipsOrphanToolCall(t *testing.T) {
	resetCodexReasoningReplayStoreForTest()
	signature := testCodexReasoningSignature()
	scope := CodexReasoningReplayScope{namespace: "tenant-a", modelName: "gpt-5", sessionKey: "execution:session-1"}
	completed := []byte(`{"response":{"output":[{"type":"reasoning","encrypted_content":"` + signature + `"},{"type":"function_call","call_id":"call_1","name":"lookup","arguments":"{}"}]}}`)
	CacheCodexReasoningReplayFromCompleted(scope, completed)

	body := []byte(`{"input":[{"type":"message","role":"user","content":"next"}]}`)
	got, _ := ApplyCodexReasoningReplay(context.Background(), "claude", "tenant-a", "gpt-5", nil, body, nil, map[string]any{"execution_session_id": "session-1"}, nil)
	if gjson.GetBytes(got, "input.#(type==\"function_call\")").Exists() {
		t.Fatalf("orphan function_call should not be replayed: %s", got)
	}
	if gotType := gjson.GetBytes(got, "input.0.type").String(); gotType != "reasoning" {
		t.Fatalf("cached reasoning should precede the next user input: %s", got)
	}
	if gotType := gjson.GetBytes(got, "input.1.type").String(); gotType != "message" {
		t.Fatalf("user input should remain after replayed reasoning: %s", got)
	}
}

func TestApplyCodexReasoningReplayAlignsSanitizedToolCallID(t *testing.T) {
	resetCodexReasoningReplayStoreForTest()
	scope := CodexReasoningReplayScope{namespace: "tenant-a", modelName: "gpt-5", sessionKey: "execution:session-1"}
	completed := []byte(`{"response":{"output":[{"type":"function_call","call_id":"call:1","name":"lookup","arguments":"{}"}]}}`)
	if !CacheCodexReasoningReplayFromCompleted(scope, completed) {
		t.Fatal("expected function call to be cached")
	}

	body := []byte(`{"input":[{"type":"function_call_output","call_id":"call_1","output":"ok"}]}`)
	got, _ := ApplyCodexReasoningReplay(context.Background(), "claude", "tenant-a", "gpt-5", nil, body, nil, map[string]any{"execution_session_id": "session-1"}, nil)
	if gotCallID := gjson.GetBytes(got, "input.0.call_id").String(); gotCallID != "call_1" {
		t.Fatalf("replayed call_id = %q, want call_1; body=%s", gotCallID, got)
	}
}

func TestClearCodexReasoningReplayOnInvalidSignature(t *testing.T) {
	resetCodexReasoningReplayStoreForTest()
	scope := CodexReasoningReplayScope{namespace: "tenant-a", modelName: "gpt-5", sessionKey: "session-1"}
	setCodexReasoningReplayItems(scope, [][]byte{[]byte(`{"type":"reasoning","encrypted_content":"` + testCodexReasoningSignature() + `"}`)})

	if !ClearCodexReasoningReplayOnInvalidSignature(scope, http.StatusBadRequest, []byte(`{"error":{"code":"invalid_encrypted_content"}}`)) {
		t.Fatal("expected invalid signature response to clear replay")
	}
	if _, ok := getCodexReasoningReplayItems(scope); ok {
		t.Fatal("replay entry still exists")
	}
}

func TestClearCodexReasoningReplayOnStructuredInvalidSignature(t *testing.T) {
	resetCodexReasoningReplayStoreForTest()
	scope := CodexReasoningReplayScope{namespace: "tenant-a", modelName: "gpt-5", sessionKey: "session-1"}
	setCodexReasoningReplayItems(scope, [][]byte{[]byte(`{"type":"reasoning","encrypted_content":"` + testCodexReasoningSignature() + `"}`)})

	if !ClearCodexReasoningReplayOnInvalidSignature(scope, http.StatusBadRequest, []byte(`{"type":"error","error":{"code":"invalid_signature","message":"verification failed"}}`)) {
		t.Fatal("expected structured invalid_signature to clear replay")
	}
	if _, ok := getCodexReasoningReplayItems(scope); ok {
		t.Fatal("replay entry still exists")
	}
}

func TestCodexReasoningReplayExpiresOldEntries(t *testing.T) {
	resetCodexReasoningReplayStoreForTest()
	scope := CodexReasoningReplayScope{namespace: "tenant-a", modelName: "gpt-5", sessionKey: "session-1"}
	key := codexReasoningReplayKey(scope)
	codexReasoningReplayStore.Lock()
	item := []byte(`{"type":"reasoning"}`)
	codexReasoningReplayStore.entries[key] = codexReasoningReplayEntry{items: [][]byte{item}, timestamp: time.Now().Add(-CodexReasoningReplayCacheTTL - time.Second), size: int64(len(item))}
	codexReasoningReplayStore.totalBytes = int64(len(item))
	codexReasoningReplayStore.Unlock()

	if _, ok := getCodexReasoningReplayItems(scope); ok {
		t.Fatal("expired replay entry should not be returned")
	}
}

func TestCodexReasoningReplayIsolatedByNamespace(t *testing.T) {
	resetCodexReasoningReplayStoreForTest()
	scopeA := CodexReasoningReplayScope{namespace: "tenant-a", modelName: "gpt-5", sessionKey: "session-1"}
	scopeB := CodexReasoningReplayScope{namespace: "tenant-b", modelName: "gpt-5", sessionKey: "session-1"}
	if !setCodexReasoningReplayItems(scopeA, [][]byte{[]byte(`{"type":"reasoning"}`)}) {
		t.Fatal("failed to cache tenant-a replay")
	}
	if _, ok := getCodexReasoningReplayItems(scopeB); ok {
		t.Fatal("tenant-b read tenant-a replay")
	}
}

func TestCodexReasoningReplayRejectsOversizedEntry(t *testing.T) {
	resetCodexReasoningReplayStoreForTest()
	scope := CodexReasoningReplayScope{namespace: "tenant-a", modelName: "gpt-5", sessionKey: "session-1"}
	if setCodexReasoningReplayItems(scope, [][]byte{make([]byte, CodexReasoningReplayCacheMaxEntryBytes+1)}) {
		t.Fatal("oversized replay entry was cached")
	}
	if _, ok := getCodexReasoningReplayItems(scope); ok {
		t.Fatal("oversized replay entry remained in cache")
	}
}

func TestCodexReplaySessionHeaderAliasesShareKey(t *testing.T) {
	first := http.Header{"Session-Id": []string{"session-1"}}
	second := http.Header{"Session_id": []string{"session-1"}}
	if gotFirst, gotSecond := codexReplaySessionKeyFromHeaders(first), codexReplaySessionKeyFromHeaders(second); gotFirst != gotSecond {
		t.Fatalf("session aliases = %q and %q", gotFirst, gotSecond)
	}
}

func TestNormalizeCodexToolSelection(t *testing.T) {
	withoutTools := NormalizeCodexToolSelection([]byte(`{"tools":[],"tool_choice":"required","parallel_tool_calls":true}`))
	if gjson.GetBytes(withoutTools, "tool_choice").Exists() || gjson.GetBytes(withoutTools, "parallel_tool_calls").Exists() {
		t.Fatalf("tool-only controls should be removed: %s", withoutTools)
	}

	withTools := NormalizeCodexToolSelection([]byte(`{"tools":[{"type":"function","name":"lookup"}],"tool_choice":"required","parallel_tool_calls":true}`))
	if !gjson.GetBytes(withTools, "tool_choice").Exists() || !gjson.GetBytes(withTools, "parallel_tool_calls").Exists() {
		t.Fatalf("tool-only controls should be preserved: %s", withTools)
	}
}

func testCodexReasoningSignature() string {
	decoded := make([]byte, 73)
	decoded[0] = 0x80
	return base64.RawURLEncoding.EncodeToString(decoded)
}

func resetCodexReasoningReplayStoreForTest() {
	codexReasoningReplayStore.Lock()
	codexReasoningReplayStore.entries = make(map[string]codexReasoningReplayEntry)
	codexReasoningReplayStore.totalBytes = 0
	codexReasoningReplayStore.lastPurge = time.Time{}
	codexReasoningReplayStore.Unlock()
}
