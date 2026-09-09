package helps

import (
	"context"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexCumulativeReplayPublishesOnlySuccessfulMatchingHistory(t *testing.T) {
	resetCodexReasoningReplayStoreForTest()
	t.Cleanup(resetCodexReasoningReplayStoreForTest)
	apply := func(input string) ([]byte, CodexReasoningReplayScope) {
		return ApplyCodexReasoningReplay(t.Context(), "claude", "tenant:credential", "model", nil, []byte(`{"prompt_cache_key":"keep","input":`+input+`}`), nil, nil, nil)
	}
	_, first := apply(`[{"role":"user","content":"question1"}]`)
	firstOutput := []byte(`{"type":"response.completed","response":{"status":"completed","output":[` + string(reasoningTurnItem(1)) + `,{"type":"message","role":"assistant","content":"answer1"}]}}`)
	if !CacheCodexReasoningReplayFromCompleted(first, firstOutput, t.Context()) {
		t.Fatal("first completed turn was not stored")
	}
	secondBody, second := apply(`[{"role":"user","content":"question1"},{"role":"assistant","content":"answer1"},{"role":"user","content":"question2"}]`)
	if gjson.GetBytes(secondBody, "input.1").Raw != string(reasoningTurnItem(1)) {
		t.Fatal("matching first turn was not restored")
	}
	secondOutput := []byte(`{"type":"response.completed","response":{"status":"completed","output":[` + string(reasoningTurnItem(2)) + `,{"type":"message","role":"assistant","content":"answer2"}]}}`)
	if !CacheCodexReasoningReplayFromCompleted(second, secondOutput, t.Context()) {
		t.Fatal("second completed turn was not stored")
	}
	for _, invalid := range []string{
		`{"type":"response.incomplete","response":{"status":"incomplete","output":[]}}`,
		`{"type":"response.failed","response":{"status":"failed","output":[]}}`,
		`{"type":"response.completed","response":{"status":"completed","error":{"code":"blocked"},"output":[]}}`,
		`{"type":"response.completed","error":"blocked","response":{"output":[]}}`,
		`{"type":"response.completed","response":{"output":[]}} trailing`,
	} {
		invalid = strings.Replace(invalid, `"output":[]`, `"output":[`+string(reasoningTurnItem(9))+`]`, 1)
		if CacheCodexReasoningReplayFromCompleted(second, []byte(invalid), t.Context()) {
			t.Fatal("invalid result published replay history")
		}
	}
	if CacheCodexReasoningReplayFromCompleted(second, []byte(`{"type":"response.completed","response":{"output":[]}}`), t.Context()) {
		t.Fatal("empty completion published replay history")
	}
	out, last := apply(`[{"role":"user","content":"question1"},{"role":"assistant","content":"answer1"},{"role":"user","content":"question2"},{"role":"assistant","content":"answer2"},{"role":"user","content":"question3"}]`)
	if gjson.GetBytes(out, "input.#").Int() != 7 || gjson.GetBytes(out, "input.1").Raw != string(reasoningTurnItem(1)) || gjson.GetBytes(out, "input.4").Raw != string(reasoningTurnItem(2)) || gjson.GetBytes(out, "prompt_cache_key").String() != "keep" {
		t.Fatal("later failure or empty completion lost committed cumulative history")
	}
	if len(getCodexReasoningReplayTurns(last)) != 2 {
		t.Fatal("unexpected cumulative history count")
	}
	if !ClearCodexReasoningReplayOnInvalidSignature(last, 400, []byte(`{"error":{"code":"invalid_encrypted_content"}}`)) || len(getCodexReasoningReplayTurns(last)) != 0 {
		t.Fatal("signature rejection did not clear cumulative state")
	}
}

func TestCodexReplayCancellationWhileWaitingForPublication(t *testing.T) {
	resetCodexReasoningReplayStoreForTest()
	t.Cleanup(resetCodexReasoningReplayStoreForTest)
	scope := CodexReasoningReplayScope{namespace: "tenant", modelName: "model", sessionKey: "session"}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started := make(chan struct{})
	result := make(chan bool, 1)
	codexReasoningReplayStore.Lock()
	go func() {
		close(started)
		result <- appendCodexReasoningReplayTurn(scope, replayTurnFixture(0), ctx)
	}()
	<-started
	cancel()
	codexReasoningReplayStore.Unlock()
	if <-result || len(getCodexReasoningReplayTurns(scope)) != 0 {
		t.Fatal("cancelled request published after waiting for the cache lock")
	}
}
