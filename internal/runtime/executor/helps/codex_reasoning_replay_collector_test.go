package helps

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexReplayCollectorReconstructsOwnedOrderedOutput(t *testing.T) {
	resetCodexReasoningReplayStoreForTest()
	t.Cleanup(resetCodexReasoningReplayStoreForTest)
	_, scope := ApplyCodexReasoningReplay(t.Context(), "claude", "tenant", "model", nil, []byte(`{"prompt_cache_key":"session","input":[]}`), nil, nil, nil)
	c := NewCodexReasoningReplayCollector(scope)
	message := []byte(`{"type":"response.output_item.done","output_index":1,"item":{"type":"message","role":"assistant","content":"answer"}}`)
	reasoning := []byte(`{"type":"response.output_item.done","output_index":0,"item":` + string(reasoningTurnItem(1)) + `}`)
	c.Observe(message)
	c.Observe(reasoning)
	c.Observe(reasoning)
	clear(message)
	clear(reasoning)
	const terminal = `{"type":"response.completed","response":{"status":"completed","output":[]}}`
	data := []byte(terminal)
	if !c.Commit(t.Context(), data) || string(data) != terminal || c.Commit(t.Context(), data) || c.indexed != nil || c.bytes != 0 {
		t.Fatal("collector lost ownership, changed the response or committed twice")
	}
	out, _ := ApplyCodexReasoningReplay(t.Context(), "claude", "tenant", "model", nil, []byte(`{"prompt_cache_key":"session","input":[{"role":"assistant","content":"answer"}]}`), nil, nil, nil)
	if gjson.GetBytes(out, "input.0.encrypted_content").String() != gjson.GetBytes(reasoningTurnItem(1), "encrypted_content").String() {
		t.Fatal("out-of-order done events did not reconstruct a matched turn")
	}
}

func TestCodexReplayCollectorRejectsUncommittableOutputs(t *testing.T) {
	for _, mode := range []string{"cancel", "partial", "error", "conflict", "budget", "invalid index", "invalid output"} {
		t.Run(mode, func(t *testing.T) {
			resetCodexReasoningReplayStoreForTest()
			t.Cleanup(resetCodexReasoningReplayStoreForTest)
			_, scope := ApplyCodexReasoningReplay(t.Context(), "claude", "tenant", "model", nil, []byte(`{"prompt_cache_key":"session","input":[]}`), nil, nil, nil)
			c := NewCodexReasoningReplayCollector(scope)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			event := []byte(`{"type":"response.output_item.done","output_index":0,"item":` + string(reasoningTurnItem(1)) + `}`)
			terminal := []byte(`{"type":"response.completed","response":{"status":"completed","output":[]}}`)
			if mode == "invalid index" {
				event = []byte(strings.Replace(string(event), `"output_index":0`, `"output_index":1.5`, 1))
			}
			c.Observe(event)
			switch mode {
			case "cancel":
				cancel()
			case "partial":
				terminal = []byte(`{"type":"response.incomplete","response":{"output":[]}}`)
			case "error":
				terminal = []byte(`{"type":"response.completed","response":{"output":[],"error":{"code":"blocked"}}}`)
			case "conflict":
				c.Observe([]byte(`{"type":"response.output_item.done","output_index":0,"item":` + string(reasoningTurnItem(2)) + `}`))
			case "budget":
				c.Observe(fmt.Appendf(nil, `{"type":"response.output_item.done","item":{"type":"message","role":"assistant","content":%q}}`, strings.Repeat("x", CodexReasoningReplayCacheMaxEntryBytes)))
			case "invalid output":
				terminal = []byte(`{"type":"response.completed","response":{"output":{}}}`)
			}
			if c.Commit(ctx, terminal) || len(getCodexReasoningReplayTurns(scope)) != 0 || c.indexed != nil || c.bytes != 0 {
				t.Fatal("uncommittable output published a turn or retained captured items")
			}
		})
	}
	var disabled *CodexReasoningReplayCollector
	disabled.Observe([]byte(`{}`))
	if NewCodexReasoningReplayCollector(CodexReasoningReplayScope{}) != nil || disabled.Commit(t.Context(), []byte(`{}`)) {
		t.Fatal("invalid scope enabled collection")
	}
	const incremental = `{"prompt_cache_key":"session","previous_response_id":"prior","input":[]}`
	body, scope := ApplyCodexReasoningReplay(t.Context(), "claude", "tenant", "model", nil, []byte(incremental), nil, nil, nil)
	if string(body) != incremental || scope.valid() || NewCodexReasoningReplayCollector(scope) != nil {
		t.Fatal("incremental server context enabled full-history replay")
	}
}
