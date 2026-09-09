package responses

import (
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func claudeReasoningCarrierFixture() []string {
	return []string{
		`{"type":"message_start","message":{"id":"carrier"}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"initial-","signature":"head-"}}`,
		`{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"paired","name":"lookup"}}`,
		`{"type":"content_block_start","index":2,"content_block":{"type":"thinking","signature":"second-"}}`,
		`{"type":"content_block_start","index":3,"content_block":{"type":"redacted_thinking","data":" opaque data "}}`,
		`{"type":"content_block_start","index":4,"content_block":{"type":"text"}}`,
		`{"type":"content_block_delta","index":2,"delta":{"type":"signature_delta","signature":"tail"}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"a"}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"text"}}`,
		`{"type":"content_block_delta","delta":{"type":"signature_delta","signature":"wrong"}}`,
		`{"type":"content_block_delta","index":"0","delta":{"type":"signature_delta","signature":"wrong"}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":123}}`,
		`{"type":"content_block_delta","index":1,"delta":{"type":"signature_delta","signature":"wrong-tool"}}`,
		`{"type":"content_block_delta","index":9,"delta":{"type":"signature_delta","signature":"missing-start"}}`,
		`{"type":"content_block_delta","index":3,"delta":{"type":"signature_delta","signature":"wrong-redacted"}}`,
		`{"type":"content_block_delta","index":3,"delta":{"type":"thinking_delta","thinking":"not-visible"}}`,
		`{"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"wrong"}}`,
		`{"type":"content_block_stop"}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"b"}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"late"}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"thinking","signature":"replacement"}}`,
		`{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{}"}}`,
		`{"type":"content_block_delta","index":2,"delta":{"type":"thinking_delta","thinking":"second-text"}}`,
		`{"type":"content_block_delta","index":4,"delta":{"type":"text_delta","text":"answer"}}`,
		`{"type":"message_stop"}`,
		`{"type":"message_stop"}`,
	}
}

func TestClaudeResponsesReasoningCarriersKeepIndexesAndOpaqueData(t *testing.T) {
	events := claudeReasoningCarrierFixture()
	var state any
	var completed gjson.Result
	deltas := map[int64]string{}
	done := map[int64]string{}
	counts := map[string]int{}
	for _, event := range events {
		wire := []byte("data: " + event)
		chunks := ConvertClaudeResponseToOpenAIResponses(t.Context(), "", nil, nil, wire, &state)
		clear(wire)
		for _, chunk := range chunks {
			for _, line := range strings.Split(string(chunk), "\n") {
				if !strings.HasPrefix(line, "data:") {
					continue
				}
				data := gjson.Parse(strings.TrimPrefix(line, "data:"))
				kind := data.Get("type").String()
				counts[kind]++
				index := data.Get("output_index").Int()
				if kind == "response.reasoning_summary_text.delta" {
					deltas[index] += data.Get("delta").String()
				}
				if strings.HasPrefix(kind, "response.reasoning_summary_") && index == 3 {
					t.Fatal("redacted data acquired a visible summary lifecycle")
				}
				if kind == "response.output_item.done" {
					done[index] = data.Get("item.encrypted_content").String()
				}
				if kind == "response.completed" {
					completed = data.Get("response")
				}
			}
		}
	}
	if counts["response.output_item.added"] != 5 || counts["response.output_item.done"] != 5 || counts["response.completed"] != 1 ||
		deltas[0] != "initial-text" || deltas[2] != "second-text" || len(deltas) != 2 {
		t.Fatal("signature events changed the content lifecycle or partial text")
	}
	raw := []byte("data: " + strings.Join(events, "\n\ndata: ") + "\n\n")
	nonstream := gjson.ParseBytes(ConvertClaudeResponseToOpenAIResponsesNonStream(t.Context(), "", nil, nil, raw, nil))
	for _, result := range []gjson.Result{completed, nonstream} {
		if result.Get("output.#").Int() != 5 ||
			result.Get("output.0.encrypted_content").String() != "head-ab" || result.Get("output.0.summary.0.text").String() != "initial-text" ||
			result.Get("output.1.call_id").String() != "paired" || result.Get("output.2.encrypted_content").String() != "second-tail" ||
			result.Get("output.3.encrypted_content").String() != ClaudeResponsesRedactedThinkingPrefix+" opaque data " ||
			result.Get("output.3.summary").Raw != "[]" || result.Get("output.4.content.0.text").String() != "answer" {
			t.Fatal("terminal carriers lost opaque data, ordering or text")
		}
	}
	if done[0] != "head-ab" || done[2] != "second-tail" || done[3] != ClaudeResponsesRedactedThinkingPrefix+" opaque data " {
		t.Fatal("item completion and terminal signature disagree")
	}
	next := []string{`{"type":"message_start","message":{"id":"next"}}`, `{"type":"content_block_start","index":0,"content_block":{"type":"thinking"}}`, `{"type":"message_stop"}`}
	for _, event := range next {
		ConvertClaudeResponseToOpenAIResponses(t.Context(), "", nil, nil, []byte("data: "+event), &state)
	}
	block := state.(*claudeToResponsesState).ReasoningBlocks[0]
	if block.Signature.Len() != 0 || block.Redacted {
		t.Fatal("new response inherited a previous signature")
	}
}
