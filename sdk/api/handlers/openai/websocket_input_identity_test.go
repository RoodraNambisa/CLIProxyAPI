package openai

import (
	"fmt"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/tidwall/gjson"
)

func TestWebsocketInputIDCollisionPreservesDistinctProtocolItems(t *testing.T) {
	for _, input := range []string{
		`[{"id":"same","type":"function_call","call_id":"a"},{"id":"same","type":"function_call","call_id":"b"}]`,
		`[{"id":"same","type":"function_call_output","call_id":"a"},{"id":"same","type":"function_call_output","call_id":"b"}]`,
		`[{"id":"same","type":"custom_tool_call","call_id":"a"},{"id":"same","type":"custom_tool_call_output","call_id":"a"}]`,
		`[{"id":"same","type":"message","role":"assistant"},{"id":"same","type":"reasoning"}]`,
	} {
		out, err := dedupeInputItemsByID(input)
		if err != nil {
			t.Fatal(err)
		}
		if len(gjson.Parse(out).Array()) != 2 {
			t.Error("a reused item ID removed a distinct protocol item")
		}
	}
}

func TestWebsocketInputDedupeRetainsFirstSemanticDuplicate(t *testing.T) {
	for _, input := range []string{
		`[{"id":"same","type":"message","role":"assistant","content":"first"},{"id":"same","role":"assistant","content":"later"}]`,
		`[{"id":"same","type":"function_call","call_id":"a","arguments":"first"},{"id":"same","type":"function_call","call_id":"a","arguments":"later"}]`,
	} {
		out, err := dedupeInputItemsByID(input)
		if err != nil {
			t.Fatal(err)
		}
		items := gjson.Parse(out).Array()
		if len(items) != 1 || (items[0].Get("content").String() != "first" && items[0].Get("arguments").String() != "first") {
			t.Fatal("semantic duplicates no longer retain the first local history item")
		}
	}
}

func TestWebsocketCollisionSurvivesMergeRepairAndCodexIDNormalization(t *testing.T) {
	for _, custom := range []bool{false, true} {
		t.Run(fmt.Sprintf("custom=%t", custom), func(t *testing.T) {
			callKind, outputKind, valueField := "function_call", "function_call_output", "arguments"
			if custom {
				callKind, outputKind, valueField = "custom_tool_call", "custom_tool_call_output", "input"
			}
			previous := []byte(fmt.Sprintf(`{"model":"fixture","input":[{"id":"same","type":%q,"call_id":"a","name":"tool",%q:"{}"},{"id":"same","type":%q,"call_id":"b","name":"tool",%q:"{}"}]}`, callKind, valueField, callKind, valueField))
			next := []byte(fmt.Sprintf(`{"type":"response.append","input":[{"id":"result","type":%q,"call_id":"a","output":"one"},{"id":"result","type":%q,"call_id":"b","output":"two"}]}`, outputKind, outputKind))
			merged, history, err := normalizeResponseSubsequentRequest(next, previous, nil, false)
			if err != nil {
				t.Fatal(err.Error)
			}
			if len(merged) == 0 || &merged[0] == &history[0] {
				t.Fatal("request and committed-history buffers must remain independently owned")
			}
			turn := newResponsesWebsocketToolCacheTurn(newWebsocketToolPairState())
			out := helps.SanitizeCodexInputItemIDs(turn.repairRequest(merged))
			items := gjson.GetBytes(out, "input").Array()
			if len(items) != 4 {
				t.Fatalf("merged/repaired input items = %d, want both call/result pairs", len(items))
			}
			ids := make(map[string]bool)
			for index, item := range items {
				id := item.Get("id").String()
				if id == "" || ids[id] {
					t.Fatal("Codex output still has colliding input IDs")
				}
				ids[id] = true
				wantCall := "a"
				if index == 1 || index == 3 {
					wantCall = "b"
				}
				if item.Get("call_id").String() != wantCall {
					t.Fatal("ID collision repair changed a tool-pair call_id")
				}
			}
		})
	}
}
