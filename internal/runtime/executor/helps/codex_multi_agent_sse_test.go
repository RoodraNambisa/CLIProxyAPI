package helps

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexMultiAgentSSEChunkKeepsFrameBoundaries(t *testing.T) {
	policy := CodexMultiAgentResponsePolicy{PlaintextCalls: true}
	call := `{"type":"response.output_item.done","item":{"type":"function_call","namespace":"collaboration","name":"spawn_agent","call_id":"pair","arguments":"opaque"}}`
	for _, ending := range []string{"\n", "\r\n", "\r"} {
		frame := "event: response.output_item.done" + ending + "data: " + call + ending + ending
		controls := ": keep" + ending + ending
		partial := "data: {\"type\":\"response.completed\",\"response\":"
		raw := []byte(controls + frame + frame + partial)
		got := policy.RewriteSSEChunk(raw)
		if !bytes.HasPrefix(got, []byte(controls)) || !bytes.HasSuffix(got, []byte(partial)) ||
			bytes.Count(got, []byte(`"encrypted_function_args":[]`)) != 2 ||
			bytes.Count(got, []byte("event: response.output_item.done"+ending)) != 2 {
			t.Fatal("coalesced events lost controls, markers, boundaries or the trailing fragment")
		}
		if disabled := (CodexMultiAgentResponsePolicy{}).RewriteSSEChunk(raw); !bytes.Equal(disabled, raw) || &disabled[0] != &raw[0] {
			t.Fatal("disabled mode copied or changed a chunk")
		}
	}
	for _, raw := range []string{
		"data: [DONE]\n\n", "data: {\"type\":",
		`{"type":"response.completed","response":{"output":[{"type":"message","content":[{"type":"output_text","text":"collaboration"}]}]}}`,
	} {
		payload := []byte(raw)
		if got := policy.RewriteSSEChunk(payload); !bytes.Equal(got, payload) || &got[0] != &payload[0] {
			t.Fatal("non-collaboration data changed")
		}
	}
}

func TestCodexMultiAgentSSEFramePreservesControlsAndOpaqueFields(t *testing.T) {
	policy := CodexMultiAgentResponsePolicy{NamespaceOptimized: true, PlaintextCalls: true}
	for _, ending := range []string{"\n", "\r\n", "\r"} {
		frame := []byte(strings.Join([]string{
			": keep", "event: response.completed", "id: event-id",
			`data: {"type":"response.completed",`,
			"retry: 1000",
			`data: "response":{"output":[{"type":"function_call","namespace":"collaboration-optimize","name":"spawn_agent","call_id":"pair","arguments":"exact collaboration-optimize text"}],"metadata":{"counter":9007199254740993},"prompt_cache_key":"collaboration-optimize"}}`,
			"", "",
		}, ending))
		original := bytes.Clone(frame)
		got := policy.RewriteSSEFrame(frame)
		for _, control := range []string{": keep", "event: response.completed", "id: event-id", "retry: 1000"} {
			if !bytes.Contains(got, []byte(control+ending)) {
				t.Fatal("SSE control line or ending changed")
			}
		}
		var data []string
		normalized := strings.NewReplacer("\r\n", "\n", "\r", "\n").Replace(string(got))
		for _, line := range strings.Split(normalized, "\n") {
			if strings.HasPrefix(line, "data:") {
				data = append(data, strings.TrimPrefix(line, "data:"))
			}
		}
		payload := []byte(strings.Join(data, "\n"))
		if !json.Valid(payload) || len(data) != 1 {
			t.Fatal("rewritten event is not a complete JSON data field")
		}
		if gjson.GetBytes(payload, "response.output.0.namespace").String() != "collaboration" ||
			gjson.GetBytes(payload, "response.output.0.encrypted_function_args").Raw != "[]" ||
			gjson.GetBytes(payload, "response.metadata.counter").Raw != "9007199254740993" ||
			gjson.GetBytes(payload, "response.prompt_cache_key").String() != "collaboration-optimize" ||
			gjson.GetBytes(payload, "response.output.0.arguments").String() != "exact collaboration-optimize text" {
			t.Fatal("tool identity or opaque response fields changed incorrectly")
		}
		if !bytes.Equal(frame, original) || !bytes.Equal(policy.RewriteSSEFrame(got), got) {
			t.Fatal("frame rewrite mutated input or was not idempotent")
		}
	}
}

func TestCodexMultiAgentSSEFrameNoopAndSingleLineShapes(t *testing.T) {
	policy := CodexMultiAgentResponsePolicy{NamespaceOptimized: true, PlaintextCalls: true}
	for _, raw := range []string{"", ": ping\n\n", "event: response.done\n\n", "data: [DONE]\n\n", `data: {"type":"response.completed",`,
		"data:\n\ndata: {\"type\":\"function_call\",\"namespace\":\"collaboration-optimize\",\"name\":\"spawn_agent\"}\n\n",
		`data: {"type":"message","metadata":{"namespace":"collaboration-optimize"}}`} {
		frame := []byte(raw)
		got := policy.RewriteSSEFrame(frame)
		if !bytes.Equal(frame, got) || len(frame) > 0 && &frame[0] != &got[0] {
			t.Fatal("unrelated or partial frame changed")
		}
	}
	event := `{"type":"function_call","namespace":"collaboration-optimize","name":"spawn_agent"}`
	for _, prefix := range []string{"", "data:", "data: ", "  data: "} {
		frame := []byte(prefix + event)
		got := policy.RewriteSSEFrame(frame)
		if !bytes.HasPrefix(got, []byte(prefix)) || !bytes.Contains(got, []byte(`"namespace":"collaboration"`)) || bytes.HasSuffix(got, []byte("\n")) {
			t.Fatal("raw or data-line rewrite altered framing")
		}
		if disabled := (CodexMultiAgentResponsePolicy{}).RewriteSSEFrame(frame); !bytes.Equal(disabled, frame) {
			t.Fatal("disabled policy changed a frame")
		}
	}
}
