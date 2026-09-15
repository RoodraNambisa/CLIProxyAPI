package helps

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func xaiFoldFixture(t *testing.T, count int) ([]byte, []byte) {
	t.Helper()
	var tools []any
	for i := range count {
		tools = append(tools, map[string]any{"type": "function", "name": fmt.Sprintf("tool_%d", i), "parameters": map[string]any{"type": "object", "properties": map[string]any{"x": map[string]any{"type": "integer"}}}})
	}
	original, _ := json.Marshal(map[string]any{"tools": []any{map[string]any{"type": "namespace", "name": "workspace", "tools": tools}}})
	body, _ := json.Marshal(map[string]any{"tools": tools})
	return body, original
}

func TestXAIToolFoldingBoundaryRestrictionsAndHistory(t *testing.T) {
	body, original := xaiFoldFixture(t, 201)
	body, _ = sjson.SetRawBytes(body, "input", []byte(`[{"type":"function_call","call_id":"call_one","name":"tool_1","namespace":"workspace","arguments":"{\"x\":2}"},{"type":"function_call_output","call_id":"call_one","output":"ok"}]`))
	out, plan, err := FoldXAITools(body, original)
	if err != nil || plan == nil || gjson.GetBytes(out, "tools.#").Int() != 1 {
		t.Fatalf("fold: %v %s", err, out)
	}
	if gjson.GetBytes(out, "input.0.name").String() != "workspace" || gjson.GetBytes(out, "input.1.output").String() != "ok" {
		t.Fatalf("history: %s", out)
	}
	args := gjson.Parse(gjson.GetBytes(out, "input.0.arguments").String())
	if args.Get("name").String() != "tool_1" || args.Get("arguments.x").Int() != 2 {
		t.Fatal("history arguments changed")
	}
	body, _ = sjson.SetRawBytes(body, "tool_choice", []byte(`{"type":"allowed_tools","mode":"required","tools":[{"type":"function","name":"tool_1"}]}`))
	out, plan, err = FoldXAITools(body, original)
	if err != nil || plan != nil || gjson.GetBytes(out, "tools.#").Int() != 1 || gjson.GetBytes(out, "tools.0.name").String() != "tool_1" {
		t.Fatal("allowlist was broadened")
	}
	boundary, original := xaiFoldFixture(t, 200)
	out, plan, err = FoldXAITools(boundary, original)
	if err != nil || plan != nil || string(out) != string(boundary) {
		t.Fatal("valid 200-tool request changed")
	}
	body, original = xaiFoldFixture(t, 201)
	if _, _, err = FoldXAITools(body, body); err == nil {
		t.Fatal("unfoldable overflow was silently truncated")
	}
}

func TestXAIToolFoldingRestoresStreamEnvelopeBeforeTranslation(t *testing.T) {
	body, original := xaiFoldFixture(t, 201)
	_, plan, err := FoldXAITools(body, original)
	if err != nil {
		t.Fatal(err)
	}
	added := []byte(`{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"fc_one","call_id":"call_one","name":"workspace","arguments":""}}`)
	if chunks := plan.StreamFrames(xaiFoldFrame(added)); len(chunks) != 0 {
		t.Fatal("dispatcher name leaked before child was known")
	}
	if chunks := plan.StreamFrames(xaiFoldFrame([]byte(`{"type":"response.function_call_arguments.delta","item_id":"fc_one","delta":"{\"name\":\"tool_1\""}`))); len(chunks) != 0 {
		t.Fatal("wrapper delta leaked")
	}
	done := []byte(`{"type":"response.function_call_arguments.done","item_id":"fc_one","arguments":"{\"name\":\"tool_1\",\"arguments\":{\"x\":2}}"}`)
	chunks := plan.StreamFrames(xaiFoldFrame(done))
	if len(chunks) != 3 {
		t.Fatalf("restored frames=%d", len(chunks))
	}
	first := ParseOpenAIStreamFrame(chunks[0])[0].Data
	if gjson.GetBytes(first, "item.name").String() != "tool_1" || gjson.GetBytes(first, "item.namespace").String() != "workspace" || gjson.GetBytes(first, "item.call_id").String() != "call_one" {
		t.Fatalf("restored call: %s", first)
	}
	delta := ParseOpenAIStreamFrame(chunks[1])[0].Data
	if gjson.GetBytes(delta, "delta").String() != `{"x":2}` {
		t.Fatalf("restored arguments: %s", delta)
	}
	completed := []byte(`{"type":"response.completed","response":{"id":"resp_one","output":[{"type":"function_call","id":"fc_one","call_id":"call_one","name":"workspace","arguments":"{\"name\":\"tool_1\",\"arguments\":{\"x\":2}}"}]}}`)
	out := plan.Restore(completed)
	if gjson.GetBytes(out, "response.id").String() != "resp_one" || strings.Contains(gjson.GetBytes(out, "response.output.0.arguments").String(), "tool_1") {
		t.Fatalf("completed output: %s", out)
	}
	if len(plan.StreamFrames(xaiFoldFrame(completed))) != 1 {
		t.Fatal("completed output duplicated previously streamed arguments")
	}
}
