package responses

import (
	"context"
	"fmt"
	"testing"

	"github.com/tidwall/gjson"
)

func TestInteractionsToolStreamIdentityAndLifecycle(t *testing.T) {
	var state any
	var output [][]byte
	feed := func(raw string) [][]byte {
		got := ConvertInteractionsResponseToOpenAIResponses(context.Background(), "fixture", nil, nil, []byte(raw), &state)
		output = append(output, got...)
		return got
	}
	feed(`{"event_type":"step.start","index":0,"step":{"type":"function_call","id":"item_a","call_id":"call_a","name":"first","arguments":{}}}`)
	feed(`{"event_type":"step.start","index":2,"step":{"type":"function_call","id":"item_b","call_id":"call_b","name":"second","arguments":{}}}`)
	for _, raw := range []string{
		`{"event_type":"step.start","index":0,"step":{"type":"function_call","id":"replacement","name":"wrong"}}`,
		`{"event_type":"step.start","index":0,"step":{"type":"model_output","id":"replacement"}}`,
		`{"event_type":"step.delta","index":9,"delta":{"type":"arguments_delta","arguments":"wrong"}}`,
		`{"event_type":"step.stop","index":9}`,
		`{"event_type":"step.delta","index":0,"delta":{"type":"text","text":"wrong"}}`,
	} {
		if got := feed(raw); len(got) != 0 {
			t.Errorf("unassociated or duplicate event emitted output: %s", raw)
		}
	}
	feed(`{"event_type":"step.delta","index":2,"delta":{"type":"arguments_delta","arguments":"{\"b\":2}"}}`)
	feed(`{"event_type":"step.delta","index":0,"delta":{"type":"arguments_delta","arguments":"{\"a\":1}"}}`)
	feed(`{"event_type":"step.stop","index":0}`)
	for _, raw := range []string{
		`{"event_type":"step.stop","index":0}`,
		`{"event_type":"step.delta","index":0,"delta":{"type":"arguments_delta","arguments":"late"}}`,
	} {
		if len(feed(raw)) != 0 {
			t.Errorf("closed call emitted output: %s", raw)
		}
	}
	feed(`{"event_type":"interaction.completed","interaction":{"id":"result"}}`)
	for _, raw := range []string{
		`{"event_type":"finish"}`,
		`{"event_type":"step.stop","index":2}`,
		`{"event_type":"step.delta","index":2,"delta":{"type":"arguments_delta","arguments":"late"}}`,
	} {
		if len(feed(raw)) != 0 {
			t.Errorf("completed response emitted output: %s", raw)
		}
	}
	counts := make(map[string]int)
	for _, event := range output {
		payload := gjson.ParseBytes(interactionsSSEPayload(event))
		kind := payload.Get("type").String()
		counts[kind]++
		if kind == "response.output_item.added" || kind == "response.output_item.done" {
			suffix := "a"
			if payload.Get("output_index").Int() == 2 {
				suffix = "b"
			}
			if payload.Get("item.id").String() != "item_"+suffix || payload.Get("item.call_id").String() != "call_"+suffix {
				t.Errorf("item and call identities were mixed: %s", payload.Raw)
			}
		}
	}
	for kind, want := range map[string]int{"response.output_item.added": 2, "response.output_item.done": 2, "response.function_call_arguments.delta": 2, "response.function_call_arguments.done": 2, "response.completed": 1} {
		if counts[kind] != want {
			t.Errorf("%s count = %d, want %d", kind, counts[kind], want)
		}
	}
	completed := findResponsesEventPayload(output, "response.completed")
	for i, suffix := range []string{"a", "b"} {
		item := gjson.GetBytes(completed, fmt.Sprintf("response.output.%d", i))
		if item.Get("call_id").String() != "call_"+suffix || item.Get("arguments").String() != fmt.Sprintf(`{"%s":%d}`, suffix, i+1) {
			t.Errorf("completed call = %s", item.Raw)
		}
	}
	if len(feed(`[DONE]`)) != 1 || len(feed(`[DONE]`)) != 0 {
		t.Fatal("DONE must be emitted exactly once after completion")
	}
}

func TestInteractionsToolStreamRejectsInvalidIndexes(t *testing.T) {
	for _, index := range []string{"", `,"index":null`, `,"index":"0"`, `,"index":-1`, `,"index":0.5`, `,"index":9223372036854775808`} {
		t.Run(index, func(t *testing.T) {
			var state any
			feed := func(raw string) [][]byte {
				return ConvertInteractionsResponseToOpenAIResponses(context.Background(), "fixture", nil, nil, []byte(raw), &state)
			}
			feed(`{"event_type":"step.start","index":0,"step":{"type":"function_call","id":"valid","name":"first","arguments":{}}}`)
			for _, event := range []string{
				`{"event_type":"step.start"%s,"step":{"type":"function_call","id":"wrong","name":"wrong"}}`,
				`{"event_type":"step.delta"%s,"delta":{"type":"arguments_delta","arguments":"wrong"}}`,
				`{"event_type":"step.stop"%s}`,
			} {
				if len(feed(fmt.Sprintf(event, index))) != 0 {
					t.Errorf("invalid index emitted output: %s", fmt.Sprintf(event, index))
				}
			}
			feed(`{"event_type":"step.stop","index":0}`)
			out := feed(`{"event_type":"interaction.completed"}`)
			if got := gjson.GetBytes(findResponsesEventPayload(out, "response.completed"), "response.output.0.arguments").String(); got != "{}" {
				t.Errorf("empty arguments = %q, want {}", got)
			}
		})
	}
}

func TestInteractionsToolStreamSparseIndexAndLegacyText(t *testing.T) {
	var state any
	feed := func(raw string) [][]byte {
		return ConvertInteractionsResponseToOpenAIResponses(context.Background(), "fixture", nil, nil, []byte(raw), &state)
	}
	text := feed(`{"event_type":"step.delta","delta":{"type":"text","text":"legacy"}}`)
	if gjson.GetBytes(findResponsesEventPayload(text, "response.output_text.delta"), "delta").String() != "legacy" {
		t.Fatal("text without a start event was dropped")
	}
	feed(`{"event_type":"step.start","index":2147483647,"step":{"type":"function_call","id":"sparse","name":"tool"}}`)
	out := feed(`{"event_type":"interaction.completed"}`)
	if got := gjson.GetBytes(findResponsesEventPayload(out, "response.completed"), "response.output.0.call_id").String(); got != "sparse" {
		t.Fatalf("sparse call = %q", got)
	}
}

func TestInteractionsReasoningStopWithoutStart(t *testing.T) {
	for _, delta := range []string{
		`{"type":"thought_summary","text":"summary"}`,
		`{"type":"thought_signature","signature":"fixture-signature"}`,
	} {
		t.Run(delta, func(t *testing.T) {
			var state any
			ConvertInteractionsResponseToOpenAIResponses(context.Background(), "fixture", nil, nil, []byte(`{"event_type":"step.delta","index":3,"delta":`+delta+`}`), &state)
			out := ConvertInteractionsResponseToOpenAIResponses(context.Background(), "fixture", nil, nil, []byte(`{"event_type":"step.stop","index":3}`), &state)
			item := gjson.GetBytes(findResponsesEventPayload(out, "response.output_item.done"), "item")
			if item.Get("type").String() != "reasoning" {
				t.Fatal("known reasoning state was dropped without a start event")
			}
			if gjson.Get(delta, "type").String() == "thought_summary" && item.Get("summary.0.text").String() != "summary" {
				t.Fatal("reasoning summary was lost")
			}
			if gjson.Get(delta, "type").String() == "thought_signature" && item.Get("encrypted_content").String() != "fixture-signature" {
				t.Fatal("reasoning signature was lost")
			}
		})
	}
}
