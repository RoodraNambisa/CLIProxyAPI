package claude

import (
	"fmt"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexClaudeParallelToolsRemainIndependent(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		for _, lateName := range []bool{false, true} {
			for _, terminal := range []bool{false, true} {
				t.Run(fmt.Sprintf("reverse=%t/late-name=%t/terminal=%t", reverse, lateName, terminal), func(t *testing.T) {
					name := "run"
					if lateName {
						name = ""
					}
					events := []string{
						fmt.Sprintf(`{"type":"response.output_item.added","output_index":1,"item":{"type":"function_call","id":"fc_a","call_id":"call_a","name":%q}}`, name),
						fmt.Sprintf(`{"type":"response.output_item.added","output_index":2,"item":{"type":"function_call","id":"fc_b","call_id":"call_b","name":%q}}`, name),
						`{"type":"response.function_call_arguments.delta","item_id":"fc_b","delta":"{\"b\":"}`,
						`{"type":"response.function_call_arguments.delta","item_id":"fc_a","delta":"{\"a\":"}`,
					}
					items := []string{`{"type":"function_call","id":"fc_a","call_id":"call_a","name":"run","arguments":"{\"a\":1}"}`, `{"type":"function_call","id":"fc_b","call_id":"call_b","name":"run","arguments":"{\"b\":2}"}`}
					if !terminal {
						order := []int{0, 1}
						if reverse {
							order = []int{1, 0}
						}
						for _, index := range order {
							events = append(events, `{"type":"response.output_item.done","item":`+items[index]+`}`)
						}
						// A repeated final snapshot cannot create another tool block.
						events = append(events, `{"type":"response.output_item.done","item":`+items[0]+`}`)
					}
					events = append(events, `{"type":"response.completed","response":{"output":[`+strings.Join(items, ",")+`]}}`)
					var state any
					var output [][]byte
					for _, event := range events {
						if !gjson.Valid(event) {
							t.Fatal("invalid test event")
						}
						output = append(output, ConvertCodexResponseToClaude(t.Context(), "", nil, nil, []byte("data: "+event), &state)...)
					}
					blocks := assertCodexClaudeBlocks(t, output)
					if len(blocks) != 2 || blocks[0].Get("id").String() != "call_a" || blocks[1].Get("id").String() != "call_b" {
						t.Fatalf("wrong tool order: %v", blocks)
					}
					arguments := map[int]string{}
					for _, chunk := range output {
						for _, line := range strings.Split(string(chunk), "\n") {
							if !strings.HasPrefix(line, "data: ") {
								continue
							}
							event := gjson.Parse(strings.TrimPrefix(line, "data: "))
							if event.Get("delta.type").String() == "input_json_delta" {
								arguments[int(event.Get("index").Int())] += event.Get("delta.partial_json").String()
							}
						}
					}
					if arguments[0] != `{"a":1}` || arguments[1] != `{"b":2}` {
						t.Fatalf("mixed arguments: %v", arguments)
					}
				})
			}
		}
	}
}

func TestCodexClaudeToolsRejectAmbiguousEvents(t *testing.T) {
	events := []string{
		`{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"fc_a","call_id":"call_a","name":"run"}}`,
		`{"type":"response.output_item.added","output_index":1,"item":{"type":"function_call","id":"fc_b","call_id":"call_b","name":"run"}}`,
		`{"type":"response.function_call_arguments.delta","delta":"AMBIGUOUS"}`,
		`{"type":"response.function_call_arguments.delta","item_id":"fc_a","output_index":1,"delta":"CONFLICT"}`,
		`{"type":"response.function_call_arguments.delta","item_id":"fc_a","call_id":"evil","delta":"REBOUND"}`,
		`{"type":"response.function_call_arguments.delta","call_id":"evil","delta":"POISONED_ALIAS"}`,
		`{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call_a","name":"run","arguments":"{}"}}`,
		`{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call_b","name":"run","arguments":"{}"}}`,
		`{"type":"response.function_call_arguments.delta","item_id":"fc_a","delta":"AFTER_DONE"}`,
		`{"type":"response.completed","response":{"output":[]}}`,
	}
	var state any
	var output [][]byte
	for _, event := range events {
		if !gjson.Valid(event) {
			t.Fatal("invalid test event")
		}
		output = append(output, ConvertCodexResponseToClaude(t.Context(), "", nil, nil, []byte("data: "+event), &state)...)
	}
	blocks := assertCodexClaudeBlocks(t, output)
	raw := stringJoinCodexClaudeChunks(output)
	if len(blocks) != 2 || strings.Contains(raw, "AMBIGUOUS") || strings.Contains(raw, "CONFLICT") || strings.Contains(raw, "REBOUND") || strings.Contains(raw, "POISONED_ALIAS") || strings.Contains(raw, "AFTER_DONE") {
		t.Fatal("ambiguous event changed another call")
	}
}

func TestCodexClaudeToolsDeferContentUntilBlockCloses(t *testing.T) {
	for _, pending := range []bool{false, true} {
		for _, terminal := range []bool{false, true} {
			t.Run(fmt.Sprintf("pending=%t/terminal=%t", pending, terminal), func(t *testing.T) {
				name := "run"
				if pending {
					name = ""
				}
				events := []string{
					fmt.Sprintf(`{"type":"response.output_item.added","item":{"type":"function_call","call_id":"call_a","name":%q}}`, name),
					`{"type":"response.output_text.delta","delta":"answer"}`,
				}
				item := `{"type":"function_call","call_id":"call_a","name":"run","arguments":"{}"}`
				if !terminal {
					events = append(events, `{"type":"response.output_item.done","item":`+item+`}`)
				}
				events = append(events, `{"type":"response.incomplete","response":{"output":[`+item+`],"incomplete_details":{"reason":"max_output_tokens"}}}`)
				var state any
				var output [][]byte
				for _, event := range events {
					if !gjson.Valid(event) {
						t.Fatal("invalid test event")
					}
					output = append(output, ConvertCodexResponseToClaude(t.Context(), "", nil, nil, []byte("data: "+event), &state)...)
				}
				blocks := assertCodexClaudeBlocks(t, output)
				first := "tool_use"
				if pending {
					first = "text"
				}
				if len(blocks) != 2 || blocks[0].Get("type").String() != first || !strings.Contains(stringJoinCodexClaudeChunks(output), `"stop_reason":"max_tokens"`) {
					t.Fatal("tool or deferred content lost its lifecycle")
				}
				p := state.(*ConvertCodexResponseToClaudeParams)
				if p.ActiveFunctionCall != nil || len(p.DeferredContentEvents) != 0 || len(p.FunctionCalls) != 0 {
					t.Fatal("terminal retained stream state")
				}
			})
		}
	}
}

func stringJoinCodexClaudeChunks(chunks [][]byte) string {
	var out strings.Builder
	for _, chunk := range chunks {
		out.Write(chunk)
	}
	return out.String()
}

func TestCodexClaudeToolIdentityAndDeferredBufferBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name   string
		events []string
		want   int
		text   string
	}{
		{"delta before item", []string{
			`{"type":"response.function_call_arguments.delta","item_id":"fc_a","delta":"{"}`,
			`{"type":"response.output_item.done","item":{"type":"function_call","id":"fc_a","call_id":"call_a","name":"run","arguments":"{}"}}`,
		}, 1, `"partial_json":"{}"`},
		{"unlabelled single call", []string{
			`{"type":"response.output_item.added","item":{"type":"function_call","call_id":"call_a","name":"run"}}`,
			`{"type":"response.function_call_arguments.done","arguments":"{}"}`,
			`{"type":"response.function_call_arguments.delta","delta":"LATE"}`,
		}, 1, `"partial_json":"{}"`},
		{"unresolved pairing ID", []string{
			`{"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","name":"run","arguments":"{}"}}`,
		}, 0, ""},
		{"deferred text and thinking", []string{
			`{"type":"response.output_item.added","item":{"type":"function_call","call_id":"call_a","name":"run"}}`,
			`{"type":"response.output_text.delta","delta":"answer"}`,
			`{"type":"response.reasoning_summary_text.delta","delta":"thought"}`,
			`{"type":"response.output_item.done","item":{"type":"reasoning","encrypted_content":"final"}}`,
		}, 3, `"signature":"final"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var state any
			var output [][]byte
			for _, event := range append(append([]string(nil), tc.events...), `{"type":"response.completed","response":{"output":[]}}`) {
				if !gjson.Valid(event) {
					t.Fatal("invalid fixture")
				}
				payload := []byte("data: " + event)
				output = append(output, ConvertCodexResponseToClaude(t.Context(), "", nil, nil, payload, &state)...)
				// A transport is free to reuse its input buffer after translation returns.
				clear(payload)
			}
			blocks := assertCodexClaudeBlocks(t, output)
			raw := stringJoinCodexClaudeChunks(output)
			if len(blocks) != tc.want || !strings.Contains(raw, tc.text) || strings.Contains(raw, "LATE") {
				t.Fatalf("unexpected tool output: %s", raw)
			}
		})
	}
}
