package helps

import (
	"encoding/base64"
	"fmt"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func reasoningTurnItem(tag byte) []byte {
	signature := make([]byte, 73)
	signature[0], signature[1] = 0x80, tag
	return fmt.Appendf(nil, `{"type":"reasoning","encrypted_content":%q}`, base64.RawURLEncoding.EncodeToString(signature))
}

func matchedReplayTurn(input []gjson.Result, position int, tag byte) codexReasoningReplayTurn {
	_, prefix := codexReplayPrefixIndexes(input[:position])
	assistant, _ := codexReplayAssistantFingerprint(input[position])
	return codexReasoningReplayTurn{prefix: prefix, assistant: assistant, items: [][]byte{reasoningTurnItem(tag)}}
}

func applyReplayTurnsForTest(body []byte, turns []codexReasoningReplayTurn) ([]byte, bool) {
	items := gjson.GetBytes(body, "input").Array()
	prefixes, _ := codexReplayPrefixIndexes(items)
	return insertCodexReasoningReplayTurns(body, items, prefixes, turns)
}

func TestCodexReplayTurnsMatchCompleteHistoryAndRejectAmbiguity(t *testing.T) {
	const original = `{"prompt_cache_key":"keep","input":[{"role":"user","content":"first"},{"role":"assistant","content":"same"},{"role":"user","content":"second"},{"role":"assistant","content":"same"},{"role":"user","content":"next"}]}`
	body := []byte(original)
	items := gjson.GetBytes(body, "input").Array()
	first, second := matchedReplayTurn(items, 1, 1), matchedReplayTurn(items, 3, 2)
	out, changed := applyReplayTurnsForTest(body, []codexReasoningReplayTurn{second, first})
	if !changed || gjson.GetBytes(out, "input.#").Int() != 7 || gjson.GetBytes(out, "input.1").Raw != string(first.items[0]) || gjson.GetBytes(out, "input.4").Raw != string(second.items[0]) || gjson.GetBytes(out, "prompt_cache_key").String() != "keep" || string(body) != original {
		t.Fatal("different completion order conflated repeated assistant text or changed source data")
	}
	for _, changedBody := range []string{
		strings.Replace(original, `"first"`, `"different"`, 1),
		strings.Replace(original, `"same"`, `"edited"`, 1),
	} {
		if out, changed := applyReplayTurnsForTest([]byte(changedBody), []codexReasoningReplayTurn{first, second}); changed || string(out) != changedBody {
			t.Fatal("modified history received unrelated reasoning")
		}
	}
	conflict := first
	conflict.items = [][]byte{reasoningTurnItem(3)}
	if out, changed := applyReplayTurnsForTest(body, []codexReasoningReplayTurn{first, conflict}); changed || string(out) != original {
		t.Fatal("ambiguous completed responses chose an arbitrary cipher")
	}
	first.assistant = [32]byte{}
	if _, changed := applyReplayTurnsForTest(body, []codexReasoningReplayTurn{first}); changed {
		t.Fatal("session and prefix alone supplied an output anchor")
	}
}

func TestCodexReplayTurnsKeepExistingCipherAndRestoreOnlyMissingCipher(t *testing.T) {
	first := reasoningTurnItem(1)
	body := []byte(`{"input":[{"role":"user","content":"first"},` + string(first) + `,{"role":"assistant","content":"answer"},{"role":"user","content":"second"},{"role":"assistant","content":"answer2"}]}`)
	items := gjson.GetBytes(body, "input").Array()
	oldTurn, newTurn := matchedReplayTurn(items, 2, 1), matchedReplayTurn(items, 4, 2)
	out, changed := applyReplayTurnsForTest(body, []codexReasoningReplayTurn{oldTurn, newTurn})
	if !changed || gjson.GetBytes(out, `input.#(type=="reasoning")#`).Get("#").Int() != 2 || gjson.GetBytes(out, "input.1").Raw != string(first) || gjson.GetBytes(out, "input.4").Raw != string(newTurn.items[0]) {
		t.Fatal("existing reasoning blocked a missing different cipher or was duplicated")
	}
}

func TestCodexReplayTurnsRepairOnlyUniqueCompatibleToolResults(t *testing.T) {
	for _, custom := range []bool{false, true} {
		for _, existing := range []bool{false, true} {
			t.Run(fmt.Sprintf("custom=%t/existing=%t", custom, existing), func(t *testing.T) {
				kind, field, value := "function_call", "arguments", `{"b":2,"a":9007199254740993}`
				if custom {
					kind, field, value = "custom_tool_call", "input", "plain"
				}
				call := fmt.Sprintf(`{"type":%q,"call_id":"call:1","name":"tool",%q:%q}`, kind, field, value)
				inputCall := strings.Replace(call, "call:1", "call_1", 1)
				body := `{"input":[{"role":"user","content":"request"},`
				if existing {
					body += inputCall + ","
				}
				body += fmt.Sprintf(`{"type":%q,"call_id":"call_1","output":{"business":"unchanged"}}]}`, kind+"_output")
				items := gjson.Get(body, "input").Array()
				_, prefix := codexReplayPrefixIndexes(items[:1])
				turn := codexReasoningReplayTurn{prefix: prefix, callIDs: []string{"call:1"}, items: [][]byte{reasoningTurnItem(1), []byte(call)}}
				out, changed := applyReplayTurnsForTest([]byte(body), []codexReasoningReplayTurn{turn})
				if !changed || gjson.GetBytes(out, "input.#").Int() != 4 || gjson.GetBytes(out, "input.2.call_id").String() != "call_1" || gjson.GetBytes(out, "input.3.output.business").String() != "unchanged" {
					t.Fatal("tool pairing lost compatible ID, output or uniqueness")
				}
				if existing {
					if !custom {
						formatted, _ := sjson.SetBytes([]byte(body), "input.1.arguments", `{ "a":9007199254740993, "b":2 }`)
						if _, changed := applyReplayTurnsForTest(formatted, []codexReasoningReplayTurn{turn}); !changed {
							t.Fatal("JSON formatting changed equivalent function arguments")
						}
						different, _ := sjson.SetBytes([]byte(body), "input.1.arguments", `{"a":9007199254740992,"b":2}`)
						if _, changed := applyReplayTurnsForTest(different, []codexReasoningReplayTurn{turn}); changed {
							t.Fatal("large integer precision loss matched different function arguments")
						}
					}
					wrong := strings.Replace(body, `"name":"tool"`, `"name":"another"`, 1)
					if _, changed := applyReplayTurnsForTest([]byte(wrong), []codexReasoningReplayTurn{turn}); changed {
						t.Fatal("a reused call ID attached reasoning to a different tool")
					}
				}
				wrongID := strings.ReplaceAll(body, "call_1", "call?1")
				if _, changed := applyReplayTurnsForTest([]byte(wrongID), []codexReasoningReplayTurn{turn}); changed {
					t.Fatal("two different unsafe IDs matched through a normalization collision")
				}
			})
		}
	}
}
