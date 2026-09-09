package claude

import (
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestClaudeToolPairingAcrossSystemReminders(t *testing.T) {
	for _, stream := range []bool{false, true} {
		body := []byte(`{"system":"top-level instruction","messages":[
			{"role":"assistant","content":[{"type":"tool_use","id":"call-a","name":"a","input":{}},{"type":"tool_use","id":"call-b","name":"b","input":{}}]},
			{"role":"system","content":"first reminder"},
			{"role":"system","content":[{"type":"text","text":"second reminder"}]},
			{"role":"user","content":[{"type":"text","text":"follow-up"},{"type":"tool_result","tool_use_id":"call-b","content":"B"},{"type":"tool_result","tool_use_id":"call-a","content":"A"}]}
		]}`)
		got := ConvertClaudeRequestToCodex("gpt-5.4-mini", body, stream)
		items := gjson.GetBytes(got, "input").Array()
		if len(items) != 8 || items[0].Get("role").String() != "developer" || items[3].Get("call_id").String() != "call-a" || items[3].Get("output").String() != "A" || items[4].Get("call_id").String() != "call-b" || items[4].Get("output").String() != "B" {
			t.Fatal("intervening system messages broke ordered tool pairing")
		}
		for index, text := range []string{"first reminder", "second reminder"} {
			item := items[index+5]
			if item.Get("role").String() != "user" || !strings.Contains(item.Get("content.0.text").String(), "<system-reminder>\n"+text) {
				t.Fatal("message-level reminder was promoted to top-level instruction or lost")
			}
		}
		if items[7].Get("content.0.text").String() != "follow-up" {
			t.Fatal("user content order changed after the paired tool results")
		}
	}
}

func TestClaudeUnmatchedToolResultsKeepOrderAndReminders(t *testing.T) {
	for _, content := range []string{
		`"follow-up"`,
		`[{"type":"text","text":"follow-up"},{"type":"tool_result","tool_use_id":"unknown","content":"result"}]`,
		`[{"type":"tool_result","tool_use_id":"call-a","content":"first"},{"type":"tool_result","tool_use_id":"call-a","content":"second"}]`,
	} {
		body := []byte(`{"messages":[{"role":"assistant","content":[{"type":"tool_use","id":"call-a","name":"a","input":{}}]},{"role":"system","content":"reminder"},{"role":"user","content":` + content + `}]}`)
		got := ConvertClaudeRequestToCodex("gpt-5.4-mini", body, false)
		items := gjson.GetBytes(got, "input").Array()
		if len(items) < 3 || !strings.Contains(items[1].Get("content.0.text").String(), "reminder") || items[1].Get("role").String() != "user" {
			t.Fatal("an unmatched result changed reminder ordering")
		}
		if strings.Contains(content, "unknown") && items[len(items)-1].Get("call_id").String() != "unknown" {
			t.Fatal("unknown result was reassigned to another tool call")
		}
	}
}

func TestClaudeTrailingSystemReminderSurvivesPendingTool(t *testing.T) {
	got := ConvertClaudeRequestToCodex("gpt-5.4-mini", []byte(`{"messages":[{"role":"assistant","content":[{"type":"tool_use","id":"call-a","name":"a","input":{}}]},{"role":"system","content":"keep trailing reminder"}]}`), false)
	if !strings.Contains(gjson.GetBytes(got, "input.1.content.0.text").String(), "keep trailing reminder") || gjson.GetBytes(got, "input.1.role").String() != "user" {
		t.Fatal("trailing reminder was discarded while awaiting a tool result")
	}
}

func TestClaudeToolAlignmentRejectsAmbiguousIDs(t *testing.T) {
	for _, ids := range [][]string{nil, {""}, {"call-a", "call-a"}, {"call-a", "missing"}} {
		parts := gjson.Parse(`[{"type":"text","text":"keep order"},{"type":"tool_result","tool_use_id":"call-a","content":"A"}]`).Array()
		got, ok := alignCodexClaudeToolResults(parts, ids)
		if ok || len(got) != len(parts) || got[0].Raw != parts[0].Raw || got[1].Raw != parts[1].Raw {
			t.Fatal("incomplete or ambiguous pairs were guessed or reordered")
		}
	}
}
