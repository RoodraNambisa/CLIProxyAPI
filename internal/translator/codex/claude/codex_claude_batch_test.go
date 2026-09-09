package claude

import (
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexClaudeBatchPreservesMixedContentAndReminders(t *testing.T) {
	const original = `{"system":[{"type":"text","text":"system"},{"type":"text","text":"more rules"}],"messages":[{"role":"assistant","content":[{"type":"text","text":"first"},{"type":"thinking","signature":""},{"type":"text","text":"second"},{"type":"tool_use","id":"call-a","name":"read","input":{"n":9007199254740993}},{"type":"text","text":"third"}]},{"role":"system","content":"reminder"},{"role":"user","content":[{"type":"text","text":"continue"},{"type":"tool_result","tool_use_id":"call-a","content":[{"type":"text","text":"result"},{"type":"image","source":{"data":"dGVzdA==","media_type":"image/png"}}]},{"type":"document","source":{"type":"base64","media_type":"application/pdf","data":"cGRm"}},{"type":"text","text":"last"}]}],"tools":[]}`
	body := []byte(original)
	output := ConvertClaudeRequestToCodexWithCompat("gpt-5", body, false)
	items := gjson.GetBytes(output, "input").Array()
	want := []string{"message", "message", "reasoning", "message", "function_call", "message", "function_call_output", "message", "message"}
	if len(items) != len(want) {
		t.Fatalf("input count = %d, want %d", len(items), len(want))
	}
	for i, kind := range want {
		if items[i].Get("type").String() != kind {
			t.Fatalf("item %d has wrong type: %s", i, items[i].Raw)
		}
	}
	for index, text := range map[int]string{0: "system", 1: "first", 3: "second", 5: "third", 7: "<system-reminder>\nreminder\n</system-reminder>", 8: "continue"} {
		if items[index].Get("content.0.text").String() != text {
			t.Fatalf("item %d changed text: %s", index, items[index].Raw)
		}
	}
	if items[0].Get("content.1.text").String() != "more rules" || items[2].Get("encrypted_content").Raw != `""` || items[4].Get("arguments").String() != `{"n":9007199254740993}` || items[6].Get("output.0.text").String() != "result" || items[6].Get("output.1.image_url").String() != "data:image/png;base64,dGVzdA==" || items[8].Get("content.1.file_data").String() != "data:application/pdf;base64,cGRm" || items[8].Get("content.2.text").String() != "last" {
		t.Fatal("batch assembly lost a signature, precise arguments, image or PDF")
	}
	if gjson.GetBytes(output, "tools").Raw != "[]" || string(body) != original {
		t.Fatal("empty tools or input ownership changed")
	}
}
