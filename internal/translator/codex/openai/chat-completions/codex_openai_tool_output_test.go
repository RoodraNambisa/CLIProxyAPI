package chat_completions

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexOpenAIToolOutputContent(t *testing.T) {
	cases := []struct{ name, content, want string }{
		{"text", `"plain text"`, `"plain text"`},
		{"object text", `{"nested":{"type":"input_image","image_url":"https://example.test/a"}}`, `"{\"nested\":{\"type\":\"input_image\",\"image_url\":\"https://example.test/a\"}}"`},
		{"text array", `[{"type":"text","text":"first"},{"type":"output_text","text":"second"}]`, `[{"type":"input_text","text":"first"},{"type":"input_text","text":"second"}]`},
		{"image url", `[{"type":"image_url","image_url":{"url":"https://example.test/a","detail":"low"}}]`, `[{"type":"input_image","image_url":"https://example.test/a","detail":"low"}]`},
		{"image file", `[{"type":"image_url","image_url":{"file_id":"file-a"}}]`, `[{"type":"input_image","file_id":"file-a"}]`},
		{"native image", `[{"type":"input_image","image_url":"data:image/png;base64,dGVzdA==","detail":"high"}]`, `[{"type":"input_image","image_url":"data:image/png;base64,dGVzdA==","detail":"high"}]`},
		{"native image file", `[{"type":"input_image","file_id":"file-a"}]`, `[{"type":"input_image","file_id":"file-a"}]`},
		{"file", `[{"type":"file","file":{"file_data":"data:application/pdf;base64,dGVzdA==","filename":"a.pdf"}}]`, `[{"type":"input_file","file_data":"data:application/pdf;base64,dGVzdA==","filename":"a.pdf"}]`},
		{"file reference", `[{"type":"file","file":{"file_id":"file-a","file_url":"https://example.test/a.pdf"}}]`, `[{"type":"input_file","file_id":"file-a","file_url":"https://example.test/a.pdf"}]`},
		{"unknown part", `[{"kind":"business","value":9007199254740993}]`, `[{"type":"input_text","text":"{\"kind\":\"business\",\"value\":9007199254740993}"}]`},
		{"empty image", `[{"type":"input_image"}]`, `[{"type":"input_text","text":"{\"type\":\"input_image\"}"}]`},
		{"empty file", `[{"type":"file","file":{}}]`, `[{"type":"input_text","text":"{\"type\":\"file\",\"file\":{}}"}]`},
		{"array scalars", `["hello",7,null]`, `[{"type":"input_text","text":"\"hello\""},{"type":"input_text","text":"7"},{"type":"input_text","text":"null"}]`},
		{"empty array", `[]`, `[]`},
		{"null legacy", `null`, `""`},
		{"number legacy", `7`, `"7"`},
		{"invalid encoded image", `"[{\"type\":\"input_image\",\"file_id\":\"file-a\"}] trailing"`, `"[{\"type\":\"input_image\",\"file_id\":\"file-a\"}] trailing"`},
		{"numeric image reference", `[{"type":"input_image","file_id":123}]`, `[{"type":"input_text","text":"{\"type\":\"input_image\",\"file_id\":123}"}]`},
	}
	for _, tc := range append([]struct{ name, content, want string }(nil), cases...) {
		encoded, _ := json.Marshal(tc.content)
		want := string(encoded)
		switch tc.name {
		case "image url", "image file", "native image", "native image file":
			want = tc.want
		}
		cases = append(cases, struct{ name, content, want string }{"encoded " + tc.name, string(encoded), want})
	}
	for _, custom := range []bool{false, true} {
		for _, stream := range []bool{false, true} {
			for _, tc := range cases {
				t.Run(fmt.Sprintf("custom=%t/stream=%t/%s", custom, stream, tc.name), func(t *testing.T) {
					call := `{"id":"call-a","type":"function","function":{"name":"read","arguments":"{}"}}`
					kind := "function_call_output"
					if custom {
						call = `{"id":"call-a","type":"custom","custom":{"name":"read","input":"read"}}`
						kind = "custom_tool_call_output"
					}
					original := `{"messages":[{"role":"assistant","tool_calls":[` + call + `]},{"role":"tool","tool_call_id":"call-a","content":` + tc.content + `}]}`
					body := []byte(original)
					got := ConvertOpenAIRequestToCodex("gpt-5", body, stream)
					output := gjson.GetBytes(got, "input.1")
					if output.Get("output").Raw != tc.want || output.Get("type").String() != kind || output.Get("call_id").String() != "call-a" {
						t.Fatalf("tool result = %s, want output %s", output.Raw, tc.want)
					}
					if string(body) != original {
						t.Fatal("source request mutated")
					}
				})
			}
		}
	}
}
