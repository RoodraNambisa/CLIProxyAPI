package claude

import (
	"testing"

	"github.com/tidwall/gjson"
)

func TestClaudePDFToCodexPreservesContentOrder(t *testing.T) {
	for _, dataField := range []string{"data", "base64"} {
		body := []byte(`{"messages":[{"role":"user","content":[{"type":"text","text":"before"},{"type":"document","source":{"type":"base64","media_type":"application/pdf","` + dataField + `":"JVBERi0="}},{"type":"text","text":"after"}]}]}`)
		got := ConvertClaudeRequestToCodex("gpt-5.4", body, true)
		content := gjson.GetBytes(got, "input.0.content").Array()
		if len(content) != 3 || content[0].Get("text").String() != "before" || content[2].Get("text").String() != "after" {
			t.Fatal("document conversion changed content order")
		}
		if content[1].Get("type").String() != "input_file" || content[1].Get("filename").String() != "document.pdf" || content[1].Get("file_data").String() != "data:application/pdf;base64,JVBERi0=" {
			t.Fatal("PDF was not converted to an input file")
		}
	}
}

func TestClaudeUnsupportedDocumentsKeepLegacyBehavior(t *testing.T) {
	for _, source := range []string{`{"type":"url","url":"https://example.invalid/file.pdf"}`, `{"type":"base64","media_type":"text/plain","data":"text"}`, `{"type":"base64","media_type":"application/pdf","data":42}`, `{"type":"base64","media_type":"application/pdf","data":""}`} {
		got := ConvertClaudeRequestToCodex("gpt-5.4", []byte(`{"messages":[{"role":"user","content":[{"type":"document","source":`+source+`},{"type":"text","text":"keep"}]}]}`), false)
		content := gjson.GetBytes(got, "input.0.content").Array()
		if len(content) != 1 || content[0].Get("text").String() != "keep" {
			t.Fatal("unsupported document changed legacy message behavior")
		}
	}
}
