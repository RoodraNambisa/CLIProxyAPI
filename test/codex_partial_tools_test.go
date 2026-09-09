package test

import (
	"bytes"
	"fmt"
	"testing"

	_ "github.com/router-for-me/CLIProxyAPI/v6/internal/translator"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestCodexPartialToolsDoNotBecomeEmptyObjects(t *testing.T) {
	for _, format := range []string{"claude", "gemini", "interactions"} {
		for _, args := range []string{`{"unfinished":`, ``, `[]`, `null`, `{} trailing`, `{}`, `{"number":9007199254740993}`} {
			t.Run(format+"/"+args, func(t *testing.T) {
				valid := gjson.Valid(args) && gjson.Parse(args).IsObject()
				item := fmt.Sprintf(`{"type":"function_call","status":"incomplete","id":"fc_partial","call_id":"partial","name":"partial_tool","arguments":%q}`, args)
				terminal := []byte(`{"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"max_tokens"},"output":[{"type":"message","content":[{"type":"output_text","text":"available text"}]},` + item + `],"usage":{"input_tokens":2,"output_tokens":3}}}`)
				var state any
				out := translator.TranslateNonStream(t.Context(), translator.FromString("codex"), translator.FromString(format), "model", nil, nil, terminal, &state)
				if !gjson.ValidBytes(out) || !bytes.Contains(out, []byte("available text")) {
					t.Fatal("partial response became invalid JSON or lost usable text")
				}
				path, finish := "stop_reason", "max_tokens"
				if format == "gemini" {
					path, finish = "candidates.0.finishReason", "MAX_TOKENS"
				}
				if format == "interactions" {
					path, finish = "status", "incomplete"
				}
				if gjson.GetBytes(out, path).String() != finish {
					t.Fatal("partial response lost the protocol finish reason")
				}
				if bytes.Contains(out, []byte("partial_tool")) != valid {
					t.Fatal("unfinished tool was fabricated as an empty object, or a complete tool was lost")
				}
				if valid && args != "{}" && !bytes.Contains(out, []byte("9007199254740993")) {
					t.Fatal("tool integer precision was lost")
				}
				complete, _ := sjson.SetBytes(terminal, "type", "response.completed")
				complete, _ = sjson.SetBytes(complete, "response.status", "completed")
				complete, _ = sjson.DeleteBytes(complete, "response.incomplete_details")
				completeOut := translator.TranslateNonStream(t.Context(), translator.FormatCodex, translator.FromString(format), "model", nil, nil, complete, nil)
				if !gjson.ValidBytes(completeOut) || !bytes.Contains(completeOut, []byte("partial_tool")) {
					t.Fatal("legacy complete-response argument fallback changed")
				}
				if format == "gemini" {
					var streamState any
					chunks := translator.TranslateStream(t.Context(), translator.FromString("codex"), translator.FromString(format), "model", nil, nil, []byte(`data: {"type":"response.output_item.done","item":`+item+`}`), &streamState)
					if bytes.Contains(bytes.Join(chunks, nil), []byte("partial_tool")) != valid {
						t.Fatal("Gemini emitted an unfinished function as a usable object")
					}
					for _, chunk := range chunks {
						if !gjson.ValidBytes(chunk) {
							t.Fatal("Gemini emitted malformed JSON")
						}
					}
				}
			})
		}
	}
}
