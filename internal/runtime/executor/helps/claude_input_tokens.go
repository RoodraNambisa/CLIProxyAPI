package helps

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"github.com/tiktoken-go/tokenizer"
)

var claudeInputTokenizer struct {
	once  sync.Once
	codec tokenizer.Codec
}

// EstimateClaudeInputTokens estimates text and tool input only. It keeps no
// request data after returning and never supplies billing or final usage values.
func EstimateClaudeInputTokens(ctx context.Context, payload []byte) int64 {
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil || !gjson.ValidBytes(payload) {
		return 0
	}
	text := claudeTokenText{ctx: ctx}
	root := gjson.ParseBytes(payload)
	system := root.Get("system")
	if system.Type == gjson.String {
		text.add(system.String())
	} else if system.IsArray() {
		system.ForEach(func(_, part gjson.Result) bool {
			if part.Type == gjson.String {
				text.add(part.String())
			} else if part.Get("type").String() == "text" {
				text.add(part.Get("text").String())
			}
			return ctx.Err() == nil
		})
	}
	if messages := root.Get("messages"); messages.IsArray() {
		messages.ForEach(func(_, message gjson.Result) bool {
			text.add(message.Get("role").String())
			text.content(message.Get("content"))
			return ctx.Err() == nil
		})
	}
	if tools := root.Get("tools"); tools.IsArray() {
		tools.ForEach(func(_, tool gjson.Result) bool {
			for _, field := range []string{"type", "name", "description"} {
				text.add(tool.Get(field).String())
			}
			text.addJSON(tool.Get("input_schema"))
			return ctx.Err() == nil
		})
	}
	choice := root.Get("tool_choice")
	if choice.Type == gjson.String {
		text.add(choice.String())
	} else {
		text.add(choice.Get("type").String())
		text.add(choice.Get("name").String())
	}
	if ctx.Err() != nil || text.Len() == 0 {
		return 0
	}
	claudeInputTokenizer.once.Do(func() { claudeInputTokenizer.codec, _ = tokenizer.Get(tokenizer.O200kBase) })
	if claudeInputTokenizer.codec == nil || ctx.Err() != nil {
		return 0
	}
	count, err := claudeInputTokenizer.codec.Count(text.String())
	if err != nil || ctx.Err() != nil || count < 0 {
		return 0
	}
	return int64(count)
}

type claudeTokenText struct {
	strings.Builder
	ctx context.Context
}

func (t *claudeTokenText) add(value string) {
	if value = strings.TrimSpace(value); value == "" || t.ctx.Err() != nil {
		return
	}
	if t.Len() > 0 {
		t.WriteByte('\n')
	}
	t.WriteString(value)
}

func (t *claudeTokenText) addJSON(value gjson.Result) {
	if !value.Exists() {
		return
	}
	if value.Type == gjson.String {
		t.add(value.String())
		return
	}
	var compact bytes.Buffer
	if json.Compact(&compact, []byte(value.Raw)) == nil {
		t.add(compact.String())
	}
}

func (t *claudeTokenText) content(value gjson.Result) {
	if t.ctx.Err() != nil {
		return
	}
	if value.Type == gjson.String {
		t.add(value.String())
		return
	}
	if value.IsArray() {
		value.ForEach(func(_, part gjson.Result) bool { t.content(part); return t.ctx.Err() == nil })
		return
	}
	if !value.IsObject() {
		return
	}
	switch value.Get("type").String() {
	case "text":
		t.add(value.Get("text").String())
	case "thinking":
		t.add(value.Get("thinking").String())
	case "document":
		source := value.Get("source")
		if source.Get("type").String() == "text" {
			t.add(value.Get("title").String())
			t.add(value.Get("context").String())
			t.add(source.Get("data").String())
			t.add(source.Get("content").String())
		}
	case "tool_use", "server_tool_use", "mcp_tool_use":
		t.add(value.Get("id").String())
		t.add(value.Get("name").String())
		t.addJSON(value.Get("input"))
	case "tool_result", "mcp_tool_result", "web_search_tool_result", "web_fetch_tool_result", "code_execution_tool_result", "bash_code_execution_tool_result", "text_editor_code_execution_tool_result":
		t.add(value.Get("tool_use_id").String())
		t.add(value.Get("tool_call_id").String())
		t.content(value.Get("content"))
	case "web_search_result", "search_result":
		if source := value.Get("source"); source.Type == gjson.String {
			t.add(source.String())
		}
		for _, field := range []string{"title", "url", "page_age"} {
			t.add(value.Get(field).String())
		}
		t.content(value.Get("content"))
	case "web_fetch_result":
		t.add(value.Get("url").String())
		t.add(value.Get("retrieved_at").String())
		t.content(value.Get("content"))
	case "code_execution_result", "bash_code_execution_result", "text_editor_code_execution_result":
		for _, field := range []string{"stdout", "stderr", "return_code"} {
			t.add(value.Get(field).String())
		}
		t.content(value.Get("content"))
		t.content(value.Get("output"))
	case "tool_reference":
		t.add(value.Get("tool_name").String())
	case "image", "input_audio", "audio", "video", "redacted_thinking":
	case "":
		t.addJSON(value)
	default:
		t.add(value.Get("text").String())
	}
}

// ClaudeInputTokenState applies an already computed estimate once per stream.
// Its numeric snapshot survives retries and early request-body release.
type ClaudeInputTokenState struct {
	Estimate int64
	handled  bool
}

func (s *ClaudeInputTokenState) Apply(chunks [][]byte) [][]byte {
	if s == nil || s.handled || s.Estimate <= 0 {
		return chunks
	}
	for i, chunk := range chunks {
		for offset := 0; offset < len(chunk); {
			end := bytes.IndexByte(chunk[offset:], '\n')
			if end < 0 {
				end = len(chunk)
			} else {
				end += offset
			}
			line := chunk[offset:end]
			trimmed := bytes.TrimLeft(line, " \t")
			if bytes.HasPrefix(trimmed, []byte("data:")) {
				prefix := len(line) - len(trimmed) + len("data:")
				payload := bytes.TrimSpace(line[prefix:])
				if gjson.ValidBytes(payload) && gjson.GetBytes(payload, "type").String() == "message_start" {
					s.handled = true
					if gjson.GetBytes(payload, "message.usage.input_tokens").Int() != 0 {
						return chunks
					}
					updated, err := sjson.SetBytes(payload, "message.usage.input_tokens", s.Estimate)
					if err != nil {
						return chunks
					}
					start := offset + prefix + bytes.Index(line[prefix:], payload)
					result := make([]byte, 0, len(chunk)+len(updated)-len(payload))
					result = append(result, chunk[:start]...)
					result = append(result, updated...)
					result = append(result, chunk[start+len(payload):]...)
					chunks[i] = result
					return chunks
				}
			}
			if end == len(chunk) {
				break
			}
			offset = end + 1
		}
	}
	return chunks
}
