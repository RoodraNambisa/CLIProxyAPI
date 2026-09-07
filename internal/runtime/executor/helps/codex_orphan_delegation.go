package helps

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const CodexOrphanDelegationPreparedContextKey = "codex_orphan_delegation_prepared"

// CodexOrphanDelegationEnabled resolves the caller's subagent identity before
// any outbound identity or credential headers are applied.
func CodexOrphanDelegationEnabled(ctx context.Context, headers http.Header, enabled bool) bool {
	if !enabled {
		return false
	}
	if ctx != nil {
		if c, ok := ctx.Value("gin").(*gin.Context); ok && c != nil && c.Request != nil {
			if c.GetBool(CodexOrphanDelegationPreparedContextKey) {
				return false
			}
			headers = c.Request.Header
		}
	}
	value, seen := "", false
	for name, values := range headers {
		if !strings.EqualFold(name, "X-Openai-Subagent") {
			continue
		}
		if seen || len(values) != 1 {
			return false
		}
		seen, value = true, values[0]
	}
	return seen && strings.EqualFold(strings.TrimSpace(value), "collab_spawn")
}

// RewriteCodexOrphanDelegationInput repairs only the two known Codex app
// delegation outputs. Pairing consumes calls individually, including calls
// appearing later in the same history; business output is copied as text.
func RewriteCodexOrphanDelegationInput(payload []byte, enabled bool, cachedCall ...func(string) bool) []byte {
	if !enabled || !gjson.ValidBytes(payload) {
		return payload
	}
	input := gjson.GetBytes(payload, "input")
	if !input.IsArray() {
		return payload
	}
	items := input.Array()
	calls := make(map[string]int)
	for _, item := range items {
		if item.Get("type").String() == "function_call" {
			if id := item.Get("call_id"); id.Type == gjson.String && strings.TrimSpace(id.Str) != "" {
				calls[id.Str]++
			}
		}
	}
	updated := payload
	for index, item := range items {
		if item.Get("type").String() != "function_call_output" {
			continue
		}
		id := item.Get("call_id")
		if id.Type == gjson.String && strings.TrimSpace(id.Str) != "" && len(cachedCall) > 0 && cachedCall[0] != nil {
			if _, seen := calls[id.Str]; !seen {
				calls[id.Str] = 0
				if cachedCall[0](id.Str) {
					calls[id.Str] = 1
				}
			}
		}
		if id.Type == gjson.String && calls[id.Str] > 0 {
			calls[id.Str]--
			continue
		}
		if item.Get("namespace").String() != "codex_app" {
			continue
		}
		name := item.Get("name").String()
		if name != "create_thread" && name != "send_message_to_thread" {
			continue
		}
		output := item.Get("output")
		content := output.Raw
		if output.Type == gjson.String {
			content = output.Str
		}
		message := []byte(`{"type":"message","role":"user","content":[{"type":"input_text","text":""}]}`)
		message, err := sjson.SetBytes(message, "content.0.text", "Tool output from codex_app__"+name+":\n"+content)
		if err != nil {
			return payload
		}
		updated, err = sjson.SetRawBytes(updated, fmt.Sprintf("input.%d", index), message)
		if err != nil {
			return payload
		}
	}
	return updated
}
