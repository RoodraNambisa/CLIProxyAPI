package session

import (
	"strings"

	"github.com/tidwall/gjson"
)

func (b *historyBuilder) addGoogleSystem(root gjson.Result) {
	system := root.Get("systemInstruction")
	if !system.Exists() {
		system = root.Get("system_instruction")
	}
	if parts := system.Get("parts"); parts.Exists() {
		system = parts
	}
	b.add("system", system)
}

func (b *historyBuilder) addGeminiHistory(root gjson.Result) {
	if request := root.Get("request"); request.IsObject() && !root.Get("contents").Exists() {
		root = request
		b.add("system", root.Get("tools"))
	}
	if cached := root.Get("cachedContent"); cached.Exists() {
		b.add("system-cache", cached)
	} else {
		b.add("system-cache", root.Get("cached_content"))
	}
	b.addGoogleSystem(root)
	contents := root.Get("contents")
	if !contents.IsArray() {
		b.valid = false
		return
	}
	singleTurn := contents.Get("#").Int() == 1
	contents.ForEach(func(_, content gjson.Result) bool {
		if !content.IsObject() {
			b.valid = false
			return false
		}
		producer := content.Get("role")
		role := historyRole(producer.String())
		// Single-turn Gemini prompts may omit role. Multi-turn history must
		// identify its producers rather than relying on guessed alternation.
		if singleTurn && (!producer.Exists() || (producer.Type == gjson.String && strings.TrimSpace(producer.Str) == "")) {
			role = "user"
		}
		b.add(role, content.Get("parts"))
		return b.valid
	})
}

func (b *historyBuilder) addInteractionHistory(value gjson.Result, inheritedRole string, depth int) {
	if !b.valid || !value.Exists() {
		return
	}
	if depth > historyMaxDepth {
		b.valid = false
		return
	}
	if value.IsArray() {
		value.ForEach(func(_, item gjson.Result) bool { b.addInteractionHistory(item, inheritedRole, depth+1); return b.valid })
		return
	}
	if value.Type == gjson.String {
		b.add(inheritedRole, value)
		return
	}
	if !value.IsObject() {
		b.valid = false
		return
	}
	typ := strings.ToLower(strings.TrimSpace(value.Get("type").String()))
	role := inheritedRole
	if value.Get("role").Type == gjson.String {
		role = historyRole(value.Get("role").Str)
	}
	if steps := value.Get("steps"); steps.IsArray() && (typ == "" || typ == "interaction") {
		b.addInteractionHistory(steps, role, depth+1)
		return
	}
	switch typ {
	case "thought", "reasoning", "thinking":
		return
	case "function_call", "custom_tool_call":
		b.add("assistant", value)
	case "function_result", "function_call_output", "custom_tool_call_output", "tool_result":
		b.add("tool", value)
	case "system_instruction", "developer_instruction":
		b.add("system", value.Get("content"))
	case "user_input":
		b.add("user", value.Get("content"))
	case "model_output":
		b.add("assistant", value.Get("content"))
	case "":
		if content := value.Get("content"); content.Exists() {
			b.add(role, content)
		} else {
			b.add("unknown", value)
		}
	case "text", "image", "audio", "video", "document":
		b.add(role, value)
	default:
		b.add("unknown", value)
	}
}
