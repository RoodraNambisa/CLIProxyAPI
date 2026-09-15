package helps

import (
	"encoding/json"
	"fmt"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// NormalizeXAIAllowedTools preserves the caller's restriction while adapting
// Chat's nested references and tools flattened by the Responses translator.
func NormalizeXAIAllowedTools(body, beforeNormalization []byte) ([]byte, error) {
	choice := gjson.GetBytes(body, "tool_choice")
	if namespace := choice.Get("namespace").String(); namespace != "" &&
		(choice.Get("type").String() == "function" || choice.Get("type").String() == "custom") {
		// Reuse the allowlist validation before dropping a namespace: another
		// namespace may contain a different function with the same short name.
		wrapped, _ := sjson.SetBytes(body, "tool_choice", map[string]any{"type": "allowed_tools", "mode": "required", "tools": []any{choice.Value()}})
		normalized, err := NormalizeXAIAllowedTools(wrapped, beforeNormalization)
		if err != nil {
			return nil, err
		}
		body, _ = sjson.SetRawBytes(body, "tool_choice", []byte(gjson.GetBytes(normalized, "tool_choice.tools.0").Raw))
		return body, nil
	}
	if choice.Get("type").String() == "web_search" || choice.Get("type").String() == "web_search_preview" {
		// Grok accepts hosted search through allowed_tools, not a forced hosted
		// tool object. Keep the constraint when other tools are also available.
		body, _ = sjson.SetRawBytes(body, "tool_choice", []byte(`{"type":"allowed_tools","mode":"required","tools":[{"type":"web_search"}]}`))
		choice = gjson.GetBytes(body, "tool_choice")
	}
	if choice.Get("type").String() != "allowed_tools" {
		return body, nil
	}
	allowed := choice
	if nested := choice.Get("allowed_tools"); nested.Exists() {
		allowed = nested
	}
	mode := allowed.Get("mode").String()
	if mode != "auto" && mode != "required" {
		return nil, fmt.Errorf("xai tool_choice.allowed_tools mode must be auto or required")
	}
	refs := allowed.Get("tools")
	if !refs.IsArray() || len(refs.Array()) == 0 {
		return nil, fmt.Errorf("xai tool_choice.allowed_tools must contain at least one tool")
	}
	var kept []json.RawMessage
	for _, ref := range refs.Array() {
		kind, name := ref.Get("type").String(), ref.Get("name").String()
		if name == "" {
			name = ref.Get("function.name").String()
		}
		if kind == "custom" {
			kind = "function"
		}
		if kind == "web_search_preview" {
			kind = "web_search"
		}
		if namespace := ref.Get("namespace").String(); namespace != "" &&
			!xaiNamespaceHasTool(gjson.GetBytes(beforeNormalization, "tools"), namespace, name) {
			continue
		}
		matches := 0
		for _, tool := range gjson.GetBytes(body, "tools").Array() {
			if kind != tool.Get("type").String() {
				continue
			}
			if kind == "mcp" {
				if label := ref.Get("server_label").String(); label != "" && label == tool.Get("server_label").String() {
					matches++
				}
			} else if name == tool.Get("name").String() {
				matches++
			}
		}
		if matches > 1 && name != "" {
			return nil, fmt.Errorf("xai tool_choice.allowed_tools references an ambiguous flattened tool")
		}
		if matches == 0 {
			continue
		}
		raw := []byte(ref.Raw)
		raw, _ = sjson.SetBytes(raw, "type", kind)
		if name != "" {
			raw, _ = sjson.SetBytes(raw, "name", name)
		}
		raw, _ = sjson.DeleteBytes(raw, "function")
		raw, _ = sjson.DeleteBytes(raw, "namespace")
		kept = append(kept, raw)
	}
	if len(kept) == 0 {
		return nil, fmt.Errorf("xai tool_choice.allowed_tools has no supported tools; refusing to remove the caller's tool restriction")
	}
	normalized, _ := json.Marshal(struct {
		Type  string            `json:"type"`
		Mode  string            `json:"mode"`
		Tools []json.RawMessage `json:"tools"`
	}{"allowed_tools", mode, kept})
	body, _ = sjson.SetRawBytes(body, "tool_choice", normalized)
	return body, nil
}

// xaiChatToolChoiceDefault accepts the same optional default in either wire
// format. Explicit Chat client choices do not pass through this conversion.
func xaiChatToolChoiceDefault(value any) any {
	raw, err := json.Marshal(value)
	if err != nil {
		return value
	}
	choice := gjson.ParseBytes(raw)
	if choice.Get("type").String() == "function" && choice.Get("name").Exists() && !choice.Get("function").Exists() {
		raw, _ = sjson.SetBytes(raw, "function.name", choice.Get("name").String())
		raw, _ = sjson.DeleteBytes(raw, "name")
	} else if choice.Get("type").String() == "allowed_tools" && !choice.Get("allowed_tools").Exists() {
		refs := make([]json.RawMessage, 0)
		for _, ref := range choice.Get("tools").Array() {
			item, _ := json.Marshal(xaiChatToolChoiceDefault(ref.Value()))
			refs = append(refs, item)
		}
		raw, _ = json.Marshal(map[string]any{"type": "allowed_tools", "allowed_tools": map[string]any{"mode": choice.Get("mode").String(), "tools": refs}})
	}
	return gjson.ParseBytes(raw).Value()
}

func xaiNamespaceHasTool(tools gjson.Result, namespace, name string) bool {
	for _, tool := range tools.Array() {
		if tool.Get("type").String() != "namespace" || tool.Get("name").String() != namespace {
			continue
		}
		for _, nested := range tool.Get("tools").Array() {
			if nested.Get("name").String() == name {
				return true
			}
		}
	}
	return false
}
