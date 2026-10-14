package helps

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	translatorcommon "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/common"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// AliasXAIClientWebSearch separates client functions from the hosted search tool.
// Namespace dispatchers and namespace children retain their existing identities.
func AliasXAIClientWebSearch(body, original []byte) ([]byte, string) {
	eligible := false
	names := map[string]bool{}
	for _, decl := range translatorcommon.ResponsesToolDeclarations(gjson.ParseBytes(original)) {
		names[decl.Name] = true
		if decl.Name == "web_search" && decl.Namespace == "" {
			eligible = true
		}
	}
	if !eligible {
		return body, ""
	}
	found := false
	for _, tool := range gjson.GetBytes(body, "tools").Array() {
		names[tool.Get("name").String()] = true
		if tool.Get("name").String() == "web_search" && (tool.Get("type").String() == "function" || tool.Get("type").String() == "custom") {
			found = true
		}
	}
	if !found {
		return body, ""
	}
	alias := "client_web_search"
	for i := 1; names[alias]; i++ {
		alias = fmt.Sprintf("client_web_search_%d", i)
	}
	for i, tool := range gjson.GetBytes(body, "tools").Array() {
		if tool.Get("name").String() == "web_search" && (tool.Get("type").String() == "function" || tool.Get("type").String() == "custom") {
			body, _ = sjson.SetBytes(body, fmt.Sprintf("tools.%d.name", i), alias)
		}
	}
	for _, path := range []string{"tool_choice", "tool_choice.function"} {
		value := gjson.GetBytes(body, path)
		if value.Get("name").String() == "web_search" && value.Get("namespace").String() == "" && value.Get("type").String() != "web_search" {
			body, _ = sjson.SetBytes(body, path+".name", alias)
		}
	}
	for i, ref := range gjson.GetBytes(body, "tool_choice.tools").Array() {
		if ref.Get("name").String() == "web_search" && ref.Get("namespace").String() == "" && ref.Get("type").String() != "web_search" {
			body, _ = sjson.SetBytes(body, fmt.Sprintf("tool_choice.tools.%d.name", i), alias)
		}
	}
	for i, item := range gjson.GetBytes(body, "input").Array() {
		kind := item.Get("type").String()
		if (kind == "function_call" || kind == "custom_tool_call" || kind == "function_call_output") && item.Get("name").String() == "web_search" && item.Get("namespace").String() == "" {
			body, _ = sjson.SetBytes(body, fmt.Sprintf("input.%d.name", i), alias)
		}
	}
	return body, alias
}

// RestoreXAIClientWebSearch rewrites only tool identities, never text or arguments.
func RestoreXAIClientWebSearch(payload []byte, alias string) []byte {
	if alias == "" {
		return payload
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if decoder.Decode(&value) != nil {
		return payload
	}
	changed := false
	var walk func(any)
	walk = func(v any) {
		switch node := v.(type) {
		case []any:
			for _, child := range node {
				walk(child)
			}
		case map[string]any:
			kind, _ := node["type"].(string)
			ns, _ := node["namespace"].(string)
			if (kind == "function_call" || kind == "custom_tool_call" || kind == "function_call_output" || kind == "function") && ns == "" && node["name"] == alias {
				node["name"] = "web_search"
				changed = true
			}
			// Chat function objects have no type property.
			if fn, ok := node["function"].(map[string]any); ok && fn["name"] == alias {
				fn["name"] = "web_search"
				changed = true
			}
			for key, child := range node {
				if key != "arguments" && key != "parameters" && key != "text" {
					walk(child)
				}
			}
		}
	}
	walk(value)
	if !changed {
		return payload
	}
	result, err := json.Marshal(value)
	if err != nil {
		return payload
	}
	return result
}

func restoreXAIClientWebSearchFrame(frame []byte, alias string) []byte {
	if alias == "" {
		return frame
	}
	if strings.HasPrefix(string(frame), "data:") {
		return rewriteCodexSSEChunk(frame, func(data []byte) []byte { return RestoreXAIClientWebSearch(data, alias) })
	}
	return RestoreXAIClientWebSearch(frame, alias)
}
