package helps

import (
	"fmt"
	"strings"

	translatorcommon "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/common"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// XAIResponseToolNamespaces restores only unambiguous flattened function names.
// It does not retain declarations, schemas or request buffers.
type XAIResponseToolNamespaces map[string]string

func NewXAIResponseToolNamespaces(original, translated []byte) XAIResponseToolNamespaces {
	wireCounts := make(map[string]int)
	for _, tool := range gjson.GetBytes(translated, "tools").Array() {
		if tool.Get("type").String() == "function" {
			wireCounts[tool.Get("name").String()]++
		}
	}
	counts := make(map[string]int)
	namespaces := make(XAIResponseToolNamespaces)
	for _, declaration := range translatorcommon.ResponsesToolDeclarations(gjson.ParseBytes(original)) {
		if wireCounts[declaration.Name] != 1 {
			continue
		}
		// Custom declarations also make a reused wire name ambiguous.
		counts[declaration.Name]++
		if declaration.Tool.Get("type").String() == "function" && declaration.Namespace != "" {
			namespaces[strings.Clone(declaration.Name)] = strings.Clone(declaration.Namespace)
		}
	}
	for name := range namespaces {
		if counts[name] != 1 {
			delete(namespaces, name)
		}
	}
	return namespaces
}

// Restore handles Responses JSON only. Callers keep SSE framing outside it.
func (namespaces XAIResponseToolNamespaces) Restore(payload []byte) []byte {
	if len(namespaces) == 0 || !gjson.ValidBytes(payload) {
		return payload
	}
	root := gjson.ParseBytes(payload)
	updated := payload
	apply := func(item gjson.Result, prefix string) {
		if item.Get("type").String() != "function_call" || item.Get("namespace").String() != "" {
			return
		}
		if namespace := namespaces[item.Get("name").String()]; namespace != "" {
			if result, err := sjson.SetBytes(updated, prefix+"namespace", namespace); err == nil {
				updated = result
			}
		}
	}
	apply(root, "")
	if kind := root.Get("type").String(); kind == "response.output_item.added" || kind == "response.output_item.done" {
		apply(root.Get("item"), "item.")
	}
	if kind := root.Get("type").String(); kind == "" || strings.HasPrefix(kind, "response.") {
		for _, path := range []string{"output", "response.output"} {
			for index, item := range root.Get(path).Array() {
				apply(item, fmt.Sprintf("%s.%d.", path, index))
			}
		}
	}
	return updated
}
