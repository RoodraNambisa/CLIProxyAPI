package helps

import (
	"encoding/json"
	"fmt"
	"strings"

	translatorcommon "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/common"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const XAIToolLimit = 200

type xaiFoldItem struct {
	added     []byte
	namespace string
	ready     bool
}

// XAIToolFold retains only dispatch names and request-local stream state.
// Schemas stay in the outbound declarations, never in shared session state.
type XAIToolFold struct {
	children    map[string]string
	dispatchers map[string]map[string]bool
	items       map[string]*xaiFoldItem
}

// FoldXAITools follows the upstream namespace dispatcher format above 200 tools.
// Caller restrictions are applied before folding, so a dispatcher never widens
// an allowed_tools list. Unfoldable overflow is rejected instead of truncated.
func FoldXAITools(body, original []byte) ([]byte, *XAIToolFold, error) {
	tools := gjson.GetBytes(body, "tools").Array()
	if len(tools) <= XAIToolLimit {
		return body, nil, nil
	}
	choice := gjson.GetBytes(body, "tool_choice")
	refs := choice.Get("tools").Array()
	if choice.Get("type").String() == "function" {
		refs = []gjson.Result{choice}
	}
	if choice.Get("type").String() == "allowed_tools" || choice.Get("type").String() == "function" {
		filtered := make([]gjson.Result, 0, len(tools))
		for _, tool := range tools {
			for _, ref := range refs {
				if tool.Get("type").String() == ref.Get("type").String() && tool.Get("name").String() == ref.Get("name").String() && tool.Get("server_label").String() == ref.Get("server_label").String() {
					filtered = append(filtered, tool)
					break
				}
			}
		}
		if len(filtered) == 0 {
			return nil, nil, fmt.Errorf("xai tool restriction has no executable tools")
		}
		tools = filtered
	}
	namespaces := make(map[string]string)
	ambiguous := make(map[string]bool)
	for _, decl := range translatorcommon.ResponsesToolDeclarations(gjson.ParseBytes(original)) {
		if previous, exists := namespaces[decl.Name]; exists && previous != decl.Namespace {
			ambiguous[decl.Name] = true
		}
		namespaces[decl.Name] = decl.Namespace
	}
	groups := make(map[string][]json.RawMessage)
	for _, tool := range tools {
		name := tool.Get("name").String()
		if ns := namespaces[name]; ns != "" && !ambiguous[name] && tool.Get("type").String() == "function" {
			groups[ns] = append(groups[ns], json.RawMessage(tool.Raw))
		}
	}
	plan := &XAIToolFold{children: make(map[string]string), dispatchers: make(map[string]map[string]bool), items: make(map[string]*xaiFoldItem)}
	var output []json.RawMessage
	emitted := make(map[string]bool)
	for _, tool := range tools {
		name := tool.Get("name").String()
		ns := namespaces[name]
		if len(tools) <= XAIToolLimit || ns == "" || ambiguous[name] || len(groups[ns]) < 2 || tool.Get("type").String() != "function" {
			output = append(output, json.RawMessage(tool.Raw))
			continue
		}
		if emitted[ns] {
			continue
		}
		for _, other := range tools {
			if other.Get("name").String() == ns {
				return nil, nil, fmt.Errorf("xai namespace dispatcher name conflicts with an existing tool")
			}
		}
		emitted[ns] = true
		raw, _ := json.Marshal(map[string]any{"name": ns, "tools": groups[ns]})
		dispatcher := buildXAINamespaceDispatcherTool(gjson.ParseBytes(raw))
		if len(dispatcher) == 0 {
			return nil, nil, fmt.Errorf("xai namespace dispatcher could not be constructed")
		}
		output = append(output, dispatcher)
		plan.dispatchers[ns] = make(map[string]bool)
		for _, child := range groups[ns] {
			childName := gjson.GetBytes(child, "name").String()
			plan.children[childName] = ns
			plan.dispatchers[ns][childName] = true
		}
	}
	if len(output) > XAIToolLimit {
		return nil, nil, fmt.Errorf("xai supports at most 200 tools after namespace folding; reduce the tool list")
	}
	raw, _ := json.Marshal(output)
	body, _ = sjson.SetRawBytes(body, "tools", raw)
	if len(plan.dispatchers) == 0 {
		return body, nil, nil
	}
	if choice.Get("type").String() == "allowed_tools" {
		body, _ = sjson.SetBytes(body, "tool_choice", choice.Get("mode").String())
	}
	for i, item := range gjson.GetBytes(body, "input").Array() {
		if item.Get("type").String() != "function_call" {
			continue
		}
		name := item.Get("name").String()
		ns := plan.children[name]
		if explicit := item.Get("namespace").String(); explicit != "" && explicit != ns {
			continue
		}
		if ns == "" {
			continue
		}
		args := item.Get("arguments").String()
		if !gjson.Valid(args) {
			return nil, nil, fmt.Errorf("xai historical tool arguments are not valid JSON")
		}
		wrapped, _ := json.Marshal(map[string]any{"name": name, "arguments": json.RawMessage(args)})
		prefix := fmt.Sprintf("input.%d.", i)
		body, _ = sjson.SetBytes(body, prefix+"name", ns)
		body, _ = sjson.SetBytes(body, prefix+"arguments", string(wrapped))
		body, _ = sjson.DeleteBytes(body, prefix+"namespace")
	}
	return body, plan, nil
}

func buildXAINamespaceDispatcherTool(tool gjson.Result) []byte {
	namespaceName := strings.TrimSpace(tool.Get("name").String())
	if namespaceName == "" {
		return nil
	}
	description := strings.TrimSpace(tool.Get("description").String())

	var toolNames []string
	var toolDescriptions []string
	if nestedTools := tool.Get("tools"); nestedTools.IsArray() {
		for _, child := range nestedTools.Array() {
			childName := strings.TrimSpace(child.Get("name").String())
			if childName == "" {
				continue
			}
			toolNames = append(toolNames, childName)
			childDesc := strings.TrimSpace(child.Get("description").String())

			params := child.Get("parameters")
			if !params.Exists() {
				params = child.Get("input_schema")
			}

			var paramStr string
			if params.Exists() && params.Raw != "" {
				rawParams := strings.TrimSpace(params.Raw)
				if rawParams != "" && rawParams != "{}" && rawParams != `{"type":"object","properties":{}}` {
					inlined := util.InlineLocalRefs(rawParams)
					if gjson.Valid(inlined) {
						cleaned := []byte(inlined)
						if gjson.GetBytes(cleaned, "$defs").Exists() {
							cleaned, _ = sjson.DeleteBytes(cleaned, "$defs")
						}
						if gjson.GetBytes(cleaned, "definitions").Exists() {
							cleaned, _ = sjson.DeleteBytes(cleaned, "definitions")
						}
						paramStr = string(cleaned)
					} else {
						paramStr = inlined
					}
				}
			}

			var entry string
			if childDesc != "" {
				if paramStr != "" {
					entry = fmt.Sprintf("- %s: %s\n  Parameters: %s", childName, childDesc, paramStr)
				} else {
					entry = fmt.Sprintf("- %s: %s", childName, childDesc)
				}
			} else {
				if paramStr != "" {
					entry = fmt.Sprintf("- %s\n  Parameters: %s", childName, paramStr)
				} else {
					entry = fmt.Sprintf("- %s", childName)
				}
			}
			toolDescriptions = append(toolDescriptions, entry)
		}
	}

	fullDescription := description
	if len(toolDescriptions) > 0 {
		catalog := "Available tools in this namespace:\n" + strings.Join(toolDescriptions, "\n")
		if fullDescription != "" {
			fullDescription += "\n\n" + catalog
		} else {
			fullDescription = fmt.Sprintf("Tools in namespace %s.\n\n%s", namespaceName, catalog)
		}
	} else if fullDescription == "" {
		fullDescription = fmt.Sprintf("Tools in namespace %s.", namespaceName)
	}

	nameProp := map[string]any{
		"type":        "string",
		"description": fmt.Sprintf("Child tool name to execute in namespace %s", namespaceName),
	}
	if len(toolNames) > 0 {
		nameProp["enum"] = toolNames
	}

	dispatcher := map[string]any{
		"type":        "function",
		"name":        namespaceName,
		"description": fullDescription,
		"parameters": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"name": nameProp,
				"arguments": map[string]any{
					"type":                 "object",
					"description":          "Arguments object matching the parameter schema of the selected child tool",
					"additionalProperties": true,
				},
			},
			"required": []string{"name"},
		},
	}

	raw, errMarshal := json.Marshal(dispatcher)
	if errMarshal != nil {
		return nil
	}
	return raw
}

func (p *XAIToolFold) unwrap(namespace, raw string) (string, string, bool) {
	value := gjson.Parse(raw)
	name := value.Get("name").String()
	if !gjson.Valid(raw) || !p.dispatchers[namespace][name] {
		return "", "", false
	}
	args := value.Get("arguments")
	text := args.Raw
	if args.Type == gjson.String {
		text = args.Str
	}
	if !args.Exists() {
		text = "{}"
	}
	if !gjson.Valid(text) {
		return "", "", false
	}
	return name, text, true
}

// Restore rewrites complete function-call objects before protocol translation.
func (p *XAIToolFold) Restore(data []byte) []byte {
	if p == nil {
		return data
	}
	apply := func(prefix string, item gjson.Result) {
		if item.Get("type").String() != "function_call" || item.Get("namespace").String() != "" {
			return
		}
		ns := item.Get("name").String()
		name, args, ok := p.unwrap(ns, item.Get("arguments").String())
		if !ok {
			return
		}
		data, _ = sjson.SetBytes(data, prefix+"name", name)
		data, _ = sjson.SetBytes(data, prefix+"namespace", ns)
		data, _ = sjson.SetBytes(data, prefix+"arguments", args)
	}
	root := gjson.ParseBytes(data)
	apply("", root)
	apply("item.", root.Get("item"))
	for _, path := range []string{"output", "response.output"} {
		for i, item := range root.Get(path).Array() {
			apply(fmt.Sprintf("%s.%d.", path, i), item)
		}
	}
	return data
}

func xaiFoldFrame(data []byte) []byte { return append(append([]byte("data: "), data...), '\n', '\n') }

func (p *XAIToolFold) finishItem(item *xaiFoldItem, arguments string) [][]byte {
	if item == nil || item.ready {
		return nil
	}
	name, args, ok := p.unwrap(item.namespace, arguments)
	if !ok {
		return nil
	}
	item.ready = true
	added, _ := sjson.SetBytes(item.added, "item.name", name)
	added, _ = sjson.SetBytes(added, "item.namespace", item.namespace)
	added, _ = sjson.SetBytes(added, "item.arguments", "")
	root := gjson.ParseBytes(added)
	event := map[string]any{"type": "response.function_call_arguments.delta", "item_id": root.Get("item.id").String(), "output_index": root.Get("output_index").Int(), "delta": args}
	delta, _ := json.Marshal(event)
	event["type"], event["arguments"] = "response.function_call_arguments.done", args
	delete(event, "delta")
	done, _ := json.Marshal(event)
	return [][]byte{xaiFoldFrame(added), xaiFoldFrame(delta), xaiFoldFrame(done)}
}

// StreamFrames holds only folded call envelopes until their child name and
// arguments are known. Ordinary text and non-folded tools keep streaming.
func (p *XAIToolFold) StreamFrames(frame []byte) [][]byte {
	if p == nil {
		return [][]byte{frame}
	}
	events := ParseOpenAIStreamFrame(frame)
	if len(events) == 0 {
		return [][]byte{frame}
	}
	var out [][]byte
	for _, event := range events {
		if event.Err != nil || string(event.Data) == "[DONE]" {
			out = append(out, frame)
			continue
		}
		data := event.Data
		root := gjson.ParseBytes(data)
		switch root.Get("type").String() {
		case "response.output_item.added":
			item := root.Get("item")
			if ns := item.Get("name").String(); item.Get("type").String() == "function_call" && len(p.dispatchers[ns]) > 0 && item.Get("id").String() != "" {
				p.items[item.Get("id").String()] = &xaiFoldItem{added: data, namespace: ns}
				continue
			}
		case "response.function_call_arguments.delta":
			if p.items[root.Get("item_id").String()] != nil {
				continue
			}
		case "response.function_call_arguments.done":
			if item := p.items[root.Get("item_id").String()]; item != nil {
				out = append(out, p.finishItem(item, root.Get("arguments").String())...)
				continue
			}
		case "response.output_item.done":
			if item := p.items[root.Get("item.id").String()]; item != nil {
				out = append(out, p.finishItem(item, root.Get("item.arguments").String())...)
			}
		case "response.completed", "response.incomplete":
			for _, responseItem := range root.Get("response.output").Array() {
				out = append(out, p.finishItem(p.items[responseItem.Get("id").String()], responseItem.Get("arguments").String())...)
			}
		}
		out = append(out, xaiFoldFrame(p.Restore(data)))
	}
	return out
}
