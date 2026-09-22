package config

import (
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

var credentialYAMLTypes = map[string]reflect.Type{
	"api-key-groups":              reflect.TypeFor[APIKeyGroup](),
	"codex":                       reflect.TypeFor[CodexConfig](),
	"gemini-api-key":              reflect.TypeFor[GeminiKey](),
	"interactions-api-key":        reflect.TypeFor[GeminiKey](),
	"claude-api-key":              reflect.TypeFor[ClaudeKey](),
	"xai-api-key":                 reflect.TypeFor[XAIKey](),
	"codex-api-key":               reflect.TypeFor[CodexKey](),
	"vertex-api-key":              reflect.TypeFor[VertexCompatKey](),
	"openai-compatibility":        reflect.TypeFor[OpenAICompatibility](),
	"oauth-request-scoped-errors": reflect.TypeFor[map[string][]RequestScopedErrorRule](),
}

func credentialYAMLMappingType(path []string) reflect.Type {
	if len(path) == 0 {
		return nil
	}
	if len(path) == 2 && path[0] == "oauth-request-scoped-errors" {
		return reflect.TypeFor[RequestScopedErrorRule]()
	}
	typ := credentialYAMLTypes[path[0]]
	unwrap := func(t reflect.Type) reflect.Type {
		for t != nil && (t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice) {
			t = t.Elem()
		}
		return t
	}
	for _, key := range path[1:] {
		typ = unwrap(typ)
		if typ == nil || typ.Kind() != reflect.Struct {
			return nil
		}
		var next reflect.Type
		for index := 0; index < typ.NumField(); index++ {
			if yamlFieldName(typ.Field(index)) == key {
				next = typ.Field(index).Type
				break
			}
		}
		typ = next
	}
	typ = unwrap(typ)
	if typ == nil || typ.Kind() != reflect.Struct {
		return nil
	}
	return typ
}

func yamlFieldName(field reflect.StructField) string {
	name, _, _ := strings.Cut(field.Tag.Get("yaml"), ",")
	if name == "" {
		return strings.ToLower(field.Name)
	}
	return name
}

// Credential extensions must not act as identifiers. Secondary fields separate
// shared keys and models, while a unique primary identity allows target edits.
func matchCredentialYAMLSequenceElement(original []*yaml.Node, used []bool, target *yaml.Node, path []string) (int, bool) {
	if target == nil || target.Kind != yaml.MappingNode {
		return -1, false
	}
	var primary, secondary string
	switch credentialYAMLMappingType(path) {
	case reflect.TypeFor[APIKeyGroup]():
		primary = "api-key"
	case reflect.TypeFor[RequestScopedErrorRule]():
		return matchRequestScopedErrorYAMLRule(original, used, target), true
	case reflect.TypeFor[GeminiKey](), reflect.TypeFor[ClaudeKey](), reflect.TypeFor[CodexKey](), reflect.TypeFor[VertexCompatKey]():
		primary, secondary = "api-key", "base-url"
	case reflect.TypeFor[OpenAICompatibilityAPIKey]():
		primary, secondary = "api-key", "proxy-url"
	case reflect.TypeFor[OpenAICompatibility]():
		primary, secondary = "name", "base-url"
	case reflect.TypeFor[GeminiModel](), reflect.TypeFor[ClaudeModel](), reflect.TypeFor[CodexModel](), reflect.TypeFor[VertexCompatModel](), reflect.TypeFor[OpenAICompatibilityModel]():
		primary, secondary = "name", "alias"
	default:
		return -1, false
	}
	key, qualifier := mappingScalarValue(target, primary), mappingScalarValue(target, secondary)
	count, sole, exact := 0, -1, -1
	for index, node := range original {
		if used[index] || node == nil || node.Kind != yaml.MappingNode || mappingScalarValue(node, primary) != key {
			continue
		}
		count++
		sole = index
		if exact < 0 && mappingScalarValue(node, secondary) == qualifier {
			exact = index
		}
	}
	if exact >= 0 {
		return exact, true
	}
	if count == 1 {
		return sole, true
	}
	return -1, true
}

// Status alone is not an identity: many ordered rules intentionally match the
// same status. Preserve exact rules first, then an unambiguous action edit.
func matchRequestScopedErrorYAMLRule(original []*yaml.Node, used []bool, target *yaml.Node) int {
	var wanted RequestScopedErrorRule
	if err := target.Decode(&wanted); err != nil {
		return -1
	}
	candidate, matches := -1, 0
	for index, node := range original {
		if used[index] || node == nil {
			continue
		}
		var current RequestScopedErrorRule
		if err := node.Decode(&current); err != nil {
			continue
		}
		if current.Status != wanted.Status || !slices.Equal(current.Match, wanted.Match) || !slices.Equal(current.MatchRegexr, wanted.MatchRegexr) {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(current.Action), strings.TrimSpace(wanted.Action)) {
			return index
		}
		candidate, matches = index, matches+1
	}
	if matches == 1 {
		return candidate
	}
	return -1
}

// Materialize credential and Codex policy aliases before updating fields. Otherwise
// deleting a known field can reintroduce its old value from an inherited map.
func prepareCredentialYAMLForSave(root *yaml.Node) error {
	for name := range credentialYAMLTypes {
		index := findMapKeyIndex(root, name)
		if index < 0 {
			inherited, err := credentialYAMLField(root, name, make(map[*yaml.Node]bool))
			if err != nil {
				return err
			}
			if inherited == nil {
				continue
			}
			index = len(root.Content)
			root.Content = append(root.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: name}, cloneCredentialYAMLNode(inherited))
		}
		if err := materializeCredentialYAML(root.Content[index+1], []string{name}); err != nil {
			return err
		}
	}
	return nil
}

// Detached copies must not redefine anchors still owned by the original tree.
func cloneCredentialYAMLNode(node *yaml.Node) *yaml.Node {
	cloned := deepCopyNode(node)
	var clearAnchors func(*yaml.Node)
	clearAnchors = func(value *yaml.Node) {
		if value != nil {
			value.Anchor = ""
			for _, child := range value.Content {
				clearAnchors(child)
			}
		}
	}
	clearAnchors(cloned)
	return cloned
}

func materializeCredentialYAML(node *yaml.Node, path []string) error {
	isOAuthErrorMap := len(path) == 1 && path[0] == "oauth-request-scoped-errors"
	if node == nil || (!isOAuthErrorMap && credentialYAMLMappingType(path) == nil) {
		return nil
	}
	if node.Kind == yaml.AliasNode {
		if node.Alias == nil {
			return fmt.Errorf("credential YAML contains an unresolved alias")
		}
		copyNodeShallow(node, cloneCredentialYAMLNode(node.Alias))
		node.Alias = nil
	}
	if node.Kind == yaml.SequenceNode {
		for _, item := range node.Content {
			if err := materializeCredentialYAML(item, path); err != nil {
				return err
			}
		}
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return nil
	}
	merged := false
	for index := 0; index+1 < len(node.Content); index += 2 {
		merged = merged || node.Content[index].Tag == "!!merge"
	}
	if merged {
		var resolved map[string]yaml.Node
		if err := node.Decode(&resolved); err != nil {
			return fmt.Errorf("invalid credential YAML inheritance at line %d", node.Line)
		}
		kept := make([]*yaml.Node, 0, len(node.Content))
		for index := 0; index+1 < len(node.Content); index += 2 {
			if node.Content[index].Tag != "!!merge" {
				kept = append(kept, node.Content[index], node.Content[index+1])
			}
		}
		node.Content = kept
		keys := make([]string, 0, len(resolved))
		for key := range resolved {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if findMapKeyIndex(node, key) >= 0 {
				continue
			}
			value := resolved[key]
			node.Content = append(node.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key}, cloneCredentialYAMLNode(&value))
		}
	}
	for index := 0; index+1 < len(node.Content); index += 2 {
		if err := materializeCredentialYAML(node.Content[index+1], appendPath(path, node.Content[index].Value)); err != nil {
			return err
		}
	}
	return nil
}

// Known omitted fields are cleared; unknown credential extensions survive.
func pruneMissingCredentialYAMLKeys(dst, src *yaml.Node, path []string) {
	typ := credentialYAMLMappingType(path)
	if typ == nil {
		pruneMissingMapKeys(dst, src)
		return
	}
	known := make(map[string]bool, typ.NumField())
	var collect func(reflect.Type)
	collect = func(t reflect.Type) {
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if slices.Contains(strings.Split(field.Tag.Get("yaml"), ","), "inline") {
				nested := field.Type
				if nested.Kind() == reflect.Pointer {
					nested = nested.Elem()
				}
				if nested.Kind() == reflect.Struct {
					collect(nested)
				}
			} else {
				known[yamlFieldName(field)] = true
			}
		}
	}
	collect(typ)
	retained := *src
	retained.Content = append([]*yaml.Node(nil), src.Content...)
	for index := 0; index+1 < len(dst.Content); index += 2 {
		if !known[dst.Content[index].Value] {
			retained.Content = append(retained.Content, dst.Content[index], dst.Content[index+1])
		}
	}
	pruneMissingMapKeys(dst, &retained)
}
