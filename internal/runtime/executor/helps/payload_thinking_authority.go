package helps

import (
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/tidwall/gjson"
)

// ApplyPayloadConfigWithThinkingAuthority preserves the existing rule order and
// reports whether an OpenAI effort field was explicitly controlled by a rule.
func ApplyPayloadConfigWithThinkingAuthority(cfg *config.Config, model, protocol, root string, payload, original []byte, requestedModel string) ([]byte, bool) {
	field := ""
	switch strings.ToLower(strings.TrimSpace(protocol)) {
	case "openai":
		field = "reasoning_effort"
	case "codex", "openai-response":
		field = "reasoning.effort"
	}
	if field == "" {
		return ApplyPayloadConfigWithRoot(cfg, model, protocol, root, payload, original, requestedModel), false
	}
	field = buildPayloadPath(root, field)
	controlled := false
	body := ApplyPayloadConfigWithFieldObserver(cfg, model, protocol, root, payload, original, requestedModel, func(path string, before, after []byte) {
		if !controlled {
			controlled = payloadRuleControlsField(path, field, before, after)
		}
	})
	return body, controlled
}

func payloadRuleControlsField(path, field string, before, after []byte) bool {
	parts, simple := simplePayloadPathParts(path)
	fieldParts, fieldSimple := simplePayloadPathParts(field)
	if simple && fieldSimple {
		for i := 0; i < len(parts) && i < len(fieldParts); i++ {
			if parts[i] != fieldParts[i] {
				return false
			}
		}
		if len(parts) <= len(fieldParts) {
			return true
		}
	}
	oldField, newField := gjson.GetBytes(before, field), gjson.GetBytes(after, field)
	if oldField.Exists() != newField.Exists() || oldField.Raw != newField.Raw {
		return true
	}
	if !oldField.Exists() || oldField.Index <= 0 {
		return false
	}
	selected := gjson.GetBytes(before, path)
	overlaps := func(index, length int) bool {
		return index > 0 && length > 0 && index < oldField.Index+len(oldField.Raw) && oldField.Index < index+length
	}
	if selected.Index > 0 && overlaps(selected.Index, len(selected.Raw)) {
		return true
	}
	// SJSON complex paths use these original-buffer offsets for matched values.
	matched := false
	index := 0
	selected.ForEach(func(_, value gjson.Result) bool {
		if index >= len(selected.Indexes) {
			return false
		}
		matched = overlaps(selected.Indexes[index], len(value.Raw))
		index++
		return !matched
	})
	return matched
}

// Match SJSON's simple path syntax, including escaped dots and forced keys.
// Complex selectors are resolved against the actual pre-rule JSON instead.
func simplePayloadPathParts(path string) ([]string, bool) {
	var parts []string
	for {
		path = strings.TrimPrefix(path, ":")
		var part strings.Builder
		next := -1
		for i := 0; i < len(path); i++ {
			switch path[i] {
			case '\\':
				i++
				if i < len(path) {
					part.WriteByte(path[i])
				}
			case '.':
				next = i + 1
				i = len(path)
			case '|', '#', '@', '*', '?':
				return nil, false
			default:
				part.WriteByte(path[i])
			}
		}
		parts = append(parts, part.String())
		if next < 0 {
			return parts, true
		}
		path = path[next:]
	}
}
