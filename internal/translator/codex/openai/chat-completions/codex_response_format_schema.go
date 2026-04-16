package chat_completions

import (
	"encoding/json"
	"sort"
)

var codexResponseFormatSchemaObjectFields = map[string]struct{}{
	"additionalItems":       {},
	"additionalProperties":  {},
	"contains":              {},
	"contentSchema":         {},
	"else":                  {},
	"if":                    {},
	"items":                 {},
	"not":                   {},
	"propertyNames":         {},
	"then":                  {},
	"unevaluatedItems":      {},
	"unevaluatedProperties": {},
}

var codexResponseFormatSchemaMapFields = map[string]struct{}{
	"$defs":             {},
	"definitions":       {},
	"dependentSchemas":  {},
	"patternProperties": {},
	"properties":        {},
}

var codexResponseFormatSchemaArrayFields = map[string]struct{}{
	"allOf":       {},
	"anyOf":       {},
	"oneOf":       {},
	"prefixItems": {},
}

func normalizeCodexResponseFormatSchema(rawSchema []byte) []byte {
	if len(rawSchema) == 0 || !json.Valid(rawSchema) {
		return rawSchema
	}

	var schema any
	if err := json.Unmarshal(rawSchema, &schema); err != nil {
		return rawSchema
	}

	normalized, changed := normalizeCodexResponseFormatSchemaValue(schema)
	if !changed {
		return rawSchema
	}

	out, errMarshal := json.Marshal(normalized)
	if errMarshal != nil {
		return rawSchema
	}
	return out
}

func normalizeCodexResponseFormatSchemaValue(value any) (any, bool) {
	switch schema := value.(type) {
	case map[string]any:
		changed := false

		for key, child := range schema {
			if _, ok := codexResponseFormatSchemaMapFields[key]; ok {
				childMap, ok := child.(map[string]any)
				if !ok {
					continue
				}
				for name, nested := range childMap {
					normalized, nestedChanged := normalizeCodexResponseFormatSchemaValue(nested)
					if !nestedChanged {
						continue
					}
					childMap[name] = normalized
					changed = true
				}
				continue
			}

			if _, ok := codexResponseFormatSchemaObjectFields[key]; ok {
				normalized, nestedChanged := normalizeCodexResponseFormatSchemaValue(child)
				if !nestedChanged {
					continue
				}
				schema[key] = normalized
				changed = true
				continue
			}

			if _, ok := codexResponseFormatSchemaArrayFields[key]; ok {
				children, ok := child.([]any)
				if !ok {
					continue
				}
				for i := range children {
					normalized, nestedChanged := normalizeCodexResponseFormatSchemaValue(children[i])
					if !nestedChanged {
						continue
					}
					children[i] = normalized
					changed = true
				}
			}
		}

		properties, hasProperties := schema["properties"].(map[string]any)
		if !hasProperties || !codexResponseFormatSchemaUsesObjectSemantics(schema) {
			return schema, changed
		}

		propertyNames := codexResponseFormatSchemaSortedPropertyNames(properties)
		if len(propertyNames) == 0 {
			if _, hasRequired := schema["required"]; !hasRequired {
				return schema, changed
			}
		}
		if codexResponseFormatSchemaRequiredMatches(schema["required"], propertyNames) {
			return schema, changed
		}

		schema["required"] = propertyNames
		return schema, true

	case []any:
		changed := false
		for i := range schema {
			normalized, nestedChanged := normalizeCodexResponseFormatSchemaValue(schema[i])
			if !nestedChanged {
				continue
			}
			schema[i] = normalized
			changed = true
		}
		return schema, changed

	default:
		return value, false
	}
}

func codexResponseFormatSchemaUsesObjectSemantics(schema map[string]any) bool {
	typeValue, exists := schema["type"]
	if !exists {
		return true
	}

	switch typed := typeValue.(type) {
	case string:
		return typed == "object"
	case []any:
		for _, item := range typed {
			itemStr, ok := item.(string)
			if ok && itemStr == "object" {
				return true
			}
		}
	}

	return false
}

func codexResponseFormatSchemaSortedPropertyNames(properties map[string]any) []string {
	names := make([]string, 0, len(properties))
	for name := range properties {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func codexResponseFormatSchemaRequiredMatches(requiredValue any, propertyNames []string) bool {
	requiredNames, ok := codexResponseFormatSchemaRequiredNames(requiredValue)
	if !ok || len(requiredNames) != len(propertyNames) {
		return false
	}
	sort.Strings(requiredNames)
	for i := range requiredNames {
		if requiredNames[i] != propertyNames[i] {
			return false
		}
	}
	return true
}

func codexResponseFormatSchemaRequiredNames(requiredValue any) ([]string, bool) {
	switch typed := requiredValue.(type) {
	case []string:
		out := append([]string(nil), typed...)
		return out, true
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			itemStr, ok := item.(string)
			if !ok {
				return nil, false
			}
			out = append(out, itemStr)
		}
		return out, true
	default:
		return nil, false
	}
}
