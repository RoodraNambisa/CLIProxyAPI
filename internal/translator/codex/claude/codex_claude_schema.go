package claude

import "github.com/tidwall/gjson"

// codexSchemaMissesRequired inspects schema positions only. Defaults and examples
// contain application data and must not affect the inferred strict mode.
func codexSchemaMissesRequired(schema gjson.Result) bool {
	pending := []gjson.Result{schema}
	for len(pending) > 0 {
		current := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if current.IsArray() {
			pending = append(pending, current.Array()...)
			continue
		}
		if !current.IsObject() {
			continue
		}
		if properties := current.Get("properties"); properties.IsObject() {
			required := make(map[string]bool)
			if names := current.Get("required"); names.IsArray() {
				for _, name := range names.Array() {
					if name.Type == gjson.String {
						required[name.String()] = true
					}
				}
			}
			missing := false
			properties.ForEach(func(name, _ gjson.Result) bool {
				missing = !required[name.String()]
				return !missing
			})
			if missing {
				return true
			}
		}
		for _, keyword := range []string{"properties", "$defs", "definitions", "patternProperties", "dependentSchemas", "dependencies"} {
			if children := current.Get(keyword); children.IsObject() {
				children.ForEach(func(_, child gjson.Result) bool {
					pending = append(pending, child)
					return true
				})
			}
		}
		for _, keyword := range []string{"items", "prefixItems", "contains", "additionalProperties", "propertyNames", "unevaluatedProperties", "unevaluatedItems", "additionalItems", "contentSchema", "anyOf", "oneOf", "allOf", "not", "if", "then", "else"} {
			if child := current.Get(keyword); child.IsObject() || child.IsArray() {
				pending = append(pending, child)
			}
		}
	}
	return false
}
