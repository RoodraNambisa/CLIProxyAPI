package config

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// Validate new catalog fields before optional-load fallback can hide type
// errors. Unrelated legacy YAML errors keep their existing startup behavior.
func validateModelCatalogFieldsYAML(data []byte) error {
	var document yaml.Node
	if err := yaml.Unmarshal(data, &document); err != nil || len(document.Content) == 0 {
		return nil
	}
	resolve := func(node *yaml.Node) *yaml.Node {
		seen := make(map[*yaml.Node]bool)
		for node != nil && node.Kind == yaml.AliasNode {
			if seen[node] {
				return nil
			}
			seen[node] = true
			node = node.Alias
		}
		return node
	}
	validate := func(models *yaml.Node, contextLengths, compatibility, inputModalities bool) error {
		models = resolve(models)
		if models == nil || models.Kind != yaml.SequenceNode {
			return nil
		}
		for index, model := range models.Content {
			name, err := credentialYAMLField(model, "display-name", make(map[*yaml.Node]bool))
			if err != nil {
				return err
			}
			name = resolve(name)
			if name != nil && name.Tag != "!!null" && (name.Kind != yaml.ScalarNode || name.Tag != "!!str") {
				return fmt.Errorf("models[%d].display-name must be a string", index)
			}
			if !contextLengths {
				continue
			}
			if inputModalities {
				modalities, err := credentialYAMLField(model, "input-modalities", make(map[*yaml.Node]bool))
				if err != nil {
					return err
				}
				if err := validateModelInputModalitiesYAML(modalities); err != nil {
					return fmt.Errorf("models[%d]: %w", index, err)
				}
			}
			if compatibility {
				compat, err := credentialYAMLField(model, "is-compat", make(map[*yaml.Node]bool))
				if err != nil {
					return err
				}
				compat = resolve(compat)
				if compat != nil && compat.Tag != "!!null" && (compat.Kind != yaml.ScalarNode || compat.Tag != "!!bool") {
					return fmt.Errorf("models[%d].is-compat must be a boolean", index)
				}
			}
			thinking, err := credentialYAMLField(model, "thinking", make(map[*yaml.Node]bool))
			if err != nil {
				return err
			}
			if err := validateModelThinkingYAML(thinking); err != nil {
				return fmt.Errorf("models[%d].thinking: %w", index, err)
			}
			limit, err := credentialYAMLField(model, "max-context-length", make(map[*yaml.Node]bool))
			if err != nil {
				return err
			}
			limit = resolve(limit)
			if limit == nil || limit.Tag == "!!null" {
				continue
			}
			if limit.Kind != yaml.ScalarNode || limit.Tag != "!!int" {
				return fmt.Errorf("models[%d].max-context-length must be an integer", index)
			}
			var value int64
			if err := limit.Decode(&value); err != nil {
				return fmt.Errorf("models[%d].max-context-length exceeds the signed integer range", index)
			}
			if err := ValidateModelContextLength(value); err != nil {
				return fmt.Errorf("models[%d]: %w", index, err)
			}
		}
		return nil
	}
	for _, family := range []string{"gemini-api-key", "interactions-api-key", "claude-api-key", "codex-api-key", "xai-api-key", "vertex-api-key", "openai-compatibility"} {
		entries, err := credentialYAMLField(document.Content[0], family, make(map[*yaml.Node]bool))
		if err != nil {
			return err
		}
		entries = resolve(entries)
		if entries == nil || entries.Kind != yaml.SequenceNode {
			continue
		}
		for _, entry := range entries.Content {
			models, err := credentialYAMLField(entry, "models", make(map[*yaml.Node]bool))
			if err != nil {
				return err
			}
			if err := validate(models, true, family != "openai-compatibility", family == "openai-compatibility"); err != nil {
				return fmt.Errorf("%s: %w", family, err)
			}
		}
	}
	aliases, err := credentialYAMLField(document.Content[0], "oauth-model-alias", make(map[*yaml.Node]bool))
	if err != nil {
		return err
	}
	aliases = resolve(aliases)
	if aliases != nil && aliases.Kind == yaml.MappingNode {
		var providers map[string]yaml.Node
		if err := aliases.Decode(&providers); err != nil {
			return nil
		}
		for _, rules := range providers {
			if err := validate(&rules, false, false, false); err != nil {
				return fmt.Errorf("oauth-model-alias: %w", err)
			}
		}
	}
	return nil
}
