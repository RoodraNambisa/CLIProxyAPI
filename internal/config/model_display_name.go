package config

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// Validate only the new field before optional-load fallback can hide its type
// error. Unrelated legacy YAML errors keep their existing startup behavior.
func validateModelDisplayNamesYAML(data []byte) error {
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
	validate := func(models *yaml.Node) error {
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
		}
		return nil
	}
	for _, family := range []string{"gemini-api-key", "interactions-api-key", "claude-api-key", "codex-api-key", "vertex-api-key", "openai-compatibility"} {
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
			if err := validate(models); err != nil {
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
			if err := validate(&rules); err != nil {
				return fmt.Errorf("oauth-model-alias: %w", err)
			}
		}
	}
	return nil
}
