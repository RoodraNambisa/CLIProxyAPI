package config

import (
	"fmt"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/credentialweight"
	"gopkg.in/yaml.v3"
)

const MaxCredentialWeight = int(credentialweight.Max)

func ValidateCredentialWeight(weight *int) error {
	if weight == nil {
		return nil
	}
	_, err := credentialweight.Normalize(int64(*weight))
	return err
}

func (cfg *Config) ValidateCredentialWeights() error {
	if cfg == nil {
		return nil
	}
	check := func(path string, weight *int) error {
		if err := ValidateCredentialWeight(weight); err != nil {
			return fmt.Errorf("%s.weight: %w", path, err)
		}
		return nil
	}
	for index, key := range cfg.GeminiKey {
		if err := check(fmt.Sprintf("gemini-api-key[%d]", index), key.Weight); err != nil {
			return err
		}
	}
	for index, key := range cfg.InteractionsKey {
		if err := check(fmt.Sprintf("interactions-api-key[%d]", index), key.Weight); err != nil {
			return err
		}
	}
	for index, key := range cfg.ClaudeKey {
		if err := check(fmt.Sprintf("claude-api-key[%d]", index), key.Weight); err != nil {
			return err
		}
	}
	for index, key := range cfg.CodexKey {
		if err := check(fmt.Sprintf("codex-api-key[%d]", index), key.Weight); err != nil {
			return err
		}
	}
	for index, key := range cfg.VertexCompatAPIKey {
		if err := check(fmt.Sprintf("vertex-api-key[%d]", index), key.Weight); err != nil {
			return err
		}
	}
	for index, provider := range cfg.OpenAICompatibility {
		for keyIndex, key := range provider.APIKeyEntries {
			if err := check(fmt.Sprintf("openai-compatibility[%d].api-key-entries[%d]", index, keyIndex), key.Weight); err != nil {
				return err
			}
		}
	}
	return nil
}

// Weight fields must be integer scalars before yaml.v3 can coerce fractional
// values into Go integers. Resolve aliases and merges using YAML precedence.
func validateCredentialWeightYAML(data []byte) error {
	var document yaml.Node
	if err := yaml.Unmarshal(data, &document); err != nil || len(document.Content) == 0 {
		return nil
	}
	root := document.Content[0]
	validateSequence := func(sequence *yaml.Node, path string) error {
		if sequence == nil {
			return nil
		}
		if sequence.Kind == yaml.AliasNode {
			sequence = sequence.Alias
		}
		if sequence == nil || sequence.Kind != yaml.SequenceNode {
			return nil
		}
		for index, item := range sequence.Content {
			weight, err := credentialWeightYAMLField(item, "weight", make(map[*yaml.Node]bool))
			if err != nil {
				return err
			}
			if weight == nil {
				continue
			}
			if weight.Kind != yaml.ScalarNode || weight.Tag != "!!int" {
				return fmt.Errorf("%s[%d].weight must be an integer", path, index)
			}
			var value int64
			if err := weight.Decode(&value); err != nil {
				return fmt.Errorf("%s[%d].weight exceeds the signed integer range", path, index)
			}
			if _, err := credentialweight.Normalize(value); err != nil {
				return fmt.Errorf("%s[%d].weight: %w", path, index, err)
			}
		}
		return nil
	}
	for _, name := range []string{"gemini-api-key", "interactions-api-key", "claude-api-key", "codex-api-key", "vertex-api-key"} {
		sequence, err := credentialWeightYAMLField(root, name, make(map[*yaml.Node]bool))
		if err != nil {
			return err
		}
		if err := validateSequence(sequence, name); err != nil {
			return err
		}
	}
	providers, err := credentialWeightYAMLField(root, "openai-compatibility", make(map[*yaml.Node]bool))
	if err != nil {
		return err
	}
	if providers != nil && providers.Kind == yaml.AliasNode {
		providers = providers.Alias
	}
	if providers != nil && providers.Kind == yaml.SequenceNode {
		for index, provider := range providers.Content {
			sequence, err := credentialWeightYAMLField(provider, "api-key-entries", make(map[*yaml.Node]bool))
			if err != nil {
				return err
			}
			if err := validateSequence(sequence, fmt.Sprintf("openai-compatibility[%d].api-key-entries", index)); err != nil {
				return err
			}
		}
	}
	return nil
}

func credentialWeightYAMLField(node *yaml.Node, name string, visited map[*yaml.Node]bool) (*yaml.Node, error) {
	if node == nil {
		return nil, nil
	}
	if visited[node] || len(visited) > 256 {
		return nil, fmt.Errorf("credential weight YAML contains a cycle or exceeds the nesting limit")
	}
	visited[node] = true
	defer delete(visited, node)
	if node.Kind == yaml.AliasNode {
		return credentialWeightYAMLField(node.Alias, name, visited)
	}
	if node.Kind != yaml.MappingNode {
		return nil, nil
	}
	for index := 0; index+1 < len(node.Content); index += 2 {
		if node.Content[index].Value == name {
			value := node.Content[index+1]
			if value.Kind == yaml.AliasNode {
				value = value.Alias
			}
			return value, nil
		}
	}
	for index := 0; index+1 < len(node.Content); index += 2 {
		if node.Content[index].Tag != "!!merge" {
			continue
		}
		merge := node.Content[index+1]
		if merge.Kind == yaml.SequenceNode {
			for _, item := range merge.Content {
				if field, err := credentialWeightYAMLField(item, name, visited); field != nil || err != nil {
					return field, err
				}
			}
		} else if field, err := credentialWeightYAMLField(merge, name, visited); field != nil || err != nil {
			return field, err
		}
	}
	return nil, nil
}
