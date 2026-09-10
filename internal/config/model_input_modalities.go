package config

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// NormalizeModelInputModalities returns owned, unique values in declared order.
// Missing/null/empty lists inherit the catalog. Unknown or empty entries fail
// closed rather than silently turning a malformed list into text-only input.
func NormalizeModelInputModalities(raw []string) ([]string, error) {
	var out []string
	seen := make(map[string]bool)
	for _, value := range raw {
		modality := strings.ToLower(strings.TrimSpace(value))
		switch modality {
		case "text", "image", "audio", "video":
		default:
			return nil, fmt.Errorf("input-modalities must contain only text, image, audio or video")
		}
		if !seen[modality] {
			seen[modality] = true
			out = append(out, modality)
		}
	}
	return out, nil
}

func (cfg *Config) ValidateModelInputModalities() error {
	if cfg == nil {
		return nil
	}
	for index, provider := range cfg.OpenAICompatibility {
		for modelIndex, model := range provider.Models {
			if _, err := NormalizeModelInputModalities(model.InputModalities); err != nil {
				return fmt.Errorf("openai-compatibility[%d].models[%d]: %w", index, modelIndex, err)
			}
		}
	}
	return nil
}

func validateModelInputModalitiesYAML(node *yaml.Node) error {
	resolve := func(value *yaml.Node) (*yaml.Node, error) {
		seen := make(map[*yaml.Node]bool)
		for value != nil && value.Kind == yaml.AliasNode {
			if seen[value] {
				return nil, fmt.Errorf("input-modalities contains a cyclic YAML alias")
			}
			seen[value] = true
			value = value.Alias
		}
		return value, nil
	}
	node, err := resolve(node)
	if err != nil {
		return err
	}
	if node == nil || node.Tag == "!!null" {
		return nil
	}
	if node.Kind != yaml.SequenceNode {
		return fmt.Errorf("input-modalities must be a list")
	}
	values := make([]string, 0, len(node.Content))
	for _, item := range node.Content {
		item, err = resolve(item)
		if err != nil {
			return err
		}
		if item == nil || item.Kind != yaml.ScalarNode || item.Tag != "!!str" {
			return fmt.Errorf("input-modalities must contain strings")
		}
		values = append(values, item.Value)
	}
	_, err = NormalizeModelInputModalities(values)
	return err
}
