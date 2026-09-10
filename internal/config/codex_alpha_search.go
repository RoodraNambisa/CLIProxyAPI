package config

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

func validateCodexAlphaSearchYAML(data []byte) error {
	var document yaml.Node
	if err := yaml.Unmarshal(data, &document); err != nil || len(document.Content) == 0 {
		return nil
	}
	entries, err := credentialYAMLField(document.Content[0], "codex-api-key", make(map[*yaml.Node]bool))
	if err != nil {
		return err
	}
	if entries == nil || entries.Kind != yaml.SequenceNode {
		return nil
	}
	for index, entry := range entries.Content {
		value, err := credentialYAMLField(entry, "alpha-search", make(map[*yaml.Node]bool))
		if err != nil {
			return err
		}
		if err := validateRequestPolicyBoolean(value, fmt.Sprintf("codex-api-key[%d].alpha-search", index)); err != nil {
			return err
		}
	}
	return nil
}
