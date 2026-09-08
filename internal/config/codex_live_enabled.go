package config

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

func validateCodexLiveEnabledYAML(data []byte) error {
	var document yaml.Node
	if err := yaml.Unmarshal(data, &document); err != nil || len(document.Content) == 0 {
		return nil
	}
	codex, err := credentialYAMLField(document.Content[0], "codex", make(map[*yaml.Node]bool))
	if err != nil {
		return err
	}
	value, err := credentialYAMLField(codex, "live-enabled", make(map[*yaml.Node]bool))
	if err != nil {
		return err
	}
	if value == nil || value.Tag == "!!null" {
		return nil
	}
	if value.Kind != yaml.ScalarNode || value.Tag != "!!bool" {
		return fmt.Errorf("codex.live-enabled must be a boolean")
	}
	return nil
}
