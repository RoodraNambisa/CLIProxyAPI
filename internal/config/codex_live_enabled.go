package config

import (
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
	return validateRequestPolicyBoolean(value, "codex.live-enabled")
}
