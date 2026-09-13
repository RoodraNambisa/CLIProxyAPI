package config

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// Validate request policy flags before optional loading can discard a decode
// error. YAML 1.1 strings such as yes/on must not silently enable a policy.
func validateRequestPolicyBooleansYAML(data []byte) error {
	var document yaml.Node
	if err := yaml.Unmarshal(data, &document); err != nil || len(document.Content) == 0 {
		return nil
	}
	for _, section := range []struct {
		name string
		keys []string
	}{
		{"codex", []string{"passthrough-prompt-cache-key", "stream-bootstrap-buffering", "optimize-multi-agent-v2", "orphan-delegation-compatibility", "estimate-claude-input-tokens", "observe-quota"}},
		{"routing", []string{"session-affinity-lcp", "session-affinity-subagents", "session-affinity-across-priorities", "session-affinity-use-history"}},
	} {
		node, err := credentialYAMLField(document.Content[0], section.name, make(map[*yaml.Node]bool))
		if err != nil {
			return err
		}
		for _, key := range section.keys {
			value, err := credentialYAMLField(node, key, make(map[*yaml.Node]bool))
			if err != nil {
				return err
			}
			if err := validateRequestPolicyBoolean(value, section.name+"."+key); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateRequestPolicyBoolean(value *yaml.Node, field string) error {
	if value == nil {
		return nil
	}
	if value.Kind != yaml.ScalarNode || (value.Tag != "!!bool" && value.Tag != "!!null") {
		return fmt.Errorf("%s must be a boolean", field)
	}
	var enabled bool
	if err := value.Decode(&enabled); err != nil {
		return fmt.Errorf("%s must be a boolean", field)
	}
	return nil
}
