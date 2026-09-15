package config

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// MaxCredentialRequestRetry keeps configuration counts portable across Go
// architectures and exactly representable by management clients.
const MaxCredentialRequestRetry = 2147483647

func ValidateCredentialRequestRetry(value *int) error {
	if value != nil && *value > MaxCredentialRequestRetry {
		return fmt.Errorf("request-retry must not exceed %d", MaxCredentialRequestRetry)
	}
	return nil
}

func (cfg *Config) ValidateCredentialRequestRetries() error {
	if cfg == nil {
		return nil
	}
	check := func(family string, index int, value *int) error {
		if err := ValidateCredentialRequestRetry(value); err != nil {
			return fmt.Errorf("%s[%d].%w", family, index, err)
		}
		return nil
	}
	for index, entry := range cfg.GeminiKey {
		if err := check("gemini-api-key", index, entry.RequestRetry); err != nil {
			return err
		}
	}
	for index, entry := range cfg.InteractionsKey {
		if err := check("interactions-api-key", index, entry.RequestRetry); err != nil {
			return err
		}
	}
	for index, entry := range cfg.ClaudeKey {
		if err := check("claude-api-key", index, entry.RequestRetry); err != nil {
			return err
		}
	}
	for index, entry := range cfg.CodexKey {
		if err := check("codex-api-key", index, entry.RequestRetry); err != nil {
			return err
		}
	}
	for index, entry := range cfg.XAIKey {
		if err := check("xai-api-key", index, entry.RequestRetry); err != nil {
			return err
		}
	}
	for index, entry := range cfg.VertexCompatAPIKey {
		if err := check("vertex-api-key", index, entry.RequestRetry); err != nil {
			return err
		}
	}
	for index, entry := range cfg.OpenAICompatibility {
		if err := check("openai-compatibility", index, entry.RequestRetry); err != nil {
			return err
		}
	}
	return nil
}

// Validate scalar types before yaml.v3 can coerce fractions into integers.
func validateCredentialRequestRetryYAML(data []byte) error {
	var document yaml.Node
	if err := yaml.Unmarshal(data, &document); err != nil || len(document.Content) == 0 {
		return nil
	}
	for _, family := range []string{"gemini-api-key", "interactions-api-key", "claude-api-key", "codex-api-key", "xai-api-key", "vertex-api-key", "openai-compatibility"} {
		sequence, err := credentialYAMLField(document.Content[0], family, make(map[*yaml.Node]bool))
		if err != nil {
			return err
		}
		if sequence == nil || sequence.Kind != yaml.SequenceNode {
			continue
		}
		for index, entry := range sequence.Content {
			value, err := credentialYAMLField(entry, "request-retry", make(map[*yaml.Node]bool))
			if err != nil {
				return err
			}
			if value == nil || (value.Kind == yaml.ScalarNode && value.Tag == "!!null") {
				continue
			}
			if value.Kind != yaml.ScalarNode || value.Tag != "!!int" {
				return fmt.Errorf("%s[%d].request-retry must be an integer or null", family, index)
			}
			var count int
			if err := value.Decode(&count); err != nil {
				return fmt.Errorf("%s[%d].request-retry exceeds the signed integer range", family, index)
			}
			if err := ValidateCredentialRequestRetry(&count); err != nil {
				return fmt.Errorf("%s[%d].%w", family, index, err)
			}
		}
	}
	return nil
}
