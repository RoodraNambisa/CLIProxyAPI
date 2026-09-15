package config

import (
	"fmt"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// UnmarshalYAML rejects coercions such as a numeric pattern or fractional status.
// Validation follows YAML aliases and merge precedence just like decoding.
func (rule *RequestScopedErrorRule) UnmarshalYAML(node *yaml.Node) error {
	for _, field := range []string{"status", "action", "match", "match-regexr"} {
		value, err := credentialYAMLField(node, field, make(map[*yaml.Node]bool))
		if err != nil {
			return err
		}
		if value == nil || value.Tag == "!!null" {
			continue
		}
		switch field {
		case "status":
			if value.Kind != yaml.ScalarNode || value.Tag != "!!int" {
				return fmt.Errorf("request-scoped-errors.status must be an integer")
			}
		case "action":
			if value.Kind != yaml.ScalarNode || value.Tag != "!!str" {
				return fmt.Errorf("request-scoped-errors.action must be a string")
			}
		default:
			if value.Kind != yaml.SequenceNode {
				return fmt.Errorf("request-scoped-errors.%s must be a string list", field)
			}
			for _, item := range value.Content {
				if item.Kind == yaml.AliasNode {
					item = item.Alias
				}
				if item == nil || item.Kind != yaml.ScalarNode || (item.Tag != "!!str" && item.Tag != "!!null") {
					return fmt.Errorf("request-scoped-errors.%s must contain strings", field)
				}
			}
		}
	}
	type plain RequestScopedErrorRule
	var decoded plain
	if err := node.Decode(&decoded); err != nil {
		return fmt.Errorf("request-scoped-errors contains an invalid field type or integer")
	}
	*rule = RequestScopedErrorRule(decoded)
	return nil
}

func validateRequestScopedErrorsYAML(data []byte) error {
	var document yaml.Node
	if err := yaml.Unmarshal(data, &document); err != nil || len(document.Content) == 0 {
		return nil
	}
	root := document.Content[0]
	providers, err := credentialYAMLField(root, "oauth-request-scoped-errors", make(map[*yaml.Node]bool))
	if err != nil {
		return err
	}
	if providers != nil {
		var rules map[string][]RequestScopedErrorRule
		if err := providers.Decode(&rules); err != nil {
			return fmt.Errorf("oauth-request-scoped-errors contains invalid rule types")
		}
	}
	for _, family := range []string{"gemini-api-key", "interactions-api-key", "claude-api-key", "codex-api-key", "xai-api-key", "vertex-api-key", "openai-compatibility"} {
		entries, err := credentialYAMLField(root, family, make(map[*yaml.Node]bool))
		if err != nil {
			return err
		}
		if entries == nil || entries.Kind != yaml.SequenceNode {
			continue
		}
		for index, entry := range entries.Content {
			value, err := credentialYAMLField(entry, "request-scoped-errors", make(map[*yaml.Node]bool))
			if err != nil {
				return err
			}
			if value == nil {
				continue
			}
			var rules []RequestScopedErrorRule
			if err := value.Decode(&rules); err != nil {
				return fmt.Errorf("%s[%d].request-scoped-errors contains invalid rule types", family, index)
			}
		}
	}
	return nil
}

// CompileOAuthRequestScopedErrors returns a detached provider map for one
// logical request policy. It never modifies the saved provider spelling.
func (cfg *Config) CompileOAuthRequestScopedErrors() (map[string]*CompiledRequestScopedErrors, error) {
	if cfg == nil || len(cfg.OAuthRequestScopedErrors) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(cfg.OAuthRequestScopedErrors))
	for key := range cfg.OAuthRequestScopedErrors {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make(map[string]*CompiledRequestScopedErrors, len(keys))
	seen := make(map[string]bool, len(keys))
	for index, key := range keys {
		provider := strings.ToLower(strings.TrimSpace(key))
		if provider == "" || seen[provider] {
			return nil, fmt.Errorf("oauth-request-scoped-errors provider entry %d is empty or duplicated", index)
		}
		seen[provider] = true
		rules, err := CompileRequestScopedErrors(cfg.OAuthRequestScopedErrors[key])
		if err != nil {
			return nil, fmt.Errorf("oauth provider entry %d: %w", index, err)
		}
		if rules != nil {
			result[provider] = rules
		}
	}
	return result, nil
}

func (cfg *Config) ValidateRequestScopedErrorRules() error {
	if cfg == nil {
		return nil
	}
	if _, err := cfg.CompileOAuthRequestScopedErrors(); err != nil {
		return err
	}
	families := []struct {
		name  string
		rules [][]RequestScopedErrorRule
	}{
		{name: "gemini-api-key"}, {name: "interactions-api-key"}, {name: "claude-api-key"},
		{name: "codex-api-key"}, {name: "vertex-api-key"}, {name: "openai-compatibility"}, {name: "xai-api-key"},
	}
	for _, entry := range cfg.GeminiKey {
		families[0].rules = append(families[0].rules, entry.RequestScopedErrors)
	}
	for _, entry := range cfg.InteractionsKey {
		families[1].rules = append(families[1].rules, entry.RequestScopedErrors)
	}
	for _, entry := range cfg.ClaudeKey {
		families[2].rules = append(families[2].rules, entry.RequestScopedErrors)
	}
	for _, entry := range cfg.CodexKey {
		families[3].rules = append(families[3].rules, entry.RequestScopedErrors)
	}
	for _, entry := range cfg.VertexCompatAPIKey {
		families[4].rules = append(families[4].rules, entry.RequestScopedErrors)
	}
	for _, entry := range cfg.OpenAICompatibility {
		families[5].rules = append(families[5].rules, entry.RequestScopedErrors)
	}
	for _, entry := range cfg.XAIKey {
		families[6].rules = append(families[6].rules, entry.RequestScopedErrors)
	}
	for _, family := range families {
		for index, rules := range family.rules {
			if _, err := CompileRequestScopedErrors(rules); err != nil {
				return fmt.Errorf("%s[%d]: %w", family.name, index, err)
			}
		}
	}
	return nil
}
