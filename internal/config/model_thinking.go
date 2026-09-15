package config

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"gopkg.in/yaml.v3"
)

const MaxModelThinkingBudget = math.MaxInt32

// ModelThinkingSignature compares effective declarations without exposing their
// contents in change logs. A missing declaration remains distinct from an object.
func ModelThinkingSignature(raw *registry.ThinkingSupport) string {
	if raw == nil {
		return ""
	}
	encoded, _ := json.Marshal(NormalizeModelThinkingSupport(raw))
	return string(encoded)
}

// NormalizeModelThinkingSupport creates a private capability snapshot. Preserve
// level order because it may determine the provider's lowest allowed effort.
func NormalizeModelThinkingSupport(raw *registry.ThinkingSupport) *registry.ThinkingSupport {
	if raw == nil {
		return nil
	}
	normalized := *raw
	normalized.Levels = nil
	seen := make(map[string]bool, len(raw.Levels))
	for _, value := range raw.Levels {
		level := strings.ToLower(strings.TrimSpace(value))
		if level == "" || seen[level] {
			continue
		}
		seen[level] = true
		normalized.Levels = append(normalized.Levels, level)
		if level == "none" {
			normalized.ZeroAllowed = true
		}
		if level == "auto" {
			normalized.DynamicAllowed = true
		}
	}
	return &normalized
}

// ValidateModelThinkingSupport validates declarations without changing saved
// values. Level normalization belongs to the immutable execution snapshot.
func ValidateModelThinkingSupport(support *registry.ThinkingSupport) error {
	if support == nil {
		return nil
	}
	if support.Min < 0 || support.Max < 0 || support.Min > MaxModelThinkingBudget || support.Max > MaxModelThinkingBudget || support.Min > support.Max {
		return fmt.Errorf("thinking budgets must satisfy 0 <= min <= max <= %d", MaxModelThinkingBudget)
	}
	for _, level := range support.Levels {
		switch strings.ToLower(strings.TrimSpace(level)) {
		case "none", "minimal", "low", "medium", "high", "xhigh", "max", "auto":
		default:
			return fmt.Errorf("thinking.levels contains an unsupported reasoning level")
		}
	}
	return nil
}

func validateModelThinkingEntries[T interface {
	GetThinking() *registry.ThinkingSupport
}](path string, models []T) error {
	for i, model := range models {
		if err := ValidateModelThinkingSupport(model.GetThinking()); err != nil {
			return fmt.Errorf("%s.models[%d]: %w", path, i, err)
		}
	}
	return nil
}

func (cfg *Config) ValidateModelThinking() error {
	if cfg == nil {
		return nil
	}
	for i, entry := range cfg.GeminiKey {
		if err := validateModelThinkingEntries(fmt.Sprintf("gemini-api-key[%d]", i), entry.Models); err != nil {
			return err
		}
	}
	for i, entry := range cfg.InteractionsKey {
		if err := validateModelThinkingEntries(fmt.Sprintf("interactions-api-key[%d]", i), entry.Models); err != nil {
			return err
		}
	}
	for i, entry := range cfg.ClaudeKey {
		if err := validateModelThinkingEntries(fmt.Sprintf("claude-api-key[%d]", i), entry.Models); err != nil {
			return err
		}
	}
	for i, entry := range cfg.CodexKey {
		if err := validateModelThinkingEntries(fmt.Sprintf("codex-api-key[%d]", i), entry.Models); err != nil {
			return err
		}
	}
	for i, entry := range cfg.XAIKey {
		if err := validateModelThinkingEntries(fmt.Sprintf("xai-api-key[%d]", i), entry.Models); err != nil {
			return err
		}
	}
	for i, entry := range cfg.VertexCompatAPIKey {
		if err := validateModelThinkingEntries(fmt.Sprintf("vertex-api-key[%d]", i), entry.Models); err != nil {
			return err
		}
	}
	for i, entry := range cfg.OpenAICompatibility {
		if err := validateModelThinkingEntries(fmt.Sprintf("openai-compatibility[%d]", i), entry.Models); err != nil {
			return err
		}
	}
	return nil
}

func validateModelThinkingYAML(node *yaml.Node) error {
	seen := make(map[*yaml.Node]bool)
	resolve := func(node *yaml.Node) (*yaml.Node, error) {
		clear(seen)
		for node != nil && node.Kind == yaml.AliasNode {
			if seen[node] {
				return nil, fmt.Errorf("thinking contains a cyclic YAML alias")
			}
			seen[node] = true
			node = node.Alias
		}
		return node, nil
	}
	node, err := resolve(node)
	if err != nil {
		return err
	}
	if node == nil || node.Tag == "!!null" {
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("thinking must be an object")
	}
	for _, field := range []string{"min", "max", "zero-allowed", "dynamic-allowed", "levels"} {
		value, err := credentialYAMLField(node, field, make(map[*yaml.Node]bool))
		if err != nil {
			return err
		}
		value, err = resolve(value)
		if err != nil {
			return err
		}
		if value == nil || value.Tag == "!!null" {
			continue
		}
		switch field {
		case "min", "max":
			if value.Kind != yaml.ScalarNode || value.Tag != "!!int" {
				return fmt.Errorf("thinking.%s must be an integer", field)
			}
		case "zero-allowed", "dynamic-allowed":
			if value.Kind != yaml.ScalarNode || value.Tag != "!!bool" {
				return fmt.Errorf("thinking.%s must be a boolean", field)
			}
		case "levels":
			if value.Kind != yaml.SequenceNode {
				return fmt.Errorf("thinking.levels must be a list")
			}
			for _, level := range value.Content {
				level, err = resolve(level)
				if err != nil {
					return err
				}
				if level == nil || level.Kind != yaml.ScalarNode || level.Tag != "!!str" {
					return fmt.Errorf("thinking.levels must contain strings")
				}
			}
		}
	}
	var support registry.ThinkingSupport
	if err := node.Decode(&support); err != nil {
		return fmt.Errorf("thinking contains invalid capability values")
	}
	return ValidateModelThinkingSupport(&support)
}
