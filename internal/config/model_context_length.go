package config

import (
	"fmt"
	"math"
)

// Use a portable integer bound shared by Go targets and browser configuration.
const MaxModelContextLength = math.MaxInt32

func ValidateModelContextLength(value int64) error {
	if value < 0 || value > MaxModelContextLength {
		return fmt.Errorf("max-context-length must be an integer between 0 and %d", MaxModelContextLength)
	}
	return nil
}

func validateModelContextEntries[T interface{ GetMaxContextLength() int }](path string, models []T) error {
	for index, model := range models {
		if err := ValidateModelContextLength(int64(model.GetMaxContextLength())); err != nil {
			return fmt.Errorf("%s.models[%d]: %w", path, index, err)
		}
	}
	return nil
}

func (cfg *Config) ValidateModelContextLengths() error {
	if cfg == nil {
		return nil
	}
	for index, entry := range cfg.GeminiKey {
		if err := validateModelContextEntries(fmt.Sprintf("gemini-api-key[%d]", index), entry.Models); err != nil {
			return err
		}
	}
	for index, entry := range cfg.InteractionsKey {
		if err := validateModelContextEntries(fmt.Sprintf("interactions-api-key[%d]", index), entry.Models); err != nil {
			return err
		}
	}
	for index, entry := range cfg.ClaudeKey {
		if err := validateModelContextEntries(fmt.Sprintf("claude-api-key[%d]", index), entry.Models); err != nil {
			return err
		}
	}
	for index, entry := range cfg.CodexKey {
		if err := validateModelContextEntries(fmt.Sprintf("codex-api-key[%d]", index), entry.Models); err != nil {
			return err
		}
	}
	for index, entry := range cfg.XAIKey {
		if err := validateModelContextEntries(fmt.Sprintf("xai-api-key[%d]", index), entry.Models); err != nil {
			return err
		}
	}
	for index, entry := range cfg.VertexCompatAPIKey {
		if err := validateModelContextEntries(fmt.Sprintf("vertex-api-key[%d]", index), entry.Models); err != nil {
			return err
		}
	}
	for index, entry := range cfg.OpenAICompatibility {
		if err := validateModelContextEntries(fmt.Sprintf("openai-compatibility[%d]", index), entry.Models); err != nil {
			return err
		}
	}
	return nil
}
