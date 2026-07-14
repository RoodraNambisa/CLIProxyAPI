package config

import (
	"fmt"
	"math"
	"strings"
)

// UsagePricingConfig contains shared model prices used by usage cost reports.
type UsagePricingConfig struct {
	Models map[string]UsageModelPrice `yaml:"models" json:"models"`
}

// UsageModelPrice stores USD prices per one million tokens.
type UsageModelPrice struct {
	InputPerMillion       float64 `yaml:"input-per-million" json:"input-per-million"`
	OutputPerMillion      float64 `yaml:"output-per-million" json:"output-per-million"`
	CachedInputPerMillion float64 `yaml:"cached-input-per-million" json:"cached-input-per-million"`
}

// NormalizeUsagePricing validates model prices and canonicalizes model names.
func NormalizeUsagePricing(input UsagePricingConfig) (UsagePricingConfig, error) {
	output := UsagePricingConfig{Models: make(map[string]UsageModelPrice, len(input.Models))}
	for rawModel, price := range input.Models {
		model := strings.ToLower(strings.TrimSpace(rawModel))
		if model == "" {
			return UsagePricingConfig{}, fmt.Errorf("invalid usage-pricing model: model name is empty")
		}
		if _, exists := output.Models[model]; exists {
			return UsagePricingConfig{}, fmt.Errorf("invalid usage-pricing model %q: duplicate normalized model", model)
		}
		if err := validateUsageModelPrice(price); err != nil {
			return UsagePricingConfig{}, fmt.Errorf("invalid usage-pricing model %q: %w", model, err)
		}
		output.Models[model] = price
	}
	return output, nil
}

func validateUsageModelPrice(price UsageModelPrice) error {
	fields := []struct {
		name  string
		value float64
	}{
		{name: "input-per-million", value: price.InputPerMillion},
		{name: "output-per-million", value: price.OutputPerMillion},
		{name: "cached-input-per-million", value: price.CachedInputPerMillion},
	}
	for _, field := range fields {
		if field.value < 0 || math.IsNaN(field.value) || math.IsInf(field.value, 0) {
			return fmt.Errorf("%s must be a non-negative finite number", field.name)
		}
	}
	return nil
}
