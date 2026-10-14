package config

import (
	"fmt"
	"strings"
)

// XAIKey shares the native key model and routing fields with Codex keys.
type XAIKey = CodexKey

type XAIImageCapabilities struct {
	MaxEditImages *int      `yaml:"max-edit-images,omitempty" json:"max-edit-images,omitempty"`
	QualityValues *[]string `yaml:"quality-values,omitempty" json:"quality-values,omitempty"`
	FileIDInputs  *bool     `yaml:"file-id-inputs,omitempty" json:"file-id-inputs,omitempty"`
}

func (c *XAIImageCapabilities) Clone() *XAIImageCapabilities {
	if c == nil {
		return nil
	}
	out := *c
	if c.MaxEditImages != nil {
		value := *c.MaxEditImages
		out.MaxEditImages = &value
	}
	if c.FileIDInputs != nil {
		value := *c.FileIDInputs
		out.FileIDInputs = &value
	}
	if c.QualityValues != nil {
		value := append([]string{}, (*c.QualityValues)...)
		out.QualityValues = &value
	}
	return &out
}

func (cfg *Config) ValidateXAIKeys() error {
	if cfg == nil {
		return nil
	}
	for index, key := range cfg.XAIKey {
		if _, errURL := NormalizeXAIBaseURL(key.BaseURL); errURL != nil {
			return fmt.Errorf("xai-api-key[%d].base-url: %w", index, errURL)
		}
		for modelIndex, model := range key.Models {
			if c := model.ImageCapabilities; c != nil {
				if c.MaxEditImages != nil && (*c.MaxEditImages < 1 || *c.MaxEditImages > 64) {
					return fmt.Errorf("xai-api-key[%d].models[%d]: max-edit-images must be 1–64", index, modelIndex)
				}
				if c.QualityValues != nil {
					if len(*c.QualityValues) > 32 {
						return fmt.Errorf("too many Grok quality values")
					}
					for _, value := range *c.QualityValues {
						if len(value) > 64 || strings.ContainsAny(value, "\r\n\x00") {
							return fmt.Errorf("invalid Grok quality value")
						}
					}
				}
			}
		}
	}
	return nil
}

func (cfg *Config) SanitizeXAIKeys() {
	if cfg == nil {
		return
	}
	values := append([]CodexKey(nil), cfg.XAIKey...)
	for i := range values {
		values[i].APIKey = strings.TrimSpace(values[i].APIKey)
		if strings.TrimSpace(values[i].BaseURL) == "" {
			values[i].BaseURL, _ = XAIBaseURLForMode("api")
		}
		if canonical, errURL := NormalizeXAIBaseURL(values[i].BaseURL); errURL == nil {
			values[i].BaseURL = canonical
		}
		values[i].AlphaSearch = false
	}
	temporary := Config{CodexKey: values}
	temporary.SanitizeCodexKeys()
	cfg.XAIKey = temporary.CodexKey
}
