package config

import (
	"fmt"
	"strings"
)

const ChatGPTWebImageUsageByModel = "by-model"

func ChatGPTWebImageUsageProfiles() []string {
	return []string{"gpt-image-2", "gpt-image-2.5", "gpt-image-2.6"}
}

func validChatGPTWebImageUsageProfile(profile string) bool {
	return profile == "gpt-image-2" || profile == "gpt-image-2.5" || profile == "gpt-image-2.6"
}

func DefaultChatGPTWebImageUsageProfile(model string) string {
	switch strings.ToLower(strings.TrimSpace(model)) {
	case "gpt-image-2.5", "gpt-image-2.5-flare", "gpt-image-2.5-sunburst":
		return "gpt-image-2.5"
	case "gpt-image-2.6", "gpt-image-2.6-flare", "gpt-image-2.6-sunburst":
		return "gpt-image-2.6"
	default:
		return "gpt-image-2"
	}
}

func (cfg ChatGPTWebImageUsageConfig) ResolvedEstimationProfile() string {
	if profile := strings.ToLower(strings.TrimSpace(cfg.EstimationProfile)); profile != "" {
		return profile
	}
	return ChatGPTWebImageUsageByModel
}

// NormalizedModelProfiles returns an independent map, rejecting ambiguous keys.
func (cfg ChatGPTWebImageUsageConfig) NormalizedModelProfiles() (map[string]string, error) {
	if cfg.ModelProfiles == nil {
		return nil, nil
	}
	result := make(map[string]string, len(cfg.ModelProfiles))
	for model, value := range cfg.ModelProfiles {
		key := strings.ToLower(strings.TrimSpace(model))
		profile := strings.ToLower(strings.TrimSpace(value))
		if key == "" || !validChatGPTWebImageUsageProfile(profile) {
			return nil, fmt.Errorf("chatgpt-web.image-usage.model-profiles: invalid alias or profile for %q", model)
		}
		if _, exists := result[key]; exists {
			return nil, fmt.Errorf("chatgpt-web.image-usage.model-profiles: duplicate normalized alias %q", model)
		}
		result[key] = profile
	}
	return result, nil
}

func (cfg ChatGPTWebImageUsageConfig) ResolvedModelProfiles() map[string]string {
	result, _ := cfg.NormalizedModelProfiles()
	return result
}

func (cfg ChatGPTWebImageUsageConfig) ProfileForModel(model string) string {
	if profile := cfg.ResolvedEstimationProfile(); validChatGPTWebImageUsageProfile(profile) {
		return profile
	}
	key := strings.ToLower(strings.TrimSpace(model))
	// Request snapshots already contain normalized keys; avoid copying per output.
	if profile := cfg.ModelProfiles[key]; validChatGPTWebImageUsageProfile(profile) {
		return profile
	}
	return DefaultChatGPTWebImageUsageProfile(key)
}

func (cfg ChatGPTWebImageUsageConfig) ValidateModelProfiles(images ImagesConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	profiles, err := cfg.NormalizedModelProfiles()
	if err != nil {
		return err
	}
	aliases := make(map[string]bool)
	for _, alias := range images.ResolvedChatGPTWebImageModels() {
		aliases[strings.ToLower(alias)] = true
	}
	for alias := range profiles {
		if !aliases[alias] {
			return fmt.Errorf("chatgpt-web.image-usage.model-profiles: unknown Web image alias %q", alias)
		}
	}
	return nil
}

func (cfg ChatGPTWebImageUsageConfig) EffectiveModelProfiles(images ImagesConfig) map[string]string {
	cfg.ModelProfiles = cfg.ResolvedModelProfiles()
	result := make(map[string]string)
	for _, model := range images.ResolvedChatGPTWebImageModels() {
		result[model] = cfg.ProfileForModel(model)
	}
	return result
}
