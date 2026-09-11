package config

import "strings"

// DefaultCodexImageModels follows the upstream Codex image model catalog.
func DefaultCodexImageModels() []string {
	return []string{"gpt-image-2", "gpt-image-1.5", "gpt-image-2.5", "gpt-image-2.5-flare", "gpt-image-2.5-sunburst"}
}

// ResolvedImageModel is the default for callers that omit the image model.
func (c ImagesConfig) ResolvedImageModel() string {
	if model := strings.TrimSpace(c.ImageModel); model != "" {
		return model
	}
	return "gpt-image-2"
}

// ResolvedImageModels includes the default so legacy single-model configuration
// and requests without an explicit model remain routable.
func (c ImagesConfig) ResolvedImageModels() []string {
	models := normalizeImageModelIDs(c.ImageModels)
	if len(models) == 0 {
		if model := c.ResolvedImageModel(); model != "gpt-image-2" {
			return []string{model}
		}
		models = []string{"gpt-image-2", "gpt-image-2.5", "gpt-image-2.5-flare", "gpt-image-2.5-sunburst"}
	}
	return normalizeImageModelIDs(append([]string{c.ResolvedImageModel()}, models...))
}

// ResolvedChatGPTWebImageModels keeps legacy aliases only when the independent
// Web list is not configured. These names never select an upstream image engine.
func (c ImagesConfig) ResolvedChatGPTWebImageModels() []string {
	if models := normalizeImageModelIDs(c.ChatGPTWeb.ImageModels); len(models) > 0 {
		return models
	}
	if model := c.ResolvedImageModel(); model != "gpt-image-2" {
		return []string{model}
	}
	return normalizeImageModelIDs([]string{
		c.ResolvedImageModel(), "gpt-image-2", "gpt-image-2.5", "gpt-image-2.5-flare", "gpt-image-2.5-sunburst",
	})
}

func normalizeImageModelIDs(models []string) []string {
	var result []string
	seen := make(map[string]struct{}, len(models))
	for _, model := range models {
		model = strings.TrimSpace(model)
		key := strings.ToLower(model)
		if key == "" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, model)
	}
	return result
}
