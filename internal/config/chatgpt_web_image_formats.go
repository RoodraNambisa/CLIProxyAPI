package config

import (
	"fmt"
	"slices"
	"strings"
)

func ChatGPTWebImageOutputFormats() []string { return []string{"png", "jpeg", "webp", "gif"} }

func DefaultChatGPTWebImageOutputFormats(model string) []string {
	formats := []string{"png", "jpeg", "webp"}
	switch strings.ToLower(strings.TrimSpace(model)) {
	case "gpt-image-2.6", "gpt-image-2.6-flare", "gpt-image-2.6-sunburst":
		formats = append(formats, "gif")
	}
	return formats
}

// NormalizedChatGPTWebModelOutputFormats validates without mutating the caller.
func (c ImagesConfig) NormalizedChatGPTWebModelOutputFormats() (map[string][]string, error) {
	if c.ChatGPTWeb.ModelOutputFormats == nil {
		return nil, nil
	}
	aliases := make(map[string]bool)
	for _, model := range c.ResolvedChatGPTWebImageModels() {
		aliases[strings.ToLower(model)] = true
	}
	result := make(map[string][]string, len(c.ChatGPTWeb.ModelOutputFormats))
	for model, formats := range c.ChatGPTWeb.ModelOutputFormats {
		key := strings.ToLower(strings.TrimSpace(model))
		if !aliases[key] {
			return nil, fmt.Errorf("images.chatgpt-web.model-output-formats: unknown Web image alias %q", model)
		}
		if _, exists := result[key]; exists {
			return nil, fmt.Errorf("images.chatgpt-web.model-output-formats: duplicate normalized alias %q", model)
		}
		if len(formats) == 0 {
			return nil, fmt.Errorf("images.chatgpt-web.model-output-formats.%s must contain at least one format", model)
		}
		allowed := make(map[string]bool)
		for _, format := range formats {
			format = strings.ToLower(strings.TrimSpace(format))
			if format == "jpg" {
				format = "jpeg"
			}
			if !slices.Contains(ChatGPTWebImageOutputFormats(), format) {
				return nil, fmt.Errorf("images.chatgpt-web.model-output-formats.%s: unsupported format %q", model, format)
			}
			allowed[format] = true
		}
		for _, format := range ChatGPTWebImageOutputFormats() {
			if allowed[format] {
				result[key] = append(result[key], format)
			}
		}
	}
	return result, nil
}

// ResolvedChatGPTWebModelOutputFormats returns an independent request snapshot.
func (c ImagesConfig) ResolvedChatGPTWebModelOutputFormats() map[string][]string {
	result := make(map[string][]string)
	for _, model := range c.ResolvedChatGPTWebImageModels() {
		result[strings.ToLower(model)] = DefaultChatGPTWebImageOutputFormats(model)
	}
	for model, formats := range c.ChatGPTWeb.ModelOutputFormats {
		key := strings.ToLower(strings.TrimSpace(model))
		if _, exists := result[key]; !exists {
			continue
		}
		result[key] = nil
		for _, f := range formats {
			f = strings.ToLower(strings.TrimSpace(f))
			if f == "jpg" {
				f = "jpeg"
			}
			if slices.Contains(ChatGPTWebImageOutputFormats(), f) && !slices.Contains(result[key], f) {
				result[key] = append(result[key], f)
			}
		}
	}
	return result
}

func (c ImagesConfig) ResolvedChatGPTWebDefaultImageModel() string {
	models := c.ResolvedChatGPTWebImageModels()
	for _, m := range models {
		if strings.EqualFold(m, c.ResolvedImageModel()) {
			return m
		}
	}
	return models[0]
}
