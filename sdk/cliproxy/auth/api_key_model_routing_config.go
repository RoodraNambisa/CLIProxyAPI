package auth

import "github.com/router-for-me/CLIProxyAPI/v6/internal/config"

// Copy only the credential matching and model fields used by alias resolution.
// Unrelated runtime configuration and live credential instances are not retained.
func copyAPIKeyModelRoutingConfig(source *config.Config) *config.Config {
	next := &config.Config{}
	if source == nil {
		return next
	}
	next.GeminiKey = copyGeminiModelRoutingKeys(source.GeminiKey)
	next.InteractionsKey = copyGeminiModelRoutingKeys(source.InteractionsKey)
	for _, key := range source.ClaudeKey {
		entry := config.ClaudeKey{APIManagement: key.APIManagement, APIKey: key.APIKey, BaseURL: key.BaseURL, Models: append([]config.ClaudeModel(nil), key.Models...)}
		for i := range entry.Models {
			entry.Models[i].Thinking = config.NormalizeModelThinkingSupport(entry.Models[i].Thinking)
		}
		next.ClaudeKey = append(next.ClaudeKey, entry)
	}
	for _, key := range source.CodexKey {
		entry := config.CodexKey{APIManagement: key.APIManagement, APIKey: key.APIKey, BaseURL: key.BaseURL, Models: append([]config.CodexModel(nil), key.Models...)}
		for i := range entry.Models {
			entry.Models[i].Thinking = config.NormalizeModelThinkingSupport(entry.Models[i].Thinking)
		}
		next.CodexKey = append(next.CodexKey, entry)
	}
	for _, key := range source.XAIKey {
		entry := config.XAIKey{APIManagement: key.APIManagement, APIKey: key.APIKey, BaseURL: key.BaseURL, Models: append([]config.CodexModel(nil), key.Models...)}
		for i := range entry.Models {
			entry.Models[i].Thinking = config.NormalizeModelThinkingSupport(entry.Models[i].Thinking)
			entry.Models[i].ImageCapabilities = entry.Models[i].ImageCapabilities.Clone()
		}
		next.XAIKey = append(next.XAIKey, entry)
	}
	for _, key := range source.VertexCompatAPIKey {
		entry := config.VertexCompatKey{APIManagement: key.APIManagement, APIKey: key.APIKey, BaseURL: key.BaseURL, Models: append([]config.VertexCompatModel(nil), key.Models...)}
		for i := range entry.Models {
			entry.Models[i].Thinking = config.NormalizeModelThinkingSupport(entry.Models[i].Thinking)
		}
		next.VertexCompatAPIKey = append(next.VertexCompatAPIKey, entry)
	}
	for _, provider := range source.OpenAICompatibility {
		entry := config.OpenAICompatibility{APIManagement: provider.APIManagement, Name: provider.Name, Disabled: provider.Disabled, Models: append([]config.OpenAICompatibilityModel(nil), provider.Models...)}
		for i := range entry.Models {
			entry.Models[i].Thinking = config.NormalizeModelThinkingSupport(entry.Models[i].Thinking)
			entry.Models[i].InputModalities = entry.Models[i].GetInputModalities()
		}
		for _, key := range provider.APIKeyEntries {
			copyKey := key
			if key.Models != nil {
				models := append([]config.OpenAICompatibilityModel(nil), (*key.Models)...)
				for i := range models {
					models[i].Thinking = config.NormalizeModelThinkingSupport(models[i].Thinking)
					models[i].InputModalities = models[i].GetInputModalities()
				}
				copyKey.Models = &models
			}
			entry.APIKeyEntries = append(entry.APIKeyEntries, copyKey)
		}
		next.OpenAICompatibility = append(next.OpenAICompatibility, entry)
	}
	next.IndexAPICredentials()
	return next
}

func copyGeminiModelRoutingKeys(source []config.GeminiKey) []config.GeminiKey {
	var entries []config.GeminiKey
	for _, key := range source {
		entry := config.GeminiKey{APIManagement: key.APIManagement, APIKey: key.APIKey, BaseURL: key.BaseURL, Models: append([]config.GeminiModel(nil), key.Models...)}
		for i := range entry.Models {
			entry.Models[i].Thinking = config.NormalizeModelThinkingSupport(entry.Models[i].Thinking)
		}
		entries = append(entries, entry)
	}
	return entries
}
