package auth

import "github.com/router-for-me/CLIProxyAPI/v6/internal/config"

func resolveOpenAICompatAuthConfig(cfg *config.Config, auth *Auth, providerKey, compatName, authProvider string) *config.OpenAICompatibility {
	if auth != nil {
		if ref, ok := cfg.APICredentialByID(auth.Attributes["config_id"]); ok && ref.Parent != nil {
			if key, ok := ref.Value.(*config.OpenAICompatibilityAPIKey); ok {
				return config.EffectiveOpenAIEntry(ref.Parent, key)
			}
			return ref.Parent
		}
	}
	parent := resolveOpenAICompatConfig(cfg, providerKey, compatName, authProvider)
	if auth == nil {
		return parent
	}
	return config.EffectiveOpenAIKey(parent, auth.Attributes["config_id"], auth.Attributes["api_key"])
}
