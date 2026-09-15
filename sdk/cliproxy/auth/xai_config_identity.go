package auth

import "strings"

// Config synthesis has no credential JSON to carry runtime-prepared identities.
// Preserve only the seed, and only while the key and endpoint remain the same.
func carryForwardXAIConfigIdentity(previous, next *Auth) {
	if previous == nil || next == nil || previous.ID != next.ID || previous.Provider != "xai" || next.Provider != "xai" ||
		previous.Attributes["runtime_only"] != "true" || next.Attributes["runtime_only"] != "true" ||
		!strings.HasPrefix(next.Attributes["source"], "config:xai[") ||
		previous.Attributes["api_key"] == "" || previous.Attributes["api_key"] != next.Attributes["api_key"] ||
		previous.Attributes["base_url"] != next.Attributes["base_url"] {
		return
	}
	if _, exists := next.Metadata["xai_identity_seed"]; exists {
		return
	}
	seed, ok := previous.Metadata["xai_identity_seed"].(string)
	if !ok || seed == "" {
		return
	}
	if next.Metadata == nil {
		next.Metadata = make(map[string]any)
	}
	next.Metadata["xai_identity_seed"] = seed
}
