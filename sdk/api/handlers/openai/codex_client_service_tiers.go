package openai

import "github.com/router-for-me/CLIProxyAPI/v6/internal/config"

// applyCodexClientServiceTiers projects the same client policy used at execution.
// The caller supplies an independently owned catalog entry.
func applyCodexClientServiceTiers(entry map[string]any, cfg *config.SDKConfig) {
	if cfg == nil || !cfg.CodexAllowClientServiceTier {
		clearCodexClientServiceTiers(entry)
		return
	}
	rule := config.CodexServiceTierRuleForModel(cfg.CodexServiceTierRules, stringModelValue(entry, "slug"))
	if rule == nil {
		return
	}
	previous, _ := entry["service_tiers"].([]any)
	clearCodexClientServiceTiers(entry)
	tiers := make([]any, 0, len(rule.Tiers))
	legacy := make([]any, 0, 1)
	for _, id := range rule.Tiers {
		name, description := codexServiceTierLabel(id)
		item := map[string]any{"id": id, "name": name, "description": description}
		for _, raw := range previous {
			if old, ok := raw.(map[string]any); ok && config.NormalizeCodexServiceTier(stringModelValue(old, "id")) == id {
				item = cloneCodexClientModelMap(old)
				item["id"] = id
				break
			}
		}
		tiers = append(tiers, item)
		if id == "priority" {
			legacy = append(legacy, "fast")
		}
	}
	entry["service_tiers"] = tiers
	entry["additional_speed_tiers"] = legacy
	if rule.Default != "" && rule.Default != "default" {
		entry["default_service_tier"] = rule.Default
	}
}

func clearCodexClientServiceTiers(entry map[string]any) {
	entry["service_tiers"] = []any{}
	entry["additional_speed_tiers"] = []any{}
	entry["default_service_tier"] = nil
}

func codexServiceTierLabel(id string) (string, string) {
	switch id {
	case "priority":
		return "Fast", "Faster processing with increased usage; upstream availability applies"
	case "ultrafast":
		return "Ultrafast", "Highest speed with increased usage; requires upstream model and account access"
	case "flex":
		return "Flex", "Lower cost with slower processing; upstream availability applies"
	default:
		return id, id
	}
}
