package helps

import (
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// ApplyCodexPayloadConfig gates client tier selection before administrator rules.
// A rejected client tier must not suppress an administrator's configured default.
func ApplyCodexPayloadConfig(cfg *config.Config, allowClientServiceTier bool, model, protocol string, payload, original []byte, requestedModel string, rules ...*config.CodexServiceTierRule) []byte {
	if !allowClientServiceTier {
		payload = stripCodexClientServiceTier(payload)
		original = stripCodexClientServiceTier(original)
	} else if len(rules) > 0 && rules[0] != nil {
		payload = filterCodexClientServiceTier(payload, rules[0])
		original = filterCodexClientServiceTier(original, rules[0])
	}
	return ApplyPayloadConfigWithRoot(cfg, model, protocol, "", payload, original, requestedModel)
}

func stripCodexClientServiceTier(payload []byte) []byte {
	// Remove every occurrence so duplicate JSON keys cannot bypass the policy.
	for gjson.GetBytes(payload, "service_tier").Exists() {
		next, err := sjson.DeleteBytes(payload, "service_tier")
		if err != nil || len(next) >= len(payload) {
			break
		}
		payload = next
	}
	return payload
}

// Duplicate selections are treated as invalid rather than allowing parser disagreement.
func filterCodexClientServiceTier(payload []byte, rule *config.CodexServiceTierRule) []byte {
	count := 0
	var tier gjson.Result
	gjson.ParseBytes(payload).ForEach(func(key, value gjson.Result) bool {
		if key.String() == "service_tier" {
			count++
			tier = value
		}
		return true
	})
	if count == 0 {
		return payload
	}
	if count != 1 || tier.Type != gjson.String || !rule.AllowsClientTier(tier.String()) {
		return stripCodexClientServiceTier(payload)
	}
	normalized := config.NormalizeCodexServiceTier(tier.String())
	if normalized != tier.String() {
		payload, _ = sjson.SetBytes(payload, "service_tier", normalized)
	}
	return payload
}
