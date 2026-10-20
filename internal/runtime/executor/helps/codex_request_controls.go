package helps

import (
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// ApplyCodexPayloadConfig gates client tier selection before administrator rules.
// A rejected client tier must not suppress an administrator's configured default.
func ApplyCodexPayloadConfig(cfg *config.Config, allowClientServiceTier bool, model, protocol string, payload, original []byte, requestedModel string) []byte {
	if !allowClientServiceTier {
		payload = stripCodexClientServiceTier(payload)
		original = stripCodexClientServiceTier(original)
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
