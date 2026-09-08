package thinking

import (
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
)

func configuredThinkingCrossesFamily(from, to string, model *registry.ModelInfo) bool {
	if from != to {
		return true
	}
	if model == nil {
		return false
	}
	family := strings.ToLower(strings.TrimSpace(model.Type))
	return family != "" && !isSameProviderFamily(to, family)
}

// Cross-family maximum intent uses the nearest declared spelling. Native
// requests still receive the existing strict unsupported-level validation.
func configuredThinkingHighIntent(level ThinkingLevel, support *registry.ThinkingSupport) ThinkingLevel {
	if support == nil || len(support.Levels) == 0 {
		return level
	}
	var candidates []ThinkingLevel
	switch ThinkingLevel(strings.ToLower(strings.TrimSpace(string(level)))) {
	case LevelXHigh:
		candidates = []ThinkingLevel{LevelXHigh, LevelMax, LevelHigh}
	case LevelMax:
		candidates = []ThinkingLevel{LevelMax, LevelXHigh, LevelHigh}
	default:
		return level
	}
	for _, candidate := range candidates {
		if isLevelSupported(string(candidate), support.Levels) {
			return candidate
		}
	}
	return level
}
