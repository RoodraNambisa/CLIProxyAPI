package diff

import (
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestCodexInputTokenEstimateChangeDetails(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		old := &config.Config{Codex: config.CodexConfig{EstimateClaudeInputTokens: !enabled}}
		current := &config.Config{Codex: config.CodexConfig{EstimateClaudeInputTokens: enabled}}
		changes := BuildConfigChangeDetails(old, current)
		if len(changes) != 1 || !strings.HasPrefix(changes[0], "codex.estimate-claude-input-tokens:") {
			t.Fatalf("missing change detail: %v", changes)
		}
		if len(BuildConfigChangeDetails(current, current)) != 0 {
			t.Fatal("unchanged configuration reported")
		}
	}
}
