package diff

import (
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestCredentialWeightChangesAreVisibleWithoutKeyMaterial(t *testing.T) {
	zero := 0
	before := &config.Config{CodexKey: []config.CodexKey{{APIKey: "local-key-marker"}}, OpenAICompatibility: []config.OpenAICompatibility{{Name: "test", APIKeyEntries: []config.OpenAICompatibilityAPIKey{{APIKey: "local-key-marker"}}}}}
	after := &config.Config{CodexKey: []config.CodexKey{{APIKey: "local-key-marker", Weight: &zero}}, OpenAICompatibility: []config.OpenAICompatibility{{Name: "test", APIKeyEntries: []config.OpenAICompatibilityAPIKey{{APIKey: "local-key-marker", Weight: &zero}}}}}
	details := strings.Join(BuildConfigChangeDetails(before, after), "\n")
	if !strings.Contains(details, "codex[0].weight: updated") || !strings.Contains(details, "weights updated") || strings.Contains(details, "local-key-marker") {
		t.Fatal("weight changes are invisible or expose credential material")
	}
}
