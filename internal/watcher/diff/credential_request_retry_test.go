package diff

import (
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestCredentialRequestRetryChangesAreVisibleWithoutKeyMaterial(t *testing.T) {
	before := &config.Config{
		GeminiKey: []config.GeminiKey{{APIKey: "key-material-marker"}}, InteractionsKey: []config.GeminiKey{{APIKey: "key-material-marker"}},
		ClaudeKey: []config.ClaudeKey{{APIKey: "key-material-marker"}}, CodexKey: []config.CodexKey{{APIKey: "key-material-marker"}},
		VertexCompatAPIKey:  []config.VertexCompatKey{{APIKey: "key-material-marker"}},
		OpenAICompatibility: []config.OpenAICompatibility{{Name: "compat", APIKeyEntries: []config.OpenAICompatibilityAPIKey{{APIKey: "key-material-marker"}}}},
	}
	after, err := config.Clone(before)
	if err != nil {
		t.Fatal(err)
	}
	zero := 0
	after.GeminiKey[0].RequestRetry, after.InteractionsKey[0].RequestRetry, after.ClaudeKey[0].RequestRetry = &zero, &zero, &zero
	after.CodexKey[0].RequestRetry, after.VertexCompatAPIKey[0].RequestRetry, after.OpenAICompatibility[0].RequestRetry = &zero, &zero, &zero
	for _, pair := range [][2]*config.Config{{before, after}, {after, before}} {
		details := strings.Join(BuildConfigChangeDetails(pair[0], pair[1]), "\n")
		if strings.Contains(details, "key-material-marker") {
			t.Fatal("retry change logs expose credential material")
		}
		for _, marker := range []string{"gemini[0].request-retry", "interactions[0].request-retry", "claude[0].request-retry", "codex[0].request-retry", "vertex[0].request-retry", "request-retry updated"} {
			if !strings.Contains(details, marker) {
				t.Errorf("missing retry change marker: %s", marker)
			}
		}
	}
	if details := BuildConfigChangeDetails(after, after); len(details) != 0 {
		t.Fatal("unchanged retry overrides produced configuration diffs")
	}
}
