package diff

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestModelCompatChangeDetailsPreserveRoutingIdentity(t *testing.T) {
	var before, after config.Config
	for _, family := range []string{"gemini-api-key", "interactions-api-key", "claude-api-key", "codex-api-key", "vertex-api-key", "openai-compatibility"} {
		for _, entry := range []struct {
			cfg   *config.Config
			value string
		}{{&before, "false"}, {&after, "true"}} {
			if err := json.Unmarshal([]byte(`{"`+family+`":[{"api-key":"private-compat-key","name":"compat","models":[{"name":"upstream","alias":"local","is-compat":`+entry.value+`}]}]}`), entry.cfg); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, pair := range [][2]*config.Config{{&before, &after}, {&after, &before}} {
		details := strings.Join(BuildConfigChangeDetails(pair[0], pair[1]), "\n")
		for _, marker := range []string{"gemini[0].models", "interactions[0].models", "claude[0].models", "codex[0].models", "vertex[0].models", "models updated"} {
			if !strings.Contains(details, marker) {
				t.Errorf("missing compatibility change marker %s", marker)
			}
		}
		if strings.Contains(details, "private-compat-key") {
			t.Fatal("compatibility change exposed credentials")
		}
	}
	if ComputeGeminiModelsHash(before.GeminiKey[0].Models) != ComputeGeminiModelsHash(after.GeminiKey[0].Models) ||
		ComputeClaudeModelsHash(before.ClaudeKey[0].Models) != ComputeClaudeModelsHash(after.ClaudeKey[0].Models) ||
		ComputeCodexModelsHash(before.CodexKey[0].Models) != ComputeCodexModelsHash(after.CodexKey[0].Models) ||
		ComputeVertexCompatModelsHash(before.VertexCompatAPIKey[0].Models) != ComputeVertexCompatModelsHash(after.VertexCompatAPIKey[0].Models) ||
		ComputeOpenAICompatModelsHash(before.OpenAICompatibility[0].Models) != ComputeOpenAICompatModelsHash(after.OpenAICompatibility[0].Models) {
		t.Fatal("compatibility edit changed credential routing identity")
	}
}
