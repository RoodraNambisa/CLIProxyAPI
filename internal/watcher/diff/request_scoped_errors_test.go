package diff

import (
	"reflect"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestRequestScopedErrorChangesDoNotExposePatternsOrKeys(t *testing.T) {
	before := &config.Config{
		GeminiKey: []config.GeminiKey{{APIKey: "private-key"}}, InteractionsKey: []config.GeminiKey{{APIKey: "private-key"}},
		ClaudeKey: []config.ClaudeKey{{APIKey: "private-key"}}, CodexKey: []config.CodexKey{{APIKey: "private-key"}},
		VertexCompatAPIKey:  []config.VertexCompatKey{{APIKey: "private-key"}},
		OpenAICompatibility: []config.OpenAICompatibility{{Name: "compat", APIKeyEntries: []config.OpenAICompatibilityAPIKey{{APIKey: "private-key"}}}},
	}
	after, err := config.Clone(before)
	if err != nil {
		t.Fatal(err)
	}
	rules := []config.RequestScopedErrorRule{{Status: 400, Match: []string{"private-literal"}, MatchRegexr: []string{"private-pattern"}, Action: "stop"}}
	for _, field := range []string{"GeminiKey", "InteractionsKey", "ClaudeKey", "CodexKey", "VertexCompatAPIKey", "OpenAICompatibility"} {
		reflect.ValueOf(after).Elem().FieldByName(field).Index(0).FieldByName("RequestScopedErrors").Set(reflect.ValueOf(rules))
	}
	after.OAuthRequestScopedErrors = map[string][]config.RequestScopedErrorRule{"codex": rules}
	for _, pair := range [][2]*config.Config{{before, after}, {after, before}} {
		details := strings.Join(BuildConfigChangeDetails(pair[0], pair[1]), "\n")
		if strings.Contains(details, "private-") {
			t.Fatal("change details expose a rule or credential")
		}
		for _, marker := range []string{"gemini[0].request-scoped-errors", "interactions[0].request-scoped-errors", "claude[0].request-scoped-errors", "codex[0].request-scoped-errors", "vertex[0].request-scoped-errors", "request-scoped-errors updated", "oauth-request-scoped-errors: updated"} {
			if !strings.Contains(details, marker) {
				t.Errorf("missing marker %s", marker)
			}
		}
	}
	if details := BuildConfigChangeDetails(after, after); len(details) != 0 {
		t.Fatal("unchanged rules generated changes")
	}
}
