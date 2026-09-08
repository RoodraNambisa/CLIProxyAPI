package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestRequestScopedErrorsSavePreservesUnknownRuleFields(t *testing.T) {
	for _, family := range append(append([]string{}, retryConfigFamilies...), "oauth-request-scoped-errors") {
		for _, edit := range []string{"unchanged", "action", "reorder", "same-status-reorder", "same-matcher-reorder", "alias", "root-merge"} {
			t.Run(family+"/"+edit, func(t *testing.T) {
				rules := `[{status: 500, match: [first], action: stop, future-rule: first-extension}, {status: 503, match-regexr: [second], action: continue, future-rule: second-extension}]`
				if edit == "same-status-reorder" {
					rules = `[{status: 500, match: [first], action: stop, future-rule: first-extension}, {status: 500, match-regexr: [second], action: continue, future-rule: second-extension}]`
				}
				if edit == "same-matcher-reorder" {
					rules = `[{status: 500, match: [first], action: stop, future-rule: first-extension}, {status: 500, match: [first], action: continue, future-rule: second-extension}]`
				}
				body := family + ": [{name: compat, api-key: test-key, base-url: https://example.test, request-scoped-errors: " + rules + "}]\n"
				if family == "oauth-request-scoped-errors" {
					body = family + ": {codex: " + rules + "}\n"
				}
				if edit == "alias" {
					body = "rule-source: &rules " + rules + "\n" + strings.Replace(body, rules, "*rules", 1)
				}
				if edit == "root-merge" {
					body = "defaults: &defaults\n  " + strings.TrimSpace(body) + "\n<<: *defaults\n"
				}
				path := filepath.Join(t.TempDir(), "config.yaml")
				if err := os.WriteFile(path, []byte(body), 0600); err != nil {
					t.Fatal(err)
				}
				cfg, err := LoadConfig(path)
				if err != nil {
					t.Fatal(err)
				}
				var selected *[]RequestScopedErrorRule
				switch family {
				case "gemini-api-key":
					selected = &cfg.GeminiKey[0].RequestScopedErrors
				case "interactions-api-key":
					selected = &cfg.InteractionsKey[0].RequestScopedErrors
				case "claude-api-key":
					selected = &cfg.ClaudeKey[0].RequestScopedErrors
				case "codex-api-key":
					selected = &cfg.CodexKey[0].RequestScopedErrors
				case "vertex-api-key":
					selected = &cfg.VertexCompatAPIKey[0].RequestScopedErrors
				case "openai-compatibility":
					selected = &cfg.OpenAICompatibility[0].RequestScopedErrors
				default:
					value := cfg.OAuthRequestScopedErrors["codex"]
					selected = &value
				}
				if edit == "action" {
					(*selected)[0].Action = "continue-and-cooldown"
				}
				if len(*selected) != 2 {
					t.Fatalf("fixture rules %d, want 2", len(*selected))
				}
				if edit == "reorder" || edit == "same-status-reorder" || edit == "same-matcher-reorder" {
					(*selected)[0], (*selected)[1] = (*selected)[1], (*selected)[0]
				}
				if err := SaveConfigPreserveComments(path, cfg); err != nil {
					t.Fatal(err)
				}
				saved, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				var decoded map[string]any
				if err := yaml.Unmarshal(saved, &decoded); err != nil {
					t.Fatal(err)
				}
				var got []any
				if family == "oauth-request-scoped-errors" {
					got = decoded[family].(map[string]any)["codex"].([]any)
				} else {
					got = decoded[family].([]any)[0].(map[string]any)["request-scoped-errors"].([]any)
				}
				if len(got) != 2 {
					t.Fatalf("saved rules %d, want 2", len(got))
				}
				for index, item := range got {
					row := item.(map[string]any)
					if row["action"] != (*selected)[index].Action || fmt.Sprint(row["status"]) != fmt.Sprint((*selected)[index].Status) {
						t.Fatal("save ignored rule edits or order")
					}
					name := "first"
					if row["match-regexr"] != nil || (edit == "same-matcher-reorder" && row["action"] == "continue") {
						name = "second"
					}
					if row["future-rule"] != name+"-extension" {
						t.Fatal("save lost or reassigned unknown rule data")
					}
				}
			})
		}
	}
}
