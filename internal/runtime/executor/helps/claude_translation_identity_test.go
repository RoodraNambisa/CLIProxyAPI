package helps

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	claudegemini "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/claude/gemini"
	claudechat "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/claude/openai/chat-completions"
	clauderesponses "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/claude/openai/responses"
	"github.com/tidwall/gjson"
)

func TestClaudeTranslationIdentityConcurrentInitialization(t *testing.T) {
	for name, translate := range map[string]func(string, []byte, bool) []byte{
		"chat":      claudechat.ConvertOpenAIRequestToClaude,
		"responses": clauderesponses.ConvertOpenAIResponsesRequestToClaude,
		"gemini":    claudegemini.ConvertGeminiRequestToClaude,
	} {
		t.Run(name, func(t *testing.T) {
			const workers = 64
			start := make(chan struct{})
			results := make(chan string, workers)
			var wg sync.WaitGroup
			for i := range workers {
				wg.Go(func() {
					<-start
					body := translate("claude-identity", []byte(`{}`), i%2 == 0)
					results <- gjson.GetBytes(body, "metadata.user_id").String()
				})
			}
			close(start)
			wg.Wait()
			close(results)
			first := ""
			for value := range results {
				if first == "" {
					first = value
				}
				if value != first {
					t.Fatal("concurrent first requests used inconsistent process identity")
				}
			}
			parts := strings.Split(first, "_")
			if len(parts) != 6 || parts[0] != "user" || parts[2] != "account" || parts[4] != "session" {
				t.Fatal("legacy identity format changed")
			}
			for _, value := range []string{parts[3], parts[5]} {
				if id, err := uuid.Parse(value); err != nil || id.Version() != 4 {
					t.Fatal("legacy identity UUID shape changed")
				}
			}
			if fmt.Sprintf("%x", sha256.Sum256([]byte(parts[3]+parts[5]))) != parts[1] {
				t.Fatal("legacy identity digest no longer matches its account and session")
			}
			if next := gjson.GetBytes(translate("different-model", []byte(`{}`), false), "metadata.user_id").String(); next != first {
				t.Fatal("identity became per-request or per-model")
			}
		})
	}
}
