package helps

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

func TestCodexReasoningReplaySeparatesExplicitClaudeAgents(t *testing.T) {
	for _, source := range []string{"headers", "metadata", "gin", "mixed"} {
		for _, transportID := range []string{"", "shared-transport"} {
			t.Run(fmt.Sprintf("%s/transport=%s", source, transportID), func(t *testing.T) {
				resetCodexReasoningReplayStoreForTest()
				t.Cleanup(resetCodexReasoningReplayStoreForTest)
				body := []byte(`{"prompt_cache_key":"shared-cache","input":[{"role":"user","content":"next"}]}`)
				metadata := map[string]any{"execution_session_id": transportID}
				apply := func(agent string, history bool) ([]byte, CodexReasoningReplayScope) {
					ctx := t.Context()
					headers := http.Header{}
					original := []byte(`{"messages":[]}`)
					if source != "metadata" {
						headers.Set("X-Claude-Code-Session-Id", "session-a")
						headers.Set("X-Claude-Code-Agent-Id", agent)
						if source == "gin" || source == "mixed" {
							c, _ := gin.CreateTestContext(httptest.NewRecorder())
							c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
							c.Request.Header = headers.Clone()
							ctx = context.WithValue(ctx, "gin", c)
							if source == "gin" {
								headers = nil
							} else {
								c.Request.Header.Set("X-Claude-Code-Agent-Id", "discarded")
								headers.Del("X-Claude-Code-Session-Id")
							}
						}
					} else {
						original = []byte(fmt.Sprintf(`{"metadata":{"user_id":%q},"messages":[]}`, fmt.Sprintf(`{"session_id":"session-a","agent_id":%q}`, agent)))
					}
					request := body
					if history {
						request = []byte(`{"prompt_cache_key":"shared-cache","input":[{"role":"user","content":"next"},{"role":"assistant","content":"answer"},{"role":"user","content":"followup"}]}`)
					}
					return ApplyCodexReasoningReplay(ctx, "claude", "tenant:credential", "gpt-5", original, request, nil, metadata, headers)
				}
				_, workerScope := apply("worker-a", false)
				completed := []byte(`{"response":{"output":[{"type":"reasoning","encrypted_content":"` + testCodexReasoningSignature() + `"},{"type":"message","role":"assistant","content":"answer"}]}}`)
				if !CacheCodexReasoningReplayFromCompleted(workerScope, completed) {
					t.Fatal("failed to cache agent reasoning")
				}
				for _, agent := range []string{"worker-a", "worker-b", "main", ""} {
					out, scope := apply(agent, true)
					gotReplay := gjson.GetBytes(out, `input.#(type=="reasoning")`).Exists()
					if gotReplay != (agent == "worker-a") || (codexReasoningReplayKey(scope) == codexReasoningReplayKey(workerScope)) != (agent == "worker-a") {
						t.Errorf("agent %q reused another agent's replay: %t", agent, gotReplay)
					}
					if gjson.GetBytes(out, "prompt_cache_key").String() != "shared-cache" {
						t.Fatal("replay isolation changed the outbound cache key")
					}
				}
			})
		}
	}
}

func TestCodexReplayAgentIdentityRejectsConflictingHeaders(t *testing.T) {
	headers := http.Header{"X-Claude-Code-Session-Id": {"session"}, "X-Claude-Code-Agent-Id": {"worker-a"}, "x-claude-code-agent-id": {"worker-b"}}
	if got := codexReplayClaudeAgentIdentity(t.Context(), []byte(`{}`), headers); got != "" {
		t.Fatal("conflicting agent values selected an arbitrary identity")
	}
	if headers["x-claude-code-agent-id"][0] != "worker-b" || len(headers) != 3 {
		t.Fatal("identity capture mutated source headers")
	}
	for _, agent := range []string{"", "main"} {
		headers = http.Header{"X-Claude-Code-Session-Id": {"session"}, "X-Claude-Code-Agent-Id": {agent}}
		scope := codexReasoningReplayScopeFromRequest(t.Context(), "claude", "tenant", "model", nil, []byte(`{"prompt_cache_key":"cache"}`), nil, nil, headers)
		if scope.sessionKey != "prompt-cache:cache" {
			t.Fatal("main agent changed its existing replay scope")
		}
	}
}
