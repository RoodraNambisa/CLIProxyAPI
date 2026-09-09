package executor

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestClaudeToolResultsUseLastPayloadAtFirstPosition(t *testing.T) {
	for _, source := range []translator.Format{translator.FormatOpenAIResponse, translator.FormatOpenAI} {
		for _, compat := range []bool{false, true} {
			for _, operation := range []string{"execute", "stream", "count"} {
				t.Run(fmt.Sprintf("%s/compat=%t/%s", source, compat, operation), func(t *testing.T) {
					captured := make(chan bool, 2)
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						body, err := io.ReadAll(r.Body)
						if err != nil {
							t.Error(err)
							return
						}
						var results []gjson.Result
						for _, message := range gjson.GetBytes(body, "messages").Array() {
							for _, part := range message.Get("content").Array() {
								if part.Get("type").String() == "tool_result" {
									results = append(results, part)
								}
							}
						}
						valid := len(results) == 4
						if valid {
							valid = results[0].Get("tool_use_id").String() == "pair/a" && results[0].Get("content").String() == "final" &&
								results[0].Get("cache_control.ttl").String() == "5m" && results[1].Get("tool_use_id").String() == "pair_a" &&
								results[1].Get("content").String() == "other" && results[2].Get("content").String() == "missing-1" && results[3].Get("content").String() == "missing-2"
						}
						captured <- valid
						responseKind := "stream"
						if operation == "count" {
							responseKind = "count"
						}
						writeClaudeCompatibilityFixture(w, responseKind)
					}))
					t.Cleanup(server.Close)
					manager := claudeCompatibilityManager(t, server.URL, compat)
					raw := []byte(`{"input":[{"type":"function_call_output","call_id":"pair/a","output":"stale","cache_control":{"type":"ephemeral","ttl":"1h"}},{"type":"custom_tool_call_output","call_id":"pair_a","output":"other"},{"type":"function_call_output","output":"missing-1"},{"type":"custom_tool_call_output","call_id":"pair/a","output":"final","cache_control":{"type":"ephemeral","ttl":"5m"}},{"type":"function_call_output","output":"missing-2"}]}`)
					if source == translator.FormatOpenAI {
						raw = []byte(`{"messages":[{"role":"tool","tool_call_id":"pair/a","content":"stale","cache_control":{"type":"ephemeral","ttl":"1h"}},{"role":"tool","tool_call_id":"pair_a","content":"other"},{"role":"tool","content":"missing-1"},{"role":"tool","tool_call_id":"pair/a","content":"final","cache_control":{"type":"ephemeral","ttl":"5m"}},{"role":"tool","content":"missing-2"}]}`)
					}
					runClaudeCompatibilityRequest(t, manager, operation, raw, source)
					select {
					case valid := <-captured:
						if !valid {
							t.Fatal("duplicate results, raw IDs, position or final cache control changed")
						}
					default:
						t.Fatal("request did not reach the local upstream")
					}
				})
			}
		}
	}
}
