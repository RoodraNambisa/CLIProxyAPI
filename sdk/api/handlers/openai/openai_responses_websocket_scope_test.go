package openai

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

func TestResponsesWebsocketConversationScopesOriginalThreadAndKind(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
	c.Request.Header.Set("Session-Id", "same-session")
	c.Set("apiKey", "same-caller")
	var first, second responsesWebsocketConversations
	defer first.close()
	defer second.close()
	body := func(thread, kind string) []byte {
		return []byte(fmt.Sprintf(`{"model":"test","client_metadata":{"thread_id":%q,"x-codex-turn-metadata":%q}}`, thread, fmt.Sprintf(`{"request_kind":%q}`, kind)))
	}
	main, _ := first.selectRequest(c, body("main", "turn"))
	main.tools.recordCall("same-call", []byte(`{"call_id":"same-call","name":"main-tool"}`))
	main.lastRequest = []byte(`{"model":"test","input":[{"content":"main history"}]}`)
	sub, _ := first.selectRequest(c, body("sub", "turn"))
	memory, _ := first.selectRequest(c, body("main", "memory"))
	for _, state := range []*responsesWebsocketConversation{sub, memory} {
		if _, exists := state.tools.getCall("same-call"); exists || len(state.lastRequest) != 0 {
			t.Fatal("foreign thread/kind inherited tools or history")
		}
		state.tools.recordCall("same-call", []byte(`{"name":"different-tool"}`))
	}
	reconnected, _ := second.selectRequest(c, body("main", "turn"))
	if call, ok := reconnected.tools.getCall("same-call"); !ok || gjson.GetBytes(call, "name").Str != "main-tool" || len(reconnected.lastRequest) != 0 {
		t.Fatal("reconnect lost same-scope tools or copied connection history")
	}
	revisited, _ := first.selectRequest(c, body("main", "turn"))
	if revisited != main || len(revisited.lastRequest) == 0 {
		t.Fatal("scope switch lost main history")
	}
	first.close()
	if _, ok := reconnected.tools.getCall("same-call"); !ok {
		t.Fatal("closing one connection released another holder")
	}
}

func TestResponsesWebsocketPrewarmKeepsMainHistoryAndTools(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
	var states responsesWebsocketConversations
	defer states.close()
	main, _ := states.selectRequest(c, []byte(`{"model":"test","input":[]}`))
	main.lastRequest = []byte(`{"model":"test","input":[{"content":"main"}]}`)
	main.lastResponseID = "main-response"
	warm, _ := states.selectRequest(c, []byte(`{"model":"test","generate":false}`))
	if warm == main {
		t.Fatal("prewarm shares the main scope")
	}
	warm.lastResponseID = "synthetic-warm"
	states.prewarmID, states.prewarmScope, warm.model = warm.lastResponseID, warm.scope, "test"
	resumed, body := states.selectRequest(c, []byte(`{"previous_response_id":"synthetic-warm","input":[]}`))
	if resumed != main || resumed.lastResponseID != "main-response" || gjson.GetBytes(body, "previous_response_id").Exists() || gjson.GetBytes(body, "model").Str != "test" {
		t.Fatal("prewarm polluted ordinary history")
	}
}

func TestResponsesWebsocketConversationScopeEvictionReleasesReferences(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
	c.Set("apiKey", t.Name())
	var states responsesWebsocketConversations
	defer states.close()
	first, _ := states.selectRequest(c, []byte(`{"client_metadata":{"thread_id":"first"}}`))
	for i := 0; i < responsesWebsocketConversationLimit; i++ {
		states.selectRequest(c, []byte(fmt.Sprintf(`{"client_metadata":{"thread_id":"thread-%d"}}`, i)))
	}
	defaultWebsocketToolPairStates.mu.Lock()
	_, retained := defaultWebsocketToolPairStates.states[first.sharedKey]
	defaultWebsocketToolPairStates.mu.Unlock()
	if retained || len(states.states) != responsesWebsocketConversationLimit {
		t.Fatal("scope eviction retained tool state")
	}
}

func TestResponsesWebsocketConversationHistoryBudget(t *testing.T) {
	var states responsesWebsocketConversations
	defer states.close()
	first, _ := states.selectRequest(nil, []byte(`{"client_metadata":{"thread_id":"first"}}`))
	second, _ := states.selectRequest(nil, []byte(`{"client_metadata":{"thread_id":"second"}}`))
	first.lastRequest, first.lastResponseID = []byte(`{"input":"older"}`), "old-response"
	second.lastRequest = []byte(`{"input":"current"}`)
	states.trimHistory(second, len(second.lastRequest)+len(second.lastResponseOutput))
	if len(first.lastRequest) != 0 || first.lastResponseID != "" || len(second.lastRequest) == 0 {
		t.Fatal("history budget evicted current state or left an unusable reference")
	}
}
