package handlers

import (
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
)

func TestAPIContextCopiesPreparedMultiAgentTurnPolicy(t *testing.T) {
	h := NewBaseAPIHandlers(&config.SDKConfig{}, nil)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/responses", nil)
	initial := helps.CodexMultiAgentPolicy{Enabled: true, ToolsPrepared: true, ClientVersion: "0.153.4"}
	c.Set(helps.CodexMultiAgentPolicyGinKey, initial)
	first, cancelFirst := h.GetContextWithCancel(nil, c, nil)
	defer cancelFirst()
	c.Set(helps.CodexMultiAgentPolicyGinKey, helps.CodexMultiAgentPolicy{})
	second, cancelSecond := h.GetContextWithCancel(nil, c, nil)
	defer cancelSecond()
	if helps.SnapshotCodexMultiAgentPolicy(first, nil, false) != initial || helps.SnapshotCodexMultiAgentPolicy(second, nil, true).Enabled {
		t.Fatal("API contexts did not isolate consecutive request policies")
	}
}
