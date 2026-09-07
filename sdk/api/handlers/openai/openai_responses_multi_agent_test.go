package openai

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/tidwall/gjson"
)

type multiAgentBoundaryCapture struct {
	payload []byte
	policy  helps.CodexMultiAgentPolicy
}

type multiAgentBoundaryExecutor struct {
	compactCaptureExecutor
	requests  chan multiAgentBoundaryCapture
	onExecute func()
}

func (*multiAgentBoundaryExecutor) Identifier() string { return "multi-agent-fixture" }

func (e *multiAgentBoundaryExecutor) Execute(ctx context.Context, _ *coreauth.Auth, req core.Request, _ core.Options) (core.Response, error) {
	if e.onExecute != nil {
		e.onExecute()
	}
	e.requests <- multiAgentBoundaryCapture{req.Payload, helps.SnapshotCodexMultiAgentPolicy(ctx, nil, true)}
	return core.Response{Payload: []byte(`{"id":"done","output":[]}`)}, nil
}

func (e *multiAgentBoundaryExecutor) ExecuteStream(ctx context.Context, auth *coreauth.Auth, req core.Request, opts core.Options) (*core.StreamResult, error) {
	_, _ = e.Execute(ctx, auth, req, opts)
	chunks := make(chan core.StreamChunk, 1)
	chunks <- core.StreamChunk{Payload: []byte(`{"type":"response.completed","response":{"id":"done","status":"completed","output":[]}}`)}
	close(chunks)
	return &core.StreamResult{Chunks: chunks}, nil
}

func newMultiAgentBoundaryHandler(t *testing.T) (*OpenAIResponsesAPIHandler, *multiAgentBoundaryExecutor) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	executor := &multiAgentBoundaryExecutor{requests: make(chan multiAgentBoundaryCapture, 1)}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	id, privateID := uuid.NewString(), uuid.NewString()
	if _, err := manager.Register(t.Context(), &coreauth.Auth{ID: id, Provider: executor.Identifier(), Status: coreauth.StatusActive, Attributes: map[string]string{"websockets": "true"}}); err != nil {
		t.Fatal(err)
	}
	models := registry.GetGlobalRegistry()
	models.RegisterClient(id, executor.Identifier(), []*registry.ModelInfo{{ID: "multi-agent-model", Description: "Allowed model", Thinking: &registry.ThinkingSupport{Levels: []string{"medium", "max", "ultra"}}}})
	models.RegisterClient(privateID, "private-provider", []*registry.ModelInfo{{ID: "private-model", Description: "Not allowed"}})
	t.Cleanup(func() { models.UnregisterClient(id); models.UnregisterClient(privateID) })
	return NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&config.SDKConfig{}, manager)), executor
}

const multiAgentBoundaryTools = `[{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"spawn_agent","description":"Spawns an agent. Keep this instruction.","parameters":{"properties":{"message":{"type":"string","encrypted":true}}}}]}]`

func TestResponsesMultiAgentPreparationAcrossHTTPRoutes(t *testing.T) {
	for _, route := range []string{"responses", "stream", "compact"} {
		for _, enabled := range []bool{false, true} {
			for _, ua := range []string{"codex_cli_rs/0.153.4", "codex_cli_rs/0.143.0", "other-client/1"} {
				t.Run(fmt.Sprintf("%s/%t/%s", route, enabled, ua), func(t *testing.T) {
					h, executor := newMultiAgentBoundaryHandler(t)
					h.UpdateClients(&config.SDKConfig{CodexOptimizeMultiAgentV2: enabled})
					executor.onExecute = func() { h.UpdateClients(&config.SDKConfig{CodexOptimizeMultiAgentV2: !enabled}) }
					body := fmt.Sprintf(`{"model":"multi-agent-model","stream":%t,"tools":%s,"input":[],"prompt_cache_key":"caller-cache"}`, route == "stream", multiAgentBoundaryTools)
					w := httptest.NewRecorder()
					c, _ := gin.CreateTestContext(w)
					c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
					c.Request.Header.Set("User-Agent", ua)
					c.Set("accessMetadata", map[string]string{sdkaccess.MetadataAllowedProviders: executor.Identifier()})
					if route == "compact" {
						h.Compact(c)
					} else {
						h.Responses(c)
					}
					if w.Code != http.StatusOK {
						t.Fatalf("HTTP status = %d", w.Code)
					}
					got := <-executor.requests
					wantEnabled := enabled && ua != "other-client/1"
					if got.policy.Enabled != wantEnabled || got.policy.ToolsPrepared != wantEnabled {
						t.Fatal("executor lost the boundary policy after config changed")
					}
					description := gjson.GetBytes(got.payload, "tools.0.tools.0.description").String()
					if !strings.Contains(description, "Keep this instruction.") || strings.Contains(description, "private-model") {
						t.Fatal("model list leaked a forbidden model or removed caller instructions")
					}
					if strings.Contains(description, "multi-agent-model") != wantEnabled ||
						strings.Contains(description, "ultra") != (wantEnabled && ua == "codex_cli_rs/0.153.4") {
						t.Fatal("tool description disagrees with client-visible model capabilities")
					}
					if gjson.GetBytes(got.payload, "tools.0.tools.0.parameters.properties.message.encrypted").Exists() == wantEnabled {
						t.Fatal("message schema does not follow the pinned policy")
					}
					if gjson.GetBytes(got.payload, "prompt_cache_key").String() != "caller-cache" || gjson.GetBytes(got.payload, "tools.0.name").String() != "collaboration" {
						t.Fatal("boundary preparation changed cache identity or provider-specific namespaces")
					}
				})
			}
		}
	}
}

func TestResponsesMultiAgentPreparationHandlesAdditionalToolsAndConflicts(t *testing.T) {
	h, _ := newMultiAgentBoundaryHandler(t)
	h.UpdateClients(&config.SDKConfig{CodexOptimizeMultiAgentV2: true})
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Request.Header.Set("User-Agent", "codex_cli_rs/0.153.4")
	for _, conflict := range []bool{false, true} {
		rootTools := ""
		if conflict {
			rootTools = `"tools":[{"type":"namespace","name":"collaboration-optimize"}],`
		}
		body := []byte(fmt.Sprintf(`{%s"input":[{"type":"additional_tools","tools":%s}],"metadata":{"description":"spawn_agent"}}`, rootTools, multiAgentBoundaryTools))
		got := h.prepareCodexMultiAgentV2(c, body)
		description := gjson.GetBytes(got, "input.0.tools.0.tools.0.description").String()
		if strings.Contains(description, "multi-agent-model") == conflict {
			t.Fatal("additional declaration or reserved conflict was ignored")
		}
		if gjson.GetBytes(got, "metadata").Raw != gjson.GetBytes(body, "metadata").Raw {
			t.Fatal("business JSON changed")
		}
	}
}

func TestResponsesMultiAgentWebsocketPreparesEachTurn(t *testing.T) {
	h, executor := newMultiAgentBoundaryHandler(t)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("accessMetadata", map[string]string{sdkaccess.MetadataAllowedProviders: executor.Identifier()})
		c.Next()
	})
	router.GET("/v1/responses", h.ResponsesWebsocket)
	server := httptest.NewServer(router)
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	refreshID := uuid.NewString()
	defer registry.GetGlobalRegistry().UnregisterClient(refreshID)
	for turn, enabled := range []bool{true, false, true} {
		h.UpdateClients(&config.SDKConfig{CodexOptimizeMultiAgentV2: enabled})
		if turn == 2 {
			registry.GetGlobalRegistry().RegisterClient(refreshID, executor.Identifier(), []*registry.ModelInfo{{ID: "newly-listed-model", Description: "New model"}})
		}
		previous := ""
		if turn > 0 {
			previous = `,"previous_response_id":"done"`
		}
		body := fmt.Sprintf(`{"type":"response.create","model":"multi-agent-model"%s,"input":[{"type":"additional_tools","tools":%s}]}`, previous, multiAgentBoundaryTools)
		if err := conn.WriteMessage(websocket.TextMessage, []byte(body)); err != nil {
			t.Fatal(err)
		}
		for {
			_, response, err := conn.ReadMessage()
			if err != nil {
				t.Fatal(err)
			}
			if gjson.GetBytes(response, "type").String() == "error" {
				t.Fatal("WebSocket rejected the test turn")
			}
			if gjson.GetBytes(response, "type").String() == "response.completed" {
				break
			}
		}
		got := <-executor.requests
		if got.policy.Enabled != enabled || got.policy.ToolsPrepared != enabled {
			t.Fatal("WebSocket reused a previous turn's policy")
		}
		description := gjson.GetBytes(got.payload, "input.0.tools.0.tools.0.description").String()
		if strings.Contains(description, "multi-agent-model") != enabled ||
			strings.Contains(description, "newly-listed-model") != (turn == 2) || strings.Contains(description, "private-model") {
			t.Fatal("WebSocket description lost its current caller-filtered model snapshot")
		}
	}
}
