package openai

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/tidwall/gjson"
)

func TestResponsesUnroutableModelAcrossHTTPAndWebsocket(t *testing.T) {
	gin.SetMode(gin.TestMode)
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager))
	chat := NewOpenAIAPIHandler(h.BaseAPIHandler)
	router := gin.New()
	router.POST("/v1/responses", h.Responses)
	router.POST("/v1/responses/compact", h.Compact)
	router.POST("/v1/chat/completions", chat.ChatCompletions)
	router.GET("/v1/responses", h.ResponsesWebsocket)
	model := "unroutable-test-\"quote\""
	for _, route := range []struct {
		path   string
		stream bool
	}{
		{"/v1/responses", false}, {"/v1/responses", true}, {"/v1/responses/compact", false},
		{"/v1/chat/completions", false}, {"/v1/chat/completions", true},
	} {
		body, _ := json.Marshal(map[string]any{"model": model, "stream": route.stream, "input": "hello", "messages": []any{map[string]any{"role": "user", "content": "hello"}}})
		request := httptest.NewRequest(http.MethodPost, route.path, strings.NewReader(string(body)))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest || gjson.Get(response.Body.String(), "error.code").String() != "model_not_found" {
			t.Fatalf("HTTP %s stream=%t: status=%d body=%s", route.path, route.stream, response.Code, response.Body.String())
		}
	}
	server := httptest.NewServer(router)
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	for range 2 {
		if err = conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[{"role":"user","content":"hello"}]}`, model))); err != nil {
			t.Fatal(err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, body, errRead := conn.ReadMessage()
		if errRead != nil {
			t.Fatal(errRead)
		}
		if gjson.GetBytes(body, "status").Int() != http.StatusBadRequest || gjson.GetBytes(body, "error.code").String() != "model_not_found" {
			t.Fatalf("WebSocket error=%s", body)
		}
	}
}
