package openai

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
)

func TestResponsesWebsocketHandlerRedactsDiagnosticsAcrossInvalidTurns(t *testing.T) {
	previousHooks := log.StandardLogger().ReplaceHooks(make(log.LevelHooks))
	hook := logtest.NewGlobal()
	t.Cleanup(func() { log.StandardLogger().ReplaceHooks(previousHooks) })
	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{RequestLog: true}, nil))
	timeline := make(chan string, 1)
	router := gin.New()
	router.GET("/v1/responses", func(c *gin.Context) {
		h.ResponsesWebsocket(c)
		value, _ := c.Get(wsTimelineBodyKey)
		data, _ := value.([]byte)
		timeline <- string(data)
	})
	server := httptest.NewServer(router)
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	for _, key := range []string{"first-client-cache-key", "next-client-cache-key", "a"} {
		frame, _ := json.Marshal(map[string]any{"type": "response.create", "input": []any{}, "prompt_cache_key": key})
		if err := conn.WriteMessage(websocket.TextMessage, frame); err != nil {
			t.Fatal(err)
		}
		_, response, errRead := conn.ReadMessage()
		if errRead != nil || !strings.Contains(string(response), "missing model") || strings.Contains(string(response), util.PromptCacheLogMarker) {
			t.Fatal("diagnostic policy changed the public validation error")
		}
	}
	if err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "a")); err != nil {
		t.Fatal(err)
	}
	var logged string
	select {
	case logged = <-timeline:
	case <-time.After(5 * time.Second):
		t.Fatal("websocket logging did not finish after a normal close")
	}
	for _, key := range []string{"first-client-cache-key", "next-client-cache-key", `"prompt_cache_key":"a"`} {
		if strings.Contains(logged, key) {
			t.Fatal("handler timeline leaked a cache key")
		}
	}
	if strings.Count(logged, util.PromptCacheLogMarker) < 4 {
		t.Fatal("request or close-reason diagnostics were not protected")
	}
	for _, entry := range hook.AllEntries() {
		if strings.Contains(entry.Message, "first-client-cache-key") || strings.Contains(entry.Message, "next-client-cache-key") || strings.HasSuffix(entry.Message, "normal): a") {
			t.Fatal("handler diagnostic leaked a cache key")
		}
	}
}

func TestResponsesWebsocketTimelineRedactsEachTurnWithoutChangingFrames(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
	var timeline strings.Builder
	for _, key := range []string{"first-cache-key", "second-cache-key", "a"} {
		payload, _ := json.Marshal(map[string]any{"prompt_cache_key": key, "session_id": key, "model": "astra"})
		redactor := util.RegisterPromptCacheLogPolicy(c, payload)
		appendWebsocketTimelineEvent(&timeline, "request", payload, time.Now(), redactor)
		done := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer close(done)
			upgrader := websocket.Upgrader{}
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				t.Error(err)
				return
			}
			defer func() { _ = conn.Close() }()
			if err := writeResponsesWebsocketPayload(conn, &timeline, payload, time.Now(), redactor); err != nil {
				t.Error(err)
			}
		}))
		conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
		if err != nil {
			t.Fatal(err)
		}
		_, wire, err := conn.ReadMessage()
		_ = conn.Close()
		<-done
		server.Close()
		if err != nil || string(wire) != string(payload) {
			t.Fatal("redacting timeline changed downstream bytes")
		}
		preview := websocketPayloadPreview(payload, redactor)
		var decoded map[string]any
		if err := json.Unmarshal([]byte(preview), &decoded); err != nil || decoded["prompt_cache_key"] != util.PromptCacheLogMarker || decoded["model"] != "astra" {
			t.Fatal("log preview leaked key or changed unrelated text")
		}
		appendWebsocketTimelineDisconnect(&timeline, errors.New(key), time.Now(), redactor)
	}
	for _, key := range []string{"first-cache-key", "second-cache-key", `"prompt_cache_key":"a"`, `"session_id":"a"`} {
		if strings.Contains(timeline.String(), key) {
			t.Fatal("timeline retained a previous turn's raw cache key")
		}
	}
	util.RegisterPromptCacheLogPolicy(c, []byte(`{}`))
	if util.PromptCacheLogForGin(c) != nil {
		t.Fatal("missing key inherited the preceding turn's log policy")
	}
}

func TestResponsesErrorLoggingDoesNotChangeOriginalError(t *testing.T) {
	h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{RequestLog: true}, nil))
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	util.RegisterPromptCacheLogPolicy(c, []byte(`{"prompt_cache_key":"a"}`))
	original := &interfaces.ErrorMessage{StatusCode: 400, Error: errors.New("a"), Addon: http.Header{"Session-Id": {"a"}}}
	h.LoggingAPIResponseError(context.WithValue(t.Context(), "gin", c), original)
	value, exists := c.Get("API_RESPONSE_ERROR")
	if !exists {
		t.Fatal("missing diagnostic error")
	}
	logged := value.([]*interfaces.ErrorMessage)[0]
	if logged.Error.Error() != util.PromptCacheLogMarker || logged.Addon.Get("Session-Id") != util.PromptCacheLogMarker {
		t.Fatal("error diagnostics retained the cache key")
	}
	// Exercise the public error builder separately from diagnostic projection.
	wire, err := buildResponsesWebsocketErrorPayload(original)
	if err != nil || !strings.Contains(string(wire), `"message":"a"`) {
		t.Fatal("error builder changed upstream error text")
	}
	if original.Error.Error() != "a" {
		t.Fatal("logging altered original error")
	}
}
