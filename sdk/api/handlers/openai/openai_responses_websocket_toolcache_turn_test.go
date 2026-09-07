package openai

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestResponsesWebsocketToolCacheCommitsOnlySuccessfulTurn(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, outcome := range []string{"completed", "done", "failed", "incomplete", "closed", "canceled", "write-failed", "late-error", "late-error-closed"} {
		t.Run(outcome, func(t *testing.T) {
			lateFailure := strings.HasPrefix(outcome, "late-error")
			allowFailure := make(chan struct{})
			state := newWebsocketToolPairState()
			turn := newResponsesWebsocketToolCacheTurn(state)
			turn.repairRequest([]byte(`{"input":[{"type":"function_call","name":"old","call_id":"old","arguments":"{}"},{"type":"function_call_output","call_id":"old","output":"result"}]}`))
			if _, ok := state.getOutput("old"); ok {
				t.Fatal("request was committed before success")
			}
			done := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(done)
				defer func() {
					if recovered := recover(); recovered != nil {
						t.Errorf("forwarding panicked: %v", recovered)
					}
				}()
				conn, err := responsesWebsocketUpgrader.Upgrade(w, r, nil)
				if err != nil {
					t.Error(err)
					return
				}
				defer func() { _ = conn.Close() }()
				ctx, cancel := context.WithCancel(r.Context())
				defer cancel()
				if outcome == "canceled" {
					cancel()
				}
				if outcome == "write-failed" {
					_ = conn.Close()
				}
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				c.Request = r.WithContext(ctx)
				data := make(chan []byte, 3)
				data <- []byte(`{"type":"response.output_item.done","item":{"type":"custom_tool_call","call_id":"new","name":"custom","input":"complete input"}}`)
				if outcome != "closed" {
					event, _ := json.Marshal(map[string]any{"type": "response." + outcome, "response": map[string]any{"status": outcome, "output": []any{}}})
					if outcome == "done" {
						event = []byte(`{"type":"response.done","response":{"status":"completed","output":[]}}`)
					}
					if lateFailure {
						event = []byte(`{"type":"response.completed","response":{"status":"completed","output":[]}}`)
					}
					data <- event
				}
				errs := make(chan *interfaces.ErrorMessage, 1)
				if lateFailure {
					go func() {
						<-allowFailure
						errs <- &interfaces.ErrorMessage{StatusCode: http.StatusServiceUnavailable, Error: errors.New("late stream failure")}
						if outcome == "late-error-closed" {
							close(data)
						}
						close(errs)
					}()
					if outcome == "late-error" {
						defer close(data)
					}
				} else {
					close(data)
					close(errs)
				}
				var timeline strings.Builder
				handler := &OpenAIResponsesAPIHandler{BaseAPIHandler: handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil)}
				_, forwardError, errForward := handler.forwardResponsesWebsocket(c, conn, func(...interface{}) {}, data, errs, &timeline, "test", state, turn)
				if errForward == nil && forwardError == nil {
					turn.commit()
				}
			}))
			defer server.Close()
			conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
			if err != nil {
				t.Fatal(err)
			}
			for {
				_, payload, errRead := conn.ReadMessage()
				if errRead != nil {
					break
				}
				if lateFailure && strings.Contains(string(payload), `"type":"response.completed"`) {
					close(allowFailure)
				}
			}
			_ = conn.Close()
			<-done
			want := outcome == "completed" || outcome == "done" || lateFailure
			if _, ok := state.getOutput("old"); ok != want {
				t.Fatal("request cache commit disagreed with outcome")
			}
			if _, ok := state.getCall("new"); ok != want {
				t.Fatal("response cache commit disagreed with outcome")
			}
		})
	}
}

func TestResponsesWebsocketToolCacheDoesNotPublishPartialItemsOrLoseConcurrentTurns(t *testing.T) {
	state := newWebsocketToolPairState()
	first := newResponsesWebsocketToolCacheTurn(state)
	first.recordResponse([]byte(`{"type":"response.output_item.added","item":{"type":"function_call","call_id":"partial","name":"tool","arguments":"{}"}}`))
	first.recordResponse([]byte(`{"type":"response.output_item.done","item":{"type":"function_call","call_id":"missing","name":"tool"}}`))
	first.recordResponse([]byte(`{"type":"response.output_item.done","item":{"type":"function_call","status":"incomplete","call_id":"incomplete","name":"tool","arguments":"{}"}}`))
	first.recordResponse([]byte(`{"type":"response.output_item.done","item":{"type":"function_call","call_id":"one","name":"tool","arguments":"{}"}}`))
	second := newResponsesWebsocketToolCacheTurn(state)
	second.recordResponse([]byte(`{"type":"response.output_item.done","item":{"type":"custom_tool_call","call_id":"two","name":"custom","input":""}}`))
	first.succeeded, second.succeeded = true, true
	var work sync.WaitGroup
	work.Go(first.commit)
	work.Go(second.commit)
	work.Wait()
	for _, id := range []string{"one", "two"} {
		if _, ok := state.getCall(id); !ok {
			t.Fatal("concurrent commit lost a different call")
		}
	}
	for _, id := range []string{"partial", "missing", "incomplete"} {
		if _, ok := state.getCall(id); ok {
			t.Fatal("incomplete tool item committed")
		}
	}
}
