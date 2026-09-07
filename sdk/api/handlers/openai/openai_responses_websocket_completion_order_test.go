package openai

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
)

func TestResponsesWebsocketCompletionWaitsForInternalStreamFinish(t *testing.T) {
	data := make(chan []byte, 1)
	data <- []byte(`{"type":"response.completed","response":{"status":"completed","output":[]}}`)
	canceled := make(chan struct{}, 1)
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		conn, err := responsesWebsocketUpgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = conn.Close() }()
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = r
		var timeline strings.Builder
		errs := make(chan *interfaces.ErrorMessage)
		_, _, _ = (*OpenAIResponsesAPIHandler)(nil).forwardResponsesWebsocket(c, conn, func(...interface{}) { canceled <- struct{}{} }, data, errs, &timeline, "test", newWebsocketToolPairState())
	}))
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, _, err = conn.ReadMessage(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-canceled:
		t.Error("downstream completion canceled the upstream before its terminal marker could finish")
	default:
	}
	close(data)
	<-done
	select {
	case <-canceled:
	default:
		t.Fatal("completed stream did not release its context")
	}
}
