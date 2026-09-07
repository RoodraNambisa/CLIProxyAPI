package openai

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

type websocketKeepaliveInvocation struct {
	ctx     context.Context
	release chan struct{}
}

type websocketKeepaliveExecutor struct {
	websocketCompactionCaptureExecutor
	started chan websocketKeepaliveInvocation
}

func (e *websocketKeepaliveExecutor) ExecuteStream(ctx context.Context, _ *coreauth.Auth, _ coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	call := websocketKeepaliveInvocation{ctx: ctx, release: make(chan struct{})}
	e.started <- call
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-call.release:
	}
	chunks := make(chan coreexecutor.StreamChunk, 1)
	chunks <- coreexecutor.StreamChunk{Payload: []byte(`{"type":"response.completed","response":{"id":"finished","status":"completed","output":[]}}`)}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func TestResponsesWebsocketPingDuringBootstrapUsesTurnSnapshotAndCancelsOnClose(t *testing.T) {
	gin.SetMode(gin.TestMode)
	exec := &websocketKeepaliveExecutor{started: make(chan websocketKeepaliveInvocation, 3)}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(exec)
	auth := &coreauth.Auth{ID: "keepalive-test", Provider: exec.Identifier(), Status: coreauth.StatusActive}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "keepalive-model"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
	base := handlers.NewBaseAPIHandlers(&config.SDKConfig{Streaming: config.StreamingConfig{KeepAliveSeconds: 1}}, manager)
	h := NewOpenAIResponsesAPIHandler(base)
	router := gin.New()
	router.GET("/v1/responses", h.ResponsesWebsocket)
	server := httptest.NewServer(router)
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	pings := make(chan time.Time, 8)
	conn.SetPingHandler(func(string) error { pings <- time.Now(); return nil })
	messages := make(chan []byte, 4)
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			_, payload, errRead := conn.ReadMessage()
			if errRead != nil {
				return
			}
			messages <- payload
		}
	}()
	send := func() {
		t.Helper()
		if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"keepalive-model","input":[{"role":"user","content":"test"}]}`)); errWrite != nil {
			t.Fatal(errWrite)
		}
	}
	waitPing := func() {
		t.Helper()
		select {
		case <-pings:
		case <-time.After(3 * time.Second):
			t.Fatal("missing Ping while bootstrap has no output")
		}
	}
	send()
	first := <-exec.started
	waitPing()
	base.UpdateClients(&config.SDKConfig{})
	waitPing()
	if len(messages) != 0 {
		t.Fatal("Ping entered the response body")
	}
	close(first.release)
	select {
	case <-messages:
	case <-time.After(time.Second):
		t.Fatal("missing completion after bootstrap release")
	}
	send()
	second := <-exec.started
	select {
	case <-pings:
		t.Fatal("disabled next turn inherited the previous interval")
	case <-time.After(1200 * time.Millisecond):
	}
	_ = conn.Close()
	select {
	case <-second.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("client close did not cancel pending upstream")
	}
	<-readDone
}

func TestResponsesWebsocketPingWriteFailureReleasesTurn(t *testing.T) {
	done := make(chan error, 1)
	canceled := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := responsesWebsocketUpgrader.Upgrade(w, r, nil)
		if err != nil {
			done <- err
			return
		}
		_ = conn.Close()
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = r
		h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&config.SDKConfig{Streaming: config.StreamingConfig{KeepAliveSeconds: 1}}, nil))
		var timeline strings.Builder
		_, _, err = h.forwardResponsesWebsocket(c, conn, func(...interface{}) { canceled <- struct{}{} }, make(chan []byte), make(chan *interfaces.ErrorMessage), &timeline, "test", newWebsocketToolPairState())
		done <- err
	}))
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	select {
	case errForward := <-done:
		if errForward == nil {
			t.Fatal("Ping write failure was ignored")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("failed Ping left the turn blocked")
	}
	select {
	case <-canceled:
	default:
		t.Fatal("failed Ping did not cancel upstream")
	}
}

func TestResponsesWebsocketOutputResetsPingIdleInterval(t *testing.T) {
	data := make(chan []byte)
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
		h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&config.SDKConfig{Streaming: config.StreamingConfig{KeepAliveSeconds: 1}}, nil))
		var timeline strings.Builder
		_, _, _ = h.forwardResponsesWebsocket(c, conn, func(...interface{}) {}, data, make(chan *interfaces.ErrorMessage), &timeline, "test", newWebsocketToolPairState())
	}))
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	pings := make(chan time.Time, 4)
	messages := make(chan time.Time, 4)
	conn.SetPingHandler(func(string) error { pings <- time.Now(); return nil })
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			_, _, errRead := conn.ReadMessage()
			if errRead != nil {
				return
			}
			messages <- time.Now()
		}
	}()
	select {
	case <-pings:
	case <-time.After(3 * time.Second):
		t.Fatal("no initial Ping")
	}
	// The next data event occurs well between ticks. A fixed ticker would Ping
	// after 400 ms; an idle timer must wait a complete interval after this output.
	time.Sleep(600 * time.Millisecond)
	data <- []byte(`{"type":"response.output_text.delta","delta":"content"}`)
	outputAt := <-messages
	select {
	case at := <-pings:
		if at.Sub(outputAt) < 800*time.Millisecond {
			t.Error("actual output did not reset idle interval")
		}
	case <-time.After(3 * time.Second):
		t.Error("no Ping after idle output")
	}
	data <- []byte(`{"type":"response.completed","response":{"status":"completed","output":[]}}`)
	close(data)
	<-done
	<-readerDone
}
