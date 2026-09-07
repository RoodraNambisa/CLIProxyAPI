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
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

type websocketReaderPendingExecutor struct {
	websocketCompactionCaptureExecutor
	started chan context.Context
	release chan struct{}
}

func (e *websocketReaderPendingExecutor) ExecuteStream(ctx context.Context, _ *coreauth.Auth, _ coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.started <- ctx
	select {
	case <-ctx.Done():
	case <-e.release:
	}
	return nil, context.Canceled
}

func TestResponsesWebsocketReaderCancelsPendingUpstreamOnClientClose(t *testing.T) {
	exec := &websocketReaderPendingExecutor{started: make(chan context.Context, 1), release: make(chan struct{})}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(exec)
	auth := &coreauth.Auth{ID: "reader-close-auth", Provider: exec.Identifier(), Status: coreauth.StatusActive}
	if _, err := manager.Register(t.Context(), auth); err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "reader-close-model"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
	handler := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager))
	router := gin.New()
	done := make(chan struct{})
	router.GET("/v1/responses", func(c *gin.Context) { defer close(done); handler.ResponsesWebsocket(c) })
	server := httptest.NewServer(router)
	defer server.Close()
	defer close(exec.release)
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"reader-close-model","input":[]}`)); err != nil {
		t.Fatal(err)
	}
	var upstream context.Context
	select {
	case upstream = <-exec.started:
	case <-time.After(time.Second):
		t.Fatal("upstream did not start")
	}
	_ = conn.Close()
	select {
	case <-upstream.Done():
	case <-time.After(time.Second):
		t.Fatal("client close left credential/bootstrap work running")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("request reader or handler remained blocked after cancellation")
	}
}

func TestResponsesWebsocketReaderPreservesOrderAndStopsWithQueuedInput(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(context.Canceled)
	ready := make(chan (<-chan responsesWebsocketRequestMessage), 1)
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		conn, err := responsesWebsocketUpgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
		defer stopClose()
		defer func() { _ = conn.Close() }()
		messages, readerDone := readResponsesWebsocketRequests(ctx, cancel, conn)
		ready <- messages
		<-readerDone
	}))
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	messages := <-ready
	for _, text := range []string{"first", "second", "queued", "blocked"} {
		if err := conn.WriteMessage(websocket.TextMessage, []byte(text)); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []string{"first", "second"} {
		select {
		case got := <-messages:
			if got.kind != websocket.TextMessage || string(got.payload) != want {
				t.Fatal("reader reordered or changed request frames")
			}
		case <-time.After(time.Second):
			t.Fatal("reader did not deliver queued input")
		}
	}
	cancel(context.Canceled)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("bounded queue prevented reader cancellation")
	}
}
