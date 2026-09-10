package executor

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestCodexWebsocketTerminalReadSurvivesFullQueue(t *testing.T) {
	for _, binary := range []bool{false, true} {
		for _, cancel := range []bool{false, true} {
			t.Run(fmt.Sprintf("binary=%t/cancel=%t", binary, cancel), func(t *testing.T) {
				peers := make(chan *websocket.Conn, 1)
				serverDone := make(chan struct{})
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					upgrader := websocket.Upgrader{}
					peer, err := upgrader.Upgrade(w, r, nil)
					if err != nil {
						t.Error(err)
						return
					}
					peers <- peer
					<-serverDone
				}))
				client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
				if err != nil {
					close(serverDone)
					server.Close()
					t.Fatal(err)
				}
				peer := <-peers
				session := &codexWebsocketSession{conn: client, readerConn: client, upstreamDisconnectCh: make(chan error, 1)}
				reads := make(chan codexWebsocketRead, 1)
				reads <- codexWebsocketRead{conn: client, payload: []byte("queued")}
				session.setActiveForConn(reads, client)
				loopDone := make(chan struct{})
				go func() {
					NewCodexWebsocketsExecutor(&config.Config{}).readUpstreamLoop(session, client)
					close(loopDone)
				}()
				t.Cleanup(func() {
					session.clearActiveForConn(reads, client)
					_ = client.Close()
					_ = peer.Close()
					close(serverDone)
					server.Close()
					<-loopDone
				})
				if binary {
					err = peer.WriteMessage(websocket.BinaryMessage, []byte("binary"))
				} else {
					err = peer.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseMessageTooBig, "too large"), time.Now().Add(time.Second))
				}
				if err != nil {
					t.Fatal(err)
				}
				select {
				case <-session.upstreamDisconnectCh:
				case <-time.After(5 * time.Second):
					t.Fatal("full queue prevented connection invalidation")
				}
				if cancel {
					session.clearActiveForConn(reads, client)
				} else {
					if queued := <-reads; string(queued.payload) != "queued" {
						t.Fatal("preceding frame lost")
					}
					terminal, ok := <-reads
					if !ok || terminal.err == nil {
						t.Fatal("full queue lost its terminal error")
					}
					if binary {
						if !strings.Contains(terminal.err.Error(), "unexpected binary") {
							t.Fatal("wrong binary error")
						}
					} else {
						var closed *websocket.CloseError
						if !errors.As(terminal.err, &closed) || closed.Code != websocket.CloseMessageTooBig {
							t.Fatal("1009 cause was lost")
						}
					}
				}
				select {
				case <-loopDone:
				case <-time.After(5 * time.Second):
					t.Fatal("reader did not stop after delivery or cancellation")
				}
			})
		}
	}
}
