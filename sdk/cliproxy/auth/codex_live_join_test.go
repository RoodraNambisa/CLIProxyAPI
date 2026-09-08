package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type liveJoinExecutor struct {
	liveHTTPExecutor
	dial func(context.Context, string, http.Header) (*websocket.Conn, *http.Response, error)
}

func (e *liveJoinExecutor) DialCodexLiveWebsocket(ctx context.Context, _ *Auth, target string, headers http.Header, _ []string) (*websocket.Conn, *http.Response, error) {
	return e.dial(ctx, target, headers)
}

func newLiveJoinLease(t *testing.T, e *liveJoinExecutor, persistent bool) (*Manager, *CodexLiveLease) {
	t.Helper()
	m, _, _ := newCodexLiveLeaseFixture(t)
	e.id = "codex"
	m.RegisterExecutor(e)
	var lease *CodexLiveLease
	var errAcquire error
	if persistent {
		lease, errAcquire = m.AcquireCodexLiveSession(t.Context(), t.Context(), "live-model", core.Options{})
	} else {
		lease, errAcquire = m.AcquireCodexLive(t.Context(), "live-model", core.Options{})
	}
	if errAcquire != nil {
		t.Fatal(errAcquire)
	}
	t.Cleanup(lease.Close)
	return m, lease
}

func TestCodexLiveJoinReusesCommittedSessionAndIndependentConnections(t *testing.T) {
	var joins atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		joins.Add(1)
		upgrader := websocket.Upgrader{}
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Error(errUpgrade)
			return
		}
		defer func() { _ = conn.Close() }()
		kind, body, errRead := conn.ReadMessage()
		if errRead == nil {
			_ = conn.WriteMessage(kind, body)
		}
	}))
	defer server.Close()
	_, lease := newLiveJoinLease(t, &liveJoinExecutor{dial: func(ctx context.Context, target string, headers http.Header) (*websocket.Conn, *http.Response, error) {
		return websocket.DefaultDialer.DialContext(ctx, target, headers)
	}}, true)
	if errCommit := lease.CommitUpstream(); errCommit != nil {
		t.Fatal(errCommit)
	}
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithCancel(t.Context())
		conn, response, errDial := lease.DialSessionWebsocket(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil, nil)
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		if errDial != nil {
			cancel()
			t.Fatal(errDial)
		}
		cancel()
		if errWrite := conn.WriteMessage(websocket.TextMessage, []byte("native-event")); errWrite != nil {
			t.Fatal(errWrite)
		}
		_, body, errRead := conn.ReadMessage()
		_ = conn.Close()
		if errRead != nil || string(body) != "native-event" || lease.Context().Err() != nil {
			t.Fatal("join setup context invalidated a committed session")
		}
	}
	if joins.Load() != 2 {
		t.Fatal("session join was retried or suppressed")
	}
}

func TestCodexLiveJoinCancellationAndRetirementInterruptOnlyPendingAttempt(t *testing.T) {
	for _, retire := range []bool{false, true} {
		t.Run(map[bool]string{false: "caller", true: "credential"}[retire], func(t *testing.T) {
			started := make(chan struct{})
			m, lease := newLiveJoinLease(t, &liveJoinExecutor{dial: func(ctx context.Context, _ string, _ http.Header) (*websocket.Conn, *http.Response, error) {
				close(started)
				<-ctx.Done()
				return nil, nil, context.Cause(ctx)
			}}, true)
			if errCommit := lease.CommitUpstream(); errCommit != nil {
				t.Fatal(errCommit)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, _, errDial := lease.DialSessionWebsocket(ctx, "wss://fixture.invalid/call", nil, nil)
				done <- errDial
			}()
			<-started
			if retire {
				if errDelete := m.Delete(t.Context(), lease.CloneAuth().ID); errDelete != nil {
					t.Fatal(errDelete)
				}
			} else {
				cancel()
			}
			select {
			case errDial := <-done:
				if errDial == nil {
					t.Fatal("cancelled join succeeded")
				}
			case <-time.After(time.Second):
				t.Fatal("pending join ignored cancellation")
			}
			if !retire && lease.Context().Err() != nil {
				t.Fatal("cancelled join closed persistent session")
			}
		})
	}
}

func TestCodexLiveJoinCannotBypassDirectAttemptOrUncommittedCapacity(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		e := &liveJoinExecutor{dial: func(context.Context, string, http.Header) (*websocket.Conn, *http.Response, error) {
			t.Fatal("uncommitted join reached executor")
			return nil, nil, nil
		}}
		_, lease := newLiveJoinLease(t, e, persistent)
		if !persistent {
			if errCommit := lease.CommitUpstream(); errCommit != nil {
				t.Fatal(errCommit)
			}
		}
		if _, _, errDial := lease.DialSessionWebsocket(t.Context(), "wss://fixture.invalid", nil, nil); errDial == nil {
			t.Fatal("invalid lease joined a session")
		}
		if _, _, errDial := lease.DialSessionWebsocket(nil, "wss://fixture.invalid", nil, nil); errDial == nil {
			t.Fatal("nil join context was accepted")
		}
	}
	var attempts atomic.Int32
	want := errors.New("fixture handshake rejected")
	_, lease := newLiveJoinLease(t, &liveJoinExecutor{dial: func(context.Context, string, http.Header) (*websocket.Conn, *http.Response, error) {
		attempts.Add(1)
		return nil, nil, want
	}}, false)
	if _, _, errDial := lease.DialWebsocket("wss://fixture.invalid", nil, nil); !errors.Is(errDial, want) {
		t.Fatal("original handshake error changed")
	}
	if _, _, errDial := lease.DialWebsocket("wss://fixture.invalid", nil, nil); errDial == nil || attempts.Load() != 1 {
		t.Fatal("direct lease gained an extra attempt")
	}
}
