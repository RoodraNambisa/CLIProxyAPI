package live

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func serveSidebandFixture(t *testing.T, h *Handler) string {
	t.Helper()
	engine := gin.New()
	engine.Use(func(c *gin.Context) {
		if token := c.GetHeader("Authorization"); token == "Bearer fixture-caller" || token == "Bearer other-caller" {
			c.Set("apiKey", strings.TrimPrefix(token, "Bearer "))
			c.Set("accessProvider", "fixture-access")
		}
	})
	engine.GET("/v1/live/:call_id", h.HandleSideband)
	engine.GET("/v1/realtime/calls/:call_id", h.HandleSideband)
	engine.GET("/v1/realtime", h.HandleRealtimeWebsocket)
	server := httptest.NewServer(engine)
	t.Cleanup(server.Close)
	return server.URL
}

func writeSidebandCall(w http.ResponseWriter) {
	w.Header().Set("Location", "/v1/realtime/calls/call_sideband")
	_, _ = w.Write([]byte("v=0"))
}

func echoSideband(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()
	if r.Header.Get("Authorization") != "Bearer fixture-oauth" || r.Header.Get("Cookie") != "" || r.Header.Get("Sec-Websocket-Protocol") != "realtime" {
		t.Error("sideband credential or safe subprotocol changed")
	}
	upgrader := websocket.Upgrader{Subprotocols: []string{"realtime"}}
	conn, errUpgrade := upgrader.Upgrade(w, r, nil)
	if errUpgrade != nil {
		t.Error(errUpgrade)
		return
	}
	defer closeRealtimeSocket(conn)
	for {
		kind, body, errRead := conn.ReadMessage()
		if errRead != nil {
			return
		}
		if errWrite := conn.WriteMessage(kind, body); errWrite != nil {
			return
		}
	}
}

func TestLiveSidebandExistingConnectionSurvivesDisableAndHangupEndsIt(t *testing.T) {
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	cfg.Routing.PerAuthRequestLimit, cfg.Routing.PerAuthRequestWindowMinutes = 1, 1
	var joins atomic.Int32
	h, _, _, _ := newLiveCallsFixture(t, cfg, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/hangup") {
			w.WriteHeader(204)
			return
		}
		if r.Method == http.MethodPost {
			writeSidebandCall(w)
			return
		}
		joins.Add(1)
		echoSideband(t, w, r)
	})
	if w := callHandlerRequest(t, h, "/v1/live", "application/sdp", strings.NewReader("v=0")); w.Code != 200 {
		t.Fatal("creation failed")
	}
	endpoint := serveSidebandFixture(t, h) + "/v1/realtime?call_id=call_sideband"
	conn, response, errDial := liveTestDial(t, endpoint, "fixture-caller")
	closeRealtimeResponse(response)
	if errDial != nil {
		t.Fatal(errDial)
	}
	defer closeRealtimeSocket(conn)
	h.UpdateConfig(&config.Config{})
	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"input_audio_buffer.append","audio":"fixture"}`)); errWrite != nil {
		t.Fatal(errWrite)
	}
	if _, _, errRead := conn.ReadMessage(); errRead != nil {
		t.Fatal("disable interrupted existing sideband")
	}
	second, rejected, errSecond := liveTestDial(t, endpoint, "fixture-caller")
	closeRealtimeSocket(second)
	if errSecond == nil || rejected == nil || rejected.StatusCode != 503 {
		t.Fatal("disabled existing call admitted a new connection")
	}
	body, _ := io.ReadAll(rejected.Body)
	closeRealtimeResponse(rejected)
	if !strings.Contains(string(body), liveDisabledCode) || joins.Load() != 1 {
		t.Fatal("disabled join contacted upstream")
	}
	c, w := hangupHandlerContext(t, "call_sideband", "fixture-caller")
	h.HandleHangup(c)
	if w.Code != 204 {
		t.Fatal("active sideband prevented hangup")
	}
	closed := make(chan error, 1)
	go func() { _, _, errRead := conn.ReadMessage(); closed <- errRead }()
	select {
	case errRead := <-closed:
		if errRead == nil {
			t.Fatal("hung-up sideband remained open")
		}
	case <-time.After(time.Second):
		t.Fatal("hangup did not close sideband")
	}
}

func TestLiveSidebandAliasesReconnectAndEnforceOwnershipAndBusyState(t *testing.T) {
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	paths := make(chan string, 3)
	h, _, _, _ := newLiveCallsFixture(t, cfg, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			writeSidebandCall(w)
			return
		}
		paths <- r.URL.RequestURI()
		echoSideband(t, w, r)
	})
	if w := callHandlerRequest(t, h, "/v1/live", "application/sdp", strings.NewReader("v=0")); w.Code != 200 {
		t.Fatal("creation failed")
	}
	base := serveSidebandFixture(t, h)
	for _, path := range []string{"/v1/live/call_sideband", "/v1/realtime/calls/call_sideband", "/v1/realtime?call_id=call_sideband"} {
		conn, response, errDial := liveTestDial(t, base+path, "fixture-caller")
		closeRealtimeResponse(response)
		if errDial != nil {
			t.Fatal(errDial)
		}
		for _, tc := range []struct {
			path, caller string
			status       int
		}{{path, "other-caller", 404}, {path, "unauthenticated", 401}, {path, "fixture-caller", 409}, {"/v1/realtime?call_id=", "fixture-caller", 400}} {
			second, rejected, errSecond := liveTestDial(t, base+tc.path, tc.caller)
			closeRealtimeSocket(second)
			if errSecond == nil || rejected == nil || rejected.StatusCode != tc.status {
				t.Fatalf("join guard status mismatch: want %d", tc.status)
			}
			closeRealtimeResponse(rejected)
		}
		closeRealtimeSocket(conn)
		deadline := time.After(time.Second)
		for {
			h.calls.mu.Lock()
			entry := h.calls.entries["call_sideband"]
			released := entry != nil && !entry.claimed
			h.calls.mu.Unlock()
			if released {
				break
			}
			select {
			case <-deadline:
				t.Fatal("disconnected sideband was not released")
			case <-time.After(time.Millisecond):
			}
		}
	}
	for _, want := range []string{"/v1/live/call_sideband", "/v1/realtime/calls/call_sideband", "/v1/realtime?intent=quicksilver&call_id=call_sideband"} {
		if got := <-paths; got != want {
			t.Fatalf("wrong sideband alias: %s", got)
		}
	}
}

func TestLiveSidebandFailedHandshakeLeavesCallAndDoesNotRetry(t *testing.T) {
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	var attempts atomic.Int32
	h, _, _, _ := newLiveCallsFixture(t, cfg, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			writeSidebandCall(w)
			return
		}
		if attempts.Add(1) == 1 {
			w.Header().Set("Retry-After", "6")
			w.WriteHeader(429)
			_, _ = w.Write([]byte(`{"error":{"code":"fixture_rate_limit"}}`))
			return
		}
		echoSideband(t, w, r)
	})
	if w := callHandlerRequest(t, h, "/v1/live", "application/sdp", strings.NewReader("v=0")); w.Code != 200 {
		t.Fatal("creation failed")
	}
	endpoint := serveSidebandFixture(t, h) + "/v1/live/call_sideband"
	conn, response, errDial := liveTestDial(t, endpoint, "fixture-caller")
	closeRealtimeSocket(conn)
	if errDial == nil || response == nil || response.StatusCode != 429 || response.Header.Get("Retry-After") != "6" || attempts.Load() != 1 {
		t.Fatal("sideband rejection lost status or retried")
	}
	closeRealtimeResponse(response)
	conn, response, errDial = liveTestDial(t, endpoint, "fixture-caller")
	closeRealtimeResponse(response)
	if errDial != nil {
		t.Fatal("failed handshake consumed existing call")
	}
	closeRealtimeSocket(conn)
}

type sidebandObserverExecutor struct {
	*callObserverExecutor
	afterDial func()
}

func (e *sidebandObserverExecutor) DialCodexLiveWebsocket(ctx context.Context, a *auth.Auth, target string, headers http.Header, protocols []string) (*websocket.Conn, *http.Response, error) {
	conn, response, errDial := e.CodexAutoExecutor.DialCodexLiveWebsocket(ctx, a, target, headers, protocols)
	if conn != nil && e.afterDial != nil {
		e.afterDial()
	}
	return conn, response, errDial
}

func TestLiveSidebandDisableAfterHandshakeClosesUncommittedConnection(t *testing.T) {
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	closed := make(chan struct{})
	h, manager, e, _ := newLiveCallsFixture(t, cfg, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			writeSidebandCall(w)
			return
		}
		upgrader := websocket.Upgrader{Subprotocols: []string{"realtime"}}
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Error(errUpgrade)
			return
		}
		defer closeRealtimeSocket(conn)
		_, _, _ = conn.ReadMessage()
		close(closed)
	})
	manager.RegisterExecutor(&sidebandObserverExecutor{callObserverExecutor: e, afterDial: func() { h.UpdateConfig(&config.Config{}) }})
	if w := callHandlerRequest(t, h, "/v1/live", "application/sdp", strings.NewReader("v=0")); w.Code != 200 {
		t.Fatal("creation failed")
	}
	endpoint := serveSidebandFixture(t, h) + "/v1/live/call_sideband"
	conn, response, errDial := liveTestDial(t, endpoint, "fixture-caller")
	closeRealtimeSocket(conn)
	if errDial == nil || response == nil || response.StatusCode != 503 {
		t.Fatal("uncommitted sideband escaped disable")
	}
	closeRealtimeResponse(response)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("uncommitted upstream socket leaked")
	}
	h.calls.mu.Lock()
	entry := h.calls.entries["call_sideband"]
	alive := entry != nil && entry.call.lease.Context().Err() == nil
	h.calls.mu.Unlock()
	if !alive {
		t.Fatal("rejected join invalidated established WebRTC call")
	}
}

func TestLiveSidebandPendingHandshakeCancelsOnDisableOrRetirement(t *testing.T) {
	for _, retire := range []bool{false, true} {
		t.Run(map[bool]string{false: "disable", true: "retirement"}[retire], func(t *testing.T) {
			started, cancelled := make(chan struct{}), make(chan struct{})
			cfg := &config.Config{}
			cfg.Codex.LiveEnabled = true
			h, manager, _, authID := newLiveCallsFixture(t, cfg, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					writeSidebandCall(w)
					return
				}
				close(started)
				<-r.Context().Done()
				close(cancelled)
			})
			if w := callHandlerRequest(t, h, "/v1/live", "application/sdp", strings.NewReader("v=0")); w.Code != 200 {
				t.Fatal("creation failed")
			}
			endpoint := serveSidebandFixture(t, h) + "/v1/live/call_sideband"
			finished := make(chan int, 1)
			go func() {
				conn, response, errDial := liveTestDial(t, endpoint, "fixture-caller")
				closeRealtimeSocket(conn)
				status := 0
				if errDial != nil && response != nil {
					status = response.StatusCode
				}
				closeRealtimeResponse(response)
				finished <- status
			}()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("pending join did not begin")
			}
			if retire {
				if errDelete := manager.Delete(t.Context(), authID); errDelete != nil {
					t.Fatal(errDelete)
				}
			} else {
				h.UpdateConfig(&config.Config{})
			}
			select {
			case <-cancelled:
			case <-time.After(time.Second):
				t.Fatal("pending native socket did not cancel")
			}
			select {
			case status := <-finished:
				if status < 400 || (!retire && status != 503) {
					t.Fatalf("wrong cancelled join status: %d", status)
				}
			case <-time.After(time.Second):
				t.Fatal("pending sideband handler did not finish")
			}
		})
	}
}
