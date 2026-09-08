package executor

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
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestCodexLiveLeaseDialsNativeWebsocketWithOAuthAndSafeProtocols(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/v1/realtime" || r.URL.Query().Get("model") != "live-model" || r.Header.Get("Authorization") != "Bearer fixture-oauth" {
			t.Error("native target or selected OAuth authorization changed")
		}
		if r.Header.Get("Sec-Websocket-Protocol") != "realtime" || r.Header.Get("Version") != "0.153.4" {
			t.Error("unsafe subprotocol or incorrect software version")
		}
		upgrader := websocket.Upgrader{Subprotocols: []string{"realtime"}}
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Error(errUpgrade)
			return
		}
		defer func() {
			if errClose := conn.Close(); errClose != nil {
				t.Error(errClose)
			}
		}()
		kind, data, errRead := conn.ReadMessage()
		if errRead != nil {
			t.Error(errRead)
			return
		}
		if string(data) != `{"type":"input_audio_buffer.append","audio":"fixture"}` {
			t.Error("native event was converted into Responses input")
		}
		if errWrite := conn.WriteMessage(kind, data); errWrite != nil {
			t.Error(errWrite)
		}
	}))
	defer upstream.Close()
	cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}}
	m := auth.NewManager(nil, nil, nil)
	m.SetConfig(cfg)
	m.RegisterExecutor(NewCodexAutoExecutor(cfg))
	a := &auth.Auth{ID: "native-live-" + t.Name(), Provider: "codex", Metadata: map[string]any{"access_token": "fixture-oauth", "installation_id": "fixture-installation"}}
	if _, errRegister := m.Register(t.Context(), a); errRegister != nil {
		t.Fatal(errRegister)
	}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "live-model"}})
	defer reg.UnregisterClient(a.ID)
	lease, errAcquire := m.AcquireCodexLive(t.Context(), "live-model", core.Options{})
	if errAcquire != nil {
		t.Fatal(errAcquire)
	}
	defer lease.Close()
	headers := http.Header{"Authorization": {"Bearer downstream-fixture"}, "Sec-Websocket-Protocol": {"openai-insecure-api-key.downstream-fixture"}}
	conn, resp, errDial := lease.DialWebsocket("ws"+strings.TrimPrefix(upstream.URL, "http")+"/v1/realtime?model="+lease.Model(), headers, []string{"openai-insecure-api-key.downstream-fixture", "realtime", "realtime", "openai-project.private-fixture"})
	if resp != nil && resp.Body != nil {
		if errClose := resp.Body.Close(); errClose != nil {
			t.Error(errClose)
		}
	}
	if errDial != nil {
		t.Fatal(errDial)
	}
	defer func() {
		if errClose := conn.Close(); errClose != nil {
			t.Error(errClose)
		}
	}()
	if headers.Get("Authorization") != "Bearer downstream-fixture" || conn.Subprotocol() != "realtime" {
		t.Fatal("caller headers changed or protocol was not negotiated")
	}
	event := []byte(`{"type":"input_audio_buffer.append","audio":"fixture"}`)
	if errWrite := conn.WriteMessage(websocket.TextMessage, event); errWrite != nil {
		t.Fatal(errWrite)
	}
	_, data, errRead := conn.ReadMessage()
	if errRead != nil || string(data) != string(event) || calls.Load() != 1 {
		t.Fatal("native WebSocket event did not round trip exactly once")
	}
	if second, _, errSecond := lease.DialWebsocket("ws"+strings.TrimPrefix(upstream.URL, "http")+"/v1/realtime?model="+lease.Model(), nil, nil); second != nil || errSecond == nil || calls.Load() != 1 {
		t.Fatal("reused one reservation for another WebSocket connection")
	}
}

func TestCodexLiveDialRejectsUnsupportedAuthAndCancellation(t *testing.T) {
	e := NewCodexAutoExecutor(&config.Config{})
	for _, a := range []*auth.Auth{
		{Provider: "codex", Attributes: map[string]string{"api_key": "fixture"}},
		{Provider: "codex", Metadata: map[string]any{"access_token": "fixture", "auth_mode": "agent_identity"}},
	} {
		conn, _, errDial := e.DialCodexLiveWebsocket(t.Context(), a, "ws://127.0.0.1:1/unused", nil, nil)
		var status interface{ StatusCode() int }
		if conn != nil || !errors.As(errDial, &status) || status.StatusCode() != http.StatusNotImplemented {
			t.Fatal("unsupported authentication reached the dialer")
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	conn, _, errDial := e.DialCodexLiveWebsocket(ctx, &auth.Auth{Provider: "codex", Metadata: map[string]any{"access_token": "fixture"}}, "ws://127.0.0.1:1/unused", nil, nil)
	if conn != nil || !errors.Is(errDial, context.Canceled) {
		t.Fatal("cancelled request reached the dialer")
	}
}

func TestCodexLiveDialCancellationInterruptsPendingHTTPUpgrade(t *testing.T) {
	entered, cancelled := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
		close(cancelled)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	executor := NewCodexAutoExecutor(&config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}})
	done := make(chan error, 1)
	go func() {
		conn, response, errDial := executor.DialCodexLiveWebsocket(ctx, &auth.Auth{Provider: "codex", Metadata: map[string]any{"access_token": "fixture"}}, "ws"+strings.TrimPrefix(server.URL, "http"), nil, nil)
		if conn != nil {
			_ = conn.Close()
		}
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		done <- errDial
	}()
	<-entered
	cancel()
	select {
	case errDial := <-done:
		if !errors.Is(errDial, context.Canceled) {
			t.Fatal("handshake lost the cancellation cause")
		}
	case <-time.After(time.Second):
		server.CloseClientConnections()
		<-done
		t.Fatal("cancelled HTTP upgrade waited for the handshake timeout")
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("pending upstream HTTP connection was not closed")
	}
}
