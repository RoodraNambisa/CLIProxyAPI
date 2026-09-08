package live

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
)

func scopedLiveToken(t *testing.T, h *Handler, model string) (string, ClientSecretAuthorization) {
	t.Helper()
	session, _ := json.Marshal(map[string]any{"type": "realtime", "model": model, "instructions": "scoped instructions", "audio": map[string]any{"output": map[string]any{"voice": "marin"}}})
	token, a, errPrepare := prepareClientSecret(h.secretRandom, h.clientSecrets.now(), session, model, callOwner{1}, clientSecretDefaultLifetime)
	if errPrepare != nil {
		t.Fatal(errPrepare)
	}
	if errPut := h.clientSecrets.put(token, a); errPut != nil {
		t.Fatal(errPut)
	}
	grant, errAuth := h.clientSecrets.authenticate(token)
	if errAuth != nil {
		t.Fatal(errAuth)
	}
	return token, grant
}

func serveClientSecretDirect(t *testing.T, h *Handler) string {
	t.Helper()
	engine := gin.New()
	engine.Use(func(c *gin.Context) {
		grant, handled, errAuth := h.AuthenticateClientSecret(c.Request)
		if errAuth != nil || !handled {
			h.WriteClientSecretError(c, errAuth)
			return
		}
		h.ApplyClientSecretAuthorization(c, grant)
	})
	engine.GET("/v1/realtime", h.HandleRealtimeWebsocket)
	server := httptest.NewServer(engine)
	t.Cleanup(server.Close)
	return "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/realtime"
}

func TestLiveClientSecretDirectUsesBoundAliasAndInitialSession(t *testing.T) {
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	var calls atomic.Int32
	h, manager, _, authID := newLiveCallsFixture(t, cfg, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Query().Get("model") != registry.CodexRealtimeModelID || r.Header.Get("Authorization") != "Bearer fixture-oauth" {
			t.Error("bound direct model or upstream auth changed")
		}
		upgrader := websocket.Upgrader{}
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Error(errUpgrade)
			return
		}
		defer closeRealtimeSocket(conn)
		_, body, errRead := conn.ReadMessage()
		if errRead != nil {
			t.Error(errRead)
			return
		}
		var event struct {
			Type    string                     `json:"type"`
			Session map[string]json.RawMessage `json:"session"`
		}
		if errJSON := json.Unmarshal(body, &event); errJSON != nil || event.Type != "session.update" || event.Session["model"] != nil || string(event.Session["instructions"]) != `"scoped instructions"` || !bytes.Contains(event.Session["audio"], []byte("marin")) {
			t.Error("credential initial session was not applied correctly")
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"session.updated"}`))
		_, _, _ = conn.ReadMessage()
	})
	a, _ := manager.GetByID(authID)
	a.Prefix = "team"
	if _, errUpdate := manager.Update(t.Context(), a); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	manager.SetOAuthModelAlias(map[string][]config.OAuthModelAlias{"codex": {{Name: registry.CodexLiveModelID, Alias: "voice"}}})
	registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: "team/voice", UpstreamID: registry.CodexLiveModelID, Type: registry.CodexRealtimeModelType}})
	token, _ := scopedLiveToken(t, h, "team/voice")
	endpoint := serveClientSecretDirect(t, h)
	for _, query := range []string{"", "?model=team%2Fvoice"} {
		conn, response, errDial := websocket.DefaultDialer.Dial(endpoint+query, http.Header{"Authorization": {"Bearer " + token}})
		closeRealtimeResponse(response)
		if errDial != nil {
			t.Fatal("bound direct connection failed")
		}
		_, body, errRead := conn.ReadMessage()
		closeRealtimeSocket(conn)
		if errRead != nil || !bytes.Contains(body, []byte("session.updated")) {
			t.Fatal("native session acknowledgement was not relayed")
		}
	}
	_, response, errDial := websocket.DefaultDialer.Dial(endpoint+"?model=another-model", http.Header{"Authorization": {"Bearer " + token}})
	if errDial == nil || response == nil || response.StatusCode != 403 || calls.Load() != 2 {
		t.Fatal("credential selected a model outside its scope")
	}
	closeRealtimeResponse(response)
}

func TestLiveClientSecretDirectRevokedDuringHandshakeDoesNotUpgradeClient(t *testing.T) {
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	closed := make(chan struct{})
	h, manager, e, _ := newLiveCallsFixture(t, cfg, func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Error(errUpgrade)
			return
		}
		defer closeRealtimeSocket(conn)
		_, _, _ = conn.ReadMessage()
		close(closed)
	})
	token, grant := scopedLiveToken(t, h, registry.CodexRealtimeModelID)
	manager.RegisterExecutor(&sidebandObserverExecutor{callObserverExecutor: e, afterDial: func() { h.clientSecrets.remove(token, grant.principal) }})
	endpoint := serveClientSecretDirect(t, h)
	conn, response, errDial := websocket.DefaultDialer.Dial(endpoint, http.Header{"Authorization": {"Bearer " + token}})
	closeRealtimeSocket(conn)
	if errDial == nil || response == nil || response.StatusCode != 401 {
		t.Fatal("revoked grant completed client upgrade")
	}
	closeRealtimeResponse(response)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("revoked setup leaked its upstream socket")
	}
}
