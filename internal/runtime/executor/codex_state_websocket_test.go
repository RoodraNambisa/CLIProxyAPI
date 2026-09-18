package executor

import (
	"errors"
	"net/http"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexstate"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestCodexStateWebsocketDoesNotReuseManagedModelForUnmanagedRequest(t *testing.T) {
	enforce := false
	cfg := &config.Config{Codex: config.CodexConfig{EnforceSoftwareIdentity: &enforce}}
	exec := NewCodexWebsocketsExecutor(cfg)
	a := &auth.Auth{ID: "state-ws-isolation", Provider: "codex"}
	conn := &websocket.Conn{}
	sess := &codexWebsocketSession{conn: conn, readerConn: conn, authID: a.ID, wsURL: "ws://unused.invalid", proxyIdentity: websocketProxyIdentity(cfg, a), managedStateModel: "managed-model"}
	_, _, err := exec.ensureUpstreamConn(core.WithRequiredUpstreamWebsocket(t.Context()), a, sess, a.ID, sess.wsURL, http.Header{}, "unmanaged-model")
	var replay *core.UpstreamWebsocketReplayRequiredError
	if !errors.As(err, &replay) {
		t.Fatalf("model transition reused a managed handshake: %v", err)
	}
}

func TestCodexStateWebsocketMissingAdmissionClearsPendingDial(t *testing.T) {
	cfg := &config.Config{Codex: config.CodexConfig{StateOverride: config.CodexStateOverrideConfig{Enabled: true, MissingPolicy: "error"}}}
	a := &auth.Auth{ID: "state-ws-admission", Provider: "codex"}
	r := registry.GetGlobalRegistry()
	r.RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "model"}})
	defer r.UnregisterClient(a.ID)
	codexstate.Default.Sync(cfg.Codex.StateOverride, []codexstate.Credential{helps.StateCredential(a, "model")})
	defer codexstate.Default.Sync(config.CodexStateOverrideConfig{}, nil)
	sess := &codexWebsocketSession{}
	_, _, err := NewCodexWebsocketsExecutor(cfg).ensureUpstreamConn(t.Context(), a, sess, a.ID, "ws://unused.invalid", http.Header{}, "model")
	if !core.PreserveErrorResponse(err) || sess.pendingAuthID != "" || sess.pendingAuthInstanceID != "" {
		t.Fatalf("missing-state admission retained pending dial: %v", err)
	}
}
