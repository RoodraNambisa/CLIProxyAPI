package executor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestCodexIncrementalRequestCannotOpenNewUpstreamOrFallBackToHTTP(t *testing.T) {
	var connections atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		connections.Add(1)
		w.WriteHeader(http.StatusUpgradeRequired)
	}))
	defer server.Close()
	auth := &auth.Auth{ID: "replay-test", Provider: "codex", Attributes: map[string]string{"api_key": "test", "base_url": server.URL}}
	req := core.Request{Model: "gpt-5.4", Payload: []byte(`{"model":"gpt-5.4","input":[],"previous_response_id":"unavailable"}`)}
	opts := core.Options{SourceFormat: translator.FromString("codex")}
	ws := NewCodexWebsocketsExecutor(&config.Config{})
	for _, stream := range []bool{false, true} {
		var err error
		if stream {
			_, err = ws.ExecuteStream(t.Context(), auth, req, opts)
		} else {
			_, err = ws.Execute(t.Context(), auth, req, opts)
		}
		var replay *core.UpstreamWebsocketReplayRequiredError
		if !errors.As(err, &replay) {
			t.Fatalf("expected replay requirement, got %v", err)
		}
		ctx := core.WithRequiredUpstreamWebsocket(t.Context())
		httpExec := NewCodexExecutor(&config.Config{})
		if stream {
			_, err = httpExec.ExecuteStream(ctx, auth, req, opts)
		} else {
			_, err = httpExec.Execute(ctx, auth, req, opts)
		}
		if !errors.As(err, &replay) {
			t.Fatal("HTTP fallback ignored connection requirement")
		}
	}
	if connections.Load() != 0 {
		t.Fatal("incomplete context caused an upstream network attempt")
	}
}

func TestCodexReplayRequirementGuardsCompactAndKeepsCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("connection requirement reached an HTTP upstream")
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()
	credential := &auth.Auth{ID: "compact-replay", Provider: "codex", Attributes: map[string]string{"api_key": "test", "base_url": server.URL}}
	req := core.Request{Model: "gpt-5.4", Payload: []byte(`{"input":[]}`)}
	opts := core.Options{Alt: "responses/compact"}
	httpExec := NewCodexExecutor(&config.Config{})
	wsExec := NewCodexWebsocketsExecutor(&config.Config{})
	for _, canceled := range []bool{false, true} {
		ctx, cancel := context.WithCancel(core.WithRequiredUpstreamWebsocket(t.Context()))
		if canceled {
			cancel()
		}
		for _, execute := range []func() error{
			func() error { _, err := httpExec.Execute(ctx, credential, req, opts); return err },
			func() error { _, err := httpExec.ExecuteStream(ctx, credential, req, opts); return err },
			func() error { _, err := wsExec.Execute(ctx, credential, req, opts); return err },
		} {
			err := execute()
			var replay *core.UpstreamWebsocketReplayRequiredError
			if canceled && !errors.Is(err, context.Canceled) {
				t.Fatal("local replay rejection replaced cancellation")
			}
			if !canceled && !errors.As(err, &replay) {
				t.Fatal("compact bypassed the connection requirement")
			}
		}
		cancel()
	}
}

func TestCodexReplayFailureKeepsCauseAndHardErrors(t *testing.T) {
	cause := errors.New("closed connection")
	err := helps.CodexWebsocketReplayError(t.Context(), cause)
	var replay *core.UpstreamWebsocketReplayRequiredError
	if !errors.As(err, &replay) || !errors.Is(err, cause) {
		t.Fatal("replay error lost original cause")
	}
	tooBig := statusErr{code: 413, msg: "too big"}
	if got := helps.CodexWebsocketReplayError(t.Context(), tooBig); got != tooBig {
		t.Fatal("size limit was replaced by replay signal")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got := helps.CodexWebsocketReplayError(ctx, cause); !errors.Is(got, context.Canceled) {
		t.Fatal("cancellation was replaced by replay signal")
	}
}

func TestCodexIncrementalConnectionRequiresSameCredentialInstanceAndProxy(t *testing.T) {
	e := NewCodexWebsocketsExecutor(&config.Config{})
	credential := &auth.Auth{ID: "original", Attributes: map[string]string{"api_key": "test", "proxy_url": "direct"}}
	conn := &websocket.Conn{}
	for _, mismatch := range []string{"none", "credential", "instance", "proxy binding", "proxy identity", "URL", "missing connection"} {
		sess := &codexWebsocketSession{conn: conn, readerConn: conn, authID: credential.ID, authInstanceID: credential.RuntimeInstanceID(), proxyBindingID: credential.EffectiveProxyBindingID(), proxyIdentity: websocketProxyIdentity(e.cfg, credential), wsURL: "ws://unused.test/responses"}
		switch mismatch {
		case "credential":
			sess.authID = "different"
		case "instance":
			sess.authInstanceID = "different-instance"
		case "proxy binding":
			sess.proxyBindingID = "different-binding"
		case "proxy identity":
			sess.proxyIdentity = "different-proxy"
		case "URL":
			sess.wsURL = "ws://other.test/responses"
		case "missing connection":
			sess.conn = nil
		}
		got, _, err := e.ensureUpstreamConn(core.WithRequiredUpstreamWebsocket(t.Context()), credential, sess, credential.ID, "ws://unused.test/responses", nil)
		if mismatch == "none" {
			if got != conn || err != nil {
				t.Fatal("matching established connection was not reused")
			}
		} else {
			var replay *core.UpstreamWebsocketReplayRequiredError
			if got != nil || !errors.As(err, &replay) {
				t.Fatalf("%s crossed an upstream context boundary", mismatch)
			}
			if mismatch != "missing connection" && sess.conn != conn {
				t.Fatal("invalid continuation destroyed another target's connection")
			}
		}
	}
}
