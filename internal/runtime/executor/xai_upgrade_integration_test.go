package executor

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestXAISpeechExecutesOnlySpeechAndClassifiesVoiceFault(t *testing.T) {
	var calls atomic.Int32
	var code atomic.Int32
	code.Store(200)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/v1/tts" || r.Header.Get("Accept") != "*/*" {
			t.Errorf("speech request: %s %s", r.URL.Path, r.Header.Get("Accept"))
		}
		body, _ := io.ReadAll(r.Body)
		if gjson.GetBytes(body, "voice_id").String() != "eve" {
			t.Error(string(body))
		}
		w.Header().Set("Content-Type", "audio/mpeg")
		w.WriteHeader(int(code.Load()))
		if code.Load() == 404 {
			_, _ = w.Write([]byte(`{"error":"voice not found"}`))
		} else {
			_, _ = w.Write([]byte("ID3fixture"))
		}
	}))
	defer server.Close()
	auth := &coreauth.Auth{ID: "speech-fixture", Provider: "xai", Attributes: map[string]string{"base_url": server.URL + "/v1", "api_key": "fixture"}}
	cfg := &config.Config{XAI: config.XAIConfig{HeaderDefaults: config.XAIHeaderDefaults{OfficialBuild: true}}}
	exec := NewXAIExecutor(cfg)
	req := core.Request{Model: "grok-tts", Payload: []byte(`{"text":"fixture","voice_id":"eve"}`)}
	opts := core.Options{SourceFormat: sdktranslator.FromString("openai-speech")}
	result, err := exec.Execute(t.Context(), auth, req, opts)
	if err != nil || string(result.Payload) != "ID3fixture" || result.Headers.Get("Content-Type") != "audio/mpeg" {
		t.Fatalf("speech: %+v %v", result, err)
	}
	code.Store(404)
	_, err = exec.Execute(t.Context(), auth, req, opts)
	fault, ok := err.(statusErr)
	if !ok || !fault.SkipAuthResult() || fault.RetryOtherAuth() {
		t.Fatalf("voice error affects credential: %v", err)
	}
	_, err = exec.Execute(t.Context(), auth, req, core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse})
	if err == nil || calls.Load() != 2 {
		t.Fatal("speech-only model reached text endpoint")
	}
	_, err = exec.ExecuteStream(t.Context(), auth, req, opts)
	if err == nil || calls.Load() != 2 {
		t.Fatal("speech allowed on streaming text endpoint")
	}
}

func TestXAIClientSearchRestoredFromUpstreamWebsocket(t *testing.T) {
	var observed atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_, request, err := conn.ReadMessage()
		if err != nil {
			return
		}
		alias := gjson.GetBytes(request, "tools.0.name").String()
		observed.Store(alias != "web_search" && strings.HasPrefix(alias, "client_web_search"))
		event := `{"type":"response.completed","response":{"id":"resp_alias","status":"completed","output":[{"type":"function_call","name":"` + alias + `","call_id":"call_alias","arguments":"{}"}]}}`
		_ = conn.WriteMessage(websocket.TextMessage, []byte(event))
	}))
	defer server.Close()
	exec := NewXAIWebsocketsExecutor(&config.Config{})
	auth := &coreauth.Auth{ID: "alias-fixture", Provider: "xai", Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
	req := core.Request{Model: "grok-4.7", Payload: []byte(`{"input":[],"tools":[{"type":"function","name":"web_search","parameters":{"type":"object"}}]}`)}
	result, err := exec.executeStream(core.WithDownstreamWebsocket(t.Context()), auth, req, core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Stream: true})
	if err != nil {
		t.Fatal(err)
	}
	var output []byte
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatal(chunk.Err)
		}
		output = append(output, chunk.Payload...)
	}
	if !observed.Load() || !bytes.Contains(output, []byte(`"name":"web_search"`)) || bytes.Contains(output, []byte(`"name":"client_web_search`)) {
		t.Fatalf("WS alias: %s", output)
	}
}

func TestXAISpeechCannotBypassRestrictionThroughWebsocket(t *testing.T) {
	exec := NewXAIWebsocketsExecutor(&config.Config{})
	_, err := exec.executeStream(t.Context(), &coreauth.Auth{}, core.Request{Model: "grok-tts"}, core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse})
	if err == nil || !strings.Contains(err.Error(), "audio/speech") {
		t.Fatalf("WS speech restriction: %v", err)
	}
}

func TestXAIWebsocketFailedHandshakeDoesNotPoisonReconnect(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusUpgradeRequired)
			return
		}
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_, _, _ = conn.ReadMessage()
	}))
	defer server.Close()
	exec := NewXAIWebsocketsExecutor(&config.Config{})
	exec.store = &codexWebsocketSessionStore{sessions: map[string]*codexWebsocketSession{}}
	sess := exec.getOrCreateSession("reconnect-fixture")
	auth := &coreauth.Auth{ID: "reconnect-auth", Provider: "xai"}
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	_, response, err := exec.ensureUpstreamConn(t.Context(), auth, sess, auth.ID, wsURL, http.Header{})
	if err == nil || response == nil || response.StatusCode != 426 {
		t.Fatalf("initial handshake: %v", err)
	}
	closeHTTPResponseBody(response, "test handshake body")
	conn, response, err := exec.ensureUpstreamConn(t.Context(), auth, sess, auth.ID, wsURL, http.Header{})
	closeHTTPResponseBody(response, "test handshake body")
	if err != nil || conn == nil {
		t.Fatalf("reconnect failed: %v", err)
	}
	exec.CloseExecutionSession("reconnect-fixture")
}
