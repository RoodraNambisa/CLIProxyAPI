package executor

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestXAIExplicitParametersSurviveComposition(t *testing.T) {
	auth := &coreauth.Auth{ID: "fixture", Provider: "xai"}
	exec := NewXAIExecutor(&config.Config{})
	for _, stream := range []bool{false, true} {
		for _, include := range []string{"", `,"include":[]`, `,"include":["reasoning.encrypted_content"]`} {
			payload := []byte(`{"input":"hello","reasoning":{"summary":"auto"},"service_tier":"priority","safety_identifier":"safe"` + include + `}`)
			p, err := exec.prepareResponsesRequest(t.Context(), auth, core.Request{Model: "grok-4.7", Payload: payload}, core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse}, stream)
			if err != nil {
				t.Fatal(err)
			}
			for _, path := range []string{"reasoning.summary", "service_tier", "safety_identifier"} {
				if gjson.GetBytes(p.body, path).String() != gjson.GetBytes(payload, path).String() {
					t.Fatalf("lost %s: %s", path, p.body)
				}
			}
			if got := gjson.GetBytes(p.body, "include"); include == "" && got.Exists() || include != "" && got.Raw != gjson.GetBytes(payload, "include").Raw {
				t.Fatalf("include: %s", p.body)
			}
		}
	}
	for _, kind := range []string{"json_object", "text", "json_schema"} {
		payload := []byte(`{"messages":[{"role":"user","content":"hi"}],"response_format":{"type":"` + kind + `","json_schema":{"name":"n","schema":{"type":"object"}}}}`)
		p, err := exec.prepareResponsesRequest(t.Context(), auth, core.Request{Model: "grok-4.7", Payload: payload}, core.Options{SourceFormat: sdktranslator.FormatOpenAI}, true)
		if err != nil {
			t.Fatal(err)
		}
		if gjson.GetBytes(p.body, "text.format.type").String() != kind {
			t.Fatalf("response_format %s lost: %s", kind, p.body)
		}
	}
}

func TestXAIHeaderPreviewUsesTheActualResponsesStreamingTransport(t *testing.T) {
	auth := &coreauth.Auth{ID: "preview", Provider: "xai", Attributes: map[string]string{"api_key": "fixture", "base_url": "https://api.x.ai/v1"}}
	ctx := helps.WithXAIDiagnostics(t.Context(), true, nil)
	result, err := NewXAIExecutor(&config.Config{}).PreviewHeaders(ctx, auth, helps.XAIHeaderPreviewInput{Model: "grok-4.7", Purpose: "inference", Protocol: "responses", Stream: false})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, header := range result.Headers {
		if header.Name == "Accept" {
			found = header.Value == "text/event-stream"
		}
	}
	if !found {
		t.Fatal("non-streaming downstream preview did not match upstream SSE execution")
	}
}

func TestXAIPayloadDefaultsAndOverridesRetainInclude(t *testing.T) {
	for _, override := range []bool{false, true} {
		cfg := &config.Config{}
		rule := config.PayloadRule{Models: []config.PayloadModelRule{{Name: "grok-4.7"}}, Params: map[string]any{"include": []string{"reasoning.encrypted_content"}, "service_tier": "priority"}}
		if override {
			cfg.Payload.Override = []config.PayloadRule{rule}
		} else {
			cfg.Payload.Default = []config.PayloadRule{rule}
		}
		for _, explicit := range []bool{false, true} {
			body := []byte(`{"input":"hello"}`)
			if explicit {
				body = []byte(`{"input":"hello","include":[],"service_tier":"default"}`)
			}
			p, err := NewXAIExecutor(cfg).prepareResponsesRequest(t.Context(), &coreauth.Auth{}, core.Request{Model: "grok-4.7", Payload: body}, core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse}, true)
			if err != nil {
				t.Fatal(err)
			}
			if override || !explicit {
				if gjson.GetBytes(p.body, "include.0").String() != "reasoning.encrypted_content" || gjson.GetBytes(p.body, "service_tier").String() != "priority" {
					t.Fatalf("override=%v explicit=%v: %s", override, explicit, p.body)
				}
			} else if gjson.GetBytes(p.body, "include").Raw != "[]" || gjson.GetBytes(p.body, "service_tier").String() != "default" {
				t.Fatal(string(p.body))
			}
		}
	}
}

func TestXAIImagesUseSelectedCapabilitiesAndPreserveLegacyOmission(t *testing.T) {
	var quality string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		quality = gjson.GetBytes(b, "quality").String()
		_, _ = io.WriteString(w, `{"data":[{"url":"https://example.test/image"}]}`)
	}))
	defer server.Close()
	auth := &coreauth.Auth{ID: "image", Provider: "xai", Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
	exec := NewXAIExecutor(&config.Config{})
	req := core.Request{Model: "grok-imagine-image", Payload: []byte(`{"prompt":"hi","image":{"file_id":"f"},"quality":"medium"}`)}
	if _, err := exec.executeImages(t.Context(), auth, req, core.Options{}, "/images/edits"); err != nil {
		t.Fatal(err)
	}
	if quality != "" {
		t.Fatal("legacy fallback gained quality")
	}
	catalog, err := helps.ParseXAIModels([]byte(`{"data":[{"id":"grok-imagine-image","image_capabilities":{"quality_values":["medium"],"max_edit_images":1}}]}`), server.URL+"/models")
	if err != nil {
		t.Fatal(err)
	}
	auth.Metadata = map[string]any{helps.XAIModelCatalogKey: catalog}
	if _, err := exec.executeImages(t.Context(), auth, req, core.Options{}, "/images/edits"); err != nil {
		t.Fatal(err)
	}
	if quality != "medium" {
		t.Fatal("explicit supported quality was discarded")
	}
	req.Payload = []byte(`{"prompt":"hi","images":[{"file_id":"f"},{"url":"https://example.test/i"}]}`)
	if _, err := exec.executeImages(t.Context(), auth, req, core.Options{}, "/images/edits"); err == nil {
		t.Fatal("credential limit not applied")
	}
}

func TestXAIActualDiagnosticsCompressionRedirectAndRedaction(t *testing.T) {
	var sent int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/settings":
			_, _ = io.WriteString(w, `{"accept_request_encodings":["zstd"]}`)
		case "/responses":
			if r.Header.Get("Content-Encoding") != "zstd" {
				t.Error("not compressed")
			}
			http.Redirect(w, r, "/redirected", 307)
		default:
			b, _ := io.ReadAll(r.Body)
			sent = int64(len(b))
			if r.Header.Get("Content-Encoding") != "" {
				t.Error("redirect kept compression")
			}
			_, _ = io.WriteString(w, `{}`)
		}
	}))
	defer server.Close()
	auth := &coreauth.Auth{ID: "diag", Provider: "xai", Attributes: map[string]string{"base_url": server.URL, "api_key": "private-token"}, Metadata: map[string]any{"sub": "private-account"}}
	cfg := &config.Config{XAI: config.XAIConfig{RequestCompression: "auto"}}
	var diagnostics []helps.XAITransmission
	ctx := helps.WithXAIDiagnostics(t.Context(), false, func(v helps.XAITransmission) {
		for i := range diagnostics {
			if diagnostics[i].ID == v.ID {
				diagnostics[i] = v
				return
			}
		}
		diagnostics = append(diagnostics, v)
	})
	payload := []byte(`{"model":"grok-4.7","input":"` + strings.Repeat("x", 70000) + `"}`)
	req, _ := http.NewRequestWithContext(ctx, "POST", server.URL+"/responses", bytes.NewReader(payload))
	applyXAIChatHeaders(req, auth, "private-token", false, "", cfg)
	req.Header.Set("Cookie", "__oailb=private-cookie")
	resp, err := executeXAIHTTPRequest(server.Client(), req, auth, core.Options{}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if len(diagnostics) != 3 || diagnostics[0].Stage != "settings" || diagnostics[1].Compression.Reason != "compressed" || diagnostics[2].Compression.Reason != "redirect_plain" || diagnostics[2].Status != 200 || *diagnostics[2].Compression.SentBytes != sent {
		t.Fatalf("diagnostics: %+v", diagnostics)
	}
	for _, d := range diagnostics {
		for _, h := range d.Headers {
			if strings.Contains(h.Value, "private-") {
				t.Fatalf("secret leaked in %s", h.Name)
			}
		}
	}
}

func TestXAIWebsocketDiagnosticsRetainOriginalHandshake(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	exec := NewXAIWebsocketsExecutor(&config.Config{XAI: config.XAIConfig{Headers: map[string]string{"User-Agent": "custom-secret-value"}}})
	auth := &coreauth.Auth{ID: "ws", Provider: "xai"}
	sess := exec.getOrCreateSession("diag-handshake")
	defer exec.CloseExecutionSession("diag-handshake")
	headers := http.Header{"X-Grok-Req-Id": {"first"}}
	headers.Set("User-Agent", "custom-secret-value")
	conn, _, err := exec.ensureUpstreamConn(t.Context(), auth, sess, auth.ID, "ws"+strings.TrimPrefix(server.URL, "http"), headers)
	if err != nil {
		t.Fatal(err)
	}
	var got helps.XAITransmission
	ctx := helps.WithXAIDiagnostics(t.Context(), false, func(v helps.XAITransmission) { got = v })
	headers.Set("X-Grok-Req-Id", "second")
	again, _, err := exec.ensureUpstreamConn(ctx, auth, sess, auth.ID, "ws"+strings.TrimPrefix(server.URL, "http"), headers)
	if err != nil || again != conn || got.Stage != "websocket_reuse" || got.Status != 101 {
		t.Fatalf("reuse %v %+v", err, got)
	}
	for _, header := range got.Headers {
		if strings.Contains(header.Value, "custom-secret-value") {
			t.Fatal("original custom handshake value leaked")
		}
		if header.Name == "X-Grok-Req-Id" && !strings.Contains(header.Value, helps.XAIIdentityDigest("first")) {
			t.Fatal("computed unsent header presented as handshake")
		}
	}
}
