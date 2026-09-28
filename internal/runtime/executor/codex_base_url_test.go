package executor

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestCodexBaseURLRoutesAllModelTransports(t *testing.T) {
	const response = `{"id":"fixture","status":"completed","model":"gpt-5.4","output":[],"usage":{"input_tokens":1,"output_tokens":1}}`
	for _, override := range []bool{false, true} {
		for _, mode := range []string{"http", "stream", "compact", "images/generations", "images/edits", "images/generations-stream", "images/edits-stream", "alpha/search", "websocket", "websocket-stream"} {
			t.Run(fmt.Sprintf("%s/override=%t", mode, override), func(t *testing.T) {
				wire := make(chan *http.Request, 1)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					wire <- r.Clone(context.Background())
					if websocket.IsWebSocketUpgrade(r) {
						upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
						conn, err := upgrader.Upgrade(w, r, nil)
						if err != nil {
							t.Error(err)
							return
						}
						defer func() {
							if errClose := conn.Close(); errClose != nil {
								t.Error(errClose)
							}
						}()
						if _, _, errRead := conn.ReadMessage(); errRead != nil {
							t.Error(errRead)
							return
						}
						if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":`+response+`}`)); errWrite != nil {
							t.Error(errWrite)
						}
						return
					}
					_, _ = io.Copy(io.Discard, r.Body)
					if mode == "compact" {
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, response)
						return
					}
					if mode == "alpha/search" {
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, `{"output":"fixture"}`)
						return
					}
					if strings.HasPrefix(mode, "images/") && !strings.HasSuffix(mode, "-stream") {
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, `{"data":[{"b64_json":"Zml4dHVyZQ=="}]}`)
						return
					}
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: "+`{"type":"response.completed","response":`+response+`}`+"\n\ndata: [DONE]\n\n")
				}))
				defer server.Close()
				cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}, Codex: config.CodexConfig{BaseURL: server.URL + "/global/codex"}}
				a := &auth.Auth{ID: t.Name(), Provider: "codex", Metadata: map[string]any{"access_token": "fixture-access", "account_id": "fixture-account"}}
				prefix := "/global/codex"
				if override {
					prefix = "/credential/codex"
					a.Metadata["base_url"] = server.URL + prefix + "/"
				}
				req := core.Request{Model: "gpt-5.4", Payload: []byte(`{"model":"gpt-5.4","input":[]}`)}
				opts := core.Options{SourceFormat: translator.FormatCodex}
				suffix := "/responses"
				if mode == "compact" {
					opts.Alt = "responses/compact"
					suffix = "/responses/compact"
				}
				if mode == "alpha/search" {
					opts.SourceFormat = translator.FormatCodexAlphaSearch
					suffix = "/alpha/search"
				}
				if strings.HasPrefix(mode, "images/") {
					opts.SourceFormat = translator.FromString(codexOpenAIImageSourceFormat)
					opts.Alt = strings.TrimSuffix(mode, "-stream")
					suffix = "/" + opts.Alt
					req.Model = "gpt-image-2"
					req.Payload = []byte(`{"model":"gpt-image-2","prompt":"fixture"}`)
				}
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				// Never allow an HTTP regression to contact a real credential endpoint.
				ctx = context.WithValue(ctx, "cliproxy.roundtripper", alphaSearchRoundTripper(func(r *http.Request) (*http.Response, error) {
					if r.URL.Host != strings.TrimPrefix(server.URL, "http://") {
						return nil, fmt.Errorf("unexpected upstream host")
					}
					return http.DefaultTransport.RoundTrip(r)
				}))
				var exec auth.ProviderExecutor = NewCodexExecutor(cfg)
				if strings.HasPrefix(mode, "websocket") {
					exec = NewCodexWebsocketsExecutor(cfg)
				}
				if mode == "stream" || strings.HasSuffix(mode, "-stream") {
					result, err := exec.ExecuteStream(ctx, a, req, opts)
					if err != nil {
						t.Fatal(err)
					}
					for chunk := range result.Chunks {
						if chunk.Err != nil {
							t.Fatal(chunk.Err)
						}
					}
				} else {
					if _, err := exec.Execute(ctx, a, req, opts); err != nil {
						t.Fatal(err)
					}
				}
				select {
				case r := <-wire:
					if r.URL.Path != prefix+suffix || r.Header.Get("Authorization") != "Bearer fixture-access" {
						t.Fatalf("incorrect route or authentication: %s", r.URL.Path)
					}
				case <-ctx.Done():
					t.Fatal("upstream was not called")
				}
			})
		}
	}
}
