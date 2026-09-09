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
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestCodexUsageStreamModeThroughDirectExecutors(t *testing.T) {
	for _, mode := range []string{"http", "sse", "ws", "ws-stream", "image", "image-stream"} {
		for _, pinned := range []int{-1, 0, 1} {
			t.Run(fmt.Sprintf("%s/pinned=%d", mode, pinned), func(t *testing.T) {
				collector := &alphaUsageCollector{authID: t.Name()}
				usage.RegisterPlugin(collector)
				t.Cleanup(func() { collector.mu.Lock(); collector.closed = true; collector.records = nil; collector.mu.Unlock() })
				websocketMode := strings.HasPrefix(mode, "ws")
				stream := mode == "sse" || mode == "ws-stream" || mode == "image-stream"
				terminal := []byte(`{"type":"response.completed","response":{"id":"fixture","status":"completed","output":[],"usage":{"input_tokens":2,"output_tokens":1,"total_tokens":3}}}`)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if websocketMode {
						upgrader := websocket.Upgrader{}
						conn, err := upgrader.Upgrade(w, r, nil)
						if err != nil {
							t.Error(err)
							return
						}
						defer func() { _ = conn.Close() }()
						if _, _, errRead := conn.ReadMessage(); errRead == nil {
							_ = conn.WriteMessage(websocket.TextMessage, terminal)
						}
						return
					}
					_, _ = io.Copy(io.Discard, r.Body)
					if mode == "image" {
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, `{"data":[{"b64_json":"fixture"}],"usage":{"input_tokens":2,"output_tokens":1,"total_tokens":3}}`)
						return
					}
					w.Header().Set("Content-Type", "text/event-stream")
					if mode == "image-stream" {
						_, _ = io.WriteString(w, "data: {\"type\":\"image_generation.partial_image\",\"b64_json\":\"fixture\"}\n\ndata: {\"type\":\"image_generation.completed\",\"usage\":{\"input_tokens\":2,\"output_tokens\":1,\"total_tokens\":3}}\n\ndata: [DONE]\n\n")
						return
					}
					_, _ = fmt.Fprintf(w, "data: %s\n\n", terminal)
				}))
				defer server.Close()
				ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
				defer cancel()
				want := stream
				if pinned >= 0 {
					want = pinned == 1
					ctx = usage.WithStream(ctx, want)
				}
				auth := &coreauth.Auth{ID: collector.authID, Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
				var executor coreauth.ProviderExecutor = NewCodexExecutor(&config.Config{})
				if websocketMode {
					executor = NewCodexWebsocketsExecutor(&config.Config{})
				}
				req := core.Request{Model: "gpt-5.4-mini", Payload: []byte(`{"model":"gpt-5.4-mini","input":"fixture"}`)}
				opts := core.Options{SourceFormat: translator.FormatOpenAIResponse}
				if strings.HasPrefix(mode, "image") {
					req = core.Request{Model: "gpt-image-2", Payload: []byte(`{"model":"gpt-image-2","prompt":"fixture"}`)}
					opts.SourceFormat = translator.FromString(codexOpenAIImageSourceFormat)
					opts.Alt = codexOpenAIImageGenerations
				}
				if stream {
					response, err := executor.ExecuteStream(ctx, auth, req, opts)
					if err != nil {
						t.Fatal(err)
					}
					for chunk := range response.Chunks {
						if chunk.Err != nil {
							t.Fatal(chunk.Err)
						}
					}
				} else if _, err := executor.Execute(ctx, auth, req, opts); err != nil {
					t.Fatal(err)
				}
				if err := usage.DefaultManager().Barrier(ctx); err != nil {
					t.Fatal(err)
				}
				collector.mu.Lock()
				records := append([]usage.Record(nil), collector.records...)
				collector.mu.Unlock()
				if len(records) != 1 || records[0].Stream != want || records[0].Failed || records[0].Detail.TotalTokens != 3 {
					t.Fatal("direct executor lost its logical mode or altered token accounting")
				}
				if strings.HasPrefix(mode, "image") && (records[0].FirstPacketLatency <= 0 || records[0].TTFT < records[0].FirstPacketLatency) {
					t.Fatal("native image content lost its packet or content timing")
				}
			})
		}
	}
}
