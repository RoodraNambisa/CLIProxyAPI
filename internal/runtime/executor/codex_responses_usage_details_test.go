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
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexResponsesUsageDetailsAcrossTransports(t *testing.T) {
	for _, mode := range []string{"http", "sse", "ws", "ws-stream", "compact", "image-passthrough"} {
		for _, trust := range []bool{false, true} {
			for _, withUsage := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/trust=%t/usage=%t", mode, trust, withUsage), func(t *testing.T) {
					usageField := ""
					if withUsage {
						usageField = `,"usage":{"input_tokens":2,"output_tokens":1,"total_tokens":3}`
					}
					terminal := []byte(`{"type":"response.completed","response":{"id":"fixture","status":"completed","output":[]` + usageField + `}}`)
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if strings.HasPrefix(mode, "ws") {
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
						if mode == "compact" {
							w.Header().Set("Content-Type", "application/json")
							_, _ = io.WriteString(w, `{"object":"response.compaction","output":[]`+usageField+`}`)
							return
						}
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = fmt.Fprintf(w, "data: %s\n\n", terminal)
					}))
					defer server.Close()
					ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
					defer cancel()
					var executor coreauth.ProviderExecutor = NewCodexExecutor(&config.Config{})
					if strings.HasPrefix(mode, "ws") {
						executor = NewCodexWebsocketsExecutor(&config.Config{})
					}
					auth := &coreauth.Auth{ID: t.Name(), Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
					req := core.Request{Model: "gpt-5.4-mini", Payload: []byte(`{"model":"gpt-5.4-mini","input":"fixture","tools":[{"type":"image_generation"}]}`)}
					opts := core.Options{SourceFormat: translator.FormatOpenAIResponse, Metadata: map[string]any{core.TrustUpstreamSSEMetadataKey: trust, core.ImageGenerationStreamPassthroughMetadataKey: mode == "image-passthrough"}}
					stream := mode == "sse" || mode == "ws-stream" || mode == "image-passthrough"
					var output []byte
					if stream {
						response, err := executor.ExecuteStream(ctx, auth, req, opts)
						if err != nil {
							t.Fatal(err)
						}
						for chunk := range response.Chunks {
							if chunk.Err != nil {
								t.Fatal(chunk.Err)
							}
							output = append(output, chunk.Payload...)
							output = append(output, '\n')
						}
						output, _ = codexSSEFrameDataPayload(output)
						output = []byte(gjson.GetBytes(output, "response").Raw)
					} else {
						if mode == "compact" {
							opts.Alt = "responses/compact"
						}
						response, err := executor.Execute(ctx, auth, req, opts)
						if err != nil {
							t.Fatal(err)
						}
						output = response.Payload
					}
					usage := gjson.GetBytes(output, "usage")
					wantDetails := withUsage && mode != "compact" && !(stream && trust)
					if !gjson.ValidBytes(output) || usage.Exists() != withUsage || usage.Get("input_tokens_details.cached_tokens").Exists() != wantDetails || usage.Get("output_tokens_details.reasoning_tokens").Exists() != wantDetails {
						t.Fatal("usage details, absent usage, compact or trusted-stream behavior changed")
					}
					if withUsage && usage.Get("total_tokens").Int() != 3 {
						t.Fatal("default details changed actual token totals")
					}
				})
			}
		}
	}
}
