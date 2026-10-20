package executor

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexCompressionUsesFinalHTTPPayload(t *testing.T) {
	for _, mode := range []string{"", "zstd"} {
		for _, transport := range []string{"http", "stream", "compact"} {
			t.Run(mode+"/"+transport, func(t *testing.T) {
				var calls atomic.Int64
				ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", autoCookieExecutorTransport(func(req *http.Request) (*http.Response, error) {
					calls.Add(1)
					wire, err := io.ReadAll(req.Body)
					if err != nil {
						return nil, err
					}
					_ = req.Body.Close()
					if (req.Header.Get("Content-Encoding") == "zstd") != (mode == "zstd") {
						t.Error("unexpected request encoding")
					}
					if mode == "zstd" {
						decoder, err := zstd.NewReader(nil)
						if err != nil {
							return nil, err
						}
						wire, err = decoder.DecodeAll(wire, nil)
						decoder.Close()
						if err != nil {
							return nil, err
						}
					}
					if gjson.GetBytes(wire, "instructions").String() != "final projected instructions" || gjson.GetBytes(wire, "model").String() != "gpt-6-astra" {
						t.Error("compression preceded final projection")
					}
					body := "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"fixture\",\"output\":[]}}\n\n"
					contentType := "text/event-stream"
					if transport == "compact" {
						body = `{"id":"compact","object":"response.compaction","output":[]}`
						contentType = "application/json"
					}
					return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
				}))
				cfg := &config.Config{Codex: config.CodexConfig{UpstreamRequestCompression: mode}}
				cfg.Payload.Override = []config.PayloadRule{{Models: []config.PayloadModelRule{{Name: "*", Protocol: "codex"}, {Name: "*", Protocol: "openai-response"}}, Params: map[string]any{"instructions": "final projected instructions"}}}
				credential := &auth.Auth{ID: "compression", Provider: "codex", Metadata: map[string]any{"access_token": "fixture"}}
				executor := NewCodexExecutor(cfg)
				request := core.Request{Model: "gpt-6-astra", Payload: []byte(`{"model":"gpt-6-astra","input":[{"role":"user","content":"` + strings.Repeat("hello ", 1024) + `"}]}`)}
				options := core.Options{SourceFormat: translator.FormatCodex}
				if transport == "compact" {
					options.Alt = "responses/compact"
				}
				var err error
				if transport == "stream" {
					var result *core.StreamResult
					result, err = executor.ExecuteStream(ctx, credential, request, options)
					if err == nil {
						for chunk := range result.Chunks {
							if chunk.Err != nil {
								err = chunk.Err
							}
						}
					}
				} else {
					_, err = executor.Execute(ctx, credential, request, options)
				}
				if err != nil {
					t.Fatal(err)
				}
				if calls.Load() != 1 {
					t.Fatalf("unexpected upstream replay: %d", calls.Load())
				}
			})
		}
	}
}
