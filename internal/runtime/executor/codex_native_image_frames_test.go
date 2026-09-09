package executor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestCodexNativeImageFramesKeepMultilineDataAndEventErrors(t *testing.T) {
	for _, endpoint := range []string{codexOpenAIImageGenerations, codexOpenAIImageEdits} {
		for index, ending := range []string{"\n", "\r", "\r\n"} {
			for _, refusal := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%d/refusal=%t", endpoint, index, refusal), func(t *testing.T) {
					lines := []string{`event: response.incomplete`, `data: {"type":"response.incomplete",`, `data: "response":{"status":"incomplete","incomplete_details":{"reason":"max_tokens"},"output":[],"usage":{"input_tokens":2,"output_tokens":3}}}`}
					if refusal {
						lines = []string{`event: error`, `data: {"code":"misalignment_policy_violation",`, `data: "message":"denied"}`}
					}
					wire := strings.Join(lines, ending) + ending + ending
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						_, _ = io.Copy(io.Discard, r.Body)
						w.Header().Set("Content-Type", "text/event-stream")
						for offset := 0; offset < len(wire); offset += 7 {
							end := min(offset+7, len(wire))
							if _, err := io.WriteString(w, wire[offset:end]); err != nil {
								return
							}
							w.(http.Flusher).Flush()
						}
						if ending == "\r" {
							// Resolve the existing framer's CR/CRLF lookahead without closing the peer.
							_, _ = io.WriteString(w, ":")
							w.(http.Flusher).Flush()
						}
						<-r.Context().Done()
					}))
					defer server.Close()
					ctx, cancel := context.WithTimeout(t.Context(), time.Second)
					defer cancel()
					executor := NewCodexExecutor(&config.Config{})
					auth := &coreauth.Auth{ID: t.Name(), Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL}}
					result, err := executor.ExecuteStream(ctx, auth, core.Request{Model: "gpt-image-2", Payload: []byte(`{"model":"gpt-image-2","prompt":"fixture"}`)}, core.Options{SourceFormat: translator.FromString(codexOpenAIImageSourceFormat), Alt: endpoint, Metadata: map[string]any{core.StreamTerminalMarkerMetadataKey: true}})
					if err != nil {
						t.Fatal(err)
					}
					var output []byte
					markers := 0
					for chunk := range result.Chunks {
						if chunk.Err != nil {
							err = chunk.Err
						}
						if core.IsSuccessfulStreamTerminalChunk(chunk) {
							markers++
						} else if !core.IsBootstrapCommitStreamChunk(chunk) {
							output = append(output, chunk.Payload...)
						}
					}
					if ctx.Err() != nil {
						t.Fatal("parsed terminal did not end the stream")
					}
					if refusal {
						if !coreauth.IsPolicyRefusalError(err) || markers != 0 {
							t.Fatal("event-level policy refusal was lost")
						}
						return
					}
					if err != nil || markers != 1 || !bytes.Equal(output, []byte(wire)) {
						t.Fatal("valid multiline image stream was rejected or changed")
					}
				})
			}
		}
	}
}
