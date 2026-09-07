package executor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexMultiAgentSSEAcrossModesReleaseAndReload(t *testing.T) {
	for _, mode := range []string{"translated", "image-passthrough", "trusted"} {
		for _, enabled := range []bool{false, true} {
			for _, release := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/enabled=%t/release=%t", mode, enabled, release), func(t *testing.T) {
					ready := make(chan struct{})
					sendEvents := sync.OnceFunc(func() { close(ready) })
					captured := make(chan []byte, 1)
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						body, err := io.ReadAll(r.Body)
						if err != nil {
							t.Error(err)
							return
						}
						captured <- body
						w.Header().Set("Content-Type", "text/event-stream")
						w.WriteHeader(200)
						w.(http.Flusher).Flush()
						select {
						case <-ready:
						case <-r.Context().Done():
							return
						}
						namespace := gjson.GetBytes(body, "tools.0.name").String()
						item := fmt.Sprintf(`{"type":"function_call","id":"fc_worker","call_id":"pair","namespace":%q,"name":"spawn_agent","arguments":"{\"message\":\"exact collaboration-optimize business\"}"}`, namespace)
						events := []string{
							fmt.Sprintf(`{"type":"response.output_item.added","output_index":0,"item":%s}`, item),
							`{"type":"response.function_call_arguments.delta","item_id":"fc_worker","delta":"exact"}`,
							fmt.Sprintf(`{"type":"response.output_item.done","output_index":0,"item":%s}`, item),
							fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp_worker","status":"completed","output":[%s],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`, item),
						}
						for _, event := range events {
							if mode == "trusted" {
								left, right, _ := strings.Cut(event, ",")
								_, _ = fmt.Fprintf(w, ": keep\nid: fixture\nretry: 1000\ndata: %s,\ndata: %s\n\n", left, right)
							} else {
								_, _ = fmt.Fprintf(w, "data: %s\n\n", event)
							}
						}
					}))
					defer server.Close()
					defer sendEvents()
					extra := ""
					if mode == "image-passthrough" {
						extra = `,{"type":"image_generation"}`
					}
					raw := []byte(fmt.Sprintf(`{"model":"gpt-5.4","stream":true,"tools":[{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"spawn_agent","parameters":{"properties":{"message":{"type":"string","encrypted":true}}}}]}%s],"input":"create a worker","prompt_cache_key":"stable-cache"}`, extra))
					ctrl := core.NewRequestBodyReleaseController(int64(len(raw)), []byte("<released>"))
					ctx := core.WithRequestBodyReleaseController(t.Context(), ctrl)
					cfg := &config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: enabled, PassthroughPromptCacheKey: true}}
					executor := NewCodexExecutor(cfg)
					opts := core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, OriginalRequest: raw, Stream: true, Headers: http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}}, Metadata: map[string]any{
						core.StreamTerminalMarkerMetadataKey:             true,
						core.TrustUpstreamSSEMetadataKey:                 mode == "trusted",
						core.ImageGenerationStreamPassthroughMetadataKey: mode == "image-passthrough",
					}}
					auth := &cliproxyauth.Auth{ID: "multi-agent-stream", Provider: "codex", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL}}
					result, err := executor.ExecuteStream(ctx, auth, core.Request{Model: "gpt-5.4", Payload: raw}, opts)
					if err != nil {
						t.Fatal(err)
					}
					body := <-captured
					cfg.Codex.OptimizeMultiAgentV2 = !enabled
					if release {
						ctrl.Release()
					}
					sendEvents()
					var output []byte
					var frames [][]byte
					terminals := 0
					for chunk := range result.Chunks {
						if chunk.Err != nil {
							t.Fatalf("stream fixture failed: %v", chunk.Err)
						}
						if core.IsSuccessfulStreamTerminalChunk(chunk) {
							terminals++
							continue
						}
						output = append(output, chunk.Payload...)
						frames = append(frames, bytes.Clone(chunk.Payload))
					}
					want := "collaboration"
					if enabled {
						want = "collaboration-optimize"
					}
					if gjson.GetBytes(body, "tools.0.name").String() != want || gjson.GetBytes(body, "prompt_cache_key").String() != "stable-cache" {
						t.Fatal("outbound declaration or cache key changed incorrectly")
					}
					if mode == "image-passthrough" && gjson.GetBytes(body, "tools.1.type").String() != "image_generation" {
						t.Fatal("fixture did not exercise image passthrough")
					}
					checked := 0
					for _, frame := range frames {
						payload, ok := codexSSEFrameDataPayload(frame)
						if !ok {
							continue
						}
						itemPath := "item"
						if gjson.GetBytes(payload, "type").String() == "response.completed" {
							itemPath = "response.output.0"
						}
						item := gjson.GetBytes(payload, itemPath)
						if !item.Exists() {
							continue
						}
						checked++
						if item.Get("namespace").String() != "collaboration" || item.Get("call_id").String() != "pair" || !strings.Contains(item.Get("arguments").String(), "collaboration-optimize business") {
							t.Fatal("stream tool identity, pairing or business content changed")
						}
						marker := item.Get("encrypted_function_args")
						if enabled && marker.Raw != "[]" || !enabled && marker.Exists() {
							t.Fatal("stream adopted a later policy or lost plaintext metadata")
						}
					}
					if checked != 3 || terminals != 1 {
						t.Fatalf("checked items=%d terminals=%d, want 3/1", checked, terminals)
					}
					if mode == "trusted" && (!bytes.Contains(output, []byte(": keep\n")) || !bytes.Contains(output, []byte("id: fixture\nretry: 1000\n"))) {
						t.Fatal("SSE control fields changed")
					}
				})
			}
		}
	}
}

func TestCodexMultiAgentSSECancellationDiscardsPendingFrame(t *testing.T) {
	flushed, closed := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(closed)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"type\":\"response.output_item.added\",\"item\":\n")
		w.(http.Flusher).Flush()
		close(flushed)
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cfg := &config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: true}}
	raw := []byte(`{"model":"gpt-5.4","stream":true,"input":"hello","tools":[{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"spawn_agent"}]}]}`)
	ctrl := core.NewRequestBodyReleaseController(int64(len(raw)), []byte("<released>"))
	ctx = core.WithRequestBodyReleaseController(ctx, ctrl)
	executor := NewCodexExecutor(cfg)
	auth := &cliproxyauth.Auth{ID: "cancel-fixture", Provider: "codex", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL}}
	result, err := executor.ExecuteStream(ctx, auth, core.Request{Model: "gpt-5.4", Payload: raw}, core.Options{
		Stream: true, SourceFormat: sdktranslator.FormatOpenAIResponse, Headers: http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}},
		Metadata: map[string]any{core.TrustUpstreamSSEMetadataKey: true, core.StreamTerminalMarkerMetadataKey: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	<-flushed
	ctrl.Release()
	cancel()
	for chunk := range result.Chunks {
		if len(chunk.Payload) > 0 || core.IsSuccessfulStreamTerminalChunk(chunk) {
			t.Fatal("cancellation committed an incomplete event")
		}
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not close the local upstream request")
	}
}
