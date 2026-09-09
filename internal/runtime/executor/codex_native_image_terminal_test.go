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

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestCodexNativeImageStreamUsesResponseTerminalClassification(t *testing.T) {
	for _, endpoint := range []string{codexOpenAIImageGenerations, codexOpenAIImageEdits} {
		for _, tc := range []struct {
			name, body string
			status     int
			skip       bool
		}{
			{"partial", `{"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"max_tokens"},"output":[]}}`, 0, false},
			{"quota", `{"type":"response.done","status":429,"response":{"status":"cancelled","error":null}}`, 429, false},
			{"cancelled", `{"type":"response.done","response":{"status":"cancelled","error":null}}`, 502, true},
			{"policy", `{"type":"response.failed","response":{"status":"failed","error":{"code":"misalignment_policy_violation","message":"denied"}}}`, 500, false},
		} {
			t.Run(endpoint+"/"+tc.name, func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/"+endpoint {
						t.Error("wrong native image route")
					}
					_, _ = io.Copy(io.Discard, r.Body)
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = fmt.Fprintf(w, "data: %s\n\n", tc.body)
					w.(http.Flusher).Flush()
					<-r.Context().Done()
				}))
				defer server.Close()
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				executor := NewCodexExecutor(&config.Config{})
				auth := &coreauth.Auth{ID: t.Name(), Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL}}
				result, err := executor.ExecuteStream(ctx, auth, core.Request{Model: "gpt-image-2", Payload: []byte(`{"model":"gpt-image-2","prompt":"fixture","images":[{"file_id":"fixture"}]}`)}, core.Options{SourceFormat: translator.FromString(codexOpenAIImageSourceFormat), Alt: endpoint, Metadata: map[string]any{core.StreamTerminalMarkerMetadataKey: true}})
				if err != nil {
					t.Fatal(err)
				}
				markers := 0
				for chunk := range result.Chunks {
					if chunk.Err != nil {
						err = chunk.Err
					}
					if core.IsSuccessfulStreamTerminalChunk(chunk) {
						markers++
					}
				}
				if ctx.Err() != nil {
					t.Fatal("native image stream waited for upstream close instead of its terminal")
				}
				if tc.status == 0 {
					if err != nil || markers != 1 {
						t.Fatal("valid partial image response did not terminate cleanly")
					}
					return
				}
				classified, ok := err.(interface {
					StatusCode() int
					SkipAuthResult() bool
				})
				if !ok || classified.StatusCode() != tc.status || classified.SkipAuthResult() != tc.skip || markers != 0 {
					t.Fatal("native image route lost the terminal error status or scope")
				}
				if tc.name == "policy" && (!coreauth.IsPolicyRefusalError(err) || !strings.Contains(err.Error(), "misalignment_policy_violation")) {
					t.Fatal("native image policy refusal was lost")
				}
			})
		}
	}
}
