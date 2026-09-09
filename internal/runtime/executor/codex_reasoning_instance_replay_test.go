package executor

import (
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexReasoningReplayDoesNotCrossReuploadedCredentialInstance(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			var calls atomic.Int32
			signature := make([]byte, 73)
			signature[0] = 0x80
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, errRead := io.ReadAll(r.Body)
				if errRead != nil {
					t.Error(errRead)
				}
				call := calls.Add(1)
				if gjson.GetBytes(body, `input.#(type=="reasoning")`).Exists() != (call == 2) {
					t.Errorf("call %d crossed a credential instance or lost clone continuity", call)
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = fmt.Fprintf(w, "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"type\":\"reasoning\",\"encrypted_content\":%q},{\"type\":\"message\",\"role\":\"assistant\",\"content\":\"answer\"}]}}\n\n", base64.RawURLEncoding.EncodeToString(signature))
			}))
			defer upstream.Close()
			manager := coreauth.NewManager(nil, nil, nil)
			source := &coreauth.Auth{ID: uuid.NewString(), Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"api_key": "fixture", "base_url": upstream.URL}}
			credential, errRegister := manager.Register(t.Context(), source.Clone())
			if errRegister != nil {
				t.Fatal(errRegister)
			}
			if credential.RuntimeInstanceID() == "" {
				t.Fatal("manager did not create an instance")
			}
			executor := NewCodexExecutor(&config.Config{})
			for call := 1; call <= 3; call++ {
				payload := []byte(`{"messages":[{"role":"user","content":"question"}]}`)
				if call > 1 {
					payload = []byte(`{"messages":[{"role":"user","content":"question"},{"role":"assistant","content":"answer"},{"role":"user","content":"followup"}]}`)
				}
				if call == 3 {
					previous := credential
					if errDelete := manager.Delete(t.Context(), source.ID); errDelete != nil {
						t.Fatal(errDelete)
					}
					credential, errRegister = manager.Register(t.Context(), source.Clone())
					if errRegister != nil || credential.RuntimeInstanceID() == previous.RuntimeInstanceID() || !previous.RuntimeInstanceRetired() {
						t.Fatalf("failed to create replacement instance: %v", errRegister)
					}
				}
				req := core.Request{Model: "gpt-5.4-mini", Payload: payload}
				opts := core.Options{SourceFormat: translator.FormatClaude, Metadata: map[string]any{core.ExecutionSessionMetadataKey: t.Name()}}
				if stream {
					result, err := executor.ExecuteStream(t.Context(), credential.Clone(), req, opts)
					if err != nil {
						t.Fatal(err)
					}
					for chunk := range result.Chunks {
						if chunk.Err != nil {
							t.Fatal(chunk.Err)
						}
					}
				} else if _, err := executor.Execute(t.Context(), credential.Clone(), req, opts); err != nil {
					t.Fatal(err)
				}
			}
			if calls.Load() != 3 {
				t.Fatalf("calls = %d, want 3", calls.Load())
			}
		})
	}
}
