package executor

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexCacheSessionValidationStopsLocalRetry(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, tc := range []struct {
			name, key, session string
			enabled, rejected  bool
		}{
			{"uuid", "019e417b-e000-7000-8000-000000000001", "", true, false},
			{"non_uuid", "ordinary-cache-key", "", true, false},
			{"surrounding_space", " cache-key ", "", true, true},
			{"newline", "cache\nkey", "", true, true},
			{"bad_session", "valid-cache", "bad\nsession", true, true},
			{"independent_session", " cache-key ", "valid-session", true, false},
			{"disabled_space", " cache-key ", "", false, false},
			{"disabled_newline", "cache\nkey", "", false, false},
		} {
			t.Run(fmt.Sprintf("%s/stream=%t", tc.name, stream), func(t *testing.T) {
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					body, _ := io.ReadAll(r.Body)
					if gjson.GetBytes(body, "prompt_cache_key").Str != tc.key {
						t.Error("cache value changed on the wire")
					}
					if tc.enabled {
						want := tc.session
						if want == "" {
							want = tc.key
						}
						if r.Header.Get("Session-Id") != want || r.Header.Get("Session_id") != want {
							t.Error("session header differs from explicit session or cache fallback")
						}
					}
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"validation-fixture\",\"status\":\"completed\",\"output\":[]}}\n\n")
				}))
				defer server.Close()
				manager := coreauth.NewManager(nil, &coreauth.FillFirstSelector{}, nil)
				manager.SetRetryConfig(2, 0, 2)
				manager.RegisterExecutor(NewCodexExecutor(&config.Config{Codex: config.CodexConfig{PassthroughPromptCacheKey: tc.enabled}}))
				ids := []string{"cache-validation-a", "cache-validation-b"}
				for _, id := range ids {
					if _, err := manager.Register(t.Context(), &coreauth.Auth{ID: id, Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL}}); err != nil {
						t.Fatal(err)
					}
					registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: "gpt-5.4"}})
					t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
				}
				body, _ := json.Marshal(map[string]string{"model": "gpt-5.4", "input": "fixture", "prompt_cache_key": tc.key})
				opts := core.Options{SourceFormat: translator.FormatOpenAIResponse, Stream: stream, Headers: http.Header{"Session_id": {tc.session}}}
				var err error
				if stream {
					var result *core.StreamResult
					result, err = manager.ExecuteStream(t.Context(), []string{"codex"}, core.Request{Model: "gpt-5.4", Payload: body}, opts)
					if result != nil {
						for chunk := range result.Chunks {
							if chunk.Err != nil {
								err = chunk.Err
							}
						}
					}
				} else {
					_, err = manager.Execute(t.Context(), []string{"codex"}, core.Request{Model: "gpt-5.4", Payload: body}, opts)
				}
				if !tc.rejected {
					if err != nil || calls.Load() != 1 {
						t.Fatalf("valid or disabled request did not execute once: %v", err)
					}
					return
				}
				var failure interface{ StatusCode() int }
				if !errors.As(err, &failure) || failure.StatusCode() != 500 || gjson.Get(err.Error(), "error.code").Str != "invalid_prompt_cache_key" || gjson.Get(err.Error(), "error.type").Str != "invalid_request_error" {
					t.Fatalf("validation did not return the agreed error contract: %v", err)
				}
				if calls.Load() != 0 {
					t.Fatal("local validation called upstream or spent retry attempts")
				}
				for _, id := range ids {
					current, _ := manager.GetByID(id)
					if current.LastError != nil || current.Unavailable || len(current.ModelStates) != 0 {
						t.Fatal("local parameter validation changed credential health")
					}
				}
			})
		}
	}
}
