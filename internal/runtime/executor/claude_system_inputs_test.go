package executor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestClaudeSystemInputsRejectBeforeUpstreamAcrossOperations(t *testing.T) {
	for _, source := range []translator.Format{translator.FormatClaude, translator.FormatOpenAIResponse, translator.FormatOpenAI} {
		for _, compat := range []bool{false, true} {
			for _, operation := range []string{"execute", "stream", "count"} {
				t.Run(fmt.Sprintf("%s/compat=%t/%s", source, compat, operation), func(t *testing.T) {
					var calls atomic.Int32
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
						calls.Add(1)
						writeClaudeCompatibilityFixture(w, "stream")
					}))
					t.Cleanup(server.Close)
					manager := claudeCompatibilityManager(t, server.URL, compat)
					manager.SetRetryConfig(3, 0, 3)
					raw := []byte(`{"system":[{"type":"image","source":{"data":"private-fixture"}}],"messages":[{"role":"user","content":"question"}]}`)
					if source == translator.FormatOpenAIResponse {
						raw = []byte(`{"input":[{"role":"developer","content":[{"type":"input_image","image_url":"private-fixture"}]},{"role":"user","content":"question"}]}`)
					} else if source == translator.FormatOpenAI {
						raw = []byte(`{"messages":[{"role":"developer","content":[{"type":"image_url","image_url":{"url":"private-fixture"}}]},{"role":"user","content":"question"}]}`)
					}
					req := core.Request{Model: "bound-claude", Payload: raw}
					opts := core.Options{SourceFormat: source, OriginalRequest: raw}
					var err error
					switch operation {
					case "count":
						_, err = manager.ExecuteCount(t.Context(), []string{"claude"}, req, opts)
					case "stream":
						var result *core.StreamResult
						result, err = manager.ExecuteStream(t.Context(), []string{"claude"}, req, opts)
						if err == nil {
							for chunk := range result.Chunks {
								if chunk.Err != nil {
									err = chunk.Err
								}
							}
						}
					default:
						_, err = manager.Execute(t.Context(), []string{"claude"}, req, opts)
					}
					var status interface{ StatusCode() int }
					if !errors.As(err, &status) || status.StatusCode() != 400 || !strings.Contains(err.Error(), "claude_system_content_unsupported") ||
						strings.Contains(err.Error(), "private-fixture") || calls.Load() != 0 {
						t.Fatal("unsupported system content was dropped, retried upstream or exposed")
					}
					auth, ok := manager.GetByID(t.Name())
					if !ok || auth.LastError != nil || !auth.NextRetryAfter.IsZero() || len(auth.ModelStates) > 0 {
						t.Fatal("local system error changed credential availability")
					}
				})
			}
		}
	}
}

func TestClaudeSystemValidationRespectsExistingStrictCloakingBoundaries(t *testing.T) {
	for _, tc := range []struct {
		mode, model, prefix string
		strict, discard     bool
	}{
		{"always", "claude-fixture", "", true, true},
		{"always", "claude-fixture", "", false, false},
		{"never", "claude-fixture", "", true, false},
		{"always", "claude-3-5-haiku", "", true, false},
		{"always", "claude-fixture", `{"type":"text","text":"x-anthropic-billing-header: fixture"},`, true, false},
	} {
		auth := &coreauth.Auth{Attributes: map[string]string{"cloak_mode": tc.mode, "cloak_strict_mode": fmt.Sprint(tc.strict)}}
		body := []byte(`{"system":[` + tc.prefix + `{"type":"image","data":"private-fixture"}],"messages":[{"role":"user","content":"question"}]}`)
		out, err := applyCloaking(t.Context(), &config.Config{}, auth, body, tc.model, "fixture")
		if (err == nil) != tc.discard || (tc.discard && (gjson.GetBytes(out, "system.#").Int() != 3 || strings.Contains(string(out), "private-fixture"))) {
			t.Fatal("validation changed which requests discard caller system content")
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err = applyCloaking(ctx, &config.Config{}, auth, body, tc.model, "fixture"); !errors.Is(err, context.Canceled) {
			t.Fatal("strict cloaking replaced request cancellation")
		}
	}
}

func TestClaudeSystemInstructionsReachMessagesAndCountInOrder(t *testing.T) {
	for _, source := range []translator.Format{translator.FormatOpenAIResponse, translator.FormatOpenAI} {
		for _, operation := range []string{"execute", "stream", "count"} {
			t.Run(fmt.Sprintf("%s/%s", source, operation), func(t *testing.T) {
				captured := make(chan bool, 2)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					body, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
						return
					}
					first := strings.Index(string(body), "SYS_SOURCE_A")
					second := strings.Index(string(body), "SYS_SOURCE_B")
					third := strings.Index(string(body), "SYS_SOURCE_C")
					valid := first >= 0 && second > first && third > second
					if operation != "count" {
						valid = valid && gjson.GetBytes(body, "system.#").Int() == 3 && gjson.GetBytes(body, "messages.#").Int() == 1
					}
					captured <- valid
					responseKind := "stream"
					if operation == "count" {
						responseKind = "count"
					}
					writeClaudeCompatibilityFixture(w, responseKind)
				}))
				t.Cleanup(server.Close)
				executor := NewClaudeExecutor(&config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}})
				auth := &coreauth.Auth{ID: t.Name(), Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL, "cloak_mode": "never"}}
				raw := []byte(`{"instructions":"SYS_SOURCE_A","input":[{"role":"developer","content":"SYS_SOURCE_B"},{"role":"user","content":"question"},{"role":"system","content":"SYS_SOURCE_C"}]}`)
				if source == translator.FormatOpenAI {
					raw = []byte(`{"messages":[{"role":"system","content":"SYS_SOURCE_A"},{"role":"developer","content":"SYS_SOURCE_B"},{"role":"user","content":"question"},{"role":"system","content":"SYS_SOURCE_C"}]}`)
				}
				req := core.Request{Model: "claude-fixture", Payload: raw}
				opts := core.Options{SourceFormat: source, OriginalRequest: raw}
				var err error
				switch operation {
				case "count":
					_, err = executor.CountTokens(t.Context(), auth, req, opts)
				case "stream":
					var result *core.StreamResult
					result, err = executor.ExecuteStream(t.Context(), auth, req, opts)
					if err == nil {
						for chunk := range result.Chunks {
							if chunk.Err != nil {
								err = chunk.Err
							}
						}
					}
				default:
					_, err = executor.Execute(t.Context(), auth, req, opts)
				}
				if err != nil {
					t.Fatal(err)
				}
				select {
				case valid := <-captured:
					if !valid {
						t.Fatal("upstream did not receive every system source in order")
					}
				default:
					t.Fatal("request did not reach the local upstream")
				}
			})
		}
	}
}
