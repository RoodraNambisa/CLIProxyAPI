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
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

const codexPlainAgentHistory = `{"model":"gpt-5.4","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"before"}]},{"type":"agent_message","author":{"agent_id":"worker"},"content":[{"type":"input_text","text":"worker result"}]},{"type":"message","role":"user","content":[{"type":"input_text","text":"after"}]}]}`
const codexEncryptedAgentHistory = `{"model":"gpt-5.4","input":[{"type":"agent_message","content":[{"type":"encrypted_content","encrypted_content":"fixture-ciphertext"}]}]}`

type multiAgentTranslationExecutor interface {
	Execute(context.Context, *cliproxyauth.Auth, core.Request, core.Options) (core.Response, error)
	ExecuteStream(context.Context, *cliproxyauth.Auth, core.Request, core.Options) (*core.StreamResult, error)
	CountTokens(context.Context, *cliproxyauth.Auth, core.Request, core.Options) (core.Response, error)
}

func executeMultiAgentTranslation(ctx context.Context, executor multiAgentTranslationExecutor, operation string, auth *cliproxyauth.Auth, req core.Request, opts core.Options) (core.Response, error) {
	switch operation {
	case "count":
		return executor.CountTokens(ctx, auth, req, opts)
	case "stream":
		result, err := executor.ExecuteStream(ctx, auth, req, opts)
		if err != nil {
			return core.Response{}, err
		}
		for chunk := range result.Chunks {
			if chunk.Err != nil && err == nil {
				err = chunk.Err
			}
		}
		return core.Response{}, err
	default:
		return executor.Execute(ctx, auth, req, opts)
	}
}

func TestOpenAICompatMultiAgentPlaintextHistory(t *testing.T) {
	for _, operation := range []string{"execute", "stream"} {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%t", operation, enabled), func(t *testing.T) {
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					body, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
					}
					messages := gjson.GetBytes(body, "messages").Array()
					expected := []string{"before", "after"}
					if enabled {
						expected = []string{"before", "worker result", "after"}
					}
					if len(messages) != len(expected) {
						t.Errorf("message count = %d, want %d", len(messages), len(expected))
					} else {
						for index, message := range messages {
							if message.Get("role").String() != "user" || message.Get("content.0.text").String() != expected[index] {
								t.Error("translated collaboration history lost content order or role")
							}
						}
					}
					if operation == "stream" {
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = io.WriteString(w, "data: {\"id\":\"result\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
					} else {
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, `{"id":"result","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`)
					}
				}))
				defer server.Close()
				cfg := &config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: enabled}}
				executor := NewOpenAICompatExecutor("openai-compatibility", cfg)
				auth := &cliproxyauth.Auth{Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
				req := core.Request{Model: "gpt-5.4", Payload: []byte(codexPlainAgentHistory)}
				opts := core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, OriginalRequest: req.Payload, Headers: http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}}}
				if _, err := executeMultiAgentTranslation(t.Context(), executor, operation, auth, req, opts); err != nil {
					t.Fatal(err)
				}
				if calls.Load() != 1 {
					t.Fatal("translation changed upstream call count")
				}
			})
		}
	}
}

func TestOpenAICompatMultiAgentCiphertextStopsBeforeUpstream(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls.Add(1); w.WriteHeader(http.StatusBadRequest) }))
	defer server.Close()
	executor := NewOpenAICompatExecutor("openai-compatibility", &config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: true}})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
	req := core.Request{Model: "gpt-5.4", Payload: []byte(codexEncryptedAgentHistory)}
	opts := core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Headers: http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}}}
	for _, operation := range []string{"execute", "stream", "count"} {
		_, err := executeMultiAgentTranslation(t.Context(), executor, operation, auth, req, opts)
		var local interface {
			SkipAuthResult() bool
			RetryOtherAuth() bool
		}
		if err == nil || !errors.As(err, &local) || !local.SkipAuthResult() || local.RetryOtherAuth() ||
			!strings.Contains(err.Error(), "codex_encrypted_agent_message_unsupported") || strings.Contains(err.Error(), "fixture-ciphertext") {
			t.Fatalf("%s did not preserve the request-scoped unsupported error", operation)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := executeMultiAgentTranslation(ctx, executor, operation, auth, req, opts); !errors.Is(err, context.Canceled) {
			t.Fatal("encrypted input replaced cancellation")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("unsupported ciphertext reached an upstream")
	}
}

func TestOpenAICompatMultiAgentCompactPreservesCiphertext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		if r.URL.Path != "/responses/compact" || gjson.GetBytes(body, "input").Raw != gjson.Get(codexEncryptedAgentHistory, "input").Raw {
			t.Error("native Responses target lost opaque ciphertext")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"compact","output":[]}`)
	}))
	defer server.Close()
	executor := NewOpenAICompatExecutor("openai-compatibility", &config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: true}})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
	_, err := executor.Execute(t.Context(), auth, core.Request{Model: "gpt-5.4", Payload: []byte(codexEncryptedAgentHistory)}, core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Alt: "responses/compact", Headers: http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}}})
	if err != nil {
		t.Fatal(err)
	}
}

func TestOpenAICompatMultiAgentCountsPlaintextHistory(t *testing.T) {
	var counts []int64
	for _, enabled := range []bool{false, true} {
		executor := NewOpenAICompatExecutor("openai-compatibility", &config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: enabled}})
		response, err := executor.CountTokens(t.Context(), nil, core.Request{Model: "gpt-5.4", Payload: []byte(codexPlainAgentHistory)}, core.Options{
			SourceFormat: sdktranslator.FormatOpenAIResponse, Headers: http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}},
		})
		if err != nil {
			t.Fatal(err)
		}
		counts = append(counts, gjson.GetBytes(response.Payload, "usage.prompt_tokens").Int())
	}
	if counts[0] <= 0 || counts[1] <= counts[0] {
		t.Fatal("token counting did not include the plaintext collaboration message")
	}
}

func TestClaudeAndKimiMultiAgentPlaintextHistory(t *testing.T) {
	for _, provider := range []string{"claude", "kimi"} {
		for _, operation := range []string{"execute", "stream", "count"} {
			if provider == "kimi" && operation == "count" {
				continue
			}
			for _, enabled := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/%t", provider, operation, enabled), func(t *testing.T) {
					var calls atomic.Int32
					handle := func(w http.ResponseWriter, r *http.Request) {
						calls.Add(1)
						body, err := io.ReadAll(r.Body)
						if err != nil {
							t.Error(err)
						}
						messages := gjson.GetBytes(body, "messages").Raw
						before, worker, after := strings.Index(messages, `"before"`), strings.Index(messages, `"worker result"`), strings.Index(messages, `"after"`)
						if before < 0 || after <= before || (worker >= 0) != enabled || enabled && (worker <= before || worker >= after) {
							t.Error("collaboration content order or default behavior changed")
						}
						if operation == "count" {
							w.Header().Set("Content-Type", "application/json")
							_, _ = io.WriteString(w, `{"input_tokens":10}`)
						} else if provider == "kimi" {
							if operation == "stream" {
								w.Header().Set("Content-Type", "text/event-stream")
								_, _ = io.WriteString(w, "data: {\"id\":\"result\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
							} else {
								w.Header().Set("Content-Type", "application/json")
								_, _ = io.WriteString(w, `{"id":"result","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`)
							}
						} else {
							w.Header().Set("Content-Type", "text/event-stream")
							for _, event := range []string{
								`{"type":"message_start","message":{"id":"msg_fixture","type":"message","role":"assistant","model":"claude-sonnet-4-6","content":[],"usage":{"input_tokens":1,"output_tokens":0}}}`,
								`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
								`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"ok"}}`,
								`{"type":"content_block_stop","index":0}`,
								`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}`,
								`{"type":"message_stop"}`,
							} {
								_, _ = io.WriteString(w, "data: "+event+"\n\n")
							}
						}
					}
					server := httptest.NewServer(http.HandlerFunc(handle))
					defer server.Close()
					cfg := &config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: enabled}}
					var executor multiAgentTranslationExecutor = NewClaudeExecutor(cfg)
					ctx := t.Context()
					if provider == "kimi" {
						executor = NewKimiExecutor(cfg)
						ctx = context.WithValue(ctx, "cliproxy.roundtripper", streamTerminalRoundTripFunc(func(r *http.Request) (*http.Response, error) {
							recorder := httptest.NewRecorder()
							handle(recorder, r)
							return recorder.Result(), nil
						}))
					}
					auth := &cliproxyauth.Auth{Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
					req := core.Request{Model: "claude-sonnet-4-6", Payload: []byte(codexPlainAgentHistory)}
					opts := core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, OriginalRequest: req.Payload, Headers: http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}}}
					if _, err := executeMultiAgentTranslation(ctx, executor, operation, auth, req, opts); err != nil {
						t.Fatal(err)
					}
					if calls.Load() != 1 {
						t.Fatal("translation changed upstream call count")
					}
				})
			}
		}
	}
}

func TestClaudeAndKimiMultiAgentCiphertextStopsBeforeUpstream(t *testing.T) {
	var calls atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls.Add(1); w.WriteHeader(http.StatusBadRequest) }))
	defer proxy.Close()
	cfg := &config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: true}}
	for _, executor := range []multiAgentTranslationExecutor{NewClaudeExecutor(cfg), NewKimiExecutor(cfg)} {
		for _, operation := range []string{"execute", "stream", "count"} {
			auth := &cliproxyauth.Auth{ProxyURL: proxy.URL, Attributes: map[string]string{"base_url": proxy.URL, "api_key": "fixture"}}
			req := core.Request{Model: "claude-sonnet-4-6", Payload: []byte(codexEncryptedAgentHistory)}
			opts := core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Headers: http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}}}
			_, err := executeMultiAgentTranslation(t.Context(), executor, operation, auth, req, opts)
			var local interface {
				SkipAuthResult() bool
				RetryOtherAuth() bool
			}
			if err == nil || !errors.As(err, &local) || !local.SkipAuthResult() || local.RetryOtherAuth() || !strings.Contains(err.Error(), "codex_encrypted_agent_message_unsupported") {
				t.Fatalf("%T/%s lost the request-scoped unsupported error", executor, operation)
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if _, err := executeMultiAgentTranslation(ctx, executor, operation, auth, req, opts); !errors.Is(err, context.Canceled) {
				t.Fatal("encrypted input replaced cancellation")
			}
		}
	}
	if calls.Load() != 0 {
		t.Fatal("unsupported ciphertext reached the fixture proxy")
	}
}
