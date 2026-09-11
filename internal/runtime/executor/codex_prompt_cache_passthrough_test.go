package executor

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/websocket"
	codexauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexPromptCachePassthroughIdentityMatrix(t *testing.T) {
	for _, mode := range []string{"off", "device", "session", "full"} {
		for _, confuse := range []bool{false, true} {
			for _, spoof := range []bool{false, true} {
				for _, key := range []string{"body-session", " Case Key ", "019e417b-e000-7000-8000-000000000001"} {
					t.Run(fmt.Sprintf("%s/confuse=%t/spoof=%t/key=%q", mode, confuse, spoof, key), func(t *testing.T) {
						cfg := &config.Config{Routing: config.RoutingConfig{SessionAffinity: true}, Codex: config.CodexConfig{
							PassthroughPromptCacheKey: true, IdentityConfuse: confuse, SpoofSessionIdentity: spoof,
						}}
						executor := NewCodexExecutor(cfg)
						auth := prepareCodexFingerprintAuthForTest(t, executor, &cliproxyauth.Auth{ID: "cache-test", Provider: "codex", Metadata: map[string]any{
							"type": "codex", "account_id": "cache-test", codexauth.FingerprintModeMetadataKey: mode,
						}})
						turn, _ := json.Marshal(map[string]string{"prompt_cache_key": key, "turn_id": key})
						body, _ := json.Marshal(map[string]any{"prompt_cache_key": key, "input": key, "client_metadata": map[string]string{
							"session_id": "body-session", "thread_id": "body-thread", "x-codex-turn-metadata": string(turn),
						}})
						req := cliproxyexecutor.Request{Model: "gpt-5.4", Payload: body}
						opts, err := executor.ensureCodexPreparedSessionIdentity(t.Context(), req, cliproxyexecutor.Options{}, cliproxyexecutor.RequestOperationExecute)
						if err != nil {
							t.Fatal(err)
						}
						cfg.Codex.PassthroughPromptCacheKey = false
						for _, authID := range []string{"cache-test", "retry-auth"} {
							attempt := auth.Clone()
							attempt.ID = authID
							httpReq, upstream, identity, errCache := executor.cacheHelper(t.Context(), sdktranslator.FromString("openai-response"), "http://example.invalid/responses", attempt, req, opts, body, body, true)
							if errCache != nil {
								t.Fatal(errCache)
							}
							upstream, err = executor.applyCodexHTTPSessionIdentity(t.Context(), attempt, req, opts, httpReq, upstream, &identity)
							closeCodexRequestBody(httpReq)
							if err != nil {
								t.Fatal(err)
							}
							if gjson.GetBytes(upstream, "prompt_cache_key").Str != key || gjson.GetBytes(upstream, "input").Str != key {
								t.Fatal("request key or business content changed across identity projection/retry")
							}
							metadata := gjson.GetBytes(upstream, "client_metadata.x-codex-turn-metadata").Str
							if gjson.Get(metadata, "prompt_cache_key").Str != key {
								t.Fatal("metadata cache mirror changed")
							}
							if gjson.GetBytes(upstream, "client_metadata.session_id").Str != "body-session" || httpReq.Header.Get("Session-Id") != "body-session" || httpReq.Header.Get("Session_id") != "body-session" {
								t.Fatal("final passthrough did not preserve the explicit session above identity projection")
							}
							if confuse && gjson.Get(metadata, "turn_id").Str == key {
								t.Fatal("cache protection disabled turn remapping")
							}
							response, _ := json.Marshal(map[string]any{"prompt_cache_key": key, "turn_id": gjson.Get(metadata, "turn_id").Str, "output": []map[string]string{{"text": key}}})
							client := applyCodexIdentityExposeResponsePayload(response, identity)
							if gjson.GetBytes(client, "prompt_cache_key").Str != key || gjson.GetBytes(client, "output.0.text").Str != key {
								t.Fatal("response identity restoration changed a cache or business role")
							}
						}
					})
				}
			}
		}
	}
}

func TestCodexPromptCachePassthroughWireEntrypoints(t *testing.T) {
	const key = " User Cache Key "
	for _, endpoint := range []string{"responses", "compact", "chat", "images", "websocket"} {
		for _, stream := range []bool{false, true} {
			if endpoint == "compact" && stream {
				continue
			}
			t.Run(fmt.Sprintf("%s/stream=%t", endpoint, stream), func(t *testing.T) {
				captured := make(chan []byte, 1)
				capturedHeaders := make(chan http.Header, 1)
				completed := []byte(`{"type":"response.completed","response":{"id":"resp_test","object":"response","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					capturedHeaders <- r.Header.Clone()
					if endpoint == "websocket" {
						upgrader := websocket.Upgrader{}
						conn, err := upgrader.Upgrade(w, r, nil)
						if err != nil {
							t.Error(err)
							return
						}
						defer func() { _ = conn.Close() }()
						_, payload, errRead := conn.ReadMessage()
						if errRead != nil {
							t.Error(errRead)
							return
						}
						captured <- payload
						if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
							t.Error(errWrite)
						}
						return
					}
					payload, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
						return
					}
					captured <- payload
					if endpoint == "compact" {
						w.Header().Set("Content-Type", "application/json")
						_, _ = w.Write([]byte(`{"id":"compact_test","object":"response.compaction","output":[]}`))
					} else if endpoint == "images" && !stream {
						w.Header().Set("Content-Type", "application/json")
						_, _ = w.Write([]byte(`{"created":1,"data":[{"b64_json":"aW1n"}]}`))
					} else {
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = w.Write(append(append([]byte("data: "), completed...), []byte("\n\n")...))
					}
				}))
				defer server.Close()
				cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}, Routing: config.RoutingConfig{SessionAffinity: true}, Codex: config.CodexConfig{PassthroughPromptCacheKey: true, IdentityConfuse: true}}
				executor := NewCodexExecutor(cfg)
				auth := &cliproxyauth.Auth{ID: "wire-test", Provider: "codex", Attributes: map[string]string{"api_key": "test-key", "base_url": server.URL}}
				req := cliproxyexecutor.Request{Model: "gpt-5.4", Payload: []byte(`{"model":"gpt-5.4","input":"hello","messages":[{"role":"user","content":"hello"}],"prompt":"draw","prompt_cache_key":" User Cache Key "}`)}
				opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai-response"), Stream: stream, Headers: http.Header{"Session_id": {"wire-client-session"}}}
				switch endpoint {
				case "compact":
					opts.Alt = "responses/compact"
				case "chat":
					opts.SourceFormat = sdktranslator.FromString("openai")
				case "images":
					opts.SourceFormat = sdktranslator.FromString(codexOpenAIImageSourceFormat)
					opts.Alt = codexOpenAIImageGenerations
					req.Model = "gpt-image-2"
				case "websocket":
					opts.SourceFormat = sdktranslator.FromString("codex")
				}
				if stream {
					var result *cliproxyexecutor.StreamResult
					var err error
					if endpoint == "websocket" {
						result, err = NewCodexWebsocketsExecutor(cfg).ExecuteStream(t.Context(), auth, req, opts)
					} else {
						result, err = executor.ExecuteStream(t.Context(), auth, req, opts)
					}
					if err != nil {
						t.Fatal(err)
					}
					for chunk := range result.Chunks {
						if chunk.Err != nil {
							t.Fatal(chunk.Err)
						}
					}
				} else {
					var err error
					if endpoint == "websocket" {
						_, err = NewCodexWebsocketsExecutor(cfg).Execute(t.Context(), auth, req, opts)
					} else {
						_, err = executor.Execute(t.Context(), auth, req, opts)
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				select {
				case body := <-captured:
					if gjson.GetBytes(body, "prompt_cache_key").Str != key {
						t.Fatal("explicit key did not reach the wire")
					}
					headers := <-capturedHeaders
					if headers.Get("Session-Id") != "wire-client-session" || headers.Get("Session_id") != "wire-client-session" {
						t.Fatal("explicit session did not reach the wire alongside the cache key")
					}
				default:
					t.Fatal("no upstream request captured")
				}
			})
		}
	}
}

func TestCodexPromptCachePassthroughSnapshotSurvivesReleaseAndReplacement(t *testing.T) {
	for _, initiallyEnabled := range []bool{false, true} {
		cfg := &config.Config{Routing: config.RoutingConfig{SessionAffinity: true}, Codex: config.CodexConfig{
			PassthroughPromptCacheKey: initiallyEnabled, IdentityConfuse: true,
		}}
		executor := NewCodexExecutor(cfg)
		req := cliproxyexecutor.Request{Payload: []byte(`{"prompt_cache_key":"original"}`)}
		opts, err := executor.ensureCodexPreparedSessionIdentity(t.Context(), req, cliproxyexecutor.Options{OriginalRequest: req.Payload}, cliproxyexecutor.RequestOperationExecute)
		if err != nil {
			t.Fatal(err)
		}
		clear(req.Payload)
		req.Payload, opts.OriginalRequest = nil, nil
		replacement := *cfg
		replacement.Codex.PassthroughPromptCacheKey = !initiallyEnabled
		executor = NewCodexExecutor(&replacement)
		prepared := executor.codexPreparedSessionIdentity(t.Context(), req, opts)
		if prepared.PromptCacheLog == nil || !prepared.PromptCacheLog.ProtectsKey("original") {
			t.Fatal("request release lost log protection independently of the passthrough setting")
		}
		upstream, state := applyCodexPreparedIdentityConfuseBody(&replacement, &cliproxyauth.Auth{ID: "retry-auth"}, nil, []byte(`{"prompt_cache_key":"generated"}`), prepared)
		want := "generated"
		if initiallyEnabled {
			want = "original"
		}
		if gjson.GetBytes(upstream, "prompt_cache_key").Str != want {
			t.Fatal("request changed policy after release/config replacement")
		}
		if initiallyEnabled && state.originalPromptCacheKey != "original" {
			t.Fatal("release lost the independent identity mapping source")
		}
	}
}
