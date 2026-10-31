package executor

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexPromptCacheLookupKeyDoesNotRetainAPIKey(t *testing.T) {
	const apiKey = "secret-downstream-api-key"
	first := codexPromptCacheLookupKey("openai", apiKey)
	second := codexPromptCacheLookupKey("openai", apiKey)
	if first != second {
		t.Fatalf("lookup keys differ: %q != %q", first, second)
	}
	if strings.Contains(first, apiKey) {
		t.Fatalf("lookup key retains plaintext API key: %q", first)
	}
}

func TestCodexExecutorCacheHelper_OpenAIChatCompletions_StablePromptCacheKeyFromAPIKey(t *testing.T) {
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Set("apiKey", "test-api-key")

	ctx := context.WithValue(context.Background(), "gin", ginCtx)
	executor := &CodexExecutor{}
	rawJSON := []byte(`{"model":"gpt-5.3-codex","stream":true}`)
	req := cliproxyexecutor.Request{
		Model:   "gpt-5.3-codex",
		Payload: []byte(`{"model":"gpt-5.3-codex"}`),
	}
	url := "https://example.com/responses"

	httpReq, _, _, err := executor.cacheHelper(ctx, sdktranslator.FromString("openai"), url, nil, req, cliproxyexecutor.Options{}, req.Payload, rawJSON, true)
	if err != nil {
		t.Fatalf("cacheHelper error: %v", err)
	}

	body, errRead := io.ReadAll(httpReq.Body)
	if errRead != nil {
		t.Fatalf("read request body: %v", errRead)
	}

	gotKey := gjson.GetBytes(body, "prompt_cache_key").String()
	if parsed, errParse := uuid.Parse(gotKey); errParse != nil || parsed.Version() != 7 {
		t.Fatalf("prompt_cache_key = %q, want UUIDv7", gotKey)
	}
	if gotConversation := httpReq.Header.Get("Conversation_id"); gotConversation != "" {
		t.Fatalf("Conversation_id = %q, want empty", gotConversation)
	}
	if gotSession := httpReq.Header.Get("Session-Id"); gotSession != gotKey {
		t.Fatalf("Session-Id = %q, want %q", gotSession, gotKey)
	}
	if gotLegacySession := headerValueCaseInsensitive(httpReq.Header, "Session_id"); gotLegacySession != "" {
		t.Fatalf("Session_id = %q, want empty", gotLegacySession)
	}

	httpReq2, _, _, err := executor.cacheHelper(ctx, sdktranslator.FromString("openai"), url, nil, req, cliproxyexecutor.Options{}, req.Payload, rawJSON, true)
	if err != nil {
		t.Fatalf("cacheHelper error (second call): %v", err)
	}
	body2, errRead2 := io.ReadAll(httpReq2.Body)
	if errRead2 != nil {
		t.Fatalf("read request body (second call): %v", errRead2)
	}
	gotKey2 := gjson.GetBytes(body2, "prompt_cache_key").String()
	if gotKey2 != gotKey {
		t.Fatalf("prompt_cache_key (second call) = %q, want %q", gotKey2, gotKey)
	}
}

func TestCodexExecutorCacheHelper_IdentityConfuseRemapsBodyAndHeaders(t *testing.T) {
	cfg := &config.Config{Routing: config.RoutingConfig{SessionAffinity: true}, Codex: config.CodexConfig{IdentityConfuse: true}}
	executor := NewCodexExecutor(cfg)
	auth := &cliproxyauth.Auth{ID: "auth-1", Provider: "codex", Metadata: map[string]any{"codex_fingerprint_mode": "off"}}
	body := []byte(`{"prompt_cache_key":"cache-1","client_metadata":{"session_id":"cache-1","thread_id":"cache-1","turn_id":"turn-1","x-codex-installation-id":"install-1"}}`)
	req := cliproxyexecutor.Request{Model: "gpt-5-codex", Payload: body}
	opts, err := executor.ensureCodexPreparedSessionIdentity(t.Context(), req, cliproxyexecutor.Options{}, cliproxyexecutor.RequestOperationExecute)
	if err != nil {
		t.Fatal(err)
	}
	httpReq, raw, state, err := executor.cacheHelper(t.Context(), sdktranslator.FromString("openai-response"), "https://example.com/responses", auth, req, opts, body, body, true)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != string(body) {
		t.Fatal("body rewritten before the single projection stage")
	}
	projected, err := executor.applyCodexHTTPSessionIdentity(t.Context(), auth, req, opts, httpReq, raw, &state)
	if err != nil {
		t.Fatal(err)
	}
	want := helps.StableCodexIdentity(auth.ID, "conversation", "cache-1")
	if gjson.GetBytes(projected, "prompt_cache_key").String() != want || httpReq.Header.Get("Session-Id") != want || httpReq.Header.Get("Thread-Id") != want {
		t.Fatalf("header/body mismatch: %s, %v", projected, httpReq.Header)
	}
	if gjson.GetBytes(projected, "client_metadata.x-codex-installation-id").String() == "install-1" {
		t.Fatal("device identity unchanged")
	}
	if gjson.GetBytes(body, "client_metadata.session_id").String() != "cache-1" {
		t.Fatal("original request mutated")
	}
}

func TestCodexIdentityConfuseKeepsClientBodySeparateFromUpstreamBody(t *testing.T) {
	cfg := &config.Config{
		Routing: config.RoutingConfig{Strategy: "fill-first", SessionAffinity: true},
		Codex:   config.CodexConfig{IdentityConfuse: true},
	}
	auth := &cliproxyauth.Auth{ID: "auth-1", Provider: "codex"}
	clientBody := []byte(`{"model":"gpt-5-codex","prompt_cache_key":"cache-1"}`)

	upstreamBody, identityState := applyCodexIdentityConfuseBody(cfg, auth, clientBody, clientBody)
	expectedPromptCacheKey := codexIdentityConfuseUUID("auth-1", "prompt-cache", "cache-1")
	if identityState.promptCacheKey != expectedPromptCacheKey {
		t.Fatalf("identity prompt_cache_key = %q, want %q", identityState.promptCacheKey, expectedPromptCacheKey)
	}
	if gotKey := gjson.GetBytes(upstreamBody, "prompt_cache_key").String(); gotKey != expectedPromptCacheKey {
		t.Fatalf("upstream prompt_cache_key = %q, want %q", gotKey, expectedPromptCacheKey)
	}
	if gotKey := gjson.GetBytes(clientBody, "prompt_cache_key").String(); gotKey != "cache-1" {
		t.Fatalf("client prompt_cache_key = %q, want cache-1", gotKey)
	}
}
