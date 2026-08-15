package executor

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexPrepareProviderRequestDisabled(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{})
	prepared, err := executor.PrepareProviderRequest(t.Context(), cliproxyexecutor.Request{}, cliproxyexecutor.Options{}, cliproxyexecutor.RequestOperationExecute)
	if err != nil {
		t.Fatalf("PrepareProviderRequest() error = %v", err)
	}
	if prepared != nil {
		t.Fatalf("PrepareProviderRequest() = %#v, want nil", prepared)
	}
}

func TestCodexPrepareProviderRequestUsesStablePromptCacheIdentity(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{Codex: config.CodexConfig{SpoofSessionIdentity: true}})
	req := cliproxyexecutor.Request{Payload: []byte(`{"prompt_cache_key":"shared-cache"}`)}

	first := prepareCodexSessionIdentityForTest(t, executor, contextWithCodexTestAPIKey("tenant-a"), req, cliproxyexecutor.Options{})
	second := prepareCodexSessionIdentityForTest(t, executor, contextWithCodexTestAPIKey("tenant-a"), req, cliproxyexecutor.Options{})
	otherTenant := prepareCodexSessionIdentityForTest(t, executor, contextWithCodexTestAPIKey("tenant-b"), req, cliproxyexecutor.Options{})

	if first.ThreadID != second.ThreadID {
		t.Fatalf("same tenant prompt cache thread IDs differ: %q != %q", first.ThreadID, second.ThreadID)
	}
	if first.ThreadID == otherTenant.ThreadID {
		t.Fatalf("different tenants share thread ID %q", first.ThreadID)
	}
	if first.TurnID == second.TurnID {
		t.Fatalf("independent requests share turn ID %q", first.TurnID)
	}
}

func TestCodexPrepareProviderRequestKeepsExplicitUUID(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{Codex: config.CodexConfig{SpoofSessionIdentity: true}})
	explicit := uuid.NewString()
	req := cliproxyexecutor.Request{Payload: []byte(`{"prompt_cache_key":"` + explicit + `"}`)}
	prepared := prepareCodexSessionIdentityForTest(t, executor, t.Context(), req, cliproxyexecutor.Options{})
	if prepared.ThreadID != explicit || prepared.SessionID != explicit || prepared.WindowID != explicit+":0" {
		t.Fatalf("prepared identity = %#v, want explicit UUID %q", prepared, explicit)
	}
}

func TestCodexPrepareProviderRequestUsesExecutionSessionAndRequestID(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{Codex: config.CodexConfig{SpoofSessionIdentity: true}})
	executionSessionID := uuid.NewString()
	wsPrepared := prepareCodexSessionIdentityForTest(t, executor, t.Context(), cliproxyexecutor.Request{}, cliproxyexecutor.Options{
		Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: executionSessionID},
	})
	if wsPrepared.ThreadID != executionSessionID {
		t.Fatalf("websocket ThreadID = %q, want %q", wsPrepared.ThreadID, executionSessionID)
	}

	ctx := logging.WithRequestID(t.Context(), "request-123")
	first := prepareCodexSessionIdentityForTest(t, executor, ctx, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
	second := prepareCodexSessionIdentityForTest(t, executor, ctx, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
	if first.ThreadID != second.ThreadID {
		t.Fatalf("same request ID produced different threads: %q != %q", first.ThreadID, second.ThreadID)
	}
}

func TestCodexPrepareProviderRequestClassifiesKindsAndSkipsCount(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{Codex: config.CodexConfig{SpoofSessionIdentity: true}})
	compact := prepareCodexSessionIdentityForTest(t, executor, t.Context(), cliproxyexecutor.Request{}, cliproxyexecutor.Options{Alt: "responses/compact"})
	if compact.RequestKind != "compaction" {
		t.Fatalf("compact RequestKind = %q, want compaction", compact.RequestKind)
	}
	prewarm := prepareCodexSessionIdentityForTest(t, executor, t.Context(), cliproxyexecutor.Request{Payload: []byte(`{"generate":false}`)}, cliproxyexecutor.Options{})
	if prewarm.RequestKind != "prewarm" {
		t.Fatalf("prewarm RequestKind = %q, want prewarm", prewarm.RequestKind)
	}
	prepared, err := executor.PrepareProviderRequest(t.Context(), cliproxyexecutor.Request{}, cliproxyexecutor.Options{}, cliproxyexecutor.RequestOperationCount)
	if err != nil || prepared != nil {
		t.Fatalf("count preparation = %#v, %v; want nil, nil", prepared, err)
	}
}

func prepareCodexSessionIdentityForTest(t *testing.T, executor *CodexExecutor, ctx context.Context, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) codexPreparedSessionIdentity {
	t.Helper()
	prepared, err := executor.PrepareProviderRequest(ctx, req, opts, cliproxyexecutor.RequestOperationExecute)
	if err != nil {
		t.Fatalf("PrepareProviderRequest() error = %v", err)
	}
	identity, ok := prepared.(codexPreparedSessionIdentity)
	if !ok {
		t.Fatalf("PrepareProviderRequest() type = %T, want codexPreparedSessionIdentity", prepared)
	}
	return identity
}

func contextWithCodexTestAPIKey(apiKey string) context.Context {
	ginContext, _ := gin.CreateTestContext(httptest.NewRecorder())
	ginContext.Set("apiKey", apiKey)
	return context.WithValue(context.Background(), "gin", ginContext)
}

func TestCodexProjectSessionIdentityUsesIndependentSourcePriority(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{Codex: config.CodexConfig{SpoofSessionIdentity: true}})
	prepared := codexPreparedSessionIdentity{
		SessionID: "default-session", ThreadID: "default-thread", TurnID: "default-turn",
		WindowID: "default-thread:0", RequestKind: "turn",
	}
	opts := cliproxyexecutor.WithProviderPreparedRequest(cliproxyexecutor.Options{
		Headers: http.Header{
			"Session-Id":            []string{"client-session"},
			"Thread-Id":             []string{"client-thread"},
			"X-Codex-Window-Id":     []string{"client-window"},
			"X-Codex-Turn-Metadata": []string{`{"turn_id":"client-turn","client_unknown":true}`},
		},
	}, executor.Identifier(), prepared)
	auth := &cliproxyauth.Auth{
		ID:       "oauth-auth",
		Provider: "codex",
		Metadata: map[string]any{"access_token": "oauth-token"},
		Attributes: map[string]string{
			"header:Session-Id": "admin-session",
		},
	}
	raw := []byte(`{"model":"gpt-5.4","client_metadata":{"thread_id":"body-flat-thread","x-codex-turn-metadata":"{\"thread_id\":\"body-turn-thread\",\"turn_id\":\"body-turn\",\"workspace\":\"/tmp/project\"}"}}`)

	identityState := codexIdentityConfuseState{}
	projected, state, err := executor.projectCodexSessionIdentity(t.Context(), auth, cliproxyexecutor.Request{}, opts, raw, &identityState)
	if err != nil {
		t.Fatalf("projectCodexSessionIdentity() error = %v", err)
	}
	if !state.enabled {
		t.Fatal("session identity projection is disabled")
	}
	if state.identity.SessionID != "admin-session" {
		t.Fatalf("SessionID = %q, want admin-session", state.identity.SessionID)
	}
	if state.identity.ThreadID != "body-turn-thread" {
		t.Fatalf("ThreadID = %q, want body-turn-thread", state.identity.ThreadID)
	}
	if state.identity.TurnID != "body-turn" {
		t.Fatalf("TurnID = %q, want body-turn", state.identity.TurnID)
	}
	if state.identity.WindowID != "client-window" {
		t.Fatalf("WindowID = %q, want client-window", state.identity.WindowID)
	}
	if got := gjson.GetBytes(projected, "client_metadata.x-codex-turn-metadata").String(); gjson.Get(got, "workspace").String() != "/tmp/project" || !gjson.Get(got, "client_unknown").Bool() {
		t.Fatalf("turn metadata did not preserve unknown fields: %s", got)
	}
}

func TestCodexProjectSessionIdentitySkipsDisabledAndAPIKeyAuth(t *testing.T) {
	raw := []byte(`{"client_metadata":[]}`)
	tests := []struct {
		name     string
		executor *CodexExecutor
		auth     *cliproxyauth.Auth
	}{
		{
			name:     "disabled",
			executor: NewCodexExecutor(&config.Config{}),
			auth:     &cliproxyauth.Auth{Provider: "codex", Metadata: map[string]any{"access_token": "oauth-token"}},
		},
		{
			name:     "codex api key",
			executor: NewCodexExecutor(&config.Config{Codex: config.CodexConfig{SpoofSessionIdentity: true}}),
			auth:     &cliproxyauth.Auth{Provider: "codex", Attributes: map[string]string{"api_key": "test-key"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			identityState := codexIdentityConfuseState{}
			projected, state, err := tt.executor.projectCodexSessionIdentity(t.Context(), tt.auth, cliproxyexecutor.Request{}, cliproxyexecutor.Options{}, raw, &identityState)
			if err != nil {
				t.Fatalf("projectCodexSessionIdentity() error = %v", err)
			}
			if state.enabled {
				t.Fatal("session identity projection is enabled")
			}
			if string(projected) != string(raw) {
				t.Fatalf("projected payload = %s, want unchanged %s", projected, raw)
			}
		})
	}
}

func TestCodexProjectSessionIdentityRejectsMalformedMetadataWithoutCoolingAuth(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{Codex: config.CodexConfig{SpoofSessionIdentity: true}})
	auth := &cliproxyauth.Auth{Provider: "codex", Metadata: map[string]any{"access_token": "oauth-token"}}
	identityState := codexIdentityConfuseState{}
	_, _, err := executor.projectCodexSessionIdentity(t.Context(), auth, cliproxyexecutor.Request{}, cliproxyexecutor.Options{}, []byte(`{"client_metadata":[]}`), &identityState)
	if err == nil {
		t.Fatal("projectCodexSessionIdentity() error = nil, want validation error")
	}
	status, ok := err.(statusErr)
	if !ok {
		t.Fatalf("error type = %T, want statusErr", err)
	}
	if status.StatusCode() != http.StatusBadRequest || !status.SkipAuthResult() {
		t.Fatalf("status = %d, skip auth = %v; want 400, true", status.StatusCode(), status.SkipAuthResult())
	}
}

func TestCodexProjectSessionIdentityKeepsIdentityConfuseApplied(t *testing.T) {
	cfg := &config.Config{
		Routing: config.RoutingConfig{Strategy: "fill-first"},
		Codex: config.CodexConfig{
			IdentityConfuse:      true,
			SpoofSessionIdentity: true,
		},
	}
	executor := NewCodexExecutor(cfg)
	auth := &cliproxyauth.Auth{ID: "oauth-auth", Provider: "codex", Metadata: map[string]any{"access_token": "oauth-token"}}
	originalCacheKey := "cache-original"
	originalTurnID := "turn-original"
	requestPayload := []byte(`{"prompt_cache_key":"cache-original"}`)
	upstreamPayload := []byte(`{"model":"gpt-5.4","client_metadata":{"session_id":"session-original","thread_id":"thread-original","turn_id":"turn-original","x-codex-window-id":"thread-original:0","x-codex-turn-metadata":"{\"session_id\":\"session-original\",\"thread_id\":\"thread-original\",\"turn_id\":\"turn-original\",\"window_id\":\"thread-original:0\"}"}}`)
	upstreamPayload, identityState := applyCodexIdentityConfuseBody(cfg, auth, requestPayload, upstreamPayload)
	opts := cliproxyexecutor.Options{Headers: http.Header{
		"Session-Id":            []string{"client-session"},
		"Thread-Id":             []string{"client-thread"},
		"X-Codex-Window-Id":     []string{"client-window"},
		"X-Codex-Turn-Metadata": []string{`{"turn_id":"client-turn"}`},
	}}

	projected, state, err := executor.projectCodexSessionIdentity(t.Context(), auth, cliproxyexecutor.Request{Payload: requestPayload}, opts, upstreamPayload, &identityState)
	if err != nil {
		t.Fatalf("projectCodexSessionIdentity() error = %v", err)
	}
	expectedCacheKey := codexIdentityConfuseUUID(auth.ID, "prompt-cache", originalCacheKey)
	expectedTurnID := codexIdentityConfuseUUID(auth.ID, "turn", originalTurnID)
	if state.identity.SessionID != expectedCacheKey || state.identity.ThreadID != expectedCacheKey || state.identity.WindowID != expectedCacheKey+":0" {
		t.Fatalf("projected identity = %#v, want confused cache identity %q", state.identity, expectedCacheKey)
	}
	if state.identity.TurnID != expectedTurnID {
		t.Fatalf("TurnID = %q, want %q", state.identity.TurnID, expectedTurnID)
	}
	for _, path := range []string{"client_metadata.session_id", "client_metadata.thread_id"} {
		if got := gjson.GetBytes(projected, path).String(); got != expectedCacheKey {
			t.Fatalf("%s = %q, want %q", path, got, expectedCacheKey)
		}
	}
	if got := gjson.GetBytes(projected, "client_metadata.turn_id").String(); got != expectedTurnID {
		t.Fatalf("client_metadata.turn_id = %q, want %q", got, expectedTurnID)
	}
	foundResponseMapping := false
	for _, mapping := range identityState.turnIDs {
		if mapping.original == originalTurnID && mapping.confused == expectedTurnID {
			foundResponseMapping = true
			break
		}
	}
	if !foundResponseMapping {
		t.Fatalf("turn ID response mapping = %#v", identityState.turnIDs)
	}
}

func TestCodexExecutorProjectsSessionIdentityAcrossHTTPPaths(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		model      string
		payload    string
		opts       cliproxyexecutor.Options
		response   string
		streamCall bool
	}{
		{
			name: "responses", path: "/responses", model: "gpt-5.4",
			payload:  `{"model":"gpt-5.4","input":"hello","prompt_cache_key":"%s"}`,
			opts:     cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai-response")},
			response: "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"object\":\"response\",\"status\":\"completed\",\"output\":[]}}\n\n",
		},
		{
			name: "responses stream", path: "/responses", model: "gpt-5.4",
			payload:    `{"model":"gpt-5.4","input":"hello","prompt_cache_key":"%s"}`,
			opts:       cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai-response"), Stream: true},
			response:   "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"object\":\"response\",\"status\":\"completed\",\"output\":[]}}\n\n",
			streamCall: true,
		},
		{
			name: "compact", path: "/responses/compact", model: "gpt-5.4",
			payload:  `{"model":"gpt-5.4","input":"hello","prompt_cache_key":"%s"}`,
			opts:     cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai-response"), Alt: "responses/compact"},
			response: `{"id":"resp_1","object":"response.compaction"}`,
		},
		{
			name: "images", path: "/images/generations", model: "gpt-image-2",
			payload:  `{"model":"gpt-image-2","prompt":"draw","prompt_cache_key":"%s"}`,
			opts:     cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString(codexOpenAIImageSourceFormat), Alt: codexOpenAIImageGenerations},
			response: `{"created":1700000000,"data":[{"b64_json":"aW1n"}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			explicitID := uuid.NewString()
			var gotPath string
			var gotHeaders http.Header
			var gotBody []byte
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath = r.URL.Path
				gotHeaders = r.Header.Clone()
				gotBody, _ = io.ReadAll(r.Body)
				if strings.Contains(tt.response, "data:") {
					w.Header().Set("Content-Type", "text/event-stream")
				} else {
					w.Header().Set("Content-Type", "application/json")
				}
				_, _ = w.Write([]byte(tt.response))
			}))
			defer server.Close()

			executor := NewCodexExecutor(&config.Config{
				Codex:     config.CodexConfig{SpoofSessionIdentity: true},
				SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"},
			})
			auth := &cliproxyauth.Auth{
				ID: "oauth-auth", Provider: "codex",
				Metadata:   map[string]any{"access_token": "oauth-token"},
				Attributes: map[string]string{"base_url": server.URL},
			}
			req := cliproxyexecutor.Request{Model: tt.model, Payload: []byte(strings.Replace(tt.payload, "%s", explicitID, 1))}
			if tt.streamCall {
				result, err := executor.ExecuteStream(t.Context(), auth, req, tt.opts)
				if err != nil {
					t.Fatalf("ExecuteStream() error = %v", err)
				}
				for chunk := range result.Chunks {
					if chunk.Err != nil {
						t.Fatalf("stream chunk error = %v", chunk.Err)
					}
				}
			} else {
				if _, err := executor.Execute(t.Context(), auth, req, tt.opts); err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
			}

			if gotPath != tt.path {
				t.Fatalf("path = %q, want %q", gotPath, tt.path)
			}
			if got := gotHeaders.Get("Session-Id"); got != explicitID {
				t.Fatalf("Session-Id = %q, want %q", got, explicitID)
			}
			if got := gotHeaders.Get("Thread-Id"); got != explicitID {
				t.Fatalf("Thread-Id = %q, want %q", got, explicitID)
			}
			if got := gotHeaders.Get("X-Codex-Window-Id"); got != explicitID+":0" {
				t.Fatalf("X-Codex-Window-Id = %q, want %q", got, explicitID+":0")
			}
			if got := headerValueCaseInsensitive(gotHeaders, "session_id"); got != "" {
				t.Fatalf("legacy session_id = %q, want empty", got)
			}
			if got := gjson.GetBytes(gotBody, "client_metadata.session_id").String(); got != explicitID {
				t.Fatalf("body session_id = %q, want %q; body=%s", got, explicitID, gotBody)
			}
			if got := gjson.GetBytes(gotBody, "client_metadata.thread_id").String(); got != explicitID {
				t.Fatalf("body thread_id = %q, want %q", got, explicitID)
			}
			bodyTurn := gjson.GetBytes(gotBody, "client_metadata.x-codex-turn-metadata").String()
			if bodyTurn == "" || bodyTurn != gotHeaders.Get("X-Codex-Turn-Metadata") {
				t.Fatalf("body/header turn metadata differ: body=%q header=%q", bodyTurn, gotHeaders.Get("X-Codex-Turn-Metadata"))
			}
			if got := gjson.Get(bodyTurn, "turn_id").String(); got == "" {
				t.Fatal("turn metadata turn_id is empty")
			}
		})
	}
}
