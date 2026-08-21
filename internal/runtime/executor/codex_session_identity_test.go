package executor

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	codexauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
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
	identity, ok := prepared.(codexPreparedSessionIdentity)
	if !ok || identity.Enabled {
		t.Fatalf("PrepareProviderRequest() = %#v, want disabled identity snapshot", prepared)
	}
}

func TestCodexPreparedSessionIdentitySnapshotsEnabledState(t *testing.T) {
	auth := &cliproxyauth.Auth{Provider: "codex", Metadata: map[string]any{"access_token": "oauth-token"}}
	req := cliproxyexecutor.Request{Payload: []byte(`{"prompt_cache_key":"cache-key"}`)}

	enabledCfg := &config.Config{Codex: config.CodexConfig{SpoofSessionIdentity: true}}
	enabledExecutor := NewCodexExecutor(enabledCfg)
	enabledPrepared, err := enabledExecutor.PrepareProviderRequest(t.Context(), req, cliproxyexecutor.Options{}, cliproxyexecutor.RequestOperationExecute)
	if err != nil {
		t.Fatalf("prepare enabled identity: %v", err)
	}
	enabledOpts := cliproxyexecutor.WithProviderPreparedRequest(cliproxyexecutor.Options{}, enabledExecutor.Identifier(), enabledPrepared)
	enabledCfg.Codex.SpoofSessionIdentity = false
	_, enabledState, err := enabledExecutor.projectCodexSessionIdentity(t.Context(), auth, req, enabledOpts, []byte(`{}`), &codexIdentityConfuseState{})
	if err != nil || !enabledState.enabled {
		t.Fatalf("enabled request after config change = %#v, %v; want projection enabled", enabledState, err)
	}

	disabledCfg := &config.Config{}
	disabledExecutor := NewCodexExecutor(disabledCfg)
	disabledPrepared, err := disabledExecutor.PrepareProviderRequest(t.Context(), req, cliproxyexecutor.Options{}, cliproxyexecutor.RequestOperationExecute)
	if err != nil {
		t.Fatalf("prepare disabled identity: %v", err)
	}
	disabledOpts := cliproxyexecutor.WithProviderPreparedRequest(cliproxyexecutor.Options{}, disabledExecutor.Identifier(), disabledPrepared)
	disabledCfg.Codex.SpoofSessionIdentity = true
	raw := []byte(`{"client_metadata":[]}`)
	projected, disabledState, err := disabledExecutor.projectCodexSessionIdentity(t.Context(), auth, req, disabledOpts, raw, &codexIdentityConfuseState{})
	if err != nil || disabledState.enabled || string(projected) != string(raw) {
		t.Fatalf("disabled request after config change = %s, %#v, %v; want unchanged payload", projected, disabledState, err)
	}
}

func TestCodexPrepareProviderRequestDoesNotUseOpaquePromptCacheAsSessionIdentity(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{Codex: config.CodexConfig{SpoofSessionIdentity: true}})
	req := cliproxyexecutor.Request{Payload: []byte(`{"prompt_cache_key":"shared-cache"}`)}

	first := prepareCodexSessionIdentityForTest(t, executor, contextWithCodexTestAPIKey("tenant-a"), req, cliproxyexecutor.Options{})
	second := prepareCodexSessionIdentityForTest(t, executor, contextWithCodexTestAPIKey("tenant-a"), req, cliproxyexecutor.Options{})
	otherTenant := prepareCodexSessionIdentityForTest(t, executor, contextWithCodexTestAPIKey("tenant-b"), req, cliproxyexecutor.Options{})

	for _, identity := range []codexPreparedSessionIdentity{first, second, otherTenant} {
		if !isCodexUUIDVersion(identity.ThreadID, uuid.Version(7)) || !isCodexUUIDVersion(identity.TurnID, uuid.Version(7)) {
			t.Fatalf("prepared identity = %#v, want UUIDv7 thread and turn", identity)
		}
	}
	if first.ThreadID == second.ThreadID {
		t.Fatalf("independent requests reused opaque prompt cache as thread ID %q", first.ThreadID)
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
	wsPrepared := prepareCodexSessionIdentityForTest(t, executor, t.Context(), cliproxyexecutor.Request{Payload: []byte(`{"prompt_cache_key":"request-cache"}`)}, cliproxyexecutor.Options{
		Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: executionSessionID},
	})
	if wsPrepared.ThreadID != executionSessionID {
		t.Fatalf("websocket ThreadID = %q, want %q", wsPrepared.ThreadID, executionSessionID)
	}

	ctx := logging.WithRequestID(t.Context(), "request-123")
	first := prepareCodexSessionIdentityForTest(t, executor, ctx, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
	second := prepareCodexSessionIdentityForTest(t, executor, ctx, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
	if !isCodexUUIDVersion(first.ThreadID, uuid.Version(7)) || !isCodexUUIDVersion(second.ThreadID, uuid.Version(7)) {
		t.Fatalf("request identities = %q, %q; want UUIDv7", first.ThreadID, second.ThreadID)
	}
	if first.ThreadID == second.ThreadID {
		t.Fatalf("independent preflight snapshots share thread ID %q", first.ThreadID)
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

func prepareCodexFingerprintAuthForTest(t *testing.T, executor *CodexExecutor, auth *cliproxyauth.Auth) *cliproxyauth.Auth {
	t.Helper()
	prepared, err := executor.PrepareRequestAuth(t.Context(), auth)
	if err != nil {
		t.Fatalf("PrepareRequestAuth() error = %v", err)
	}
	return prepared
}

func contextWithCodexTestAPIKey(apiKey string) context.Context {
	ginContext, _ := gin.CreateTestContext(httptest.NewRecorder())
	ginContext.Set("apiKey", apiKey)
	return context.WithValue(context.Background(), "gin", ginContext)
}

func TestCodexProjectSessionIdentityUsesIndependentSourcePriority(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{Codex: config.CodexConfig{SpoofSessionIdentity: true}})
	prepared := codexPreparedSessionIdentity{
		Enabled:   true,
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
			auth: &cliproxyauth.Auth{Provider: "codex", Metadata: map[string]any{
				"access_token": "oauth-token", codexauth.FingerprintModeMetadataKey: "off",
			}},
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

func TestCodexProjectSessionIdentityConvergesPerCredentialMode(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{})
	prepared := codexPreparedSessionIdentity{TurnID: "prepared-turn", RequestKind: "turn"}
	opts := cliproxyexecutor.WithProviderPreparedRequest(cliproxyexecutor.Options{
		Headers: http.Header{"Session-Id": []string{"client-session"}},
	}, executor.Identifier(), prepared)
	raw := []byte(`{"model":"gpt-5.4","client_metadata":{"session_id":"body-session","thread_id":"body-thread","turn_id":"body-turn","x-codex-window-id":"body-window","x-codex-installation-id":"body-install","x-codex-turn-metadata":"{\"workspace\":\"/tmp/project\",\"session_id\":\"body-session\"}"}}`)

	tests := []struct {
		mode               codexauth.FingerprintMode
		wantProjectSession bool
	}{
		{mode: codexauth.FingerprintModeDevice, wantProjectSession: false},
		{mode: codexauth.FingerprintModeSession, wantProjectSession: true},
		{mode: codexauth.FingerprintModeFull, wantProjectSession: true},
	}
	for _, tt := range tests {
		t.Run(string(tt.mode), func(t *testing.T) {
			auth := &cliproxyauth.Auth{
				ID: "oauth-auth", Provider: "codex",
				Metadata: map[string]any{
					"type": "codex", "account_id": "account-1",
					codexauth.FingerprintModeMetadataKey: string(tt.mode),
				},
			}
			auth = prepareCodexFingerprintAuthForTest(t, executor, auth)
			projected, state, err := executor.projectCodexSessionIdentity(t.Context(), auth, cliproxyexecutor.Request{}, opts, raw, &codexIdentityConfuseState{})
			if err != nil {
				t.Fatalf("projectCodexSessionIdentity() error = %v", err)
			}
			if !state.enabled || state.projectSession != tt.wantProjectSession || !state.converged {
				t.Fatalf("state = %#v, want enabled/converged with projectSession=%v", state, tt.wantProjectSession)
			}
			wantInstallation := deriveStableCodexFingerprintUUID("installation", "account-1")
			if got := gjson.GetBytes(projected, "client_metadata.x-codex-installation-id").String(); got != wantInstallation {
				t.Fatalf("installation ID = %q, want %q", got, wantInstallation)
			}
			if tt.mode == codexauth.FingerprintModeDevice {
				if got := gjson.GetBytes(projected, "client_metadata.session_id").String(); got != "body-session" {
					t.Fatalf("device mode session ID = %q, want body-session", got)
				}
				return
			}
			pool, ok := codexSessionIdentityPool(auth.Metadata)
			if !ok || len(pool) != 1 || !isCodexUUIDVersion(pool[0], uuid.Version(7)) {
				t.Fatalf("session identity pool = %#v, %v; want one UUIDv7", pool, ok)
			}
			wantSession := pool[0]
			wantThread := wantSession
			if state.identity.SessionID != wantSession || state.identity.ThreadID != wantThread || state.identity.TurnID != "prepared-turn" || state.identity.WindowID != wantThread+":0" {
				t.Fatalf("identity = %#v, want session=%q thread=%q", state.identity, wantSession, wantThread)
			}
			turnMetadata := gjson.GetBytes(projected, "client_metadata.x-codex-turn-metadata").String()
			if gjson.Get(turnMetadata, "workspace").String() != "/tmp/project" || gjson.Get(turnMetadata, "installation_id").String() != wantInstallation {
				t.Fatalf("turn metadata = %s, want preserved workspace and converged installation", turnMetadata)
			}
		})
	}
}

func TestCodexProjectSessionIdentityRewritesDefaultPromptCacheAlias(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{ID: "oauth-auth", Provider: "codex", Metadata: map[string]any{
		"type": "codex", "account_id": "account-1",
		codexauth.FingerprintModeMetadataKey: string(codexauth.FingerprintModeSession),
	}}
	auth = prepareCodexFingerprintAuthForTest(t, executor, auth)
	raw := []byte(`{"prompt_cache_key":"body-session","client_metadata":{"session_id":"body-session","x-codex-turn-metadata":"{\"prompt_cache_key\":\"body-session\"}"}}`)

	projected, state, err := executor.projectCodexSessionIdentity(
		t.Context(), auth, cliproxyexecutor.Request{}, cliproxyexecutor.Options{}, raw, &codexIdentityConfuseState{},
	)
	if err != nil {
		t.Fatalf("projectCodexSessionIdentity() error = %v", err)
	}
	if got := gjson.GetBytes(projected, "prompt_cache_key").String(); got != state.identity.SessionID {
		t.Fatalf("prompt_cache_key = %q, want converged session %q", got, state.identity.SessionID)
	}
	turnMetadata := gjson.GetBytes(projected, "client_metadata.x-codex-turn-metadata").String()
	if got := gjson.Get(turnMetadata, "prompt_cache_key").String(); got != state.identity.SessionID {
		t.Fatalf("turn metadata prompt_cache_key = %q, want %q", got, state.identity.SessionID)
	}
}

func TestCodexProjectSessionIdentityKeepsExplicitPromptCacheKey(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{ID: "oauth-auth", Provider: "codex", Metadata: map[string]any{
		"type": "codex", "account_id": "account-1",
		codexauth.FingerprintModeMetadataKey: string(codexauth.FingerprintModeFull),
	}}
	auth = prepareCodexFingerprintAuthForTest(t, executor, auth)
	raw := []byte(`{"prompt_cache_key":"explicit-cache","client_metadata":{"session_id":"body-session"}}`)

	projected, _, err := executor.projectCodexSessionIdentity(
		t.Context(), auth, cliproxyexecutor.Request{}, cliproxyexecutor.Options{}, raw, &codexIdentityConfuseState{},
	)
	if err != nil {
		t.Fatalf("projectCodexSessionIdentity() error = %v", err)
	}
	if got := gjson.GetBytes(projected, "prompt_cache_key").String(); got != "explicit-cache" {
		t.Fatalf("prompt_cache_key = %q, want explicit-cache", got)
	}
}

func TestCodexProjectSessionIdentityKeepsWhitespaceDistinctPromptCacheKey(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{ID: "oauth-auth", Provider: "codex", Metadata: map[string]any{
		"type": "codex", "account_id": "account-1",
		codexauth.FingerprintModeMetadataKey: string(codexauth.FingerprintModeFull),
	}}
	auth = prepareCodexFingerprintAuthForTest(t, executor, auth)
	raw := []byte(`{"prompt_cache_key":" body-session ","client_metadata":{"session_id":"body-session"}}`)

	projected, _, err := executor.projectCodexSessionIdentity(
		t.Context(), auth, cliproxyexecutor.Request{}, cliproxyexecutor.Options{}, raw, &codexIdentityConfuseState{},
	)
	if err != nil {
		t.Fatalf("projectCodexSessionIdentity() error = %v", err)
	}
	if got := gjson.GetBytes(projected, "prompt_cache_key").String(); got != " body-session " {
		t.Fatalf("prompt_cache_key = %q, want whitespace-distinct explicit key", got)
	}
}

func TestCodexProjectSessionIdentityKeepsConfusedWhitespaceDistinctPromptCacheKey(t *testing.T) {
	cfg := &config.Config{
		Routing: config.RoutingConfig{Strategy: "fill-first"},
		Codex:   config.CodexConfig{IdentityConfuse: true},
	}
	executor := NewCodexExecutor(cfg)
	auth := &cliproxyauth.Auth{ID: "oauth-auth", Provider: "codex", Metadata: map[string]any{
		"type": "codex", "account_id": "account-1",
		codexauth.FingerprintModeMetadataKey: string(codexauth.FingerprintModeFull),
	}}
	auth = prepareCodexFingerprintAuthForTest(t, executor, auth)
	requestPayload := []byte(`{"prompt_cache_key":" body-session "}`)
	upstreamPayload := []byte(`{"prompt_cache_key":" body-session ","client_metadata":{"session_id":"body-session"}}`)
	upstreamPayload, identityState := applyCodexIdentityConfuseBody(cfg, auth, requestPayload, upstreamPayload)

	projected, state, err := executor.projectCodexSessionIdentity(
		t.Context(), auth, cliproxyexecutor.Request{Payload: requestPayload}, cliproxyexecutor.Options{}, upstreamPayload, &identityState,
	)
	if err != nil {
		t.Fatalf("projectCodexSessionIdentity() error = %v", err)
	}
	if got := gjson.GetBytes(projected, "prompt_cache_key").String(); got != identityState.promptCacheKey {
		t.Fatalf("prompt_cache_key = %q, want confused explicit key %q", got, identityState.promptCacheKey)
	}
	if identityState.promptCacheKey == state.identity.SessionID {
		t.Fatal("whitespace-distinct explicit key was replaced by the converged session")
	}
}

func TestCodexProjectSessionIdentityUpdatesIdentityConfusePromptMapping(t *testing.T) {
	cfg := &config.Config{
		Routing: config.RoutingConfig{Strategy: "fill-first"},
		Codex:   config.CodexConfig{IdentityConfuse: true},
	}
	executor := NewCodexExecutor(cfg)
	auth := &cliproxyauth.Auth{ID: "oauth-auth", Provider: "codex", Metadata: map[string]any{
		"type": "codex", "account_id": "account-1",
		codexauth.FingerprintModeMetadataKey: string(codexauth.FingerprintModeSession),
	}}
	auth = prepareCodexFingerprintAuthForTest(t, executor, auth)
	requestPayload := []byte(`{"prompt_cache_key":"body-session"}`)
	upstreamPayload := []byte(`{"prompt_cache_key":"body-session","client_metadata":{"session_id":"body-session","x-codex-turn-metadata":"{\"prompt_cache_key\":\"body-session\"}"}}`)
	upstreamPayload, identityState := applyCodexIdentityConfuseBody(cfg, auth, requestPayload, upstreamPayload)

	projected, state, err := executor.projectCodexSessionIdentity(
		t.Context(), auth, cliproxyexecutor.Request{Payload: requestPayload}, cliproxyexecutor.Options{}, upstreamPayload, &identityState,
	)
	if err != nil {
		t.Fatalf("projectCodexSessionIdentity() error = %v", err)
	}
	if identityState.promptCacheKey != state.identity.SessionID {
		t.Fatalf("identity confuse prompt cache mapping = %q, want %q", identityState.promptCacheKey, state.identity.SessionID)
	}
	if got := gjson.GetBytes(projected, "prompt_cache_key").String(); got != state.identity.SessionID {
		t.Fatalf("prompt_cache_key = %q, want %q", got, state.identity.SessionID)
	}
	restored := applyCodexIdentityExposeResponsePayload(
		[]byte(`{"prompt_cache_key":"`+state.identity.SessionID+`"}`), identityState,
	)
	if got := gjson.GetBytes(restored, "prompt_cache_key").String(); got != "body-session" {
		t.Fatalf("restored prompt_cache_key = %q, want body-session", got)
	}
}

func TestCodexProjectSessionIdentityDefaultsToStableInstallation(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{
		ID:       "oauth-auth",
		Provider: "codex",
		Metadata: map[string]any{"type": "codex", "account_id": "account-1"},
	}
	raw := []byte(`{"model":"gpt-5.4","client_metadata":{"session_id":"client-session","thread_id":"client-thread","x-codex-installation-id":"client-install"}}`)

	projected, state, err := executor.projectCodexSessionIdentity(t.Context(), auth, cliproxyexecutor.Request{}, cliproxyexecutor.Options{}, raw, &codexIdentityConfuseState{})
	if err != nil {
		t.Fatalf("projectCodexSessionIdentity() error = %v", err)
	}
	wantInstallation := deriveStableCodexFingerprintUUID("installation", "account-1")
	if !state.enabled || state.projectSession || !state.converged {
		t.Fatalf("state = %#v, want installation-only convergence", state)
	}
	if got := gjson.GetBytes(projected, "client_metadata.x-codex-installation-id").String(); got != wantInstallation {
		t.Fatalf("installation ID = %q, want %q", got, wantInstallation)
	}
	if got := gjson.GetBytes(projected, "client_metadata.session_id").String(); got != "client-session" {
		t.Fatalf("session ID = %q, want client-session", got)
	}
}

func TestCodexConvergedFingerprintStaysStableAndSeparatesClientSessions(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{CodexFingerprint: config.CodexFingerprintConfig{SessionIdentityPoolSize: 64}})
	auth := &cliproxyauth.Auth{ID: "auth", Provider: "codex", Metadata: map[string]any{
		"account_id": "account-1", codexauth.FingerprintModeMetadataKey: "session",
	}}
	auth = prepareCodexFingerprintAuthForTest(t, executor, auth)
	turnID, err := helps.NewCodexUUIDv7()
	if err != nil {
		t.Fatal(err)
	}
	prepared := codexPreparedSessionIdentity{
		TurnID: turnID, AffinityKind: "thread_id", AffinityDigest: codexFingerprintDigest("client-a"), TenantDigest: codexFingerprintDigest("tenant-a"),
	}
	otherPrepared := prepared
	for candidate := 0; ; candidate++ {
		otherPrepared.AffinityDigest = codexFingerprintDigest(fmt.Sprintf("client-%d", candidate))
		if codexSessionIdentityPoolIndex(auth, prepared, turnID, 64) != codexSessionIdentityPoolIndex(auth, otherPrepared, turnID, 64) {
			break
		}
	}
	first, err := resolveCodexConvergedFingerprint(auth, prepared, "")
	if err != nil {
		t.Fatalf("resolve first fingerprint: %v", err)
	}
	repeated, err := resolveCodexConvergedFingerprint(auth, prepared, "")
	if err != nil {
		t.Fatalf("resolve repeated fingerprint: %v", err)
	}
	otherClient, err := resolveCodexConvergedFingerprint(auth, otherPrepared, "")
	if err != nil {
		t.Fatalf("resolve other fingerprint: %v", err)
	}
	if first != repeated {
		t.Fatalf("same account/session fingerprint changed: %#v != %#v", first, repeated)
	}
	if first.sessionID == otherClient.sessionID || first.installationID != otherClient.installationID {
		t.Fatalf("pool identities did not separate client sessions: %#v vs %#v", first, otherClient)
	}
	if !isCodexUUIDVersion(first.sessionID, uuid.Version(7)) || !isCodexUUIDVersion(otherClient.sessionID, uuid.Version(7)) {
		t.Fatalf("pool session IDs are not UUIDv7: %q, %q", first.sessionID, otherClient.sessionID)
	}
}

func TestCodexConvergedFingerprintReplacesInvalidPersistedInstallationID(t *testing.T) {
	auth := &cliproxyauth.Auth{ID: "auth", Provider: "codex", Metadata: map[string]any{
		"account_id": "account-1", "openai_device_id": "not-a-uuid",
	}}

	got, err := resolveCodexConvergedFingerprint(auth, codexPreparedSessionIdentity{}, "")
	if err != nil {
		t.Fatalf("resolveCodexConvergedFingerprint() error = %v", err)
	}
	want := deriveStableCodexFingerprintUUID("installation", "account-1")
	if got.installationID != want {
		t.Fatalf("installation ID = %q, want %q", got.installationID, want)
	}
}

func TestCodexConvergedFingerprintMigratesPersistedInstallationIDToAccountRoot(t *testing.T) {
	persisted := uuid.NewString()
	auth := &cliproxyauth.Auth{ID: "auth", Provider: "codex", Metadata: map[string]any{
		"account_id": "account-1", "openai_device_id": persisted,
	}}

	got, err := resolveCodexConvergedFingerprint(auth, codexPreparedSessionIdentity{}, "")
	if err != nil {
		t.Fatalf("resolveCodexConvergedFingerprint() error = %v", err)
	}
	want := deriveStableCodexFingerprintUUID("installation", "account-1")
	if got.installationID != want {
		t.Fatalf("installation ID = %q, want account-derived %q", got.installationID, want)
	}
}

func TestCodexPrepareRequestAuthGeneratesPersistentInstallationID(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{ID: "oauth-auth", Provider: "codex", Metadata: map[string]any{
		"type": "codex", "account_id": "account-1", "openai_device_id": "invalid",
	}}

	if !executor.ShouldPrepareRequestAuth(auth) {
		t.Fatal("invalid installation ID was not marked for preparation")
	}
	prepared, errPrepare := executor.PrepareRequestAuth(t.Context(), auth)
	if errPrepare != nil {
		t.Fatalf("PrepareRequestAuth() error = %v", errPrepare)
	}
	installationID := codexMetadataString(prepared.Metadata, "openai_device_id")
	parsed, errParse := uuid.Parse(installationID)
	if errParse != nil || parsed.Version() != 4 {
		t.Fatalf("prepared installation ID = %q, want UUIDv4", installationID)
	}
	if want := deriveStableCodexFingerprintUUID("installation", "account-1"); installationID != want {
		t.Fatalf("prepared installation ID = %q, want account-derived %q", installationID, want)
	}
	if got := codexMetadataString(auth.Metadata, "openai_device_id"); got != "invalid" {
		t.Fatalf("caller installation ID = %q, want unchanged invalid value", got)
	}
	if executor.ShouldPrepareRequestAuth(prepared) {
		t.Fatal("valid persisted installation ID still requires preparation")
	}
	reused, errReuse := executor.PrepareRequestAuth(t.Context(), prepared)
	if errReuse != nil {
		t.Fatalf("second PrepareRequestAuth() error = %v", errReuse)
	}
	if got := codexMetadataString(reused.Metadata, "openai_device_id"); got != installationID {
		t.Fatalf("reused installation ID = %q, want %q", got, installationID)
	}
	other, errOther := executor.PrepareRequestAuth(t.Context(), &cliproxyauth.Auth{
		ID: "oauth-auth-copy", Provider: "codex",
		Metadata: map[string]any{"type": "codex", "account_id": "account-1"},
	})
	if errOther != nil {
		t.Fatalf("other PrepareRequestAuth() error = %v", errOther)
	}
	if got := codexMetadataString(other.Metadata, "openai_device_id"); got != installationID {
		t.Fatalf("same-account credential installation ID = %q, want %q", got, installationID)
	}
	projected, state, errProject := executor.projectCodexSessionIdentity(
		t.Context(),
		prepared,
		cliproxyexecutor.Request{},
		cliproxyexecutor.Options{},
		[]byte(`{"client_metadata":{"x-codex-installation-id":"client-installation"}}`),
		&codexIdentityConfuseState{},
	)
	if errProject != nil {
		t.Fatalf("projectCodexSessionIdentity() error = %v", errProject)
	}
	if got := gjson.GetBytes(projected, "client_metadata.x-codex-installation-id").String(); got != installationID {
		t.Fatalf("projected installation ID = %q, want persisted %q", got, installationID)
	}
	if state.identity.InstallationID != installationID {
		t.Fatalf("identity installation ID = %q, want persisted %q", state.identity.InstallationID, installationID)
	}
}

func TestCodexPrepareRequestAuthKeepsPersistedInstallationWithoutAccountID(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{})
	installationID := uuid.NewString()
	auth := &cliproxyauth.Auth{ID: "oauth-auth", Provider: "codex", Metadata: map[string]any{
		"type": "codex", "openai_device_id": installationID,
	}}

	if executor.ShouldPrepareRequestAuth(auth) {
		t.Fatal("valid persisted installation ID without account ID requires preparation")
	}
	prepared, err := executor.PrepareRequestAuth(t.Context(), auth)
	if err != nil {
		t.Fatalf("PrepareRequestAuth() error = %v", err)
	}
	if got := codexMetadataString(prepared.Metadata, "openai_device_id"); got != installationID {
		t.Fatalf("installation ID = %q, want persisted %q", got, installationID)
	}
}

func TestCodexPrepareRequestAuthCreatesMissingMetadata(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{ID: "oauth-auth", Provider: "codex"}

	if !executor.ShouldPrepareRequestAuth(auth) {
		t.Fatal("missing metadata was not marked for installation preparation")
	}
	prepared, errPrepare := executor.PrepareRequestAuth(t.Context(), auth)
	if errPrepare != nil {
		t.Fatalf("PrepareRequestAuth() error = %v", errPrepare)
	}
	if _, errParse := uuid.Parse(codexMetadataString(prepared.Metadata, "openai_device_id")); errParse != nil {
		t.Fatalf("prepared installation ID is invalid: %v", errParse)
	}
	if auth.Metadata != nil {
		t.Fatal("PrepareRequestAuth() mutated the caller metadata")
	}
}

func TestCodexPrepareRequestAuthSkipsDisabledConvergenceAndAPIKeys(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{})
	tests := []struct {
		name string
		auth *cliproxyauth.Auth
	}{
		{
			name: "explicit off",
			auth: &cliproxyauth.Auth{ID: "oauth-off", Provider: "codex", Metadata: map[string]any{
				"type": "codex", codexauth.FingerprintModeMetadataKey: string(codexauth.FingerprintModeOff),
			}},
		},
		{
			name: "api key",
			auth: &cliproxyauth.Auth{ID: "api-key", Provider: "codex", Attributes: map[string]string{
				"api_key": "secret",
			}, Metadata: map[string]any{"type": "codex"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if executor.ShouldPrepareRequestAuth(tt.auth) {
				t.Fatal("credential unexpectedly requires installation preparation")
			}
		})
	}
}

func TestCodexPrepareRequestAuthMaintainsSessionIdentityPool(t *testing.T) {
	baseAuth := &cliproxyauth.Auth{ID: "oauth-auth", Provider: "codex", Metadata: map[string]any{
		"type": "codex", "account_id": "account-1",
		codexauth.FingerprintModeMetadataKey: string(codexauth.FingerprintModeSession),
	}}
	executor4 := NewCodexExecutor(&config.Config{CodexFingerprint: config.CodexFingerprintConfig{SessionIdentityPoolSize: 4}})
	prepared4 := prepareCodexFingerprintAuthForTest(t, executor4, baseAuth)
	pool4, ok := codexSessionIdentityPool(prepared4.Metadata)
	if !ok || len(pool4) != 4 {
		t.Fatalf("prepared pool = %#v, %v; want 4 members", pool4, ok)
	}
	for _, identity := range pool4 {
		if !isCodexUUIDVersion(identity, uuid.Version(7)) {
			t.Fatalf("pool member = %q, want UUIDv7", identity)
		}
	}
	if _, exists := baseAuth.Metadata[codexSessionIdentityPoolMetadataKey]; exists {
		t.Fatal("PrepareRequestAuth() mutated the caller pool")
	}

	executor6 := NewCodexExecutor(&config.Config{CodexFingerprint: config.CodexFingerprintConfig{SessionIdentityPoolSize: 6}})
	if !executor6.ShouldPrepareRequestAuth(prepared4) {
		t.Fatal("pool expansion was not detected by request auth preparation")
	}
	prepared6 := prepareCodexFingerprintAuthForTest(t, executor6, prepared4)
	pool6, _ := codexSessionIdentityPool(prepared6.Metadata)
	if len(pool6) != 6 {
		t.Fatalf("expanded pool length = %d, want 6", len(pool6))
	}
	for index := range pool4 {
		if pool6[index] != pool4[index] {
			t.Fatalf("expanded pool member %d = %q, want preserved %q", index, pool6[index], pool4[index])
		}
	}

	invalid := prepared6.Clone()
	invalidMember := uuid.NewString()
	invalid.Metadata[codexSessionIdentityPoolMetadataKey] = []any{pool6[0], invalidMember, pool6[2], pool6[3], pool6[4], pool6[5]}
	repaired := prepareCodexFingerprintAuthForTest(t, executor6, invalid)
	repairedPool, _ := codexSessionIdentityPool(repaired.Metadata)
	if repairedPool[0] != pool6[0] || repairedPool[1] == invalidMember || !isCodexUUIDVersion(repairedPool[1], uuid.Version(7)) {
		t.Fatalf("repaired pool = %#v, want valid members with unaffected positions preserved", repairedPool)
	}

	executor2 := NewCodexExecutor(&config.Config{CodexFingerprint: config.CodexFingerprintConfig{SessionIdentityPoolSize: 2}})
	prepared2 := prepareCodexFingerprintAuthForTest(t, executor2, repaired)
	pool2, _ := codexSessionIdentityPool(prepared2.Metadata)
	if len(pool2) != 2 || pool2[0] != repairedPool[0] || pool2[1] != repairedPool[1] {
		t.Fatalf("shrunk pool = %#v, want first two members %#v", pool2, repairedPool[:2])
	}
}

func TestCodexSessionIdentityPoolSkipsInactiveModesAndAPIKeys(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{CodexFingerprint: config.CodexFingerprintConfig{SessionIdentityPoolSize: 4}})
	for _, test := range []struct {
		name string
		auth *cliproxyauth.Auth
	}{
		{name: "off", auth: &cliproxyauth.Auth{Provider: "codex", Metadata: map[string]any{codexauth.FingerprintModeMetadataKey: "off"}}},
		{name: "device", auth: &cliproxyauth.Auth{Provider: "codex", Metadata: map[string]any{codexauth.FingerprintModeMetadataKey: "device"}}},
		{name: "api key", auth: &cliproxyauth.Auth{Provider: "codex", Attributes: map[string]string{"api_key": "secret"}, Metadata: map[string]any{codexauth.FingerprintModeMetadataKey: "full"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			prepared := prepareCodexFingerprintAuthForTest(t, executor, test.auth)
			if _, exists := prepared.Metadata[codexSessionIdentityPoolMetadataKey]; exists {
				t.Fatal("inactive fingerprint mode created a session identity pool")
			}
		})
	}
}

func TestCodexSessionIdentityPoolMapsThreadsByOfficialUUIDRules(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{CodexFingerprint: config.CodexFingerprintConfig{SessionIdentityPoolSize: 4}})
	auth := prepareCodexFingerprintAuthForTest(t, executor, &cliproxyauth.Auth{ID: "auth", Provider: "codex", Metadata: map[string]any{
		"account_id": "account-1", codexauth.FingerprintModeMetadataKey: "session",
	}})
	turnID, err := helps.NewCodexUUIDv7()
	if err != nil {
		t.Fatal(err)
	}
	clientThreadID, err := helps.NewCodexUUIDv7()
	if err != nil {
		t.Fatal(err)
	}
	prepared := codexPreparedSessionIdentity{
		TurnID: turnID, AffinityKind: "thread_id", AffinityDigest: codexFingerprintDigest(clientThreadID), TenantDigest: "tenant",
	}
	first, err := resolveCodexConvergedFingerprint(auth, prepared, clientThreadID)
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := resolveCodexConvergedFingerprint(auth, prepared, clientThreadID)
	if err != nil {
		t.Fatal(err)
	}
	if first != repeated {
		t.Fatalf("mapped fingerprint changed: %#v != %#v", first, repeated)
	}
	originalUUID, _ := uuid.Parse(clientThreadID)
	mappedUUID, err := uuid.Parse(first.threadID)
	if err != nil || mappedUUID.Version() != uuid.Version(7) {
		t.Fatalf("mapped thread = %q, want UUIDv7", first.threadID)
	}
	for index := 0; index < 6; index++ {
		if mappedUUID[index] != originalUUID[index] {
			t.Fatalf("mapped thread changed UUIDv7 timestamp bytes: %q -> %q", clientThreadID, first.threadID)
		}
	}
	legacyThread := uuid.NewString()
	legacy, err := resolveCodexConvergedFingerprint(auth, prepared, legacyThread)
	if err != nil {
		t.Fatal(err)
	}
	if legacy.threadID != legacyThread {
		t.Fatalf("legacy client thread = %q, want preserved %q", legacy.threadID, legacyThread)
	}
	opaque, err := resolveCodexConvergedFingerprint(auth, prepared, "not-a-uuid")
	if err != nil {
		t.Fatal(err)
	}
	if opaque.threadID != opaque.sessionID {
		t.Fatalf("opaque client thread = %q, want selected session %q", opaque.threadID, opaque.sessionID)
	}
	if !isCodexUUIDVersion(first.sessionID, uuid.Version(7)) || !isCodexUUIDVersion(first.turnID, uuid.Version(7)) {
		t.Fatalf("fingerprint = %#v, want UUIDv7 session and turn", first)
	}
}

func TestCodexPreparedAffinityIgnoresPreviousResponseID(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{})
	req := cliproxyexecutor.Request{Payload: []byte(`{"previous_response_id":"resp_123"}`)}
	first := prepareCodexSessionIdentityForTest(t, executor, t.Context(), req, cliproxyexecutor.Options{})
	second := prepareCodexSessionIdentityForTest(t, executor, t.Context(), req, cliproxyexecutor.Options{})
	if first.AffinityKind != "turn" || second.AffinityKind != "turn" {
		t.Fatalf("affinity kinds = %q, %q; want turn", first.AffinityKind, second.AffinityKind)
	}
	if first.AffinityDigest == second.AffinityDigest {
		t.Fatal("independent requests reused previous_response_id as pool affinity")
	}
}

func TestCodexPreparedAffinityUsesStableSourcePriority(t *testing.T) {
	turnID, err := helps.NewCodexUUIDv7()
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"prompt_cache_key":"cache-key","client_metadata":{"session_id":"session-id","x-codex-turn-metadata":"{\"thread_id\":\"thread-id\"}"}}`)
	opts := cliproxyexecutor.Options{Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "execution-id"}}
	kind, digest, tenant, _, _ := codexPreparedRequestAffinity(contextWithCodexTestAPIKey("tenant-key"), opts, payload, turnID)
	if kind != "execution_session_id" || digest != codexFingerprintDigest("execution-id") || tenant != codexFingerprintDigest("tenant-key") {
		t.Fatalf("execution affinity = %q, %q, %q", kind, digest, tenant)
	}

	kind, digest, _, _, _ = codexPreparedRequestAffinity(t.Context(), cliproxyexecutor.Options{}, payload, turnID)
	if kind != "thread_id" || digest != codexFingerprintDigest("thread-id") {
		t.Fatalf("thread affinity = %q, %q", kind, digest)
	}
	payload = []byte(`{"prompt_cache_key":"cache-key","client_metadata":{"session_id":"session-id"}}`)
	kind, digest, _, _, _ = codexPreparedRequestAffinity(t.Context(), cliproxyexecutor.Options{}, payload, turnID)
	if kind != "prompt_cache_key" || digest != codexFingerprintDigest("cache-key") {
		t.Fatalf("prompt cache affinity = %q, %q", kind, digest)
	}
	payload = []byte(`{"client_metadata":{"session_id":"session-id"}}`)
	kind, digest, _, _, _ = codexPreparedRequestAffinity(t.Context(), cliproxyexecutor.Options{}, payload, turnID)
	if kind != "session_id" || digest != codexFingerprintDigest("session-id") {
		t.Fatalf("session affinity = %q, %q", kind, digest)
	}
}

func TestCodexProjectSessionIdentityRejectsUnpreparedPoolWithoutCoolingAuth(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Provider: "codex", Metadata: map[string]any{
		"account_id": "account-1", codexauth.FingerprintModeMetadataKey: "session",
	}}
	_, _, err := executor.projectCodexSessionIdentity(t.Context(), auth, cliproxyexecutor.Request{}, cliproxyexecutor.Options{}, []byte(`{}`), &codexIdentityConfuseState{})
	status, ok := err.(statusErr)
	if !ok || status.StatusCode() != http.StatusInternalServerError || !status.SkipAuthResult() {
		t.Fatalf("error = %#v, want local 500 with auth result skipped", err)
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
	expectedTurnID := codexIdentityConfuseUUID(auth.ID, "turn", originalTurnID)
	if state.identity.SessionID != "session-original" || state.identity.ThreadID != "thread-original" || state.identity.WindowID != "thread-original:0" {
		t.Fatalf("projected identity = %#v, want prompt cache kept separate from session identity", state.identity)
	}
	if state.identity.TurnID != expectedTurnID {
		t.Fatalf("TurnID = %q, want %q", state.identity.TurnID, expectedTurnID)
	}
	if got := gjson.GetBytes(projected, "client_metadata.session_id").String(); got != "session-original" {
		t.Fatalf("client_metadata.session_id = %q, want original session", got)
	}
	if got := gjson.GetBytes(projected, "client_metadata.thread_id").String(); got != "thread-original" {
		t.Fatalf("client_metadata.thread_id = %q, want original thread", got)
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

func TestCodexProjectSessionIdentityConfusesCredentialHeadersBeforePriorityMerge(t *testing.T) {
	cfg := &config.Config{
		Routing: config.RoutingConfig{Strategy: "fill-first"},
		Codex: config.CodexConfig{
			IdentityConfuse:      true,
			SpoofSessionIdentity: true,
		},
	}
	executor := NewCodexExecutor(cfg)
	auth := &cliproxyauth.Auth{
		ID: "oauth-auth", Provider: "codex",
		Metadata: map[string]any{"access_token": "oauth-token"},
		Attributes: map[string]string{
			"header:Session-Id":            "admin-session",
			"header:Thread-Id":             "admin-thread",
			"header:X-Codex-Window-Id":     "admin-window",
			"header:X-Codex-Turn-Metadata": `{"turn_id":"admin-turn"}`,
		},
	}
	requestPayload := []byte(`{"prompt_cache_key":"cache-original"}`)
	upstreamPayload, identityState := applyCodexIdentityConfuseBody(cfg, auth, requestPayload, []byte(`{"model":"gpt-5.4"}`))

	_, state, err := executor.projectCodexSessionIdentity(t.Context(), auth, cliproxyexecutor.Request{Payload: requestPayload}, cliproxyexecutor.Options{}, upstreamPayload, &identityState)
	if err != nil {
		t.Fatalf("projectCodexSessionIdentity() error = %v", err)
	}
	wantTurn := identityState.confuseTurnID("admin-turn")
	if state.identity.SessionID != "admin-session" || state.identity.ThreadID != "admin-thread" || state.identity.WindowID != "admin-window" {
		t.Fatalf("credential identity = %#v, want credential session identity preserved", state.identity)
	}
	if state.identity.TurnID != wantTurn {
		t.Fatalf("credential TurnID = %q, want %q", state.identity.TurnID, wantTurn)
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
