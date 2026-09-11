package helps

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
)

func TestCodexPromptCacheLogPolicySnapshotSurvivesLaterGinTurns(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx := context.WithValue(t.Context(), "gin", c)
	body := []byte(`{"prompt_cache_key":"first-cache-key"}`)
	first := util.RegisterPromptCacheLogPolicy(c, body)
	snapshot := SnapshotCodexPromptCacheLog(ctx, body)
	if snapshot != first {
		t.Fatal("snapshot rebuilt an existing immutable log policy")
	}
	clear(body)
	ctx = WithCodexPromptCacheLogRedaction(ctx, snapshot)
	util.RegisterPromptCacheLogPolicy(c, []byte(`{"prompt_cache_key":"next-cache-key"}`))
	if redactCodexPromptCacheLog(ctx, "first-cache-key") != util.PromptCacheLogMarker {
		t.Fatal("a later Gin turn changed the captured policy")
	}
	withoutKey := WithCodexPromptCacheLogRedaction(ctx, SnapshotCodexPromptCacheLog(nil, nil))
	if CodexPromptCacheLogRedactor(withoutKey) != nil {
		t.Fatal("explicitly absent key inherited a later Gin turn")
	}
	if !SnapshotCodexPromptCacheLog(nil, []byte(`{"prompt_cache_key":"sdk-cache-key"}`)).ProtectsKey("sdk-cache-key") {
		t.Fatal("SDK request without Gin did not capture the policy")
	}
}

func TestCodexPromptCacheLogProtectsWhitespaceBeforeValidation(t *testing.T) {
	for _, key := range []string{" ", "   ", "\t"} {
		body, _ := json.Marshal(map[string]string{"prompt_cache_key": key})
		for _, redactor := range []*util.PromptCacheLogRedactor{util.PromptCacheLogRedactorForRequest(body), SnapshotCodexPromptCacheLog(nil, body)} {
			if !redactor.ProtectsKey(key) || redactor.Redact(key) != util.PromptCacheLogMarker || redactor.Redact(string(body)) == string(body) {
				t.Fatal("diagnostics lost protection for a nonempty whitespace cache string")
			}
		}
	}
}

func TestCodexPromptCacheLogRedactionDoesNotChangeWireData(t *testing.T) {
	for _, key := range []string{"client-cache", "client\"cache\\key"} {
		ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
		ctx := context.WithValue(t.Context(), "gin", ginCtx)
		ctx = WithCodexPromptCacheLogRedaction(ctx, util.NewPromptCacheLogRedactor(key))
		cfg := &config.Config{SDKConfig: config.SDKConfig{RequestLog: true}}
		metadata, _ := json.Marshal(map[string]string{"prompt_cache_key": key})
		body, _ := json.Marshal(map[string]any{"prompt_cache_key": key, "client_metadata": map[string]string{"x-codex-turn-metadata": string(metadata)}})
		original := string(body)
		headers := http.Header{"X-Codex-Turn-Metadata": []string{string(metadata)}}
		info := UpstreamRequestLog{Body: body, Headers: headers}
		RecordAPIRequest(ctx, cfg, info)
		RecordAPIResponseMetadata(ctx, cfg, 200, headers)
		AppendAPIResponseChunk(ctx, cfg, body)
		RecordAPIResponseError(ctx, cfg, errors.New(key))
		RecordAPIWebsocketRequest(ctx, cfg, info)
		RecordAPIWebsocketHandshake(ctx, cfg, 101, headers)
		AppendAPIWebsocketResponse(ctx, cfg, body)
		RecordAPIWebsocketError(ctx, cfg, "receive", errors.New(key))
		for _, name := range []string{apiRequestKey, apiResponseKey, apiWebsocketTimelineKey} {
			value, ok := ginCtx.Get(name)
			if !ok {
				t.Fatal("missing log entry")
			}
			var logged string
			switch value := value.(type) {
			case []byte:
				logged = string(value)
			case string:
				logged = value
			default:
				t.Fatalf("unexpected log type %T", value)
			}
			forms := []string{key}
			for range 2 {
				encoded, _ := json.Marshal(forms[len(forms)-1])
				forms = append(forms, string(encoded[1:len(encoded)-1]))
			}
			for _, form := range forms {
				if strings.Contains(logged, form) {
					t.Fatal("cache key was written to a log")
				}
			}
			if !strings.Contains(logged, "[REDACTED_CACHE_KEY]") {
				t.Fatal("missing cache redaction marker")
			}
		}
		if string(body) != original || headers.Get("X-Codex-Turn-Metadata") != string(metadata) {
			t.Fatal("log redaction changed outbound data")
		}
		if redactCodexPromptCacheLog(t.Context(), original) != original {
			t.Fatal("legacy logging changed without a protected cache key")
		}
	}
}
